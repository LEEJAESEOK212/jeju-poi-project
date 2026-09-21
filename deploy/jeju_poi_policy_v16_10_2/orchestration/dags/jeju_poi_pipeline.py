"""Weekly lifecycle-aware classification for existing Jeju POIs."""
from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import Variable, dag, task

from orchestration.master_workflow import prepare_master
from orchestration.api_clients import KakaoLocalClient
from orchestration.evidence import build_kakao_evidence_resumable
from orchestration.incremental_preprocess import preprocess_incremental
from orchestration.poi_workflow import (build_kakao_queue_v10, export_v10_csvs,
                                        finalize_pois_v10, validate_v10_outputs)

DEFAULT_ARGS = {"owner": "jeju-poi", "depends_on_past": False,
                "email_on_failure": False, "retries": 0,
                "execution_timeout": timedelta(hours=4)}


def required_file(name: str) -> str:
    value = Variable.get(name, default="").strip()
    if not value or not Path(value).is_file():
        raise AirflowFailException(f"Airflow Variable {name}의 입력 파일을 확인하세요: {value}")
    return value


@dag(dag_id="jeju_poi_lifecycle_weekly_v10", schedule="0 3 * * 1",
     start_date=datetime(2026, 9, 1), catchup=False, max_active_runs=1,
     default_args=DEFAULT_ARGS, tags=["jeju", "poi", "operating-status"])
def operating_status_weekly():
    @task
    def validate() -> dict:
        source = required_file("JEJU_POI_CURRENT_CSV")
        incoming = Variable.get("JEJU_POI_NEW_CSV", default="").strip()
        if incoming and not Path(incoming).is_file():
            raise AirflowFailException(f"신규 POI 파일을 확인하세요: {incoming}")
        previous_master = Variable.get("JEJU_POI_PREVIOUS_MASTER_CSV", default="").strip()
        if previous_master and not Path(previous_master).is_file():
            raise AirflowFailException(f"이전 마스터 파일을 확인하세요: {previous_master}")
        official = required_file("JEJU_POI_MOIS_EVIDENCE_JSONL")
        kakao = required_file("JEJU_POI_KAKAO_EVIDENCE_JSONL")
        work = Variable.get("JEJU_POI_WORK_DIR", default="").strip()
        if not work:
            raise AirflowFailException("Airflow Variable JEJU_POI_WORK_DIR가 필요합니다.")
        run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual").replace(":", "_")
        run_dir = Path(work) / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        publish = Variable.get("JEJU_POI_OUTPUT_DIR", default=str(Path(work) / "published")).strip()
        Path(publish).mkdir(parents=True, exist_ok=True)
        return {"source": source, "incoming": incoming, "previous_master": previous_master,
                "official": official,
                "kakao": kakao, "run_dir": str(run_dir), "publish_dir": publish}

    @task
    def preprocess_new_delivery(config: dict) -> dict:
        """Only the new-delivery path calls Kakao; duplicate complementation is row-to-row."""
        if not config["incoming"]:
            return {"source_for_prepare": config["source"], "mode": "existing_snapshot"}
        output = str(Path(config["run_dir"]) / "preprocessed_incremental.csv")
        audit = str(Path(config["run_dir"]) / "preprocess_audit.jsonl")
        key = os.environ.get("KAKAO_REST_API_KEY", "").strip()
        client = KakaoLocalClient(key) if key else None
        result = preprocess_incremental(config["source"], config["incoming"], output, audit,
                                        kakao_client=client)
        result.update({"source_for_prepare": output, "mode": "incremental",
                       "kakao_enabled": bool(client)})
        return result

    @task
    def prepare(config: dict, preprocessing: dict) -> dict:
        master = str(Path(config["run_dir"]) / "prepared_master.csv")
        changes = str(Path(config["run_dir"]) / "master_changes.jsonl")
        result = prepare_master(preprocessing["source_for_prepare"],
                                config["previous_master"] or None,
                                master, changes)
        result["prepared_master"] = master
        return result

    @task
    def prepare_kakao_checks(config: dict, prepared: dict) -> dict:
        return build_kakao_queue_v10(prepared["prepared_master"], config["official"],
                                     str(Path(config["run_dir"]) / "kakao_check_queue_v10.jsonl"))

    @task
    def refresh_kakao_evidence(config: dict, queue_result: dict) -> dict:
        """Run missing/ambiguous Kakao checks and retain completed evidence."""
        queue_path = Path(queue_result["queue"])
        output_path = Path(config["run_dir"]) / "kakao_evidence_current.jsonl"
        queue_keys = {
            str(json.loads(line).get("place_key", "")).strip()
            for line in queue_path.read_text(encoding="utf-8").splitlines() if line.strip()
        }
        retained, completed = [], set()
        with Path(config["kakao"]).open(encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                item = json.loads(line)
                key = str(item.get("place_key", "")).strip()
                if key in queue_keys and item.get("result") in {"ambiguous", "error"}:
                    continue
                retained.append(item)
                completed.add(key)
        with output_path.open("w", encoding="utf-8", newline="\n") as stream:
            for item in retained:
                stream.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")

        pending = queue_keys - completed
        if pending:
            key = os.environ.get("KAKAO_REST_API_KEY", "").strip()
            if not key:
                raise AirflowFailException(
                    f"카카오 미확인 {len(pending):,}건 조회를 위해 KAKAO_REST_API_KEY가 필요합니다.")
            result = build_kakao_evidence_resumable(
                queue_path, output_path, KakaoLocalClient(key),
                workers=4, progress_every=100)
        else:
            result = {"total": len(queue_keys), "previously_completed": len(queue_keys),
                      "processed": 0, "remaining": 0, "workers": 0,
                      "output": str(output_path.resolve())}
        # Persist the merged evidence atomically so future deliveries reuse
        # every resolved identity instead of paying for the same lookup again.
        canonical = Path(config["kakao"])
        temporary = canonical.with_name(canonical.name + ".tmp")
        shutil.copyfile(output_path, temporary)
        os.replace(temporary, canonical)
        result["evidence_path"] = str(output_path)
        return result

    @task
    def classify(config: dict, prepared: dict, queue_result: dict,
                 kakao_result: dict) -> dict:
        decisions = str(Path(config["run_dir"]) / "operating_status_decisions_current.jsonl")
        result = finalize_pois_v10(
            prepared["prepared_master"], config["official"], kakao_result["evidence_path"],
            decisions)
        result["kakao_queue"] = queue_result["queued"]
        result["kakao_processed"] = kakao_result["processed"]
        result["decisions_path"] = decisions
        return result

    @task
    def export(config: dict, prepared: dict, result: dict) -> dict:
        return export_v10_csvs(prepared["prepared_master"], result["decisions_path"],
                               config["publish_dir"], version="current")

    @task
    def quality_gate(config: dict, prepared: dict, result: dict, exports: dict) -> dict:
        checked = validate_v10_outputs(prepared["prepared_master"], result["decisions_path"], exports)
        if not checked["passed"]:
            raise AirflowFailException("v10 품질검사를 통과하지 못했습니다.")
        return checked

    @task
    def report(result: dict, exports: dict, quality: dict) -> None:
        print(json.dumps({"classification": result, "exports": exports,
                          "quality": quality}, ensure_ascii=False, indent=2))
        print("현재 정책 완료: 카카오 미노출은 REVIEW, 명시적 공식 폐업만 DEACTIVATE입니다.")

    config = validate()
    prepared = prepare(config, preprocess_new_delivery(config))
    queue = prepare_kakao_checks(config, prepared)
    kakao = refresh_kakao_evidence(config, queue)
    result = classify(config, prepared, queue, kakao)
    exports = export(config, prepared, result)
    report(result, exports, quality_gate(config, prepared, result, exports))


operating_status_weekly()
