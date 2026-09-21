"""Runnable POI status pipeline used for pilots and Airflow troubleshooting."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path

from orchestration.api_clients import KakaoLocalClient, PagedPublicDataClient
from orchestration.evidence import (build_kakao_evidence, build_kakao_evidence_resumable,
                                    build_mois_evidence)
from orchestration.poi_workflow import (build_kakao_queue, build_kakao_queue_v10,
                                        classify_existing_pois, export_v10_csvs,
                                        finalize_pois_v10, validate_v10_outputs,
                                        write_jsonl_atomic)
from orchestration.master_workflow import prepare_master


def read_jsonl(path: str | Path) -> list[dict]:
    records = []
    with Path(path).open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            item = json.loads(line)
            if not isinstance(item, dict):
                raise ValueError(f"{path} {number}행이 JSON 객체가 아닙니다.")
            records.append(item)
    return records


def download(endpoint: str, service_key_env: str, output: str, *,
             key_parameter: str, page_parameter: str, size_parameter: str,
             page_size: int) -> dict:
    if not 1 <= page_size <= 100:
        raise ValueError("행안부 API page_size는 1~100이어야 합니다.")
    key = os.environ.get(service_key_env, "").strip()
    if not key:
        raise RuntimeError(f"환경변수 {service_key_env}가 없습니다.")
    client = PagedPublicDataClient(endpoint, key, key_parameter=key_parameter,
                                  page_parameter=page_parameter, size_parameter=size_parameter)
    records = (record for page in client.pages(page_size=page_size) for record in page)
    count = write_jsonl_atomic(output, records)
    return {"records": count, "output": str(Path(output).resolve())}


def download_manifest(manifest: str, service_key_env: str, output: str, raw_dir: str, *,
                      page_size: int = 100, workers: int = 2) -> dict:
    """Download every approved MOIS source, retaining per-source files for recovery."""
    if not 1 <= page_size <= 100:
        raise ValueError("행안부 API page_size는 1~100이어야 합니다.")
    key = os.environ.get(service_key_env, "").strip()
    if not key:
        raise RuntimeError(f"환경변수 {service_key_env}가 없습니다.")
    sources = json.loads(Path(manifest).read_text(encoding="utf-8"))
    if isinstance(sources, dict):
        sources = sources.get("enabled", [])
    if not isinstance(sources, list) or not sources:
        raise ValueError("운영 API manifest가 비어 있거나 형식이 잘못되었습니다.")
    raw_root = Path(raw_dir)
    raw_root.mkdir(parents=True, exist_ok=True)

    def fetch(source: dict) -> dict:
        endpoint = str(source.get("info_url", "")).strip()
        slug = str(source.get("base_url", "")).rstrip("/").rsplit("/", 1)[-1]
        if not endpoint or not slug:
            raise ValueError(f"manifest source 형식 오류: {source}")
        target = raw_root / f"{slug}.jsonl"
        address_field = ("LCTN_ROAD_NM_ADDR" if slug == "public_restroom_info_v2"
                         else "ROAD_NM_ADDR")
        client = PagedPublicDataClient(endpoint, key, extra_params={
            "returnType": "json", f"cond[{address_field}::LIKE]": "제주"})
        def records():
            for page in client.pages(page_size=page_size):
                for record in page:
                    yield {**record, "_mois_source": slug,
                           "_mois_source_name": source.get("name", "")}
        count = write_jsonl_atomic(target, records())
        return {"name": source.get("name", slug), "slug": slug,
                "records": count, "path": str(target.resolve())}

    completed, failed = [], []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 4))) as pool:
        futures = {pool.submit(fetch, source): source for source in sources}
        for future in as_completed(futures):
            try:
                completed.append(future.result())
            except Exception as error:
                source = futures[future]
                failed.append({"name": source.get("name", ""),
                               "info_url": source.get("info_url", ""),
                               "error": str(error)[:500]})
    summary_path = raw_root / "download_summary.json"
    summary_path.write_text(json.dumps({"completed": completed, "failed": failed},
                                       ensure_ascii=False, indent=2), encoding="utf-8")
    if failed:
        return {"sources": len(sources), "completed": len(completed), "failed": len(failed),
                "records": sum(item["records"] for item in completed),
                "summary": str(summary_path.resolve()), "output": None}

    completed.sort(key=lambda item: item["slug"])
    def merged():
        for item in completed:
            yield from read_jsonl(item["path"])
    count = write_jsonl_atomic(output, merged())
    return {"sources": len(sources), "completed": len(completed), "failed": 0,
            "records": count, "summary": str(summary_path.resolve()),
            "output": str(Path(output).resolve())}


def run(poi_csv: str, mois_raw: str, work_dir: str,
        *, kakao_key_env: str = "KAKAO_REST_API_KEY") -> dict:
    root = Path(work_dir)
    root.mkdir(parents=True, exist_ok=True)
    mois_evidence = root / "mois_evidence.jsonl"
    queue, kakao_evidence, decisions = (root / "kakao_queue.jsonl", root / "kakao_evidence.jsonl",
                                         root / "operating_status_decisions.jsonl")
    build_mois_evidence(poi_csv, read_jsonl(mois_raw), mois_evidence)
    queue_summary = build_kakao_queue(poi_csv, mois_evidence, queue)
    key = os.environ.get(kakao_key_env, "").strip()
    if queue_summary["queued"] and not key:
        raise RuntimeError(f"카카오 확인 대상 {queue_summary['queued']}건이 있지만 환경변수 {kakao_key_env}가 없습니다.")
    build_kakao_evidence(queue, kakao_evidence, KakaoLocalClient(key)) if queue_summary["queued"] else write_jsonl_atomic(kakao_evidence, [])
    result = classify_existing_pois(poi_csv, mois_evidence, kakao_evidence, decisions)
    result["kakao_api_calls"] = queue_summary["queued"]
    return result


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="제주 POI 영업상태 수집·판정")
    commands = root.add_subparsers(dest="command", required=True)
    fetch = commands.add_parser("download", help="공식 API 전체 페이지를 JSONL로 저장")
    fetch.add_argument("--endpoint", required=True)
    fetch.add_argument("--service-key-env", required=True)
    fetch.add_argument("--output", required=True)
    fetch.add_argument("--key-parameter", default="serviceKey")
    fetch.add_argument("--page-parameter", default="pageNo")
    fetch.add_argument("--size-parameter", default="numOfRows")
    fetch.add_argument("--page-size", type=int, default=100)
    bulk = commands.add_parser("download-manifest", help="승인된 행안부 API를 모두 수집")
    bulk.add_argument("--manifest", default="orchestration/mois_enabled_sources.json")
    bulk.add_argument("--service-key-env", default="MOIS_SERVICE_KEY")
    bulk.add_argument("--output", default="work/raw/mois_all.jsonl")
    bulk.add_argument("--raw-dir", default="work/raw/mois")
    bulk.add_argument("--page-size", type=int, default=100)
    bulk.add_argument("--workers", type=int, default=2)
    execute = commands.add_parser("run", help="원본 JSONL로 전체 판정 실행")
    execute.add_argument("--poi-csv", required=True)
    execute.add_argument("--mois-raw", required=True)
    execute.add_argument("--work-dir", required=True)
    execute.add_argument("--kakao-key-env", default="KAKAO_REST_API_KEY")
    kakao = commands.add_parser("kakao-resume", help="카카오 대기열을 중단 지점부터 확인")
    kakao.add_argument("--queue", required=True)
    kakao.add_argument("--output", required=True)
    kakao.add_argument("--kakao-key-env", default="KAKAO_REST_API_KEY")
    kakao.add_argument("--progress-every", type=int, default=100)
    kakao.add_argument("--workers", type=int, default=1,
                       help="카카오 동시 요청 수(1~8, 권장 4)")
    queue_v10 = commands.add_parser("build-queue-v10", help="생명주기상 카카오 확인이 의미 있는 POI만 대기열 생성")
    queue_v10.add_argument("--poi-csv", required=True)
    queue_v10.add_argument("--official", required=True)
    queue_v10.add_argument("--output", required=True)
    final_v10 = commands.add_parser("finalize-v10", help="KEEP/DEACTIVATE 최종 판정 생성")
    final_v10.add_argument("--poi-csv", required=True)
    final_v10.add_argument("--official", required=True)
    final_v10.add_argument("--kakao", required=True)
    final_v10.add_argument("--output", required=True)
    export_v10 = commands.add_parser("export-v10", help="v10 판정에서 최종상태·판정보존·서비스용 CSV 생성")
    export_v10.add_argument("--poi-csv", required=True)
    export_v10.add_argument("--decisions", required=True)
    export_v10.add_argument("--output-dir", required=True)
    export_v10.add_argument("--version", default="v10_4")
    prepare = commands.add_parser("prepare-master", help="새 원본을 이전 마스터와 병합해 안정 ID가 있는 마스터 생성")
    prepare.add_argument("--source-csv", required=True)
    prepare.add_argument("--previous-master")
    prepare.add_argument("--output", required=True)
    prepare.add_argument("--changes-output", required=True)
    return root


def main() -> int:
    args = parser().parse_args()
    if args.command == "download":
        result = download(args.endpoint, args.service_key_env, args.output,
                          key_parameter=args.key_parameter, page_parameter=args.page_parameter,
                          size_parameter=args.size_parameter, page_size=args.page_size)
    elif args.command == "download-manifest":
        result = download_manifest(args.manifest, args.service_key_env, args.output,
                                   args.raw_dir, page_size=args.page_size, workers=args.workers)
    elif args.command == "run":
        result = run(args.poi_csv, args.mois_raw, args.work_dir,
                     kakao_key_env=args.kakao_key_env)
    elif args.command == "kakao-resume":
        key = os.environ.get(args.kakao_key_env, "").strip()
        if not key:
            raise RuntimeError(f"환경변수 {args.kakao_key_env}가 없습니다.")
        result = build_kakao_evidence_resumable(
            args.queue, args.output, KakaoLocalClient(key),
            progress_every=args.progress_every, workers=args.workers)
    elif args.command == "build-queue-v10":
        result = build_kakao_queue_v10(args.poi_csv, args.official, args.output)
    elif args.command == "finalize-v10":
        result = finalize_pois_v10(args.poi_csv, args.official, args.kakao, args.output)
    elif args.command == "export-v10":
        result = export_v10_csvs(args.poi_csv, args.decisions, args.output_dir,
                                 version=args.version)
        result["quality"] = validate_v10_outputs(args.poi_csv, args.decisions, result)
    else:
        result = prepare_master(args.source_csv, args.previous_master,
                                args.output, args.changes_output)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
