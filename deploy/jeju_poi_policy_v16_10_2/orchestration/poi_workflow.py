"""Status-decision primitives for the Jeju POI Airflow pipeline."""
from __future__ import annotations

from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Iterable

from orchestration.csv_io import read_csv, write_csv
from orchestration.lifecycle import (LifecycleDecision, classify_lifecycle, decide_lifecycle,
                                     is_temporary_event, operating_status_label,
                                     uses_kakao)
from orchestration.master_workflow import row_fingerprint

OFFICIAL_RESULTS = {"closed", "open", "not_found", "ambiguous", "error"}
KAKAO_RESULTS = {"found", "closed", "not_found", "ambiguous", "error"}
OFFICIAL_OPEN_STATUS_WORDS = {"영업", "정상", "정상영업", "영업/정상", "영업중", "운영중"}


def effective_official_result(evidence: dict) -> str:
    """Recognize explicit current-open labels in previously generated evidence."""
    result = str(evidence.get("result") or "error")
    status = str(evidence.get("status_name") or "").strip().replace(" ", "")
    reason = str(evidence.get("match_reason") or "")
    score = int(evidence.get("match_score") or 0)
    exact_identity = ("exact_normalized_name" in reason and
                      ("exact_normalized_address" in reason or
                       "same_primary_address" in reason))
    if result == "ambiguous" and status in {x.replace(" ", "") for x in OFFICIAL_OPEN_STATUS_WORDS} \
            and score >= 95 and exact_identity:
        return "open"
    return result


def place_key(row: dict[str, str], *, row_number: int | None = None) -> str:
    """Prefer a real POI ID; otherwise identify the exact source CSV row."""
    for field in ("place_key", "pipeline_id", "place_id", "id", "poi_id"):
        value = row.get(field, "").strip()
        if value:
            return value
    identity = "\x1f".join(row.get(field, "").strip()
                            for field in ("name", "road_address", "jibun_address"))
    base = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    return f"{base}-row{row_number}" if row_number is not None else base


def decide_operating_status(*, official_result: str,
                            kakao_result: str | None) -> tuple[str, str]:
    """Return status/reason without inferring closure from map absence."""
    if official_result not in OFFICIAL_RESULTS:
        raise ValueError(f"지원하지 않는 공식 데이터 결과: {official_result}")
    if kakao_result is not None and kakao_result not in KAKAO_RESULTS:
        raise ValueError(f"지원하지 않는 카카오 결과: {kakao_result}")
    if official_result == "closed":
        return "closed", "official_closed"
    if official_result == "open":
        return "open", "official_open"
    if official_result in {"error", "ambiguous"}:
        return "needs_review", "official_check_failed"
    if kakao_result == "found":
        return "open", "kakao_same_place_found"
    if kakao_result == "not_found":
        return "needs_review", "kakao_not_found"
    if kakao_result == "ambiguous":
        return "needs_review", "kakao_identity_ambiguous"
    if kakao_result == "closed":
        return "needs_review", "kakao_closure_requires_official_confirmation"
    return "needs_review", "kakao_check_failed"


def read_jsonl_index(path: str | Path, *, allowed: set[str], source: str) -> dict[str, dict]:
    index: dict[str, dict] = {}
    with Path(path).open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            item = json.loads(line)
            key = str(item.get("place_key", "")).strip()
            result = str(item.get("result", "")).strip()
            if not key or result not in allowed:
                raise ValueError(f"{source} JSONL {number}행의 place_key/result가 잘못되었습니다.")
            if key in index:
                raise ValueError(f"{source} JSONL에 place_key 중복: {key}")
            index[key] = item
    return index


def write_jsonl_atomic(path: str | Path, records: Iterable[dict]) -> int:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    count = 0
    with temp.open("w", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            count += 1
    os.replace(temp, path)
    return count


def build_kakao_queue(poi_csv: str | Path, official_jsonl: str | Path,
                      output_jsonl: str | Path) -> dict:
    """Queue only POIs absent from MOIS."""
    _, rows, _ = read_csv(poi_csv)
    official = read_jsonl_index(official_jsonl, allowed=OFFICIAL_RESULTS, source="official")
    queue = []
    for index, row in enumerate(rows):
        key = place_key(row, row_number=index + 1)
        evidence = official.get(key, {"result": "error", "error": "official evidence missing"})
        # Open/closed official records are final.  Not-found, ambiguous and
        # failed official matches all benefit from an independent Kakao check.
        if effective_official_result(evidence) in {"open", "closed"}:
            continue
        queue.append({"row": index + 1, "place_key": key, "name": row.get("name", ""),
                      "road_address": row.get("road_address", ""),
                      "jibun_address": row.get("jibun_address", ""),
                      "latitude": row.get("latitude", ""), "longitude": row.get("longitude", ""),
                      "official_result": evidence["result"]})
    count = write_jsonl_atomic(output_jsonl, queue)
    return {"queued": count, "queue": str(Path(output_jsonl).resolve())}


def classify_existing_pois(poi_csv: str | Path, official_jsonl: str | Path,
                           kakao_jsonl: str | Path, output_jsonl: str | Path) -> dict:
    """Classify only existing POIs and retain the evidence used for every decision."""
    _, rows, _ = read_csv(poi_csv)
    official = read_jsonl_index(official_jsonl, allowed=OFFICIAL_RESULTS, source="official")
    kakao = read_jsonl_index(kakao_jsonl, allowed=KAKAO_RESULTS, source="kakao")
    checked_at = datetime.now(timezone.utc).isoformat()
    decisions = []
    for index, row in enumerate(rows):
        key = place_key(row, row_number=index + 1)
        off = official.get(key, {"result": "error", "error": "official evidence missing"})
        kak = kakao.get(key)
        status, reason = decide_operating_status(
            official_result=off["result"], kakao_result=kak.get("result") if kak else None)
        decisions.append({"row": index + 1, "place_key": key, "name": row.get("name", ""),
                          "status": status, "reason_code": reason, "checked_at": checked_at,
                          "evidence": {"official": off, "kakao": kak}})
    write_jsonl_atomic(output_jsonl, decisions)
    counts = Counter(item["status"] for item in decisions)
    return {"total": len(decisions), **dict(counts), "output": str(Path(output_jsonl).resolve())}


def build_kakao_queue_v10(poi_csv: str | Path, official_jsonl: str | Path,
                          output_jsonl: str | Path) -> dict:
    """Queue map checks only where a map listing is meaningful."""
    _, rows, _ = read_csv(poi_csv)
    official = read_jsonl_index(official_jsonl, allowed=OFFICIAL_RESULTS, source="official")
    queue = []
    skipped_by_type = Counter()
    for index, row in enumerate(rows):
        key = place_key(row, row_number=index + 1)
        lifecycle_type = classify_lifecycle(row)
        if is_temporary_event(row):
            skipped_by_type["temporary_event"] += 1
            continue
        if not uses_kakao(lifecycle_type):
            skipped_by_type[lifecycle_type] += 1
            continue
        evidence = official.get(key, {"result": "error", "error": "official evidence missing"})
        if effective_official_result(evidence) in {"open", "closed"}:
            continue
        queue.append({"row": index + 1, "place_key": key, "name": row.get("name", ""),
                      "road_address": row.get("road_address", ""),
                      "jibun_address": row.get("jibun_address", ""),
                      "latitude": row.get("latitude", ""), "longitude": row.get("longitude", ""),
                      "place_main_category": row.get("place_main_category", ""),
                      "lifecycle_type": lifecycle_type, "official_result": evidence["result"]})
    count = write_jsonl_atomic(output_jsonl, queue)
    return {"queued": count, "skipped_by_lifecycle": dict(skipped_by_type),
            "queue": str(Path(output_jsonl).resolve())}


def finalize_pois_v10(poi_csv: str | Path, official_jsonl: str | Path,
                      kakao_jsonl: str | Path, output_jsonl: str | Path,
                      filtered_csv: str | Path | None = None) -> dict:
    """Produce KEEP/DEACTIVATE records. ``filtered_csv`` is deprecated and ignored."""
    fields, rows, encoding = read_csv(poi_csv)
    official = read_jsonl_index(official_jsonl, allowed=OFFICIAL_RESULTS, source="official")
    kakao = read_jsonl_index(kakao_jsonl, allowed=KAKAO_RESULTS, source="kakao")
    checked_at = datetime.now(timezone.utc).isoformat()
    decisions = []
    for index, row in enumerate(rows):
        key = place_key(row, row_number=index + 1)
        lifecycle_type = classify_lifecycle(row)
        temporary = is_temporary_event(row)
        has_official = key in official
        has_kakao = key in kakao
        off = official.get(key, {"result": "error", "error": "official evidence missing"})
        kak = kakao.get(key)
        if not has_official and not has_kakao and not temporary:
            result = baseline_status_decision(row, lifecycle_type)
        else:
            result = decide_lifecycle(
                lifecycle_type=lifecycle_type,
                temporary_event=temporary,
                official_result=effective_official_result(off),
                kakao_result=kak.get("result") if kak else None,
                official_scope=str(off.get("scope") or "business_registry"),
            )
        decisions.append({
            "row": index + 1, "place_key": key, "name": row.get("name", ""),
            "place_main_category": row.get("place_main_category", ""),
            "lifecycle_type": lifecycle_type,
            "decision": result.decision, "verification_state": result.verification_state,
            "reason_code": result.reason_code, "checked_at": checked_at,
            "evidence": {"official": off, "kakao": kak if uses_kakao(lifecycle_type) else None},
        })

    write_jsonl_atomic(output_jsonl, decisions)
    decision_counts = Counter(item["decision"] for item in decisions)
    verification_counts = Counter(item["verification_state"] for item in decisions)
    lifecycle_counts = Counter(item["lifecycle_type"] for item in decisions)
    return {
        "total": len(decisions), "decisions": dict(decision_counts),
        "verification": dict(verification_counts), "lifecycle": dict(lifecycle_counts),
        "output": str(Path(output_jsonl).resolve()),
    }


DECISION_VALUES = {"KEEP", "DEACTIVATE", "REVIEW"}
OPERATING_STATUS_VALUES = {"영업", "폐업", "운영", "비운영", "확인 필요"}


def baseline_status_decision(row: dict, lifecycle_type: str):
    """Preserve a trusted current status only when no new evidence exists."""
    value = str(row.get("operating_status") or "").strip()
    if value in {"영업", "운영"}:
        return LifecycleDecision("KEEP", "verified_active", "baseline_status_preserved")
    if value in {"폐업", "비운영"}:
        return LifecycleDecision("DEACTIVATE", "verified_inactive", "baseline_status_preserved")
    if value in {"확인 필요", "운영 여부 확인 필요"}:
        return LifecycleDecision("KEEP", "unverified", "baseline_status_preserved")
    return LifecycleDecision("KEEP", "unverified", "status_evidence_missing")


def export_v10_csvs(poi_csv: str | Path, decisions_jsonl: str | Path,
                    output_dir: str | Path, *, version: str = "v10_4") -> dict:
    """Export an auditable master and a KEEP-only service CSV."""
    fields, rows, encoding = read_csv(poi_csv)
    decisions = []
    with Path(decisions_jsonl).open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            item = json.loads(line)
            if item.get("decision") not in DECISION_VALUES:
                raise ValueError(f"v10 판정 JSONL {number}행 decision 오류")
            decisions.append(item)
    if len(rows) != len(decisions):
        raise ValueError(f"POI/판정 행 개수 불일치: {len(rows)} != {len(decisions)}")

    extra_fields = ["pipeline_id", "place_key", "lifecycle_type", "row_fingerprint",
                     "master_change_type",
                     "operating_status", "verification_state", "status_reason_code"]
    internal_fields = {"operation_decision", *extra_fields}
    # Drop the legacy machine-facing column from published CSVs. The internal
    # JSONL keeps KEEP/DEACTIVATE for validation and repeatable processing.
    source_fields = [field for field in fields if field not in internal_fields]
    preservation_fields = ["place_id", "name", "road_address", "operating_status",
                           "status_reason_code", "verification_state", "lifecycle_type",
                           "place_key"]
    preservation, service, status_view = [], [], []
    for number, (row, decision) in enumerate(zip(rows, decisions), 1):
        if decision.get("row") != number:
            raise ValueError(f"v10 판정 {number}행 연결 오류: {decision.get('row')}")
        stable_key = str(decision.get("place_key", ""))
        enriched = {
            "place_key": row.get("place_key", "") or stable_key,
            "lifecycle_type": decision["lifecycle_type"],
            "operating_status": operating_status_label(
                decision["lifecycle_type"], decision["decision"],
                decision["verification_state"]),
            "verification_state": decision["verification_state"],
            "status_reason_code": decision["reason_code"],
        }
        status_view.append({field: row.get(field, "") for field in source_fields} |
                           {"operating_status": enriched["operating_status"]})
        # Keep only the fields needed to trace a status decision.  The full
        # prepared master remains in the run work directory for incremental
        # processing, but is never published as a bulky user-facing CSV.
        preservation.append({
            "place_id": row.get("place_id", ""),
            "name": row.get("name", ""),
            "road_address": row.get("road_address", ""),
            **enriched,
        })
        if decision["decision"] == "KEEP":
            service.append({field: row.get(field, "") for field in source_fields})

    root = Path(output_dir)
    paths = {
        "preservation_csv": root / f"제주_POI_판정보존_{version}.csv",
        "service_csv": root / f"제주_POI_서비스용_KEEP_{version}.csv",
        "status_csv": root / f"제주_POI_최종통합본_상태_{version}.csv",
    }
    write_csv(paths["preservation_csv"], preservation_fields, preservation, encoding=encoding)
    write_csv(paths["service_csv"], source_fields, service, encoding=encoding)
    write_csv(paths["status_csv"], source_fields + ["operating_status"], status_view,
              encoding=encoding)
    return {"preservation_rows": len(preservation), "service_rows": len(service),
            **{key: str(value.resolve()) for key, value in paths.items()}}


def validate_v10_outputs(poi_csv: str | Path, decisions_jsonl: str | Path,
                         export_result: dict) -> dict:
    """Fail closed before a generated service CSV can be published."""
    _, source_rows, _ = read_csv(poi_csv)
    decisions = []
    with Path(decisions_jsonl).open(encoding="utf-8") as stream:
        decisions = [json.loads(line) for line in stream if line.strip()]
    if len(decisions) != len(source_rows):
        raise ValueError("원본과 최종 판정 건수가 다릅니다.")
    keys = [str(item.get("place_key", "")) for item in decisions]
    if not all(keys) or len(keys) != len(set(keys)):
        raise ValueError("최종 판정 place_key가 비어 있거나 중복되었습니다.")
    invalid = Counter(item.get("decision") for item in decisions
                      if item.get("decision") not in DECISION_VALUES)
    if invalid:
        raise ValueError(f"지원하지 않는 최종 결정: {dict(invalid)}")
    bad_facility_closures = [item for item in decisions
                             if item.get("lifecycle_type") in {"facility_infrastructure", "fixed_public_natural"}
                             and item.get("decision") == "DEACTIVATE"
                             and item.get("reason_code") != "facility_officially_closed"]
    if bad_facility_closures:
        raise ValueError(f"공공·고정·인프라 공식 근거 규칙 위반: {len(bad_facility_closures)}건")
    counts = Counter(item["decision"] for item in decisions)
    expected = {
        "preservation_rows": len(decisions),
        "service_rows": counts["KEEP"],
    }
    for field, value in expected.items():
        if export_result.get(field) != value:
            raise ValueError(f"{field} 검증 실패: {export_result.get(field)} != {value}")
    for field in ("preservation_csv", "service_csv", "status_csv"):
        path = Path(str(export_result.get(field, "")))
        if not path.is_file():
            raise ValueError(f"출력 파일 없음: {field}")
        if field == "preservation_csv":
            with path.open(encoding="utf-8-sig", newline="") as stream:
                reader = csv.DictReader(stream)
                if reader.fieldnames != ["place_id", "name", "road_address", "operating_status",
                                         "status_reason_code", "verification_state", "lifecycle_type",
                                         "place_key"]:
                    raise ValueError("판정보존 목록 열 구성이 다릅니다.")
                published_rows = list(reader)
        else:
            _, published_rows, _ = read_csv(path)
        if field != "service_csv":
            invalid_labels = Counter(row.get("operating_status", "") for row in published_rows
                                     if row.get("operating_status", "") not in OPERATING_STATUS_VALUES)
            if invalid_labels:
                raise ValueError(f"지원하지 않는 한글 영업 상태값: {dict(invalid_labels)}")
    return {"passed": True, "total": len(decisions), "decisions": dict(counts),
            "infrastructure": sum(item.get("lifecycle_type") == "facility_infrastructure"
                                  for item in decisions),
            "fixed_public_natural": sum(item.get("lifecycle_type") == "fixed_public_natural"
                                        for item in decisions), **expected}
