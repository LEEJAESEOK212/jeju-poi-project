"""Convert source records into normalized per-POI evidence JSONL."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from orchestration.csv_io import read_csv
from orchestration.api_clients import ApiError, first
from orchestration.matching import (choose_match, industry_compatible, name_aliases,
                                    normalize_address_core, normalize_name,
                                    score_candidate)
from orchestration.poi_workflow import place_key, write_jsonl_atomic


OPEN_WORDS = {"영업", "정상", "정상영업", "영업/정상", "영업중", "운영중"}
CLOSED_WORDS = {"폐업", "폐업처리", "취소/말소/만료/정지/중지"}


def normalize_mois_record(raw: dict, *, source: str = "MOIS_LOCALDATA") -> dict:
    return {"source": source,
            "source_dataset": first(raw, "_mois_source_name"),
            "source_slug": first(raw, "_mois_source"),
            "source_id": first(raw, "MNG_NO", "MGTNO", "관리번호", "manageNo", "licenseNo"),
            "name": first(raw, "BPLC_NM", "RSTRM_NM", "BPLCNM", "사업장명", "업소명", "bplcNm"),
            "road_address": first(raw, "ROAD_NM_ADDR", "LCTN_ROAD_NM_ADDR", "RDNWHLADDR", "도로명전체주소", "도로명주소", "rdnWhlAddr"),
            "jibun_address": first(raw, "LOTNO_ADDR", "LCTN_LOTNO_ADDR", "SITEWHLADDR", "소재지전체주소", "지번주소", "siteWhlAddr"),
            "status_name": first(raw, "DTL_SALS_STTS_NM", "SALS_STTS_NM", "DTLSTATEGBNNM", "상세영업상태명", "TRDSTATENM", "영업상태명", "dtlStateGbnNm"),
            "permit_date": first(raw, "LCPMT_YMD", "APVPERMYMD", "인허가일자", "허가일자", "apvPermYmd"),
            "closed_at": first(raw, "CLSBIZ_YMD", "DCLSYMD", "폐업일자", "폐업일", "dcbYmd"),
            "updated_at": first(raw, "DAT_UPDT_PNT", "LASTMODTS", "최종수정시점", "lastModTs", "UPDATEDT", "데이터갱신일자"),
            "raw": raw}


def _index(records: list[dict]) -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        for key in name_aliases(record.get("name", "")):
            result[key].append(record)
    return result


def _date_sort_key(value: object) -> str:
    digits = "".join(character for character in str(value or "") if character.isdigit())
    return digits.ljust(14, "0")[:14]


def latest_mois_records(records: list[dict]) -> list[dict]:
    """Collapse /info + /history rows by stable management/license identifier."""
    latest: dict[str, dict] = {}
    without_id: list[dict] = []
    for record in records:
        source_id = str(record.get("source_id", "")).strip()
        if not source_id:
            without_id.append(record)
            continue
        current = latest.get(source_id)
        record_key = max(_date_sort_key(record.get("updated_at")),
                         _date_sort_key(record.get("closed_at")),
                         _date_sort_key(record.get("permit_date")))
        current_key = (max(_date_sort_key(current.get("updated_at")),
                           _date_sort_key(current.get("closed_at")),
                           _date_sort_key(current.get("permit_date"))) if current else "")
        if current is None or record_key >= current_key:
            latest[source_id] = record
    return list(latest.values()) + without_id


def _mois_result(candidate: dict) -> str:
    status = str(candidate.get("status_name", "")).strip().replace(" ", "")
    if status in {word.replace(" ", "") for word in OPEN_WORDS}:
        return "open"
    if status in {word.replace(" ", "") for word in CLOSED_WORDS} or candidate.get("closed_at"):
        return "closed"
    return "ambiguous"


def _consensus_tied_candidate(poi: dict, candidates: list[dict], score: int) -> tuple[dict | None, str | None]:
    """Resolve equal-scoring licences when they identify one physical venue."""
    tied = []
    for candidate in candidates:
        candidate_score, _ = score_candidate(poi, candidate)
        if candidate_score == score:
            tied.append(candidate)
    results = {_mois_result(candidate) for candidate in tied}
    if len(tied) < 2:
        return None, None

    # Do not combine same-name branches at different addresses.  Equal scores
    # alone are insufficient: every tied licence must share the same normalized
    # name and primary address.
    identities = {
        (normalize_name(candidate.get("name", "")),
         normalize_address_core(candidate.get("road_address", "") or
                                candidate.get("jibun_address", "")))
        for candidate in tied
    }
    if len(identities) != 1 or not all(next(iter(identities))):
        return None, None

    # Multiple licence datasets can describe one business.  An active licence
    # proves that the venue is operating even when an older/ancillary licence
    # is closed or lacks a status.  A closed result remains valid only when all
    # matched licences explicitly agree on closure.
    open_candidates = [candidate for candidate in tied if _mois_result(candidate) == "open"]
    if open_candidates:
        latest_open = max(open_candidates, key=lambda record: max(
            _date_sort_key(record.get("updated_at")),
            _date_sort_key(record.get("permit_date"))))
        return latest_open, "open"
    if results != {"closed"}:
        return None, None
    latest = max(tied, key=lambda record: max(
        _date_sort_key(record.get("updated_at")),
        _date_sort_key(record.get("closed_at")),
        _date_sort_key(record.get("permit_date"))))
    return latest, "closed"


def build_mois_evidence(poi_csv: str | Path, raw_records: list[dict], output_jsonl: str | Path) -> dict:
    _, pois, _ = read_csv(poi_csv)
    normalized = latest_mois_records([normalize_mois_record(record) for record in raw_records])
    index, checked_at = _index(normalized), datetime.now(timezone.utc).isoformat()
    evidence = []
    for row_number, poi in enumerate(pois, 1):
        named_candidates, seen = [], set()
        for alias in name_aliases(poi.get("name", "")):
            for candidate in index.get(alias, []):
                marker = id(candidate)
                if marker not in seen:
                    seen.add(marker)
                    named_candidates.append(candidate)
        compatible_candidates = [candidate for candidate in named_candidates
                                 if industry_compatible(poi, candidate)]
        match = choose_match(poi, compatible_candidates, minimum_score=95,
                             resolve_identity_ties=False)
        consensus_result = None
        if match.result == "ambiguous" and match.reason == "multiple_equal_candidates":
            candidate, consensus_result = _consensus_tied_candidate(
                poi, compatible_candidates, match.score)
            if candidate is not None:
                match = type(match)("found", match.score,
                                    f"multiple_equal_candidates_consensus_{consensus_result}", candidate)
        address_mismatch = None
        if match.result == "not_found":
            fallback = choose_match(poi, compatible_candidates, minimum_score=90,
                                    resolve_identity_ties=False)
            if fallback.result in {"found", "ambiguous"}:
                address_mismatch = fallback
        industry_rejected = len(named_candidates) - len(compatible_candidates)
        item = {"place_key": place_key(poi, row_number=row_number),
                "source": "MOIS_LOCALDATA", "checked_at": checked_at}
        if match.result == "found" and match.candidate:
            official_result = consensus_result or _mois_result(match.candidate)
            match_reason = match.reason
            item.update({"result": official_result, "match_score": match.score,
                         "match_reason": match_reason, "source_id": match.candidate.get("source_id"),
                         "official_name": match.candidate.get("name"),
                         "official_road_address": match.candidate.get("road_address"),
                         "official_jibun_address": match.candidate.get("jibun_address"),
                         "official_api": match.candidate.get("source_dataset"),
                         "official_api_slug": match.candidate.get("source_slug"),
                         "status_name": match.candidate.get("status_name"),
                         "permit_date": match.candidate.get("permit_date"),
                         "closed_at": match.candidate.get("closed_at"),
                         "official_updated_at": match.candidate.get("updated_at")})
        else:
            item.update({"result": match.result, "match_score": match.score, "match_reason": match.reason})
            if address_mismatch is not None:
                item.update({"result": "not_found", "match_score": address_mismatch.score,
                             "match_reason": "address_mismatch"})
            if match.result == "not_found" and industry_rejected and not compatible_candidates:
                item["match_reason"] = "industry_mismatch"
                item["industry_rejected_candidates"] = industry_rejected
        evidence.append(item)
    write_jsonl_atomic(output_jsonl, evidence)
    return {"total": len(evidence), "output": str(Path(output_jsonl).resolve())}


def build_kakao_evidence(queue_jsonl: str | Path, output_jsonl: str | Path, client) -> dict:
    import json
    checked_at = datetime.now(timezone.utc).isoformat()
    output = []
    with Path(queue_jsonl).open(encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            poi = json.loads(line)
            item = {"place_key": poi["place_key"], "source": "KAKAO_LOCAL", "checked_at": checked_at}
            try:
                match = choose_match(poi, client.search(poi))
                item.update({"result": match.result, "match_score": match.score, "match_reason": match.reason})
                if match.candidate:
                    item.update({"kakao_place_id": match.candidate.get("source_id"),
                                 "place_url": match.candidate.get("place_url"),
                                 "matched_name": match.candidate.get("name", ""),
                                 "matched_road_address": match.candidate.get("road_address", ""),
                                 "matched_jibun_address": match.candidate.get("jibun_address", "")})
            except ApiError as error:
                item.update({"result": "error", "error": str(error)})
            output.append(item)
    write_jsonl_atomic(output_jsonl, output)
    return {"total": len(output), "output": str(Path(output_jsonl).resolve())}


def build_kakao_evidence_resumable(queue_jsonl: str | Path, output_jsonl: str | Path,
                                   client, *, progress_every: int = 100,
                                   workers: int = 1) -> dict:
    """Append results safely, skip completed keys, and optionally search in parallel."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import json
    import os

    output_path = Path(output_jsonl)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    valid, completed = [], set()
    if output_path.exists():
        lines = output_path.read_text(encoding="utf-8").splitlines()
        for number, line in enumerate(lines, 1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                if number != len(lines):
                    raise ValueError(f"{output_path} {number}행 JSON 손상")
                break  # interrupted final write; retain all complete lines
            key = str(item.get("place_key", "")).strip()
            if not key or key in completed:
                raise ValueError(f"{output_path} {number}행 place_key 누락/중복")
            completed.add(key)
            valid.append(item)
        if len(valid) != len([line for line in lines if line.strip()]):
            write_jsonl_atomic(output_path, valid)

    queue = []
    with Path(queue_jsonl).open(encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                queue.append(json.loads(line))
    pending = [poi for poi in queue if poi["place_key"] not in completed]
    workers = max(1, min(int(workers), 8))

    def check(poi: dict) -> dict:
        checked_at = datetime.now(timezone.utc).isoformat()
        item = {"place_key": poi["place_key"], "source": "KAKAO_LOCAL",
                "checked_at": checked_at}
        try:
            candidates = client.search(poi)
            match = choose_match(poi, candidates)
            item.update({"result": match.result, "match_score": match.score,
                         "match_reason": match.reason,
                         "candidate_count": len(candidates)})
            if match.candidate:
                item.update({"kakao_place_id": match.candidate.get("source_id"),
                             "place_url": match.candidate.get("place_url"),
                             "matched_name": match.candidate.get("name", ""),
                             "matched_road_address": match.candidate.get("road_address", ""),
                             "matched_jibun_address": match.candidate.get("jibun_address", "")})
            elif candidates:
                item["candidate_samples"] = [{
                    "name": candidate.get("name", ""),
                    "road_address": candidate.get("road_address", ""),
                    "jibun_address": candidate.get("jibun_address", ""),
                    "distance": candidate.get("distance", ""),
                    "place_url": candidate.get("place_url", ""),
                } for candidate in candidates[:3]]
        except ApiError as error:
            item.update({"result": "error", "error": str(error)})
        return item

    processed = 0
    with output_path.open("a", encoding="utf-8") as stream:
        if workers == 1:
            results = map(check, pending)
            for item in results:
                stream.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
                stream.flush(); processed += 1
                if progress_every > 0 and processed % progress_every == 0:
                    os.fsync(stream.fileno())
                    print(f"카카오 확인: {len(completed) + processed:,}/{len(queue):,}", flush=True)
        else:
            executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="kakao")
            futures = [executor.submit(check, poi) for poi in pending]
            try:
                for future in as_completed(futures):
                    item = future.result()
                    stream.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
                    stream.flush(); processed += 1
                    if progress_every > 0 and processed % progress_every == 0:
                        os.fsync(stream.fileno())
                        print(f"카카오 확인({workers}개 병렬): {len(completed) + processed:,}/{len(queue):,}", flush=True)
            except BaseException:
                executor.shutdown(wait=False, cancel_futures=True)
                raise
            else:
                executor.shutdown(wait=True)
        os.fsync(stream.fileno())
    return {"total": len(queue), "previously_completed": len(completed),
            "processed": processed, "remaining": len(queue) - len(completed) - processed,
            "workers": workers,
            "output": str(output_path.resolve())}
