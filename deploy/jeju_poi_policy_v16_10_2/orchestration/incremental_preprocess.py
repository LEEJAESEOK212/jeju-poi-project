"""Incremental POI preprocessing before lifecycle/status verification.

The two enrichment paths are deliberately separate:
1. duplicate complement: fill blanks from another row judged to be the same POI;
2. external enrichment: call Kakao only when location fields remain missing.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Iterable

from orchestration.csv_io import read_csv, write_csv
from orchestration.matching import (choose_match, haversine_m, normalize_address,
                                    normalize_name)

IDENTITY_FIELDS = ("name", "road_address", "jibun_address", "latitude", "longitude",
                   "place_main_category", "place_main_category_id")
UNION_FIELDS = ("tags", "source_urls")
LONGER_TEXT_FIELDS = ("content",)


def _float(value: object) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _distance(left: dict, right: dict) -> float | None:
    values = [_float(left.get("latitude")), _float(left.get("longitude")),
              _float(right.get("latitude")), _float(right.get("longitude"))]
    return None if any(value is None for value in values) else haversine_m(*values)


def _category(row: dict) -> str:
    return str(row.get("place_main_category") or row.get("place_main_category_id") or "").strip()


def duplicate_relation(left: dict, right: dict) -> tuple[str, str]:
    """Return merge/review/separate using the presentation's conservative rules."""
    same_name = bool(normalize_name(left.get("name", ""))) and (
        normalize_name(left.get("name", "")) == normalize_name(right.get("name", "")))
    left_addresses = {normalize_address(left.get("road_address", "")),
                      normalize_address(left.get("jibun_address", ""))} - {""}
    right_addresses = {normalize_address(right.get("road_address", "")),
                       normalize_address(right.get("jibun_address", ""))} - {""}
    same_address = bool(left_addresses & right_addresses)
    same_category = bool(_category(left)) and _category(left) == _category(right)
    distance = _distance(left, right)
    if same_name and same_address and (same_category or not _category(left) or not _category(right)):
        if distance is None or distance <= 30:
            return "merge", "same_name_address_category_within_30m"
    if same_name and not left_addresses and not right_addresses and same_category:
        if distance is not None and distance <= 15:
            return "merge", "same_name_category_no_address_within_15m"
    if same_name and left_addresses and right_addresses and not same_address:
        if distance is not None and distance <= 50:
            return "review", "same_name_different_address_within_50m"
        return "separate", "same_name_different_address_outside_50m"
    if same_address and not same_name:
        if same_category:
            return "review", "same_address_similar_function_different_name"
        return "separate", "same_address_different_function"
    return "separate", "insufficient_identity_evidence"


def _tokens(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").replace(";", ",").split(",") if item.strip()]


def merge_complementary_fields(representative: dict, incoming: dict) -> list[str]:
    """Keep the representative identity and use the other row only to improve information."""
    changed = []
    for field, value in incoming.items():
        value = str(value or "").strip()
        if not value:
            continue
        current = str(representative.get(field, "") or "").strip()
        if not current:
            representative[field] = value
            changed.append(field)
        elif field in LONGER_TEXT_FIELDS and len(value) > len(current):
            representative[field] = value
            changed.append(field)
        elif field in UNION_FIELDS:
            merged = list(dict.fromkeys(_tokens(current) + _tokens(value)))
            joined = ", ".join(merged)
            if joined != current:
                representative[field] = joined
                changed.append(field)
    return changed


def _apply_kakao(row: dict, client) -> tuple[list[str], dict | None]:
    missing_location = not str(row.get("latitude", "")).strip() or not str(row.get("longitude", "")).strip()
    if not missing_location or client is None or not str(row.get("name", "")).strip():
        return [], None
    match = choose_match(row, client.search(row), minimum_score=90)
    if match.result != "found" or not match.candidate:
        return [], {"result": match.result, "reason": match.reason}
    changed = []
    for target, source in (("latitude", "latitude"), ("longitude", "longitude"),
                           ("road_address", "road_address"), ("jibun_address", "jibun_address")):
        if not str(row.get(target, "")).strip() and str(match.candidate.get(source, "")).strip():
            row[target] = str(match.candidate[source]).strip()
            changed.append(target)
    return changed, {"result": "found", "reason": match.reason,
                     "source_id": match.candidate.get("source_id", "")}


def preprocess_incremental(existing_csv: str | Path, incoming_csv: str | Path,
                           output_csv: str | Path, audit_jsonl: str | Path,
                           *, kakao_client=None) -> dict:
    """Append a new delivery to the master with enrichment, deduplication and audit."""
    existing_fields, existing, encoding = read_csv(existing_csv)
    incoming_fields, incoming, _ = read_csv(incoming_csv)
    fields = list(dict.fromkeys(existing_fields + incoming_fields))
    rows = [dict(row) for row in existing]
    audit, counts = [], Counter()
    checked_at = datetime.now(timezone.utc).isoformat()

    for source_number, raw in enumerate(incoming, 1):
        row = dict(raw)
        kakao_fields, kakao_evidence = _apply_kakao(row, kakao_client)
        if kakao_fields:
            counts["kakao_enriched"] += 1
        matches, reviews = [], []
        for index, candidate in enumerate(rows):
            relation, reason = duplicate_relation(candidate, row)
            if relation == "merge":
                matches.append((index, reason))
            elif relation == "review":
                reviews.append((index, reason))
        if len(matches) == 1:
            index, reason = matches[0]
            complemented = merge_complementary_fields(rows[index], row)
            counts["merged"] += 1
            audit.append({"incoming_row": source_number, "action": "MERGE",
                          "representative_index": index + 1, "reason": reason,
                          "complement_source": "duplicate_row",
                          "complemented_fields": complemented,
                          "kakao_enriched_fields_before_match": kakao_fields,
                          "kakao_evidence": kakao_evidence, "checked_at": checked_at})
            continue
        if len(matches) > 1:
            reviews.extend((index, "multiple_merge_candidates") for index, _ in matches)
        rows.append(row)
        action = "REVIEW" if reviews else "APPEND"
        counts[action.lower()] += 1
        audit.append({"incoming_row": source_number, "action": action,
                      "output_index": len(rows), "review_candidates": [index + 1 for index, _ in reviews],
                      "reasons": sorted({reason for _, reason in reviews}),
                      "complement_source": "kakao_api" if kakao_fields else None,
                      "kakao_enriched_fields": kakao_fields, "kakao_evidence": kakao_evidence,
                      "checked_at": checked_at})

    write_csv(output_csv, fields, rows, encoding=encoding)
    target = Path(audit_jsonl); target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("".join(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n"
                              for item in audit), encoding="utf-8")
    return {"existing_rows": len(existing), "incoming_rows": len(incoming),
            "output_rows": len(rows), **dict(counts),
            "output": str(Path(output_csv).resolve()), "audit": str(target.resolve())}
