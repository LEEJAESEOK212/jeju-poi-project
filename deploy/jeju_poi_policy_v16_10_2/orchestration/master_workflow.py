"""Build a stable prepared master from changing source CSV deliveries."""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import uuid

from orchestration.csv_io import read_csv, write_csv
from orchestration.lifecycle import classify_lifecycle
from orchestration.matching import haversine_m, normalize_address, normalize_name

MASTER_FIELDS = ["pipeline_id", "place_key", "lifecycle_type", "row_fingerprint",
                 "master_change_type"]

_CONVENIENCE_NAME = re.compile(
    r"^(?:CU|씨유|GS25|지에스25|세븐일레븐|7-?ELEVEN).+", re.I)
_ATM_NAME = re.compile(r"(?:^|\s)(?:ATM|현금지급기)(?:\s|$)|^(?:롯데|신한|제주|국민|농협|하나|우리).{0,8}ATM", re.I)


def correct_obvious_category(row: dict[str, str]) -> dict[str, str]:
    """Repair only category errors proven directly by a distinctive POI name."""
    result = dict(row)
    name = str(result.get("name", "")).strip()
    main = str(result.get("place_main_category", "")).strip()
    sub = str(result.get("place_sub_category", "")).strip()
    convenience_overlay = any(token in name.upper() for token in (
        "ATM", "무료와이파이", "전기차충전소", "공중화장실"))
    if (main not in {"편의점", "공공편의시설", "주유소(충전소)", "숙박"}
            and _CONVENIENCE_NAME.search(name) and not convenience_overlay):
        result.update({
            "place_main_category_id": "7", "place_sub_category_id": "588",
            "place_main_category": "편의점", "place_sub_category": "편의점",
            "category_assignment_basis": "policy:convenience_brand_name",
        })
    elif sub != "ATM" and _ATM_NAME.search(name):
        result.update({
            "place_main_category_id": "20", "place_sub_category_id": "612",
            "place_main_category": "공공편의시설", "place_sub_category": "ATM",
            "category_assignment_basis": "policy:atm_name",
        })
    else:
        retail_key = normalize_name(name)
        if retail_key in {"이마트제주", "롯데쇼핑롯데마트제주점"}:
            result.update({
                "place_main_category_id": "8", "place_sub_category_id": "165",
                "place_main_category": "대형마트", "place_sub_category": "대형마트",
                "category_assignment_basis": "policy:corporate_large_mart_name",
            })
    return result


def _identity(row: dict[str, str]) -> str:
    return "\x1f".join((normalize_name(row.get("name", "")),
                         normalize_address(row.get("road_address", "") or row.get("jibun_address", ""))))


def row_fingerprint(row: dict[str, str]) -> str:
    fields = ("name", "road_address", "jibun_address", "latitude", "longitude",
              "place_main_category", "place_main_category_id", "content", "tags",
              "contact_number", "homepage_url", "source_urls")
    value = "\x1f".join((row.get(field) or "").strip() for field in fields)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _new_id(row: dict[str, str]) -> str:
    seed = _identity(row) or row_fingerprint(row)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "jeju-poi:" + seed))


def _duplicate_id(row: dict[str, str], occurrence: int) -> str:
    """Return the same collision ID on every run for the same ordered source."""
    seed = _identity(row) or row_fingerprint(row)
    source_key = (row.get("place_id", "") or "").strip()
    return str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"jeju-poi-duplicate:{seed}:{source_key}:{occurrence}",
    ))


def prepare_master(source_csv: str | Path, previous_master_csv: str | Path | None,
                   output_csv: str | Path, changes_jsonl: str | Path) -> dict:
    fields, rows, encoding = read_csv(source_csv)
    previous = []
    if previous_master_csv and Path(previous_master_csv).is_file():
        _, previous, _ = read_csv(previous_master_csv)

    by_place_key = {r.get("place_key", "").strip(): r for r in previous
                    if r.get("place_key", "").strip()}
    by_place_id = {r.get("place_id", "").strip(): r for r in previous if r.get("place_id", "").strip()}
    by_identity: dict[str, list[dict[str, str]]] = {}
    by_name: dict[str, list[dict[str, str]]] = {}
    for row in previous:
        by_identity.setdefault(_identity(row), []).append(row)
        by_name.setdefault(normalize_name(row.get("name", "")), []).append(row)

    prepared, changes, seen_ids, used_previous = [], [], set(), set()
    duplicate_occurrences = Counter()
    counts = Counter()
    for source_row in rows:
        source_row = correct_obvious_category(source_row)
        source_place_key = source_row.get("place_key", "").strip()
        place_id = source_row.get("place_id", "").strip()
        if source_place_key and source_place_key in by_place_key:
            previous_row = by_place_key.pop(source_place_key)
            if place_id:
                by_place_id.pop(place_id, None)
        elif place_id and place_id in by_place_id:
            previous_row = by_place_id.pop(place_id)
        else:
            candidates = [r for r in by_identity.get(_identity(source_row), []) if id(r) not in used_previous]
            previous_row = candidates[0] if candidates else None
        # A corrected address must not create a new POI ID. Fall back only when an
        # exact normalized name has one uniquely close previous coordinate.
        if previous_row is None:
            close=[]
            try:slat,slon=float(source_row.get("latitude","")),float(source_row.get("longitude",""))
            except (TypeError,ValueError):slat=slon=None
            for candidate in by_name.get(normalize_name(source_row.get("name","")),[]):
                if id(candidate) in used_previous or slat is None:continue
                try:distance=haversine_m(slat,slon,float(candidate.get("latitude","")),float(candidate.get("longitude","")))
                except (TypeError,ValueError):continue
                if distance<=100:close.append(candidate)
            if len(close)==1:previous_row=close[0]
        if previous_row:
            used_previous.add(id(previous_row))
            pipeline_id = previous_row.get("pipeline_id", "").strip() or _new_id(previous_row)
            change_type = "unchanged" if previous_row.get("row_fingerprint") == row_fingerprint(source_row) else "changed"
        else:
            pipeline_id = _new_id(source_row)
            change_type = "new"
        # Collision is possible only for truly duplicate identity rows; keep both auditable.
        if pipeline_id in seen_ids:
            collision_seed = _identity(source_row) or row_fingerprint(source_row)
            duplicate_occurrences[collision_seed] += 1
            pipeline_id = _duplicate_id(source_row, duplicate_occurrences[collision_seed])
            while pipeline_id in seen_ids:
                duplicate_occurrences[collision_seed] += 1
                pipeline_id = _duplicate_id(source_row, duplicate_occurrences[collision_seed])
            change_type = "duplicate_identity_new_id"
        seen_ids.add(pipeline_id)
        # Lifecycle policy can change independently of source row contents.
        # Reclassify every run so a policy release also updates unchanged POIs.
        # Recompute from the current category fields on every run.  An incoming
        # master can contain a lifecycle_type written by an older policy, which
        # must not override the current rules.
        lifecycle_input = dict(source_row)
        lifecycle_input["lifecycle_type"] = ""
        lifecycle = classify_lifecycle(lifecycle_input)
        # Evidence files are keyed by the source POI identifier. On the first
        # master build, preserve that identifier instead of replacing it with
        # a generated pipeline UUID. Subsequent runs retain the prior key.
        stable_key = ((previous_row or {}).get("place_key", "").strip()
                      or source_row.get("place_id", "").strip()
                      or pipeline_id)
        record = {**source_row, "pipeline_id": pipeline_id, "place_key": stable_key,
                  "lifecycle_type": lifecycle, "row_fingerprint": row_fingerprint(source_row),
                  "master_change_type": change_type}
        prepared.append(record)
        counts[change_type] += 1
        if change_type != "unchanged":
            changes.append({"pipeline_id": pipeline_id, "name": source_row.get("name", ""),
                            "category": source_row.get("place_main_category", ""),
                            "change_type": change_type})

    missing = [r for r in previous if id(r) not in used_previous]
    for row in missing:
        changes.append({"pipeline_id": row.get("pipeline_id"), "name": row.get("name", ""),
                        "category": row.get("place_main_category", ""),
                        "change_type": "missing_from_source"})
    counts["missing_from_source"] = len(missing)

    write_csv(output_csv, fields + [f for f in MASTER_FIELDS if f not in fields], prepared, encoding=encoding)
    target = Path(changes_jsonl); target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("".join(json.dumps(x, ensure_ascii=False, sort_keys=True) + "\n" for x in changes), encoding="utf-8")
    return {"source_rows": len(rows), "master_rows": len(prepared), "changes": dict(counts),
            "output": str(Path(output_csv).resolve()), "changes_output": str(target.resolve())}
