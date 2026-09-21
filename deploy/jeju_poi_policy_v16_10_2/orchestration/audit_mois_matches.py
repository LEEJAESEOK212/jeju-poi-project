"""Export human-review CSVs for newly matched and exact-name/address-mismatch POIs."""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import json
from pathlib import Path

from orchestration.evidence import normalize_mois_record
from orchestration.matching import normalize_name
from orchestration.run_status_pipeline import read_jsonl


FIELDS = ["place_id", "poi_name", "poi_category", "poi_road_address", "poi_jibun_address",
          "previous_result", "new_result", "match_score", "match_reason",
          "official_name", "official_road_address", "official_jibun_address",
          "official_status", "official_api", "official_source_id", "permit_date",
          "closed_at", "official_updated_at"]


def _write(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def audit(poi_csv: str, raw_jsonl: str, previous_jsonl: str,
          current_jsonl: str, output_dir: str) -> dict:
    with open(poi_csv, encoding="utf-8-sig", newline="") as handle:
        pois = list(csv.DictReader(handle))
    previous, current = read_jsonl(previous_jsonl), read_jsonl(current_jsonl)
    official_by_name: dict[str, list[dict]] = defaultdict(list)
    for raw in read_jsonl(raw_jsonl):
        record = normalize_mois_record(raw)
        key = normalize_name(record.get("name", ""))
        if key:
            official_by_name[key].append(record)

    changed, remaining = [], []
    for poi, before, after in zip(pois, previous, current):
        base = {"place_id": poi.get("place_id", ""), "poi_name": poi.get("name", ""),
                "poi_category": poi.get("place_main_category", ""),
                "poi_road_address": poi.get("road_address", ""),
                "poi_jibun_address": poi.get("jibun_address", ""),
                "previous_result": before.get("result", ""),
                "new_result": after.get("result", "")}
        if before.get("result") == "not_found" and after.get("result") != "not_found":
            changed.append({**base, "match_score": after.get("match_score", ""),
                "match_reason": after.get("match_reason", ""),
                "official_name": after.get("official_name", ""),
                "official_road_address": after.get("official_road_address", ""),
                "official_jibun_address": after.get("official_jibun_address", ""),
                "official_status": after.get("status_name", ""),
                "official_api": after.get("official_api", ""),
                "official_source_id": after.get("source_id", ""),
                "permit_date": after.get("permit_date", ""),
                "closed_at": after.get("closed_at", ""),
                "official_updated_at": after.get("official_updated_at", "")})
        if after.get("result") == "not_found":
            candidates = official_by_name.get(normalize_name(poi.get("name", "")), [])
            for candidate in candidates:
                remaining.append({**base, "official_name": candidate.get("name", ""),
                    "official_road_address": candidate.get("road_address", ""),
                    "official_jibun_address": candidate.get("jibun_address", ""),
                    "official_status": candidate.get("status_name", ""),
                    "official_api": candidate.get("source_dataset", ""),
                    "official_source_id": candidate.get("source_id", ""),
                    "permit_date": candidate.get("permit_date", ""),
                    "closed_at": candidate.get("closed_at", ""),
                    "official_updated_at": candidate.get("updated_at", "")})

    root = Path(output_dir)
    changed_path = root / "newly_matched_review.csv"
    remaining_path = root / "same_name_address_mismatch_review.csv"
    _write(changed_path, changed)
    _write(remaining_path, remaining)
    return {"newly_matched": len(changed), "remaining_candidate_rows": len(remaining),
            "newly_matched_output": str(changed_path.resolve()),
            "remaining_output": str(remaining_path.resolve())}


def main() -> int:
    parser = argparse.ArgumentParser(description="행안부 매칭 변경·주소 불일치 검토 CSV 생성")
    parser.add_argument("--poi-csv", required=True)
    parser.add_argument("--raw", required=True)
    parser.add_argument("--previous", required=True)
    parser.add_argument("--current", required=True)
    parser.add_argument("--output-dir", default="work/status/audit")
    args = parser.parse_args()
    print(json.dumps(audit(args.poi_csv, args.raw, args.previous, args.current,
                           args.output_dir), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
