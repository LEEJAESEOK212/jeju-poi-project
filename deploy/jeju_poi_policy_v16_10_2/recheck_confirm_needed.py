"""Prepare and merge a targeted Kakao recheck for unresolved Jeju POIs."""
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path


def read_jsonl(path: Path) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"{path} {number}행 JSON 손상: {error}") from error
            if not str(row.get("place_key", "")).strip():
                raise ValueError(f"{path} {number}행 place_key 누락")
            rows.append(row)
    return rows


def write_jsonl_atomic(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def prepare(master_path: Path, old_path: Path, queue_path: Path,
            *, scope: str = "targeted") -> None:
    old_keys = {str(row["place_key"]) for row in read_jsonl(old_path)}
    with master_path.open(encoding="utf-8-sig", newline="") as stream:
        master = list(csv.DictReader(stream))
    targets = []
    reason_counts = {"convenience_store": 0, "missing_evidence": 0,
                     "official_unresolved": 0, "kakao_ambiguous": 0}
    seen = set()
    for row in master:
        key = str(row.get("place_key", "")).strip()
        reason = row.get("status_reason_code")
        if reason not in {"kakao_not_found_not_closure", "kakao_not_found_needs_review",
                          "authoritative_source_unresolved", "kakao_identity_unresolved"}:
            continue
        convenience = (str(row.get("place_main_category_id", "")).strip() == "7"
                       or row.get("place_main_category") == "편의점")
        missing = key not in old_keys
        official_unresolved = reason == "authoritative_source_unresolved"
        kakao_ambiguous = reason == "kakao_identity_unresolved"
        selected = (scope == "all_unresolved" or convenience or missing
                    or official_unresolved or kakao_ambiguous)
        if not selected or key in seen:
            continue
        seen.add(key)
        targets.append(row)
        if convenience:
            reason_counts["convenience_store"] += 1
        if missing:
            reason_counts["missing_evidence"] += 1
        if official_unresolved:
            reason_counts["official_unresolved"] += 1
        if kakao_ambiguous:
            reason_counts["kakao_ambiguous"] += 1
    write_jsonl_atomic(queue_path, targets)
    print(json.dumps({"targets": len(targets), **reason_counts,
                      "scope": scope,
                      "queue": str(queue_path.resolve())}, ensure_ascii=False))


def merge(old_path: Path, new_path: Path, queue_path: Path, output_path: Path) -> None:
    old_rows, new_rows, queue_rows = (read_jsonl(old_path), read_jsonl(new_path),
                                      read_jsonl(queue_path))
    target_keys = {str(row["place_key"]) for row in queue_rows}
    replacements = {str(row["place_key"]): row for row in new_rows}
    if set(replacements) != target_keys:
        missing = sorted(target_keys - set(replacements))[:10]
        extra = sorted(set(replacements) - target_keys)[:10]
        raise ValueError(f"재검증 결과 불완전: missing={missing}, extra={extra}")
    merged, emitted = [], set()
    for row in old_rows:
        key = str(row["place_key"])
        merged.append(replacements.get(key, row))
        emitted.add(key)
    for key, row in replacements.items():
        if key not in emitted:
            merged.append(row)
            emitted.add(key)
    if len(emitted) != len(merged):
        raise ValueError("병합 결과 place_key 중복")
    write_jsonl_atomic(output_path, merged)
    result_counts = {}
    for row in replacements.values():
        result = str(row.get("result", ""))
        result_counts[result] = result_counts.get(result, 0) + 1
    print(json.dumps({"old": len(old_rows), "rechecked": len(replacements),
                      "merged": len(merged), "results": result_counts,
                      "output": str(output_path.resolve())}, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare")
    prep.add_argument("--master", required=True, type=Path)
    prep.add_argument("--old-evidence", required=True, type=Path)
    prep.add_argument("--queue", required=True, type=Path)
    prep.add_argument("--scope", choices=("targeted", "all_unresolved"),
                      default="targeted")
    combine = commands.add_parser("merge")
    combine.add_argument("--old-evidence", required=True, type=Path)
    combine.add_argument("--new-evidence", required=True, type=Path)
    combine.add_argument("--queue", required=True, type=Path)
    combine.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.master, args.old_evidence, args.queue, scope=args.scope)
    else:
        merge(args.old_evidence, args.new_evidence, args.queue, args.output)


if __name__ == "__main__":
    main()
