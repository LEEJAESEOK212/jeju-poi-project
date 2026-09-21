"""CSV helpers kept local so the orchestration package is self-contained."""
from __future__ import annotations

import csv
from pathlib import Path


REQUIRED_COLUMNS = {"name", "content", "tags"}
CATEGORY_COLUMNS = {"place_main_category", "place_main_category_id"}


def read_csv(path: str | Path) -> tuple[list[str], list[dict[str, str]], str]:
    path = Path(path)
    raw = path.read_bytes()
    encoding = "utf-8-sig"
    try:
        text = raw.decode(encoding)
    except UnicodeDecodeError:
        encoding = "cp949"
        text = raw.decode(encoding)
    reader = csv.DictReader(text.splitlines())
    fields = list(reader.fieldnames or [])
    missing = REQUIRED_COLUMNS - set(fields)
    if missing:
        raise ValueError(f"필수 열 누락: {', '.join(sorted(missing))}")
    if not CATEGORY_COLUMNS.intersection(fields):
        raise ValueError("필수 열 누락: place_main_category 또는 place_main_category_id")
    return fields, [{key: value or "" for key, value in row.items()} for row in reader], encoding


def write_csv(path: str | Path, fields: list[str], rows: list[dict[str, str]],
              *, encoding: str = "utf-8-sig") -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=encoding, newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
