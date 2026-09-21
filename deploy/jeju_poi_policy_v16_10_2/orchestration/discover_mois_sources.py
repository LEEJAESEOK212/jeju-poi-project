"""Discover MOIS LOCALDATA Base URLs from the public data.go.kr catalog."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from orchestration.api_clients import ApiError, JsonApiClient, validate_public_data_response


DATA_GO = "https://www.data.go.kr"
SEARCH_URL = f"{DATA_GO}/tcs/dss/selectDataSetList.do"
DEFAULT_KEYWORDS = (
    "행정안전부 조회서비스",
    "LOCALDATA 조회서비스",
    "지방행정 인허가 조회서비스",
    "apis.data.go.kr/1741000",
)
# Portal search pagination can omit valid records even when the detail page exists.
# Keep confirmed outliers by stable public-data ID; normal parsing/probing still applies.
PINNED_SOURCE_DATA = (
    {
        "name": "행정안전부_공중화장실정보 조회서비스",
        "detail_url": "https://www.data.go.kr/data/15155058/openapi.do",
        "base_url": "https://apis.data.go.kr/1741000/public_restroom_info_v2",
        "info_path": "/info_v2",
        "history_path": "/history_v2",
    },
)
BASE_RE = re.compile(r"Base\s*URL\s*:\s*(?:https?://)?(apis\.data\.go\.kr/1741000/[A-Za-z0-9_\-]+)", re.I)
HOST_RE = re.compile(r'"host"\s*:\s*"(apis\.data\.go\.kr/1741000/[A-Za-z0-9_\-]+)"', re.I)


class CatalogParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links: dict[str, str] = {}
        self._href = ""
        self._text: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href", "")
        if re.fullmatch(r"/data/\d+/openapi\.do", href):
            self._href, self._text = href, []

    def handle_data(self, data):
        if self._href:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._href:
            name = " ".join(" ".join(self._text).split())
            self.links[self._href] = name
            self._href, self._text = "", []


def fetch_text(url: str, *, timeout: float = 30, retries: int = 3) -> str:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; JejuPOI/1.0)"})
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except Exception as error:
            last = error
            if attempt == retries:
                break
            time.sleep(0.5 * (2 ** attempt))
    raise RuntimeError(f"fetch_failed: {url}: {last}") from last


def search_catalog(*, keyword: str = "행정안전부 조회서비스", per_page: int = 100,
                   max_pages: int = 20) -> dict[str, str]:
    found: dict[str, str] = {}
    for page in range(1, max_pages + 1):
        query = urlencode({"keyword": keyword, "conditionType": "search",
                           "currentPage": page, "dType": "API", "perPage": per_page})
        parser = CatalogParser()
        parser.feed(fetch_text(f"{SEARCH_URL}?{query}"))
        before = len(found)
        found.update(parser.links)
        if len(parser.links) < per_page or len(found) == before:
            break
    return found


def search_catalog_all(*, keywords: tuple[str, ...] = DEFAULT_KEYWORDS,
                       per_page: int = 100, max_pages: int = 20) -> dict[str, str]:
    """Merge several catalog searches so title wording cannot hide a LOCALDATA API."""
    found: dict[str, str] = {}
    for keyword in keywords:
        found.update(search_catalog(keyword=keyword, per_page=per_page, max_pages=max_pages))
    return found


@dataclass(frozen=True)
class Source:
    name: str
    detail_url: str
    base_url: str
    info_url: str
    history_url: str


def parse_detail_html(name: str, detail_url: str, html: str) -> Source | None:
    plain = re.sub(r"<[^>]+>", " ", html)
    plain = " ".join(plain.replace("&nbsp;", " ").split())
    match = HOST_RE.search(html) or BASE_RE.search(plain)
    if not match or not re.search(r'(?:"|GET\s*)/info(?:"|\b)', html, re.I):
        return None
    base = f"https://{match.group(1)}"
    title = name or re.sub(r"\s+", " ", plain[:200]).strip()
    return Source(title, detail_url, base, f"{base}/info", f"{base}/history")


def parse_detail(name: str, detail_path: str) -> Source | None:
    detail_url = urljoin(DATA_GO, detail_path)
    return parse_detail_html(name, detail_url, fetch_text(detail_url))


def discover_detailed(*, workers: int = 8,
                      keywords: tuple[str, ...] = DEFAULT_KEYWORDS) -> tuple[list[Source], list[dict], int]:
    links = search_catalog_all(keywords=keywords)
    sources: list[Source] = []
    failures: list[dict] = []
    skipped_non_mois = 0
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 12))) as pool:
        futures = {pool.submit(parse_detail, name, path): path for path, name in links.items()}
        for future in as_completed(futures):
            try:
                source = future.result()
            except Exception as error:
                failures.append({"detail_path": futures[future], "error": str(error)[:500]})
                continue
            if source:
                sources.append(source)
            else:
                skipped_non_mois += 1
    unique = {source.base_url: source for source in sources}
    for item in PINNED_SOURCE_DATA:
        base = item["base_url"]
        unique[base] = Source(item["name"], item["detail_url"], base,
                              f"{base}{item.get('info_path', '/info')}",
                              f"{base}{item.get('history_path', '/history')}")
    return (sorted(unique.values(), key=lambda item: (item.name, item.base_url)),
            failures, skipped_non_mois)


def discover(*, workers: int = 8, keywords: tuple[str, ...] = DEFAULT_KEYWORDS) -> list[Source]:
    return discover_detailed(workers=workers, keywords=keywords)[0]


def write_enabled_manifest(registry_path: str | Path, output_path: str | Path) -> dict:
    records = json.loads(Path(registry_path).read_text(encoding="utf-8"))
    enabled = sorted((record for record in records if record.get("access") == "approved"),
                     key=lambda item: (item.get("name", ""), item.get("base_url", "")))
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(json.dumps(enabled, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, output)
    return {"enabled_count": len(enabled), "enabled_output": str(output.resolve())}


def probe(source: Source, service_key: str, *, http: JsonApiClient) -> tuple[str, str]:
    try:
        payload = http.get(source.info_url, params={"serviceKey": service_key,
            "pageNo": 1, "numOfRows": 1, "returnType": "json"})
        validate_public_data_response(payload)
        return "approved", ""
    except ApiError as error:
        message = str(error)
        if "403" in message or "SERVICE_KEY" in message or "인증키" in message or "PERMISSION" in message:
            return "denied", message[:300]
        return "error", message[:300]


def write_registry(path: str | Path, sources: list[Source], *, service_key: str | None = None,
                   workers: int = 8) -> dict:
    records = []
    if service_key:
        http = JsonApiClient(retries=1)
        with ThreadPoolExecutor(max_workers=max(1, min(workers, 8))) as pool:
            futures = {pool.submit(probe, source, service_key, http=http): source for source in sources}
            statuses = {future: future.result() for future in as_completed(futures)}
        probe_by_url = {futures[future].base_url: statuses[future] for future in statuses}
    else:
        probe_by_url = {}
    for source in sources:
        record = asdict(source)
        status, error = probe_by_url.get(source.base_url, ("not_probed", ""))
        record["access"] = status
        if error:
            record["probe_error"] = error
        records.append(record)
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, output)
    counts: dict[str, int] = {}
    for record in records:
        counts[record["access"]] = counts.get(record["access"], 0) + 1
    return {"catalog_sources": len(records), "access": counts, "output": str(output.resolve())}


def main() -> int:
    parser = argparse.ArgumentParser(description="행안부 LOCALDATA API Base URL 자동 수집")
    parser.add_argument("--output", default="orchestration/mois_sources.json")
    parser.add_argument("--enabled-output", default="orchestration/mois_enabled_sources.json")
    parser.add_argument("--failures-output", default="orchestration/mois_discovery_failures.json")
    parser.add_argument("--service-key-env", default="MOIS_SERVICE_KEY")
    parser.add_argument("--probe-approved", action="store_true")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--keyword", action="append", dest="keywords",
                        help="공공데이터포털 검색어. 여러 번 지정 가능")
    args = parser.parse_args()
    key = os.environ.get(args.service_key_env, "").strip() if args.probe_approved else None
    if args.probe_approved and not key:
        parser.error(f"환경변수 {args.service_key_env}가 없습니다.")
    keywords = tuple(args.keywords) if args.keywords else DEFAULT_KEYWORDS
    sources, failures, skipped_non_mois = discover_detailed(workers=args.workers, keywords=keywords)
    result = write_registry(args.output, sources,
                            service_key=key, workers=args.workers)
    failure_output = Path(args.failures_output)
    failure_output.write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
    result.update(write_enabled_manifest(args.output, args.enabled_output))
    result["discovery_failures"] = len(failures)
    result["skipped_non_mois"] = skipped_non_mois
    result["failures_output"] = str(failure_output.resolve())
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
