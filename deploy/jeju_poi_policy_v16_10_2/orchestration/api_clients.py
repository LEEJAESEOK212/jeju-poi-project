"""Small dependency-free API clients with bounded retry and test injection."""
from __future__ import annotations

import json
import random
import time
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode
from urllib.request import Request, urlopen

from orchestration.matching import search_name_variants


class ApiError(RuntimeError):
    pass


Transport = Callable[[str, dict[str, str], float], tuple[int, bytes]]


def urllib_transport(url: str, headers: dict[str, str], timeout: float) -> tuple[int, bytes]:
    request = Request(url, headers=headers, method="GET")
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except HTTPError as error:
        return error.code, error.read()
    except URLError as error:
        raise ApiError(f"network_error: {error.reason}") from error


class JsonApiClient:
    def __init__(self, *, timeout: float = 20, retries: int = 3,
                 transport: Transport = urllib_transport,
                 sleep: Callable[[float], None] = time.sleep):
        self.timeout, self.retries, self.transport, self.sleep = timeout, retries, transport, sleep

    def get(self, base_url: str, *, params: dict[str, object], headers: dict[str, str] | None = None) -> dict:
        query = urlencode({key: value for key, value in params.items() if value not in (None, "")})
        url = f"{base_url}{'&' if '?' in base_url else '?'}{query}" if query else base_url
        last = "unknown"
        for attempt in range(self.retries + 1):
            status, raw = self.transport(url, headers or {}, self.timeout)
            if status == 200:
                try:
                    payload = json.loads(raw.decode("utf-8-sig"))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ApiError(f"invalid_json: {error}") from error
                if not isinstance(payload, dict):
                    raise ApiError("invalid_json_root")
                return payload
            last = f"http_{status}: {raw[:300].decode('utf-8', errors='replace')}"
            if status not in {429, 500, 502, 503, 504} or attempt == self.retries:
                break
            retry_after = min(8.0, 0.5 * (2 ** attempt)) + random.random() * 0.1
            self.sleep(retry_after)
        raise ApiError(last)


def nested_items(payload: dict) -> list[dict]:
    """Accept common data.go.kr and SafetyData JSON envelope variants."""
    candidates: object = payload
    for path in (("response", "body", "items"), ("body", "items"), ("items",), ("data",)):
        current: object = payload
        for key in path:
            if not isinstance(current, dict) or key not in current:
                current = None
                break
            current = current[key]
        if current is not None:
            candidates = current
            break
    if isinstance(candidates, dict) and "item" in candidates:
        candidates = candidates["item"]
    if candidates is None:
        return []
    if isinstance(candidates, dict):
        return [candidates]
    if isinstance(candidates, list):
        return [item for item in candidates if isinstance(item, dict)]
    raise ApiError("items_not_list")


def first(record: dict, *keys: str) -> str:
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def validate_public_data_response(payload: dict) -> None:
    """Raise on gateway/application errors instead of treating them as no records."""
    response = payload.get("response")
    header = response.get("header") if isinstance(response, dict) else None
    if not isinstance(header, dict):
        return
    code = str(header.get("resultCode", "")).strip()
    message = str(header.get("resultMsg", "")).strip()
    if code and code not in {"0", "00", "0000"}:
        raise ApiError(f"public_data_error_{code}: {message or 'unknown'}")


class KakaoLocalClient:
    URL = "https://dapi.kakao.com/v2/local/search/keyword.json"

    def __init__(self, rest_api_key: str, *, http: JsonApiClient | None = None):
        if not rest_api_key.strip():
            raise ValueError("Kakao REST API key is required")
        self.key, self.http = rest_api_key.strip(), http or JsonApiClient()

    def search(self, poi: dict, *, radius_m: int = 2000, size: int = 15) -> list[dict]:
        name = str(poi.get("name", "")).strip()
        address = str(poi.get("road_address") or poi.get("jibun_address", "")).strip()
        # With coordinates, a name-only nearby search is much more reliable
        # than feeding Kakao a long official address including floor/building.
        documents, seen = [], set()
        for variant in search_name_variants(name):
            query = variant if poi.get("longitude") and poi.get("latitude") else f"{variant} {address}".strip()
            params: dict[str, object] = {"query": query, "size": size}
            if poi.get("longitude") and poi.get("latitude"):
                params.update({"x": poi["longitude"], "y": poi["latitude"], "radius": radius_m, "sort": "distance"})
            payload = self.http.get(self.URL, params=params,
                                    headers={"Authorization": f"KakaoAK {self.key}"})
            batch = payload.get("documents", [])
            if not isinstance(batch, list):
                raise ApiError("kakao_documents_not_list")
            for item in batch:
                marker = first(item, "id") or (first(item, "place_name"), first(item, "road_address_name"), first(item, "address_name"))
                if marker not in seen:
                    seen.add(marker)
                    documents.append(item)
        return [{"source": "kakao", "source_id": first(item, "id"),
                 "name": first(item, "place_name"), "road_address": first(item, "road_address_name"),
                 "jibun_address": first(item, "address_name"), "latitude": first(item, "y"),
                 "longitude": first(item, "x"), "phone": first(item, "phone"),
                 "distance": first(item, "distance"),
                 "place_url": first(item, "place_url")} for item in documents]


class PagedPublicDataClient:
    """Configurable paginator for MOIS/SafetyData endpoints after approval."""
    def __init__(self, endpoint: str, service_key: str, *, http: JsonApiClient | None = None,
                 key_parameter: str = "serviceKey", page_parameter: str = "pageNo",
                 size_parameter: str = "numOfRows", extra_params: dict[str, object] | None = None):
        if not endpoint.strip() or not service_key.strip():
            raise ValueError("endpoint and service_key are required")
        # data.go.kr displays both encoded and decoded keys. Normalize first so
        # urlencode() below applies exactly one encoding in either case.
        self.endpoint, self.service_key, self.http = endpoint, unquote(service_key), http or JsonApiClient()
        self.key_parameter, self.page_parameter, self.size_parameter = key_parameter, page_parameter, size_parameter
        self.extra_params = extra_params or {}

    def pages(self, *, page_size: int = 1000, max_pages: int = 10000):
        for page in range(1, max_pages + 1):
            params = dict(self.extra_params)
            params.update({self.key_parameter: self.service_key,
                           self.page_parameter: page, self.size_parameter: page_size})
            payload = self.http.get(self.endpoint, params=params)
            validate_public_data_response(payload)
            items = nested_items(payload)
            if not items:
                return
            yield items
            if len(items) < page_size:
                return
        raise ApiError(f"pagination_exceeded_{max_pages}_pages")
