"""Policy-driven lifecycle classification and status decisions for Jeju POIs."""
from __future__ import annotations

from dataclasses import dataclass
import re

BUSINESS = "business_operation"
FIXED = "fixed_public_natural"
INFRA = "facility_infrastructure"
PENDING = "policy_pending"
LIFECYCLE_TYPES = {BUSINESS, FIXED, INFRA, PENDING}
LEGACY_LIFECYCLE_MAP = {
    "fuel_station": BUSINESS, "public_facility": INFRA,
    "facility_infrastructure": INFRA, "attached_amenity": INFRA, "ev_charger": INFRA,
    "permanent_venue": FIXED, "physical_object": FIXED, "natural_space": FIXED,
    "temporary_event": PENDING,
}

MAIN_CATEGORY_BY_ID = {
    "1": "음식점", "2": "카페", "3": "숙박", "4": "관광지", "5": "병원",
    "6": "약국", "7": "편의점", "8": "대형마트", "9": "주유소(충전소)",
    "10": "주차장", "11": "공공기관", "12": "쇼핑", "13": "레저/스포츠",
    "14": "여행지원", "15": "교육시설", "16": "교통시설", "17": "공연/행사장",
    "18": "박물관/전시장", "19": "전통시장", "20": "공공편의시설",
    "21": "캠핑/아웃도어", "22": "테마파크",
}
INFRA_WORDS = ("전기차충전소", "전기자동차충전", "EV충전", "공중화장실", "공공화장실",
               "무료와이파이", "무료WI-FI", "공공WI-FI", "공영주차장", "부속주차장")
FIXED_WORDS = ("공공기관", "버스정류소", "오름", "폭포", "해변", "해수욕장", "숲", "숲길",
               "문화재", "유적", "기념물", "자연환경", "자연명소", "습지", "둘레길", "산책로",
               "초등학교", "중학교", "고등학교", "대학교", "도서관", "문화센터", "연수원")
BUSINESS_WORDS = ("음식점", "카페", "숙박", "병원", "의원", "약국", "편의점", "마트", "쇼핑",
                  "주유소", "LPG충전소", "레저", "여행지원", "여행사", "캠핑", "테마파크",
                  "전시장", "미술관", "박물관", "공연장", "체험학습장", "영화관",
                  "연예기획사", "영화영상", "방송국", "신문", "학원")
BUSINESS_MAIN_IDS = {"1", "2", "3", "5", "6", "7", "8", "9", "12", "13", "14", "17", "18", "19", "21", "22"}
FIXED_MAIN_IDS = {"11", "16"}
INFRA_MAIN_IDS = {"10", "20"}

INFRA_SUBCATEGORIES = {"전기차충전소", "전기자동차충전소", "공중화장실", "공공화장실",
                       "무료와이파이", "공공와이파이", "공영주차장", "부속주차장",
                       "민방위대피시설", "대피시설"}
FIXED_SUBCATEGORIES = {"초등학교", "중학교", "고등학교", "대학교", "도서관",
                       "문화센터", "연수원", "학교부속시설", "교육단체", "어린이집"}
BUSINESS_SUBCATEGORIES = {"주유소", "LPG충전소", "LPG", "체험학습장", "영화관",
                          "연예기획사", "영화영상", "방송국", "신문", "미술학원",
                          "플레이타임", "청소년수련시설"}


def _category_id(row: dict[str, str]) -> str:
    raw = str(row.get("place_main_category_id") or "").strip()
    try:
        return str(int(float(raw)))
    except (TypeError, ValueError):
        return raw


def category_text(row: dict[str, str]) -> str:
    return str(row.get("place_main_category") or "").strip() or MAIN_CATEGORY_BY_ID.get(_category_id(row), "")


def _detail_text(row: dict[str, str]) -> str:
    return " ".join(str(row.get(field) or "") for field in
                    ("name", "source_place_main_category"))


def _compact(value: object) -> str:
    return re.sub(r"[^0-9A-Z가-힣]", "", str(value or "").upper())


INFRA_SUBCATEGORY_KEYS = {_compact(value) for value in INFRA_SUBCATEGORIES}
FIXED_SUBCATEGORY_KEYS = {_compact(value) for value in FIXED_SUBCATEGORIES}
BUSINESS_SUBCATEGORY_KEYS = {_compact(value) for value in BUSINESS_SUBCATEGORIES}


@dataclass(frozen=True)
class LifecycleDecision:
    decision: str
    verification_state: str
    reason_code: str


def classify_lifecycle(row: dict[str, str]) -> str:
    """Apply sub-category override, then name/function, then main-category default."""
    stored = str(row.get("lifecycle_type") or "").strip()
    if stored in LIFECYCLE_TYPES:
        return stored
    if stored in LEGACY_LIFECYCLE_MAP:
        return LEGACY_LIFECYCLE_MAP[stored]
    subcategory = _compact(row.get("place_sub_category"))
    if subcategory in INFRA_SUBCATEGORY_KEYS:
        return INFRA
    if subcategory in FIXED_SUBCATEGORY_KEYS:
        return FIXED
    if subcategory in BUSINESS_SUBCATEGORY_KEYS:
        return BUSINESS

    # A meaningful sub-category is itself the strongest function signal.  Do not
    # let words in a POI description or tags (for example, "주차장 있음") change
    # the lifecycle group.
    if any(_compact(word) in subcategory for word in INFRA_WORDS):
        return INFRA
    if any(_compact(word) in subcategory for word in FIXED_WORDS):
        return FIXED
    if any(_compact(word) in subcategory for word in BUSINESS_WORDS):
        return BUSINESS

    detail = _compact(_detail_text(row))
    category = _compact(category_text(row))
    specific = detail + " " + category
    category_id = _category_id(row)
    if any(_compact(word) in specific for word in INFRA_WORDS):
        return INFRA
    if "주유소" in detail or "LPG충전소" in detail:
        return BUSINESS
    # Clear business main categories remain businesses even when the branch
    # name contains a location word such as 해변, 해수욕장, 숲 or 공원.
    if category_id in BUSINESS_MAIN_IDS:
        return BUSINESS
    if any(_compact(word) in detail for word in FIXED_WORDS):
        return FIXED
    if any(_compact(word) in detail for word in BUSINESS_WORDS):
        return BUSINESS
    if category_id in INFRA_MAIN_IDS or any(x in category for x in ("주차장", "공공편의시설")):
        return INFRA
    if category_id in FIXED_MAIN_IDS or any(x in category for x in ("공공기관", "교통시설")):
        return FIXED
    if any(_compact(word) in category for word in BUSINESS_WORDS):
        return BUSINESS
    if category_id == "4" or "관광지" in category:
        return FIXED
    return PENDING


def lifecycle_group(lifecycle_type: str) -> str:
    return {BUSINESS: "BUSINESS", FIXED: "FIXED", INFRA: "INFRA"}.get(lifecycle_type, "PENDING")


def is_temporary_event(row: dict[str, str]) -> bool:
    """Events are excluded upstream; end-date deletion is not a status policy."""
    return False


def uses_kakao(lifecycle_type: str) -> bool:
    return lifecycle_type == BUSINESS


def decide_lifecycle(*, lifecycle_type: str, temporary_event: bool,
                     official_result: str, kakao_result: str | None,
                     official_scope: str = "business_registry") -> LifecycleDecision:
    del temporary_event
    if lifecycle_type == PENDING:
        return LifecycleDecision("REVIEW", "unverified", "category_policy_pending")
    if lifecycle_type in {FIXED, INFRA}:
        if official_scope == "facility_registry":
            if official_result == "closed":
                return LifecycleDecision("DEACTIVATE", "verified_inactive", "facility_officially_closed")
            if official_result == "open":
                return LifecycleDecision("KEEP", "verified_active", "facility_officially_active")
            if official_result in {"ambiguous", "error"}:
                return LifecycleDecision("REVIEW", "unverified", "facility_evidence_unresolved")
        return LifecycleDecision("KEEP", "unverified", "facility_no_change_evidence")
    if official_result == "closed":
        return LifecycleDecision("DEACTIVATE", "verified_inactive", "authoritative_source_closed")
    if official_result == "open":
        return LifecycleDecision("KEEP", "verified_active", "authoritative_source_open")
    # A verified map identity is independent positive evidence even when the
    # licence search returned multiple candidates or a transient API error.
    if kakao_result == "found":
        return LifecycleDecision("KEEP", "verified_active", "kakao_same_place_found")
    if official_result in {"ambiguous", "error"}:
        return LifecycleDecision("REVIEW", "unverified", "authoritative_source_unresolved")
    if kakao_result in {"ambiguous", "error"}:
        return LifecycleDecision("REVIEW", "unverified", "kakao_identity_unresolved")
    return LifecycleDecision("REVIEW", "unverified", "kakao_not_found_needs_review")


def operating_status_label(lifecycle_type: str, decision: str, verification_state: str) -> str:
    if decision == "REVIEW":
        return "확인 필요"
    if lifecycle_type != BUSINESS:
        return "비운영" if decision == "DEACTIVATE" else "운영"
    if verification_state == "unverified":
        return "확인 필요"
    if lifecycle_type == BUSINESS:
        return "폐업" if decision == "DEACTIVATE" else "영업"
    return "운영"
