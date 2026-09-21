"""Conservative identity matching for Jeju POIs and official/map records."""
from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from math import asin, cos, radians, sin, sqrt
import re

from orchestration.lifecycle import category_text


_LEGAL_FORM = re.compile(
    r"(?:\(주\)|\(유\)|주식회사|유한회사|합자회사|"
    r"농업회사법인|영어조합법인|사회적협동조합)"
)
_PUNCTUATION = re.compile(r"[^0-9a-z가-힣]")
_ADDRESS_PREFIX = re.compile(r"^(?:제주특별자치도|제주도)\s*")
_PARENTHETICAL = re.compile(r"\([^)]*\)")
_ROAD_CORE = re.compile(r"^(.+?(?:대로|로|길|거리)\s*\d+(?:-\d+)?)\b")
_ROAD_PARENT_CORE = re.compile(r"^(.+?(?:대로|로|길|거리)\s*\d+)(?:-\d+)?\b")
_LOT_CORE = re.compile(r"^(.+?(?:읍|면|동|리)\s*(?:산\s*)?\d+(?:-\d+)?)\b")
_LOCALITY = re.compile(r"[가-힣0-9]+(?:읍|면|동|리)")
_NUMBER = re.compile(r"\d+")
_GENERIC_NAME_WORDS = ("민박", "펜션", "호텔", "리조트", "게스트하우스", "카페", "식당")
_LETTER_KO = {"a":"에이","b":"비","c":"씨","d":"디","e":"이","f":"에프","g":"지",
              "h":"에이치","i":"아이","j":"제이","k":"케이","l":"엘","m":"엠","n":"엔",
              "o":"오","p":"피","q":"큐","r":"알","s":"에스","t":"티","u":"유","v":"브이",
              "w":"더블유","x":"엑스","y":"와이","z":"지"}


def poi_industry(poi: dict) -> str | None:
    """Return a broad POI industry only when its category is unambiguous."""
    category = category_text(poi).replace(" ", "")
    if "동물병원" in category:
        return "animal_hospital"
    if "동물약국" in category:
        return "animal_pharmacy"
    if "병원" in category or "의원" in category:
        return "medical"
    if "약국" in category:
        return "pharmacy"
    if category.startswith("음식점") or category == "카페":
        return "food_service"
    if category.startswith("숙박"):
        return "lodging"
    if category == "관광지":
        return "tourism_facility"
    if category == "쇼핑" or category.startswith("대형마트"):
        return "retail"
    if category == "주유소(충전소)":
        return "fuel"
    for keyword, industry in (
        ("공중화장실", "restroom"), ("무료와이파이", "wifi"),
        ("민방위대피시설", "shelter"), ("세차장", "carwash"),
        ("자전거보관소", "bicycle"), ("목욕장", "bath"),
        ("안경업", "eyewear"), ("전기차충전소", "ev_charging"),
        ("편의점", "convenience_store"), ("박물관", "museum"),
        ("전시장", "museum"), ("공연", "performance"),
        ("캠핑", "camping"), ("야영", "camping"),
        ("테마파크", "theme_park"), ("승마", "sports"),
        ("노래연습장", "karaoke"), ("레저/스포츠", "sports"),
    ):
        if keyword in category:
            return industry
    return None


def official_industry(candidate: dict) -> str | None:
    """Infer the licence/service industry from its MOIS dataset title."""
    source = str(candidate.get("source_dataset", "")).replace(" ", "")
    if not source:
        return None
    if "동물병원" in source:
        return "animal_hospital"
    if "동물약국" in source:
        return "animal_pharmacy"
    if any(word in source for word in ("병원", "의원", "부속의료기관", "의료법인")):
        return "medical"
    if "약국" in source:
        return "pharmacy"
    if any(word in source for word in ("일반음식점", "휴게음식점", "제과점", "단란주점", "유흥주점")):
        return "food_service"
    if any(word in source for word in ("즉석판매", "식품제조", "식품판매", "유통전문판매", "건강기능식품")):
        return "food_retail_or_manufacturing"
    if any(word in source for word in ("숙박", "민박", "관광펜션", "한옥체험")):
        return "lodging"
    for keyword, industry in (
        ("공중화장실", "restroom"), ("무료와이파이", "wifi"),
        ("민방위", "shelter"), ("세차장", "carwash"),
        ("자전거", "bicycle"), ("목욕장", "bath"), ("안경업", "eyewear"),
        ("전기자동차", "ev_charging"), ("전기차충전", "ev_charging"),
        ("안전상비의약품", "medicine_retail"), ("공연장", "performance"),
        ("박물관", "museum"), ("미술관", "museum"),
        ("야영장", "camping"), ("테마파크", "theme_park"),
        ("노래연습장", "karaoke"), ("체력단련장", "sports"),
        ("당구장", "sports"), ("골프연습장", "sports"), ("승마장", "sports"),
        ("동물위탁관리", "animal_care"), ("전문휴양업", "tourism_facility"),
        ("대규모점포", "retail"), ("통신판매", "retail"),
        ("방문판매", "retail"), ("석유판매", "fuel"),
    ):
        if keyword in source:
            return industry
    return None


def is_overlay_poi(poi: dict) -> bool:
    category = category_text(poi).replace(" ", "")
    return "전기차충전소" in category or "무료와이파이" in category


def infer_host_industry(poi: dict) -> str | None:
    """Infer the host business behind an EV charger or public Wi-Fi POI."""
    text = " ".join(str(poi.get(field, "")) for field in ("name", "content", "tags")).lower()
    rules = (
        (("동물병원",), "animal_hospital"),
        (("병원", "의원", "메디컬"), "medical"),
        (("민박", "스테이", "호텔", "리조트", "펜션", "게스트하우스"), "lodging"),
        (("캠핑", "야영"), "camping"),
        (("골프", "당구", "승마", "피트니스", "헬스"), "sports"),
        (("테마파크",), "theme_park"),
        (("수련원", "휴양", "관광"), "tourism_facility"),
        (("하나로마트", "마트", "스토아", "스토어", "소품관"), "retail"),
        (("카페", "coffee", "커피", "식당", "키친", "베이크", "베이커리", "restaurant", "비어"), "food_service"),
    )
    for keywords, industry in rules:
        if any(keyword in text for keyword in keywords):
            return industry
    return None


def ancillary_food_conflicts_with_host_name(poi: dict, candidate: dict) -> bool:
    """Detect a food licence belonging inside an obviously non-food host."""
    if official_industry(candidate) != "food_service":
        return False
    name = str(poi.get("name", "")).replace(" ", "").lower()
    non_food_hosts = ("pc방", "수련원", "병원", "의원", "당구장", "골프연습장",
                      "캠핑장", "야영장", "시민회관", "박물관")
    return any(keyword in name for keyword in non_food_hosts)


def industry_compatible(poi: dict, candidate: dict) -> bool:
    """A categorized POI requires a positively identified matching API industry."""
    right = official_industry(candidate)
    if is_overlay_poi(poi):
        host = infer_host_industry(poi)
        if host is not None:
            return host == right
        # Food/retail/care licences commonly belong to a tenant or ancillary
        # operation, so they cannot close an otherwise unidentified host.
        return right not in {None, "food_service", "food_retail_or_manufacturing",
                             "medicine_retail", "animal_care"}
    if ancillary_food_conflicts_with_host_name(poi, candidate):
        return False
    left = poi_industry(poi)
    return left is None or left == right


def normalize_name(value: str) -> str:
    value = _LEGAL_FORM.sub("", str(value or "").strip().lower())
    return _PUNCTUATION.sub("", value)


_DISTINCT_PLACE_ROLES = (
    "atm", "현금지급기", "주차장", "세차장", "화장실", "충전소",
)


def _has_role_conflict(left: str, right: str) -> bool:
    """Keep an ancillary facility distinct from its host at one address."""
    left_key, right_key = normalize_name(left), normalize_name(right)
    return any((role in left_key) != (role in right_key) for role in _DISTINCT_PLACE_ROLES)


def fuzzy_name_ratio(left: str, right: str) -> float:
    """Return conservative typo/spacing similarity after safe normalization."""
    if _has_role_conflict(left, right):
        return 0.0
    left_aliases, right_aliases = name_aliases(left), name_aliases(right)
    if not left_aliases or not right_aliases:
        return 0.0
    return max(SequenceMatcher(None, a, b).ratio()
               for a in left_aliases for b in right_aliases)


def convenience_name_key(value: str) -> str:
    """Canonicalize store-brand spelling without equating ATM-only places."""
    key = normalize_name(value)
    key = key.replace("지에스25", "gs25").replace("씨유", "cu")
    key = key.replace("7eleven", "세븐일레븐")
    if key.endswith("점"):
        key = key[:-1]
    return key


def name_aliases(value: str) -> set[str]:
    """Conservative aliases for portal spelling/order differences."""
    raw=str(value or "").strip().lower()
    variants={raw}
    without_region=re.sub(r"^(?:제주특별자치도|제주도|제주시|서귀포시|제주)\s*", "", raw).strip()
    if without_region:variants.add(without_region)
    for item in list(variants):
        compact=item
        for word in _GENERIC_NAME_WORDS:
            compact=re.sub(rf"^(?:{word})\s*|\s*(?:{word})$", "", compact).strip()
        if compact:variants.add(compact)
    for item in list(variants):
        converted=re.sub(r"(?<![a-z])([a-z])(?![a-z])",lambda m:_LETTER_KO[m.group(1)],item)
        if converted!=item:variants.add(converted)
    aliases = {normalize_name(item) for item in variants if normalize_name(item)}
    convenience = {convenience_name_key(item) for item in variants}
    return aliases | {item for item in convenience if item}


def search_name_variants(value: str) -> list[str]:
    """Human-readable Kakao queries corresponding to the same alias policy."""
    raw=str(value or "").strip();result=[raw] if raw else []
    without_legal = _LEGAL_FORM.sub("", raw).strip()
    if without_legal and without_legal not in result:
        result.append(without_legal)
    stripped=re.sub(r"^(?:제주특별자치도|제주도|제주시|서귀포시|제주)\s*", "", raw,flags=re.I).strip()
    if stripped and stripped not in result:result.append(stripped)
    for item in list(result):
        compact=item
        for word in _GENERIC_NAME_WORDS:
            compact=re.sub(rf"^(?:{word})\s*|\s*(?:{word})$", "", compact,flags=re.I).strip()
        if compact and compact not in result:result.append(compact)
    for item in list(result):
        converted=re.sub(r"(?<![A-Za-z])([A-Za-z])(?![A-Za-z])",lambda m:_LETTER_KO[m.group(1).lower()],item)
        if converted!=item and converted not in result:result.append(converted)
    # Brand portals alternate between Latin/Korean spellings and often expose
    # an ATM entry instead of the store entry.  Query both spellings and a
    # branch form without the trailing '점'.
    compact = normalize_name(raw)
    brand_match = re.match(r"^(cu|씨유|gs25|지에스25|세븐일레븐|7eleven)(.+)$", compact)
    if brand_match:
        brand, branch = brand_match.groups()
        branch_without_point = branch[:-1] if branch.endswith("점") else branch
        branch_forms = [branch, branch_without_point]
        if branch_without_point.startswith("제주"):
            branch_forms.append(branch_without_point[2:])
        elif branch_without_point.startswith("서귀포"):
            branch_forms.append(branch_without_point[3:])
        brands = ("CU", "씨유") if brand in {"cu", "씨유"} else (
            ("GS25", "지에스25") if brand in {"gs25", "지에스25"} else
            ("세븐일레븐", "7-ELEVEN"))
        for brand_form in brands:
            for branch_form in branch_forms:
                query = f"{brand_form} {branch_form}".strip()
                if query and query not in result:
                    result.append(query)
    return result[:8]


def normalize_address(value: str) -> str:
    value = _ADDRESS_PREFIX.sub("", str(value or "").strip().lower())
    value = value.replace("제주시특별자치도", "제주시")
    return _PUNCTUATION.sub("", value)


def normalize_address_core(value: str) -> str:
    """Ignore building name/floor/unit after the primary road or lot number."""
    value = _ADDRESS_PREFIX.sub("", str(value or "").strip().lower())
    value = _PARENTHETICAL.sub(" ", value)
    value = value.replace("제주시특별자치도", "제주시")
    match = _ROAD_CORE.search(value) or _LOT_CORE.search(value)
    core = match.group(1) if match else value
    core = re.sub(r"-0$", "", core.strip())
    return _PUNCTUATION.sub("", core)


def normalize_road_parent_core(value: str) -> str:
    """Return road name plus main building number, ignoring a sub-number."""
    value = _ADDRESS_PREFIX.sub("", str(value or "").strip().lower())
    value = _PARENTHETICAL.sub(" ", value)
    match = _ROAD_PARENT_CORE.search(value)
    return _PUNCTUATION.sub("", match.group(1)) if match else ""


def similar_address(left: str, right: str, *, minimum_ratio: float = 0.72) -> bool:
    """Allow formatting/detail differences, but require shared locality or number."""
    left_raw, right_raw = str(left or "").lower(), str(right or "").lower()
    left_norm, right_norm = normalize_address(left_raw), normalize_address(right_raw)
    if not left_norm or not right_norm:
        return False
    ratio = SequenceMatcher(None, left_norm, right_norm).ratio()
    if ratio < minimum_ratio:
        return False
    shared_locality = bool(set(_LOCALITY.findall(left_raw)) & set(_LOCALITY.findall(right_raw)))
    shared_number = bool(set(_NUMBER.findall(left_raw)) & set(_NUMBER.findall(right_raw)))
    return shared_locality or shared_number


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    p1, p2 = radians(lat1), radians(lat2)
    dp, dl = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 2 * radius * asin(sqrt(a))


def _float(value: object) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class MatchResult:
    result: str
    score: int
    reason: str
    candidate: dict | None = None


def score_candidate(poi: dict, candidate: dict, *, radius_m: float = 300.0) -> tuple[int, list[str]]:
    """Require a name match plus address or nearby coordinates; never name alone."""
    poi_name, other_name = normalize_name(poi.get("name", "")), normalize_name(candidate.get("name", ""))
    aliases=name_aliases(poi.get("name", "")) & name_aliases(candidate.get("name", ""))
    fuzzy_ratio = 0.0 if aliases else fuzzy_name_ratio(
        poi.get("name", ""), candidate.get("name", ""))
    if not poi_name or (not aliases and fuzzy_ratio < 0.88):
        return 0, []
    exact=poi_name==other_name
    if exact:
        score, reasons = 60, ["exact_normalized_name"]
    elif aliases:
        score, reasons = 55, ["name_alias_match"]
    else:
        score, reasons = 55, [f"fuzzy_name_{fuzzy_ratio:.2f}"]
    poi_addresses = {normalize_address(poi.get("road_address", "")), normalize_address(poi.get("jibun_address", ""))} - {""}
    other_addresses = {normalize_address(candidate.get("road_address", "")), normalize_address(candidate.get("jibun_address", "")), normalize_address(candidate.get("address", ""))} - {""}
    if poi_addresses & other_addresses:
        score += 40
        reasons.append("exact_normalized_address")
    else:
        poi_cores = {normalize_address_core(poi.get("road_address", "")),
                     normalize_address_core(poi.get("jibun_address", ""))} - {""}
        other_cores = {normalize_address_core(candidate.get("road_address", "")),
                       normalize_address_core(candidate.get("jibun_address", "")),
                       normalize_address_core(candidate.get("address", ""))} - {""}
        if poi_cores & other_cores:
            score += 35
            reasons.append("same_primary_address")
            return score, reasons
        poi_road_parents = {normalize_road_parent_core(poi.get("road_address", ""))} - {""}
        other_road_parents = {
            normalize_road_parent_core(candidate.get("road_address", "")),
            normalize_road_parent_core(candidate.get("address", "")),
        } - {""}
        if is_overlay_poi(poi) and poi_road_parents & other_road_parents:
            score += 35
            reasons.append("same_road_parent_number")
            return score, reasons
        if any(similar_address(left, right) for left in (
                poi.get("road_address", ""), poi.get("jibun_address", "")) for right in (
                candidate.get("road_address", ""), candidate.get("jibun_address", ""),
                candidate.get("address", ""))):
            score += 30
            reasons.append("similar_address_with_shared_locality_or_number")
            return score, reasons
        plat, plon = _float(poi.get("latitude")), _float(poi.get("longitude"))
        clat, clon = _float(candidate.get("latitude")), _float(candidate.get("longitude"))
        if None not in (plat, plon, clat, clon) and haversine_m(plat, plon, clat, clon) <= radius_m:
            score += 30
            reasons.append("nearby_coordinates")
    return score, reasons


def choose_match(poi: dict, candidates: list[dict], *, minimum_score: int = 90,
                 resolve_identity_ties: bool = True) -> MatchResult:
    ranked = []
    for candidate in candidates:
        score, reasons = score_candidate(poi, candidate)
        if score >= minimum_score:
            ranked.append((score, reasons, candidate))
    ranked.sort(key=lambda item: item[0], reverse=True)
    if not ranked:
        return MatchResult("not_found", 0, "no_name_and_location_match")
    if len(ranked) > 1 and ranked[0][0] == ranked[1][0]:
        top_score = ranked[0][0]
        tied = [item for item in ranked if item[0] == top_score]
        if not resolve_identity_ties:
            return MatchResult("ambiguous", top_score, "multiple_equal_candidates")

        # Prefer the candidate supported by the strongest identity evidence.
        # A portal can return a branch, cafe, pension and charger at the same
        # parcel with an identical numeric score; exact name evidence must win
        # over a generic-name alias or fuzzy name.
        def evidence_priority(item: tuple[int, list[str], dict]) -> tuple[int, int, int, int]:
            _, reasons, candidate = item
            exact_name = int("exact_normalized_name" in reasons)
            exact_address = int("exact_normalized_address" in reasons)
            primary_address = int("same_primary_address" in reasons)
            try:
                distance = int(str(candidate.get("distance", "")).strip())
            except (TypeError, ValueError):
                distance = 10**9
            return exact_name, exact_address, primary_address, -distance

        priorities = [(evidence_priority(item), item) for item in tied]
        priorities.sort(key=lambda pair: pair[0], reverse=True)
        if len(priorities) == 1 or priorities[0][0] > priorities[1][0]:
            _, (score, reasons, candidate) = priorities[0]
            return MatchResult("found", score,
                               "+".join(reasons) + "+tie_broken_by_identity_evidence",
                               candidate)

        # Kakao occasionally exposes duplicate place IDs for exactly the same
        # name and address.  The duplicate IDs do not make the POI identity
        # ambiguous, so retain one deterministic representative.
        signatures = {
            (normalize_name(item[2].get("name", "")),
             normalize_address_core(item[2].get("road_address", "") or
                                    item[2].get("jibun_address", "") or
                                    item[2].get("address", "")))
            for item in tied
        }
        if len(signatures) == 1 and next(iter(signatures))[0] and next(iter(signatures))[1]:
            score, reasons, candidate = priorities[0][1]
            return MatchResult("found", score,
                               "+".join(reasons) + "+duplicate_portal_records",
                               candidate)

        # For an exact-name tie, accept a unique nearby candidate only when it
        # is clearly separated from the alternatives.  This resolves branches
        # such as 39 m versus 884 m without merging adjacent facilities.
        exact_tied = [item for item in tied if "exact_normalized_name" in item[1]]
        distances = []
        for item in exact_tied:
            try:
                distances.append((int(str(item[2].get("distance", "")).strip()), item))
            except (TypeError, ValueError):
                pass
        distances.sort(key=lambda pair: pair[0])
        if len(distances) >= 2 and distances[0][0] <= 100 \
                and distances[1][0] - distances[0][0] >= 100:
            _, (score, reasons, candidate) = distances[0]
            return MatchResult("found", score,
                               "+".join(reasons) + "+tie_broken_by_nearest_exact_name",
                               candidate)
        return MatchResult("ambiguous", top_score, "multiple_equal_candidates")
    score, reasons, candidate = ranked[0]
    return MatchResult("found", score, "+".join(reasons), candidate)
