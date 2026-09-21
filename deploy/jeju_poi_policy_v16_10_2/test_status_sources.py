import json
from pathlib import Path
import tempfile
import unittest

from orchestration.csv_io import write_csv
from orchestration.api_clients import ApiError, JsonApiClient, KakaoLocalClient, PagedPublicDataClient, validate_public_data_response
from orchestration.evidence import (build_kakao_evidence, build_kakao_evidence_resumable,
                                    build_mois_evidence, latest_mois_records,
                                    normalize_mois_record)
from orchestration.matching import (choose_match, normalize_address, normalize_address_core,
                                    industry_compatible, name_aliases, normalize_name,
                                    normalize_road_parent_core, search_name_variants,
                                    similar_address)
from orchestration.poi_workflow import build_kakao_queue, classify_existing_pois


FIELDS = ["place_id", "name", "road_address", "jibun_address", "latitude", "longitude", "place_main_category", "content", "tags"]
POIS = [
    {"place_id":"p1", "name":"열린식당", "road_address":"제주특별자치도 제주시 중앙로 1", "jibun_address":"", "latitude":"33.50", "longitude":"126.50", "place_main_category":"음식점", "content":"", "tags":""},
    {"place_id":"p2", "name":"행안부카페", "road_address":"제주특별자치도 제주시 중앙로 2", "jibun_address":"", "latitude":"33.50", "longitude":"126.50", "place_main_category":"카페", "content":"", "tags":""},
    {"place_id":"p3", "name":"지도숙소", "road_address":"제주특별자치도 제주시 중앙로 3", "jibun_address":"", "latitude":"33.50", "longitude":"126.50", "place_main_category":"숙박", "content":"", "tags":""},
]


class FakeKakao:
    def search(self, poi):
        return [{"source_id":"k3", "name":"지도숙소", "road_address":"제주시 중앙로 3", "latitude":"33.50", "longitude":"126.50", "place_url":"https://place.map.kakao.com/k3"}]


class SourceTests(unittest.TestCase):
    def test_name_aliases_are_conservative_and_location_is_still_required(self):
        self.assertTrue(name_aliases("제주 재석") & name_aliases("재석"))
        self.assertTrue(name_aliases("민박 재석") & name_aliases("재석 민박"))
        self.assertTrue(name_aliases("플렌A") & name_aliases("플렌에이"))
        poi = {"name": "민박 재석", "road_address": "제주시 애월로 10"}
        self.assertEqual(choose_match(poi, [{"name": "재석 민박", "road_address": "제주시 애월로 10"}]).result,
                         "found")
        self.assertEqual(choose_match(poi, [{"name": "재석", "road_address": "서귀포시 태평로 20"}]).result,
                         "not_found")

    def test_convenience_store_portal_aliases_match_only_with_location(self):
        cases = [
            ("씨유제주동부관광점", "CU 제주동부관광점"),
            ("지에스25제주보성점", "GS25 제주보성점"),
        ]
        for left, right in cases:
            with self.subTest(left=left, right=right):
                poi = {"name": left, "road_address": "제주시 중앙로 1"}
                self.assertEqual(choose_match(
                    poi, [{"name": right, "road_address": "제주시 중앙로 1"}]
                ).result, "found")
                self.assertEqual(choose_match(
                    poi, [{"name": right, "road_address": "제주시 중앙로 2"}]
                ).result, "not_found")
        self.assertEqual(choose_match(
            {"name": "세븐일레븐 제주세화점", "road_address": "제주시 세화2길 11"},
            [{"name": "롯데ATM 세븐일레븐 제주세화", "road_address": "제주시 세화2길 11"}],
        ).result, "not_found")
        self.assertEqual(choose_match(
            {"name": "CU 제주교래점", "road_address": "제주시 비자림로 684"},
            [{"name": "CU ATM 제주교래점", "road_address": "제주시 비자림로 684"}],
        ).result, "not_found")
        variants = search_name_variants("CU 제주교래점")
        self.assertIn("씨유 제주교래", variants)

    def test_safe_fuzzy_name_requires_same_place_and_rejects_ancillary_facility(self):
        poi = {"name": "모래비 커피로스터스 앤 베이커리",
               "road_address": "제주시 해맞이해안로 462"}
        candidate = {"name": "모래비 커피로스터스 & 베이커리",
                     "road_address": "제주시 해맞이해안로 462"}
        self.assertEqual(choose_match(poi, [candidate]).result, "found")
        self.assertEqual(choose_match(
            poi, [candidate | {"road_address": "제주시 태평로 1"}]
        ).result, "not_found")
        self.assertEqual(choose_match(
            {"name": "CU 제주교래점", "road_address": "제주시 비자림로 684"},
            [{"name": "CU ATM 제주교래점", "road_address": "제주시 비자림로 684"}],
        ).result, "not_found")
        self.assertEqual(choose_match(
            {"name": "렛츠런파크제주", "road_address": "제주시 평화로 2144"},
            [{"name": "렛츠런파크제주 주차장", "road_address": "제주시 평화로 2144"}],
        ).result, "not_found")

    def test_legal_form_is_removed_anywhere_and_used_as_search_variant(self):
        self.assertEqual(normalize_name("(주)제주카카오렌트카"),
                         normalize_name("제주카카오렌트카"))
        self.assertIn("제주카카오렌트카",
                      search_name_variants("(주)제주카카오렌트카"))

    def test_normalization_and_conservative_matching(self):
        self.assertEqual(normalize_name(" 열린-식당 "), "열린식당")
        self.assertEqual(normalize_address("제주특별자치도 제주시 중앙로 1"), "제주시중앙로1")
        self.assertEqual(choose_match(POIS[0], [{"name":"열린식당", "road_address":"제주시 중앙로 1"}]).result, "found")
        self.assertEqual(choose_match(POIS[0], [{"name":"열린식당", "road_address":"서울시 중앙로 1"}]).result, "not_found")
        self.assertEqual(normalize_address_core("제주시 애월읍 천덕로 264-9, 3동"),
                         "제주시애월읍천덕로2649")
        self.assertEqual(choose_match(
            {"name": "본태박물관", "road_address": "제주시 안덕면 산록남로 762번길 69, 2층"},
            [{"name": "본태박물관", "road_address": "제주시 안덕면 산록남로762번길 69"}]
        ).result, "found")
        self.assertTrue(similar_address("제주시 애월읍 천덕로 264-9, 3동",
                                        "제주시 애월읍 천덕로264-9"))
        self.assertFalse(similar_address("제주시 애월읍 천덕로 264-9",
                                         "서귀포시 성산읍 해맞이해안로 10"))
        self.assertEqual(normalize_road_parent_core("제주시 애월읍 고내북서길 15-1"),
                         "제주시애월읍고내북서길15")
        self.assertEqual(choose_match(
            {"name": "고내다운", "road_address": "제주시 애월읍 고내북서길 15",
             "place_main_category": "주유소(충전소)-전기차충전소"},
            [{"name": "고내다운", "road_address": "제주시 애월읍 고내북서길 15-1"}],
            minimum_score=95,
        ).result, "found")
        self.assertEqual(choose_match(
            {"name": "해오름정원", "road_address": "제주시 상하귀길 98-1",
             "place_main_category": "숙박"},
            [{"name": "해오름정원", "road_address": "제주시 상하귀길 98-7"}],
            minimum_score=95,
        ).result, "not_found")
        self.assertEqual(choose_match(
            {"name": "달오름", "road_address": "서귀포시 서문로28번길 1"},
            [{"name": "달오름", "road_address": "서귀포시 태평로 449"}],
            minimum_score=95,
        ).result, "not_found")

    def test_api_retry_and_pagination(self):
        calls = []
        def transport(url, headers, timeout):
            calls.append(url)
            if len(calls) == 1: return 503, b"busy"
            page = 1 if "pageNo=1" in url else 2
            body = {"response":{"body":{"items":{"item":[{"x":page}] if page == 1 else []}}}}
            return 200, json.dumps(body).encode()
        http = JsonApiClient(retries=1, transport=transport, sleep=lambda _: None)
        client = PagedPublicDataClient("https://example.test/api", "key", http=http)
        self.assertEqual(list(client.pages(page_size=1)), [[{"x":1}]])
        self.assertEqual(len(calls), 3)

    def test_encoded_service_key_is_not_double_encoded(self):
        seen = []
        def transport(url, headers, timeout):
            seen.append(url)
            return 200, b'{"response":{"body":{"items":[]}}}'
        client = PagedPublicDataClient("https://example.test/info", "abc%2Bdef%3D",
            http=JsonApiClient(transport=transport))
        list(client.pages(page_size=100))
        self.assertIn("serviceKey=abc%2Bdef%3D", seen[0])
        self.assertNotIn("%252B", seen[0])

    def test_public_data_error_is_not_treated_as_empty_result(self):
        with self.assertRaises(ApiError):
            validate_public_data_response({"response":{"header":{"resultCode":"-4", "resultMsg":"등록되지 않은 인증키"}}})

    def test_mois_history_uses_latest_record_per_management_id(self):
        raw = [
            {"MGTNO":"m1", "BPLCNM":"가게", "RDNWHLADDR":"제주시 중앙로 1", "TRDSTATENM":"폐업", "LASTMODTS":"20240101"},
            {"MGTNO":"m1", "BPLCNM":"가게", "RDNWHLADDR":"제주시 중앙로 1", "TRDSTATENM":"영업/정상", "LASTMODTS":"20250101"},
        ]
        latest = latest_mois_records([normalize_mois_record(item) for item in raw])
        self.assertEqual(len(latest), 1)
        self.assertEqual(latest[0]["status_name"], "영업/정상")

    def test_current_mois_field_names_are_normalized(self):
        item = normalize_mois_record({"_mois_source_name":"일반음식점", "_mois_source":"general_restaurants",
            "MNG_NO":"m1", "BPLC_NM":"가게",
            "ROAD_NM_ADDR":"제주시 중앙로 1", "LOTNO_ADDR":"제주시 일도동 1",
            "DTL_SALS_STTS_NM":"영업/정상", "LCPMT_YMD":"20250101",
            "CLSBIZ_YMD":"", "DAT_UPDT_PNT":"20260831010101"})
        self.assertEqual(item["source_id"], "m1")
        self.assertEqual(item["name"], "가게")
        self.assertEqual(item["status_name"], "영업/정상")
        self.assertEqual(item["source_dataset"], "일반음식점")

    def test_same_name_and_address_cannot_cross_official_industries(self):
        poi = {"name": "제주대학교 병원", "road_address": "제주시 아란13길 15",
               "place_main_category": "병원"}
        restaurant = {"name": "제주대학교병원", "road_address": "제주시 아란13길 15",
                      "source_dataset": "행정안전부_식품_일반음식점 조회서비스"}
        hospital = {"name": "제주대학교병원", "road_address": "제주시 아란13길 15",
                    "source_dataset": "행정안전부_건강_병원 조회서비스"}
        self.assertFalse(industry_compatible(poi, restaurant))
        self.assertTrue(industry_compatible(poi, hospital))

    def test_embedded_business_closure_does_not_close_parent_poi(self):
        cases = [
            ("주유소(충전소)-전기차충전소", "행정안전부 _식품_일반음식점 조회서비스"),
            ("편의점", "행정안전부 _건강_안전상비의약품 판매업소 조회서비스"),
            ("박물관/전시장", "행정안전부 _식품_휴게음식점 조회서비스"),
            ("병원-동물병원", "행정안전부 _동물_동물위탁관리업 조회서비스"),
        ]
        for category, source in cases:
            with self.subTest(category=category, source=source):
                self.assertFalse(industry_compatible(
                    {"place_main_category": category}, {"source_dataset": source}))

    def test_overlay_uses_host_closure_but_not_ancillary_closure(self):
        overlay = "주유소(충전소)-전기차충전소"
        self.assertTrue(industry_compatible(
            {"name": "고내다운", "place_main_category": overlay},
            {"source_dataset": "행정안전부 _문화_농어촌민박업 조회서비스"}))
        self.assertFalse(industry_compatible(
            {"name": "제주청소년수련원", "place_main_category": overlay},
            {"source_dataset": "행정안전부 _식품_일반음식점 조회서비스"}))
        self.assertFalse(industry_compatible(
            {"name": "제주대학교 병원", "place_main_category": overlay},
            {"source_dataset": "행정안전부 _식품_일반음식점 조회서비스"}))
        self.assertTrue(industry_compatible(
            {"name": "제주시민회관", "place_main_category": "공공편의시설-무료와이파이"},
            {"source_dataset": "행정안전부 _문화_공연장 조회서비스"}))

    def test_food_licence_does_not_close_obvious_non_food_host(self):
        food = {"source_dataset": "행정안전부 _식품_휴게음식점 조회서비스"}
        self.assertFalse(industry_compatible(
            {"name": "표선PC방", "place_main_category": "음식점"}, food))
        self.assertFalse(industry_compatible(
            {"name": "제주청소년수련원", "place_main_category": "음식점"}, food))

    def test_fuzzy_address_is_not_an_official_match_and_goes_to_kakao(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); poi_path = root / "poi.csv"; output = root / "evidence.jsonl"
            poi = {"place_id": "p1", "name": "월드카페", "road_address": "제주시 한림북동길 8-1",
                   "jibun_address": "", "latitude": "", "longitude": "",
                   "place_main_category": "카페", "content": "", "tags": ""}
            write_csv(poi_path, FIELDS, [poi])
            build_mois_evidence(poi_path, [{"_mois_source_name": "행정안전부 _식품_일반음식점 조회서비스",
                "BPLC_NM": "월드카페", "ROAD_NM_ADDR": "제주시 한림북동길 26, 1층",
                "DTL_SALS_STTS_NM": "폐업", "CLSBIZ_YMD": "20250101"}], output)
            row = json.loads(output.read_text(encoding="utf-8").strip())
            self.assertEqual(row["result"], "not_found")
            self.assertEqual(row["match_reason"], "address_mismatch")
            queue = root / "queue.jsonl"
            self.assertEqual(build_kakao_queue(poi_path, output, queue)["queued"], 1)

    def test_cross_industry_candidate_is_reported_not_found(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); poi_path = root / "poi.csv"; output = root / "evidence.jsonl"
            poi = {"place_id": "hospital1", "name": "제주대학교 병원",
                   "road_address": "제주시 아란13길 15", "jibun_address": "",
                   "latitude": "", "longitude": "", "place_main_category": "병원",
                   "content": "", "tags": ""}
            write_csv(poi_path, FIELDS, [poi])
            build_mois_evidence(poi_path, [{"_mois_source_name": "행정안전부_식품_일반음식점 조회서비스",
                "BPLC_NM": "제주대학교병원", "ROAD_NM_ADDR": "제주시 아란13길 15",
                "DTL_SALS_STTS_NM": "폐업", "CLSBIZ_YMD": "20260526"}], output)
            row = json.loads(output.read_text(encoding="utf-8").strip())
            self.assertEqual(row["result"], "not_found")
            self.assertEqual(row["match_reason"], "industry_mismatch")

    def test_public_restroom_v2_fields_are_normalized(self):
        item = normalize_mois_record({"MNG_NO": "r1", "RSTRM_NM": "제주화장실",
            "LCTN_ROAD_NM_ADDR": "제주시 중앙로 1", "LCTN_LOTNO_ADDR": "제주시 일도동 1"})
        self.assertEqual(item["name"], "제주화장실")
        self.assertEqual(item["road_address"], "제주시 중앙로 1")

    def test_full_source_fallback_flow(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); poi=root/"poi.csv"; mois=root/"mois.jsonl"
            queue=root/"queue.jsonl"; kakao=root/"kakao.jsonl"; out=root/"out.jsonl"
            write_csv(poi, FIELDS, POIS)
            build_mois_evidence(poi, [
                {"_mois_source_name":"행정안전부_식품_일반음식점 조회서비스",
                 "사업장명":"열린식당", "도로명주소":"제주시 중앙로 1", "영업상태명":"영업/정상"},
                {"_mois_source_name":"행정안전부_식품_휴게음식점 조회서비스",
                 "사업장명":"행안부카페", "도로명주소":"제주시 중앙로 2", "영업상태명":"영업/정상"}], mois)
            self.assertEqual(build_kakao_queue(poi, mois, queue)["queued"], 1)
            build_kakao_evidence(queue, kakao, FakeKakao())
            result = classify_existing_pois(poi, mois, kakao, out)
            self.assertEqual(result["open"], 3)
            decisions = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([item["reason_code"] for item in decisions],
                             ["official_open", "official_open", "kakao_same_place_found"])

    def test_duplicate_blank_id_rows_get_distinct_queue_keys(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); poi = root / "poi.csv"; mois = root / "mois.jsonl"
            queue = root / "queue.jsonl"
            duplicate = {**POIS[0], "place_id": "", "name": "중복와이파이",
                         "place_main_category": "공공편의시설-무료와이파이"}
            write_csv(poi, FIELDS, [duplicate, duplicate])
            build_mois_evidence(poi, [], mois)
            result = build_kakao_queue(poi, mois, queue)
            queued = [json.loads(line) for line in queue.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(result["queued"], 2)
            self.assertNotEqual(queued[0]["place_key"], queued[1]["place_key"])

    def test_kakao_auth_and_mapping(self):
        seen = {}; urls = []
        def transport(url, headers, timeout):
            seen.update(headers)
            urls.append(url)
            payload={"documents":[{"id":"1","place_name":"열린식당","road_address_name":"제주시 중앙로 1","address_name":"","x":"126.5","y":"33.5"}]}
            return 200, json.dumps(payload).encode()
        client=KakaoLocalClient("secret", http=JsonApiClient(transport=transport))
        self.assertEqual(client.search(POIS[0])[0]["source_id"], "1")
        self.assertEqual(seen["Authorization"], "KakaoAK secret")
        self.assertIn("query=%EC%97%B4%EB%A6%B0%EC%8B%9D%EB%8B%B9", urls[0])
        self.assertNotIn("%EC%A4%91%EC%95%99%EB%A1%9C", urls[0])

    def test_resumable_kakao_skips_completed_rows(self):
        class InterruptingKakao:
            def __init__(self): self.calls = 0
            def search(self, poi):
                self.calls += 1
                if self.calls == 2: raise KeyboardInterrupt()
                return []
        class EmptyKakao:
            def search(self, poi): return []
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); queue = root / "queue.jsonl"; output = root / "evidence.jsonl"
            queue.write_text("\n".join(json.dumps({"place_key": key, "name": key})
                                       for key in ("a", "b")), encoding="utf-8")
            with self.assertRaises(KeyboardInterrupt):
                build_kakao_evidence_resumable(queue, output, InterruptingKakao(), progress_every=0)
            result = build_kakao_evidence_resumable(queue, output, EmptyKakao(), progress_every=0)
            rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(result["previously_completed"], 1)
            self.assertEqual(result["processed"], 1)
            self.assertEqual({row["place_key"] for row in rows}, {"a", "b"})

    def test_resumable_kakao_four_workers_writes_each_key_once(self):
        class EmptyKakao:
            def search(self, poi): return []
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); queue = root / "queue.jsonl"; output = root / "evidence.jsonl"
            queue.write_text("\n".join(json.dumps({"place_key": str(i), "name": str(i)})
                                        for i in range(20)), encoding="utf-8")
            result = build_kakao_evidence_resumable(
                queue, output, EmptyKakao(), progress_every=0, workers=4)
            rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(result["workers"], 4)
            self.assertEqual(result["processed"], 20)
            self.assertEqual(len({row["place_key"] for row in rows}), 20)


if __name__ == "__main__": unittest.main()
