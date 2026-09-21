import json
from pathlib import Path
import tempfile
import unittest

from orchestration.csv_io import read_csv, write_csv
from orchestration.lifecycle import (classify_lifecycle, decide_lifecycle,
                                     is_temporary_event, operating_status_label)
from orchestration.master_workflow import correct_obvious_category, prepare_master
from orchestration.incremental_preprocess import (duplicate_relation,
                                                  preprocess_incremental)
from orchestration.evidence import _consensus_tied_candidate
from orchestration.matching import choose_match
from orchestration.poi_workflow import (build_kakao_queue, build_kakao_queue_v10,
                                        baseline_status_decision, classify_existing_pois, decide_operating_status,
                                        effective_official_result, export_v10_csvs, finalize_pois_v10,
                                        validate_v10_outputs)

FIELDS = ["place_id", "name", "road_address", "jibun_address", "latitude", "longitude",
          "place_main_category", "content", "tags"]
BASE = {field: "" for field in FIELDS} | {"place_id": "p1", "name": "제주비치펜션",
    "road_address": "제주시 애월읍 애월해안로 384-12", "place_main_category": "숙박"}


class WorkflowTests(unittest.TestCase):
    def test_kakao_tie_prefers_exact_name_over_generic_alias(self):
        poi = BASE | {"name": "은빌레식당", "road_address": "서귀포시 남원읍 의귀로 120",
                      "jibun_address": "서귀포시 남원읍 의귀리 794 1층"}
        candidates = [
            {"source_id": "food", "name": "은빌레식당", "road_address": "",
             "jibun_address": "서귀포시 남원읍 의귀리 794", "distance": "0"},
            {"source_id": "cafe", "name": "은빌레카페", "road_address": "서귀포시 남원읍 의귀로 120",
             "jibun_address": "서귀포시 남원읍 의귀리 794", "distance": "20"},
        ]
        match = choose_match(poi, candidates)
        self.assertEqual(match.result, "found")
        self.assertEqual(match.candidate["source_id"], "food")
        self.assertIn("tie_broken_by_identity_evidence", match.reason)

    def test_kakao_duplicate_ids_at_same_name_and_address_are_one_identity(self):
        poi = BASE | {"name": "레이니데이", "road_address": "제주시 애월읍 중산간서로 5384"}
        candidates = [
            {"source_id": "k1", "name": "레이니데이", "road_address": poi["road_address"], "distance": "0"},
            {"source_id": "k2", "name": "레이니데이", "road_address": poi["road_address"], "distance": "0"},
        ]
        match = choose_match(poi, candidates)
        self.assertEqual(match.result, "found")
        self.assertIn("duplicate_portal_records", match.reason)

    def test_kakao_exact_name_tie_uses_clearly_nearest_candidate(self):
        poi = BASE | {"name": "조천수산", "road_address": "제주시 조천읍 조천북1길 35-8"}
        candidates = [
            {"source_id": "near", "name": "조천수산", "road_address": "제주시 조천읍 조천북1길 35-6", "distance": "39"},
            {"source_id": "far", "name": "조천수산", "road_address": "제주시 조천읍 조함해안로 142-10", "distance": "884"},
        ]
        match = choose_match(poi, candidates)
        self.assertEqual(match.result, "found")
        self.assertEqual(match.candidate["source_id"], "near")

    def test_official_same_identity_open_license_resolves_closed_license_tie(self):
        poi = BASE | {"name": "같은가게", "road_address": "제주시 중앙로 10"}
        candidates = [
            {"source_id": "old", "name": "같은가게", "road_address": poi["road_address"],
             "status_name": "폐업", "closed_at": "2023-01-01", "updated_at": "2026-01-01"},
            {"source_id": "active", "name": "같은가게", "road_address": poi["road_address"],
             "status_name": "영업", "permit_date": "2024-01-01", "updated_at": "2026-01-01"},
        ]
        candidate, result = _consensus_tied_candidate(poi, candidates, 100)
        self.assertEqual(result, "open")
        self.assertEqual(candidate["source_id"], "active")

    def test_official_same_name_different_addresses_stays_ambiguous(self):
        poi = BASE | {"name": "동명이점", "road_address": "제주시 중앙로 10"}
        candidates = [
            {"source_id": "a", "name": "동명이점", "road_address": "제주시 중앙로 10",
             "status_name": "영업"},
            {"source_id": "b", "name": "동명이점", "road_address": "제주시 중앙로 12",
             "status_name": "폐업"},
        ]
        candidate, result = _consensus_tied_candidate(poi, candidates, 100)
        self.assertIsNone(candidate)
        self.assertIsNone(result)

    def test_duplicate_complement_and_kakao_enrichment_are_separate(self):
        class FakeKakao:
            def search(self, row):
                return [{"source_id":"k1", "name":row["name"],
                         "road_address":row["road_address"], "jibun_address":"",
                         "latitude":"33.5000", "longitude":"126.5000"}]
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); existing=root/'existing.csv'; incoming=root/'incoming.csv'
            output=root/'output.csv'; audit=root/'audit.jsonl'
            old=BASE | {"place_id":"keep-id", "content":"기존 설명", "tags":"기존",
                        "latitude":"", "longitude":""}
            new=BASE | {"place_id":"new-id", "content":"더 자세한 신규 설명입니다", "tags":"신규",
                        "latitude":"", "longitude":""}
            write_csv(existing,FIELDS,[old]); write_csv(incoming,FIELDS,[new])
            result=preprocess_incremental(existing,incoming,output,audit,kakao_client=FakeKakao())
            _,rows,_=read_csv(output)
            self.assertEqual(result["merged"],1)
            self.assertEqual(len(rows),1)
            self.assertEqual(rows[0]["place_id"],"keep-id")
            self.assertEqual(rows[0]["content"],"더 자세한 신규 설명입니다")
            self.assertEqual(rows[0]["latitude"],"33.5000")
            record=json.loads(audit.read_text(encoding='utf-8'))
            self.assertEqual(record["complement_source"],"duplicate_row")
            self.assertIn("latitude",record["kakao_enriched_fields_before_match"])

    def test_name_address_conflicts_go_to_review(self):
        same_name_left=BASE | {"road_address":"제주시 한림로 1", "latitude":"33.1", "longitude":"126.1"}
        same_name_right=BASE | {"road_address":"제주시 한림로 2", "latitude":"33.1001", "longitude":"126.1001"}
        self.assertEqual(duplicate_relation(same_name_left,same_name_right)[0],"review")
        same_address_other=BASE | {"name":"다른 시설"}
        self.assertEqual(duplicate_relation(BASE,same_address_other)[0],"review")
    def test_existing_korean_status_is_preserved_without_new_evidence(self):
        expected={
            "영업":("KEEP","verified_active"),
            "폐업":("DEACTIVATE","verified_inactive"),
            "운영":("KEEP","verified_active"),
            "비운영":("DEACTIVATE","verified_inactive"),
            "확인 필요":("KEEP","unverified"),
            "운영 여부 확인 필요":("KEEP","unverified"),
        }
        for value,pair in expected.items():
            result=baseline_status_decision({"operating_status":value},"business_operation")
            self.assertEqual((result.decision,result.verification_state),pair)
    def test_full_place_tb_numeric_category_csv_is_accepted(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"full.csv"
            fields=["place_id","name","place_main_category_id","content","tags","operating_status"]
            write_csv(path,fields,[{"place_id":"p1","name":"전체파일","place_main_category_id":"1","content":"기존","tags":"","operating_status":"영업"}])
            actual_fields,rows,_=read_csv(path)
            self.assertIn("place_main_category_id",actual_fields)
            self.assertEqual(rows[0]["name"],"전체파일")
    def test_prepare_master_preserves_id_and_detects_changes(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); first=root/"first.csv"; master1=root/"master1.csv"; changes1=root/"changes1.jsonl"
            second=root/"second.csv"; master2=root/"master2.csv"; changes2=root/"changes2.jsonl"
            write_csv(first, FIELDS, [BASE])
            prepare_master(first, None, master1, changes1)
            first_master = list(__import__('csv').DictReader(master1.open(encoding='utf-8-sig')))[0]
            write_csv(second, FIELDS, [BASE | {"content":"설명 변경"}, BASE | {"place_id":"p2", "name":"신규"}])
            result = prepare_master(second, master1, master2, changes2)
            rows = list(__import__('csv').DictReader(master2.open(encoding='utf-8-sig')))
            self.assertEqual(rows[0]["pipeline_id"], first_master["pipeline_id"])
            self.assertEqual(result["changes"]["changed"], 1)
            self.assertEqual(result["changes"]["new"], 1)

    def test_duplicate_identity_id_is_deterministic_across_fresh_runs(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "source.csv"
            write_csv(source, FIELDS, [BASE, BASE])
            outputs = []
            for suffix in ("a", "b"):
                out = root / f"master_{suffix}.csv"
                prepare_master(source, None, out, root / f"changes_{suffix}.jsonl")
                _, rows, _ = read_csv(out)
                outputs.append([row["pipeline_id"] for row in rows])
            self.assertEqual(outputs[0], outputs[1])
            self.assertEqual(len(set(outputs[0])), 2)

    def test_obvious_convenience_and_atm_category_repairs_are_narrow(self):
        convenience = correct_obvious_category(BASE | {
            "name": "씨유제주삼주점", "place_main_category": "음식점",
            "place_sub_category": "기타"})
        self.assertEqual(
            (convenience["place_main_category_id"], convenience["place_sub_category_id"],
             convenience["place_main_category"], convenience["place_sub_category"]),
            ("7", "588", "편의점", "편의점"))
        atm = correct_obvious_category(BASE | {
            "name": "롯데ATM 제주은행 롯데호텔제주", "place_main_category": "숙박",
            "place_sub_category": "호텔"})
        self.assertEqual((atm["place_main_category_id"], atm["place_sub_category_id"],
                          atm["place_sub_category"]), ("20", "612", "ATM"))
        overlay = correct_obvious_category(BASE | {
            "name": "GS25 서광점 무료와이파이", "place_main_category": "공공편의시설",
            "place_sub_category": "무료와이파이"})
        self.assertEqual(overlay["place_sub_category"], "무료와이파이")
        mart = correct_obvious_category(BASE | {
            "name": "(주)이마트제주", "place_main_category": "음식점",
            "place_sub_category": "기타"})
        self.assertEqual((mart["place_main_category_id"], mart["place_sub_category_id"],
                          mart["place_main_category"]), ("8", "165", "대형마트"))

    def test_place_key_precedes_duplicate_place_id_when_preserving_ids(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); source = root / "source.csv"; previous = root / "previous.csv"
            out = root / "out.csv"; changes = root / "changes.jsonl"
            extra = ["pipeline_id", "place_key", "row_fingerprint"]
            first = BASE | {"place_id": "duplicate", "place_key": "key-1",
                            "pipeline_id": "stable-1", "row_fingerprint": "old"}
            second = BASE | {"place_id": "duplicate", "place_key": "key-2",
                             "pipeline_id": "stable-2", "row_fingerprint": "old"}
            write_csv(previous, FIELDS + extra, [first, second])
            write_csv(source, FIELDS + extra, [first, second])
            prepare_master(source, previous, out, changes)
            _, rows, _ = read_csv(out)
            self.assertEqual([row["pipeline_id"] for row in rows],
                             ["stable-1", "stable-2"])

    def test_first_master_preserves_source_place_id_as_evidence_key(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); source=root/'source.csv'; out=root/'master.csv'; changes=root/'changes.jsonl'
            write_csv(source,FIELDS,[BASE | {"place_id":"source-123"}])
            prepare_master(source,None,out,changes)
            _,rows,_=read_csv(out)
            self.assertEqual(rows[0]["place_key"],"source-123")
    def test_prepare_master_recomputes_stale_lifecycle(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); source=root/'source.csv'; out=root/'master.csv'; changes=root/'changes.jsonl'
            fields=FIELDS + ['place_main_category_id','place_sub_category','lifecycle_type']
            row=BASE | {'place_main_category_id':'2','place_sub_category':'카페',
                        'lifecycle_type':'fixed_public_natural'}
            write_csv(source,fields,[row])
            prepare_master(source,None,out,changes)
            _,rows,_=read_csv(out)
            self.assertEqual(rows[0]['lifecycle_type'],'business_operation')
    def test_prepare_master_preserves_id_after_address_correction_near_same_coordinates(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); source=root/'source.csv'; previous=root/'previous.csv'; out=root/'out.csv'; changes=root/'changes.jsonl'
            old=BASE | {'road_address':'제주시 애월읍 옛길 1','latitude':'33.47','longitude':'126.35','pipeline_id':'stable-id','place_key':'stable-id','row_fingerprint':'old'}
            write_csv(previous,FIELDS+['pipeline_id','place_key','row_fingerprint'],[old])
            write_csv(source,FIELDS,[BASE | {'road_address':'제주시 애월읍 새길 2','latitude':'33.4701','longitude':'126.3501'}])
            prepare_master(source,previous,out,changes)
            with out.open(encoding='utf-8-sig') as f:row=next(__import__('csv').DictReader(f))
            self.assertEqual(row['pipeline_id'],'stable-id')
    def test_lifecycle_types_and_temporary_event_guard(self):
        self.assertEqual(classify_lifecycle(BASE | {"place_main_category": "공공편의시설-공중화장실"}), "facility_infrastructure")
        self.assertEqual(classify_lifecycle(BASE | {"place_main_category": "공공편의시설-무료와이파이"}), "facility_infrastructure")
        self.assertEqual(classify_lifecycle(BASE | {"place_main_category": "주유소(충전소)-전기차충전소"}), "facility_infrastructure")
        self.assertFalse(is_temporary_event(BASE | {"name": "월정리대하전어축제", "place_main_category": "음식점"}))
        self.assertFalse(is_temporary_event(BASE | {"name": "기간행사", "place_main_category": "기간형행사"}))
        self.assertFalse(is_temporary_event(BASE | {"name": "도두오래물축제장", "place_main_category": "공공편의시설-무료와이파이"}))
        self.assertEqual(classify_lifecycle(BASE | {"place_main_category": "음식점", "lifecycle_type": "public_facility"}), "facility_infrastructure")

    def test_numeric_category_schema(self):
        numeric = {key: value for key, value in BASE.items() if key != "place_main_category"}
        self.assertEqual(classify_lifecycle(numeric | {"place_main_category_id": "1", "name": "식당"}),
                         "business_operation")
        self.assertEqual(classify_lifecycle(numeric | {"place_main_category_id": "10", "name": "공영주차장"}),
                         "facility_infrastructure")
        self.assertEqual(classify_lifecycle(numeric | {"place_main_category_id": "20", "name": "동문 공중화장실"}),
                         "facility_infrastructure")
        self.assertEqual(classify_lifecycle(numeric | {"place_main_category_id": "20", "name": "시청 무료와이파이"}),
                         "facility_infrastructure")
        self.assertEqual(classify_lifecycle(numeric | {"place_main_category_id": "9", "name": "제주시청 전기차충전소"}),
                         "facility_infrastructure")
        self.assertEqual(classify_lifecycle(numeric | {"place_main_category_id": "9", "name": "행복 LPG 충전소"}),
                         "business_operation")

    def test_broad_main_category_does_not_hide_protected_subfacility(self):
        ev = BASE | {"place_main_category":"주유소(충전소)",
                     "place_sub_category":"전기차충전소", "name":"시청 충전소"}
        restroom = BASE | {"place_main_category":"공공편의시설",
                           "place_sub_category":"공중화장실", "name":"시청 화장실"}
        wifi = BASE | {"place_main_category":"공공편의시설",
                      "place_sub_category":"무료와이파이", "name":"시청 Wi-Fi"}
        self.assertEqual(classify_lifecycle(ev),"facility_infrastructure")
        self.assertEqual(classify_lifecycle(restroom),"facility_infrastructure")
        self.assertEqual(classify_lifecycle(wifi),"facility_infrastructure")

    def test_subcategory_override_precedes_main_category(self):
        self.assertEqual(classify_lifecycle(BASE | {
            "place_main_category_id":"9", "place_sub_category":"전기차충전소",
            "name":"호텔 지하 충전기"}), "facility_infrastructure")
        self.assertEqual(classify_lifecycle(BASE | {
            "place_main_category_id":"9", "place_sub_category":"LPG충전소",
            "name":"행복 LPG"}), "business_operation")

    def test_facility_requires_facility_specific_official_closure(self):
        host_closed = decide_lifecycle(lifecycle_type="facility_infrastructure",
            temporary_event=False, official_result="closed", kakao_result=None)
        facility_closed = decide_lifecycle(lifecycle_type="facility_infrastructure",
            temporary_event=False, official_result="closed", kakao_result=None,
            official_scope="facility_registry")
        self.assertEqual(host_closed.decision, "KEEP")
        self.assertEqual((facility_closed.decision, facility_closed.reason_code),
                         ("DEACTIVATE", "facility_officially_closed"))
        business_registry_error = decide_lifecycle(
            lifecycle_type="fixed_public_natural", temporary_event=False,
            official_result="error", kakao_result=None)
        self.assertEqual((business_registry_error.decision,
                          business_registry_error.reason_code),
                         ("KEEP", "facility_no_change_evidence"))

    def test_same_address_different_function_is_not_duplicate(self):
        restaurant = BASE | {"place_main_category":"음식점"}
        charger = BASE | {"name":"건물 전기차충전소", "place_main_category":"주유소(충전소)"}
        self.assertEqual(duplicate_relation(restaurant, charger)[0], "separate")

    def test_normal_business_status_and_education_overrides(self):
        evidence = {"result":"ambiguous", "status_name":"정상영업", "match_score":100,
                    "match_reason":"exact_normalized_name+exact_normalized_address"}
        self.assertEqual(effective_official_result(evidence), "open")
        self.assertEqual(classify_lifecycle(BASE | {
            "place_main_category_id":"15", "place_sub_category":"초등학교"}),
            "fixed_public_natural")
        self.assertEqual(classify_lifecycle(BASE | {
            "place_main_category_id":"15", "place_sub_category":"체험학습장"}),
            "business_operation")
        self.assertEqual(classify_lifecycle(BASE | {
            "place_main_category_id":"15", "place_sub_category":"체험학습장",
            "name":"학생문화학교 체험장"}), "business_operation")
        self.assertEqual(classify_lifecycle(BASE | {
            "place_main_category_id":"15", "place_sub_category":"영화,영상"}),
            "business_operation")
        self.assertEqual(classify_lifecycle(BASE | {
            "place_main_category_id":"15", "place_sub_category":"영어학원",
            "name":"톡톡영어"}), "business_operation")
        self.assertEqual(classify_lifecycle(BASE | {
            "place_main_category_id":"20", "place_sub_category":"민방위대피시설",
            "name":"예일외국어학원 지하1층 민방위대피시설"}),
            "facility_infrastructure")
        self.assertEqual(classify_lifecycle(BASE | {
            "place_main_category_id":"2", "place_sub_category":"카페",
            "name":"해변 카페", "content":"무료 주차장과 공중화장실 이용 가능",
            "tags":"주차장,화장실"}), "business_operation")
        self.assertEqual(classify_lifecycle(BASE | {
            "place_main_category_id":"2", "place_sub_category":"커피전문점",
            "name":"스타벅스 제주해수욕장점"}), "business_operation")
        self.assertEqual(classify_lifecycle(BASE | {
            "place_main_category_id":"3", "place_sub_category":"펜션",
            "name":"오름숲 펜션"}), "business_operation")

    def test_lifecycle_decision_never_closes_on_map_absence(self):
        decision = decide_lifecycle(lifecycle_type="business_operation", temporary_event=False,
                                    official_result="not_found", kakao_result="not_found")
        self.assertEqual((decision.decision, decision.verification_state), ("REVIEW", "unverified"))
        public = decide_lifecycle(lifecycle_type="facility_infrastructure", temporary_event=False,
                                  official_result="not_found", kakao_result="found")
        self.assertEqual((public.decision, public.verification_state, public.reason_code),
                         ("KEEP", "unverified", "facility_no_change_evidence"))
        parking = decide_lifecycle(lifecycle_type="facility_infrastructure", temporary_event=False,
                                   official_result="not_found", kakao_result="not_found")
        self.assertEqual((parking.decision, parking.verification_state, parking.reason_code),
                         ("KEEP", "unverified", "facility_no_change_evidence"))
        charger = decide_lifecycle(lifecycle_type="facility_infrastructure", temporary_event=False,
                                   official_result="closed", kakao_result="not_found")
        self.assertEqual((charger.decision, charger.verification_state, charger.reason_code),
                         ("KEEP", "unverified", "facility_no_change_evidence"))
        fuel = decide_lifecycle(lifecycle_type="business_operation", temporary_event=False,
                                official_result="not_found", kakao_result="not_found")
        self.assertEqual((fuel.decision, fuel.verification_state, fuel.reason_code),
                         ("REVIEW", "unverified", "kakao_not_found_needs_review"))

    def test_fuel_station_closes_only_with_explicit_closure_evidence(self):
        official = decide_lifecycle(lifecycle_type="business_operation", temporary_event=False,
                                    official_result="closed", kakao_result=None)
        kakao = decide_lifecycle(lifecycle_type="business_operation", temporary_event=False,
                                 official_result="not_found", kakao_result="closed")
        self.assertEqual((official.decision, official.reason_code),
                         ("DEACTIVATE", "authoritative_source_closed"))
        self.assertEqual((kakao.decision, kakao.reason_code),
                         ("REVIEW", "kakao_not_found_needs_review"))

    def test_explicit_closure_from_official_or_kakao_confirms_closed(self):
        self.assertEqual(decide_operating_status(official_result="closed", kakao_result="found"),
                         ("closed", "official_closed"))
        self.assertEqual(decide_operating_status(official_result="not_found", kakao_result="closed"),
                         ("needs_review", "kakao_closure_requires_official_confirmation"))
        self.assertEqual(decide_operating_status(official_result="open", kakao_result="closed"),
                         ("open", "official_open"))
        self.assertEqual(decide_operating_status(official_result="open", kakao_result="not_found"),
                         ("open", "official_open"))
        decision = decide_lifecycle(lifecycle_type="business_operation", temporary_event=False,
                                    official_result="not_found", kakao_result="closed")
        self.assertEqual((decision.decision, decision.verification_state, decision.reason_code),
                         ("REVIEW", "unverified", "kakao_not_found_needs_review"))

    def test_kakao_found_resolves_ambiguous_official_match(self):
        decision = decide_lifecycle(lifecycle_type="business_operation", temporary_event=False,
                                    official_result="ambiguous", kakao_result="found")
        self.assertEqual((decision.decision, decision.verification_state, decision.reason_code),
                         ("KEEP", "verified_active", "kakao_same_place_found"))

    def test_official_open_is_final_and_only_not_found_uses_kakao(self):
        self.assertEqual(decide_operating_status(official_result="open", kakao_result="found"),
                         ("open", "official_open"))
        self.assertEqual(decide_operating_status(official_result="open", kakao_result="not_found"),
                         ("open", "official_open"))
        self.assertEqual(decide_operating_status(official_result="not_found", kakao_result="found"),
                         ("open", "kakao_same_place_found"))
        self.assertEqual(decide_operating_status(official_result="error", kakao_result="found"),
                         ("needs_review", "official_check_failed"))

    def test_queue_skips_officially_closed_only(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); poi = root/"poi.csv"; official = root/"official.jsonl"; out = root/"queue.jsonl"
            write_csv(poi, FIELDS, [BASE, BASE | {"place_id": "p2", "name": "다른 장소"}])
            official.write_text('\n'.join([json.dumps({"place_key":"p1","result":"closed"}),
                json.dumps({"place_key":"p2","result":"not_found"})]), encoding="utf-8")
            self.assertEqual(build_kakao_queue(poi, official, out)["queued"], 1)
            self.assertEqual(json.loads(out.read_text(encoding="utf-8"))["place_key"], "p2")

    def test_classification_retains_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); poi=root/"poi.csv"; official=root/"official.jsonl"; kakao=root/"kakao.jsonl"; out=root/"out.jsonl"
            write_csv(poi, FIELDS, [BASE])
            official.write_text(json.dumps({"place_key":"p1","result":"open","source":"MOIS"})+'\n', encoding="utf-8")
            kakao.write_text(json.dumps({"place_key":"p1","result":"found","kakao_place_id":"123"})+'\n', encoding="utf-8")
            summary = classify_existing_pois(poi, official, kakao, out)
            decision = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(summary["open"], 1)
            self.assertEqual(decision["evidence"]["kakao"]["kakao_place_id"], "123")

    def test_v10_filters_only_temporary_event_and_keeps_unverified_public_place(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); poi=root/"poi.csv"; official=root/"official.jsonl"; kakao=root/"kakao.jsonl"
            out=root/"v10.jsonl"; filtered=root/"filtered.csv"; queue=root/"queue.jsonl"
            festival = BASE | {"place_id":"event", "name":"기간행사", "place_main_category":"기간형행사"}
            restroom = BASE | {"place_id":"restroom", "name":"새별오름 들불축제장 공중화장실", "place_main_category":"공공편의시설-공중화장실"}
            shop = BASE | {"place_id":"shop", "name":"지도없는가게", "place_main_category":"카페"}
            write_csv(poi, FIELDS, [festival, restroom, shop])
            official.write_text("\n".join(json.dumps({"place_key": key, "result":"not_found"}) for key in ("event","restroom","shop")), encoding="utf-8")
            kakao.write_text(json.dumps({"place_key":"shop", "result":"not_found"})+"\n", encoding="utf-8")
            q = build_kakao_queue_v10(poi, official, queue)
            self.assertEqual(q["queued"], 1)
            result = finalize_pois_v10(poi, official, kakao, out, filtered)
            decisions = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([d["decision"] for d in decisions], ["REVIEW", "KEEP", "REVIEW"])

    def test_v10_queue_includes_unresolved_official_candidates(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); poi=root/"poi.csv"; official=root/"official.jsonl"; queue=root/"queue.jsonl"
            rows=[BASE | {"place_id":"amb"}, BASE | {"place_id":"err"},
                  BASE | {"place_id":"open"}, BASE | {"place_id":"closed"}]
            write_csv(poi,FIELDS,rows)
            official.write_text("\n".join([
                json.dumps({"place_key":"amb","result":"ambiguous"}),
                json.dumps({"place_key":"err","result":"error"}),
                json.dumps({"place_key":"open","result":"open"}),
                json.dumps({"place_key":"closed","result":"closed"}),
            ]),encoding="utf-8")
            result=build_kakao_queue_v10(poi,official,queue)
            self.assertEqual(result["queued"],2)
            keys={json.loads(line)["place_key"] for line in queue.read_text(encoding="utf-8").splitlines()}
            self.assertEqual(keys,{"amb","err"})

    def test_v10_exports_and_quality_gate(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); poi=root/"poi.csv"; official=root/"official.jsonl"; kakao=root/"kakao.jsonl"
            decisions=root/"decisions.jsonl"; filtered=root/"filtered.csv"; exports=root/"exports"
            charger = BASE | {"place_id":"charger", "name":"바뀐가게", "place_main_category":"주유소(충전소)-전기차충전소"}
            closed = BASE | {"place_id":"closed", "name":"폐업숙소", "place_main_category":"숙박"}
            event = BASE | {"place_id":"event", "name":"기간행사", "place_main_category":"기간형행사"}
            write_csv(poi, FIELDS, [charger, closed, event])
            official.write_text("\n".join([
                json.dumps({"place_key":"charger", "result":"closed"}),
                json.dumps({"place_key":"closed", "result":"closed"}),
                json.dumps({"place_key":"event", "result":"not_found"}),
            ]), encoding="utf-8")
            kakao.write_text("", encoding="utf-8")
            finalize_pois_v10(poi, official, kakao, decisions, filtered)
            result = export_v10_csvs(poi, decisions, exports)
            checked = validate_v10_outputs(poi, decisions, result)
            self.assertEqual((result["preservation_rows"], result["service_rows"]), (3, 1))
            self.assertEqual(checked["infrastructure"], 1)
            service_fields, service_rows, _ = read_csv(result["service_csv"])
            status_fields, status_rows, _ = read_csv(result["status_csv"])
            self.assertEqual(service_fields, FIELDS)
            self.assertEqual(len(service_rows), 1)
            self.assertEqual(status_fields, FIELDS + ["operating_status"])
            self.assertEqual([row["operating_status"] for row in status_rows],
                             ["운영", "폐업", "확인 필요"])
            with Path(result["preservation_csv"]).open(encoding="utf-8-sig", newline="") as stream:
                reader = __import__('csv').DictReader(stream)
                preservation_fields = reader.fieldnames
                rows = list(reader)
            self.assertEqual(preservation_fields, [
                "place_id", "name", "road_address", "operating_status",
                "status_reason_code", "verification_state", "lifecycle_type", "place_key",
            ])
            self.assertEqual([row["operating_status"] for row in rows],
                             ["운영", "폐업", "확인 필요"])
            self.assertNotIn("operation_decision", rows[0])

    def test_korean_operating_status_labels(self):
        self.assertEqual(operating_status_label("business_operation", "KEEP", "verified_active"), "영업")
        self.assertEqual(operating_status_label("business_operation", "DEACTIVATE", "verified_inactive"), "폐업")
        self.assertEqual(operating_status_label("public_facility", "KEEP", "verified_active"), "운영")
        self.assertEqual(operating_status_label("public_facility", "DEACTIVATE", "verified_inactive"), "비운영")
        self.assertEqual(operating_status_label("natural_space", "KEEP", "not_applicable"), "운영")
        self.assertEqual(operating_status_label("fixed_public_natural", "KEEP", "unverified"), "운영")
        self.assertEqual(operating_status_label("facility_infrastructure", "KEEP", "unverified"), "운영")
        self.assertEqual(operating_status_label("business_operation", "KEEP", "unverified"),
                         "확인 필요")


if __name__ == "__main__": unittest.main()
