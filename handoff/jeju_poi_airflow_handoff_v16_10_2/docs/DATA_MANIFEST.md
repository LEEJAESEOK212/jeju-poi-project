# 별도 전달 데이터

이 묶음의 `data/` 폴더에는 현재 실행 기준의 원본과 증거 파일을 포함한다. API 키는 포함하지 않는다.

| 파일 | 설명 |
|---|---|
| `data/jeju_poi_master_v5.csv` | `JEJU_POI_CURRENT_CSV`로 지정할 현재 POI 원본 |
| `data/mois_evidence_v16_3.jsonl` | 행안부·공공 원천 증거 |
| `data/kakao_evidence_merged_v16_9.jsonl` | 기존 카카오 검색 결과 재사용용 |
| 카카오 REST API 키 | 증거가 없거나 재검증이 필요할 때 사용 |

새 실행의 입력 원본이나 증거 파일을 교체하면 결과 건수도 달라질 수 있다.
