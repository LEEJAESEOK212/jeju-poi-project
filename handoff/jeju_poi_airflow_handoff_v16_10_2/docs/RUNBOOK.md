# 설치와 실행

## 사전 조건

- Linux 환경
- Python 3.12
- Apache Airflow 3.x
- Airflow 작업자에서 카카오 REST API 키를 환경 변수로 전달할 수 있는 설정

## 배포

```bash
cd /path/to/jeju_poi_airflow_handoff_v16_10_2/airflow_deploy
bash APPLY_V16_10_2.sh
```

기본 스크립트는 `/home/ai-010/airflow`와 `/home/ai-010/.venvs/jeju-airflow`를 사용한다. 다른 경로를 쓸 경우 스크립트 상단의 `AIRFLOW_HOME_DIR`, `PYTHON_BIN`, `AIRFLOW_BIN`을 해당 환경에 맞게 수정한다.

## Airflow 변수

묶음에 포함된 기준 데이터를 그대로 사용할 경우 아래 스크립트가 변수를 한 번에 등록한다.

```bash
AIRFLOW_BIN=/path/to/venv/bin/airflow bash SETUP_VARIABLES.sh /data/jeju_poi_work /data/jeju_poi_published
```

다른 데이터 버전을 사용할 경우 다음처럼 직접 등록한다.

```bash
airflow variables set JEJU_POI_CURRENT_CSV /data/jeju_poi_current.csv
airflow variables set JEJU_POI_MOIS_EVIDENCE_JSONL /data/mois_evidence.jsonl
airflow variables set JEJU_POI_KAKAO_EVIDENCE_JSONL /data/kakao_evidence.jsonl
airflow variables set JEJU_POI_WORK_DIR /data/jeju_poi_work
airflow variables set JEJU_POI_OUTPUT_DIR /data/jeju_poi_published
```

신규 납품을 합칠 때만 다음 변수를 추가한다.

```bash
airflow variables set JEJU_POI_NEW_CSV /data/jeju_poi_new.csv
```

## 실행

```bash
export KAKAO_REST_API_KEY='발급받은_키'
DAG_ID=jeju_poi_lifecycle_weekly_v10
RUN_ID="manual_$(date +%Y%m%d_%H%M%S)"

airflow dags unpause "$DAG_ID"
airflow dags trigger --run-id "$RUN_ID" "$DAG_ID"
sleep 30
airflow tasks states-for-dag-run "$DAG_ID" "$RUN_ID"
```

모든 작업이 `success`이면 출력 폴더의 세 CSV를 확인한다.
