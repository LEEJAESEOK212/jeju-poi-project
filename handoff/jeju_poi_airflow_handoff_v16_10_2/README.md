# 제주 POI 운영상태 Airflow 인수인계

이 묶음은 기존 POI의 영업·운영·확인 필요·폐업 상태를 판정하는 Airflow DAG 배포본이다.

## 구성

- `airflow_deploy/`: DAG, 파이프라인 코드, 테스트, 배포 스크립트
- `docs/INPUT_OUTPUT.md`: 입력·출력과 상태 규칙
- `docs/RUNBOOK.md`: 새 컴퓨터 또는 새 Airflow 환경에서의 설치와 실행
- `docs/DATA_MANIFEST.md`: 별도로 전달해야 할 데이터 목록
- `env.example`: API 키 환경 변수 예시

## 현재 판정 기준

- 행정 원천의 명시적 폐업 근거가 있을 때만 `폐업`
- 카카오에서 동일 장소를 찾으면 `영업`
- 카카오 미발견은 `확인 필요`
- 행정 후보가 여러 개여서 동일 장소를 확정하지 못하면 `확인 필요`
- 고정시설·인프라는 변경 근거가 없으면 `운영`

## 생성 파일

실행 때마다 Airflow 출력 폴더에 다음 세 파일을 생성한다.

| 파일 | 용도 | 열 구성 |
|---|---|---|
| `제주_POI_서비스용_KEEP_current.csv` | 서비스 업로드 | 현재 입력 양식, 영업·운영 대상만 |
| `제주_POI_최종통합본_상태_current.csv` | 전체 상태 확인 | 현재 입력 양식 + `operating_status` |
| `제주_POI_판정보존_current.csv` | 판정 추적 | 8개 최소 보존 열 |

`제주_POI_판정보존_current.csv`의 열은 `place_id`, `name`, `road_address`,
`operating_status`, `status_reason_code`, `verification_state`, `lifecycle_type`,
`place_key`다.

배포와 실행 순서는 `docs/RUNBOOK.md`를 따른다.
