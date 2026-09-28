# 네이버 API 기반 제주 장소 자료 수집

2026-09-28 로컬 수집기 스냅샷. Python 3.10 이상, Linux 터미널 기준이다. Python 표준 라이브러리로 실행한다. 선택적으로 설치된 truststore가 있으면 인증서 처리에 사용한다.

## 처리 흐름

장소명·지역·업종별 연관어 → API HUB local/webkr/blog 검색 → 관련 링크 우선순위 → 접근 가능한 본문 최대 6개 → 상호·주소 근접 일치 표시 → SQLite 체크포인트와 근거 파일.

`collect.py`는 수집만 한다. 콘텐츠·태그 생성이나 통합본 반영은 수행하지 않는다. `jeju_free.py`, `jeju_enrich.py`는 기존 공통 모듈 의존성 때문에 함께 보존했다. 이 모듈의 별도 실행 기능은 이 수집 명령에서 호출되지 않으며 모델 API 키도 필요하지 않다.

API 주소는 코드의 BASE, 인증 헤더는 API._search에서 확인할 수 있다. 환경 변수는 `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`이다. 저장된 환경 변수가 있으면 재입력하지 않는다. 없으면 터미널에서 입력을 받으며 비밀키는 화면에 표시하지 않는다. 키 파일과 실제 대상 CSV는 공개 저장소에 포함하지 않는다.

## 실행

기존 서버의 scraper 폴더에서 사용 중인 `naver_api_research_v2`와 이 공개용 폴더는 같은 수집 코드 스냅샷이다. 실행 중인 작업에 파일을 덮어쓰지 않는다. 완료분은 기존 output의 체크포인트에 보존된다.

```bash
cd pipelines/naver_research
python -u collect.py --input /path/to/targets.csv --output /path/to/result_v22 --workers 4
```

입력은 UTF-8 BOM 허용 CSV이며 `merge_key`, `place_id`, `name`, `place_sub_category`, `road_address`, `jibun_address`를 사용한다. merge_key는 유일해야 한다. 주소가 부족하면 동일 장소 검증이 제한된다.

- workers: 1~6, 기본 3. 스크린샷만으로 실제 실행 workers를 확인할 수 없다.
- API 전체 요청 시작 간격: 0.25초 이상. 공식 한도 보장값이 아닌 수집기의 설정값이다.
- api-budget: 실행당 요청 시도 최대 30,000회. 재시도도 차감한다. 실제 계약의 할당량에 맞춰 더 낮게 설정할 수 있다.
- max-pages: 장소당 본문 조회 시도 최대 6개. limit으로 일부만 시험할 수 있다.
- 같은 입력·설정·output으로 재실행하면 완료 결과를 건너뛴다. 입력 해시·버전·max-pages가 달라지면 새 output이 필요하다.
- `--reuse-cache /path/to/old_result`: 검색·본문 캐시만 재사용한다. 완료 결과를 복사하지 않는다.
- Ctrl+C: 새 작업 배정을 멈추고 진행 중 요청을 정리한 뒤 저장한다. 소켓 timeout 때문에 종료까지 기다릴 수 있다.

## 동시성 보장 범위

API 응답 대기를 DB 잠금 밖에서 수행한다. 동일 검색어는 개별 잠금으로 중복 요청을 막고, 전역 요청 간격·429 cooldown·요청 예산을 공유한다. 작업 수는 workers 이내로 유지하며 결과 저장은 메인 스레드가 담당한다. 동일 웹 원점의 요청은 잠금과 robots 규칙으로 제한한다. robots 캐시는 Fetcher 인스턴스별이며 전역 공유 캐시가 아니다.

`본문근거확보`는 상호·주소가 근처 문맥에 발견됐다는 자동 신호다. 최신 영업정보, 상세 서비스 정보 확보 또는 최종 승인과 동의어가 아니다. 다중 업체를 소개하는 본문, 같은 건물의 다른 매장, 오래된 가격·시간은 후속 검토가 필요하다.

## 로컬 검증

```bash
python -m unittest discover -s tests -v
```

모의 HTTP로 동시 요청, 동일 쿼리 캐시, 요청 예산을 검사한다. 실제 네이버 성능이나 실데이터의 정답률을 검증하는 테스트는 아니다.
