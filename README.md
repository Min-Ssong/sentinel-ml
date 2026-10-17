# Sentinel-ML

실시간 금융 사기 탐지 시스템 (Fraud Detection System)

![Java](https://img.shields.io/badge/Java-17-007396?style=flat-square&logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.4.0-6DB33F?style=flat-square&logo=springboot)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.8-231F20?style=flat-square&logo=apachekafka)
![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?style=flat-square&logo=redis)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql)

---

## 프로젝트 개요

카드 도용, 비정상 고액 결제, 물리적으로 불가능한 위치 이동 등 이상 거래 패턴을 실시간으로 탐지하는 시스템입니다. Rule Engine 1차 필터 → ML 모델 추론 → 결과 저장의 파이프라인으로 동작합니다.

**포트폴리오 핵심 차별점**
- Transaction ID 기반 분산 추적 로깅 — 하나의 거래 흐름을 처음부터 끝까지 추적
- Circuit Breaker + DLQ + Fallback — ML 서버 장애 시에도 거래 처리 중단 없음 (P95 100ms SLA는 k6 검증 예정)
- MLOps 자동화 — 성능 게이트 기반 모델 승격, Drift 모니터링(PSI, Precision/Recall), 판정 DAG → 드리프트 재학습 DAG → 승격 → 서빙 반영 루프 (합성 드리프트 시나리오로 검증. Shadow Deployment 구현 예정)

---

## 기술 스택

| 영역 | 기술 |
|---|---|
| Backend | Java 17, Spring Boot 3.4.0 |
| Streaming | Apache Kafka, Spring Kafka |
| Cache | Redis (Sorted Set, Sliding Window) |
| ML | Python 3.11, FastAPI, XGBoost / LightGBM |
| MLOps | MLflow 3.15.1, Apache Airflow 3.x |
| 장애 내성 | Resilience4j (Circuit Breaker, Retry) |
| 모니터링 | Prometheus, Grafana |
| Infra | Docker Compose |
| DB | PostgreSQL 15 |
| Test | JUnit 5, Mockito, k6 |

---

## 프로젝트 구조

```
sentinel-ml/
├── common/               # 공통 DTO, 유틸
│   └── src/main/java/com/sentinel/common/
│       ├── dto/
│       │   └── TransactionMessage.java       # Kafka 메시지 계약. features(ML 입력 30개), isFraud(정답)
│       └── util/
│           └── TransactionIdGenerator.java   # UUID 기반 Transaction ID 생성
├── domain/               # JPA Entity, Repository, 도메인 인터페이스
│   └── src/main/java/com/sentinel/domain/
│       ├── entity/
│       │   ├── Transaction.java        # 거래 원본 데이터
│       │   ├── FraudResults.java       # 사기 탐지 판단 결과 (N:1)
│       │   └── ModelMetadata.java      # ML 모델 배포 이력
│       ├── model/
│       │   └── FraudDetectionResult.java  # 최종 탐지 결과 도메인 모델
│       ├── repository/
│       │   └── TransactionRepository.java
│       └── rule/
│           ├── RuleEvaluator.java         # Rule Engine 인터페이스
│           ├── RedisRuleEvaluator.java    # Redis 기반 규칙 평가 구현체
│           └── FraudScoreResult.java      # Rule 평가 결과 DTO
├── api/                  # Kafka Consumer, Service, REST Controller
│   └── src/main/
│       ├── java/com/sentinel/api/
│       │   ├── SentinelApplication.java          # Spring Boot 진입점
│       │   ├── consumer/
│       │   │   └── TransactionConsumer.java      # 수신 → Rule → ML 호출 → 저장, 예측 결과 카운터 집계
│       │   ├── service/
│       │   │   ├── TransactionService.java
│       │   │   └── RedisService.java              # Redis Sorted Set / Sliding Window
│       │   ├── config/
│       │   │   ├── JpaConfig.java                 # 멀티모듈 JPA Repository 스캔
│       │   │   ├── KafkaConfig.java               # Kafka Consumer / DLQ 설정
│       │   │   └── RedisConfig.java               # RedisTemplate Bean 설정
│       │   └── filter/
│       │       └── MDCFilter.java                 # Transaction ID MDC 등록/해제
│       └── resources/
│           ├── logback-spring.xml                 # 환경별 JSON/텍스트 로그 설정
│           ├── application.yml                    # 포트·DB 기본값, Actuator(prometheus) 노출
│           ├── application-local.yml              # git 제외
│           └── application-local.yml.example
├── ml-client/            # FastAPI 추론 서버 WebClient + Circuit Breaker
│   └── src/main/java/com/sentinel/mlclient/
│       ├── MlScoreClient.java                     # WebClient + Resilience4j Circuit Breaker
│       └── dto/
│           ├── MlScoreRequest.java
│           └── MlScoreResponse.java
├── ml/                   # Python ML 파이프라인
│   ├── explore.py        # 데이터 탐색 (EDA)
│   ├── train.py          # 6월 베이스라인 학습 (랜덤 분할), scaler.pkl 저장
│   ├── check_split.py    # Time 분할 시 배치별 사기 건수 확인
│   ├── retrain_sim.py    # 누적 재학습 시뮬레이션
│   ├── gate.py           # 성능 게이트: ROC-AUC 기준 champion/challenger 승격 판정
│   ├── test_gate.py      # gate.py 단위 테스트
│   ├── drift.py          # Data Drift: PSI 계산 + Pushgateway 전송
│   ├── check_drift.py    # 재학습 3단계 시나리오별 PSI 검증
│   ├── check_importance.py         # champion 모델 feature importance 조회 (PSI 대상 피처 선정 근거)
│   ├── compare_serving_models.py   # 이전 서빙 모델과 champion 의 holdout 비교 (읽기 전용)
│   ├── drift_experiment.py         # 드리프트 시나리오 오프라인 실험 (읽기 전용)
│   ├── make_drift_files.py         # 드리프트 시나리오 csv 생성 + 자체 검증
│   ├── backfill_champion_scaler.py # scaler 를 저장하지 않던 시기의 champion run 에 scaler.json 보충 (1회성)
│   ├── scaler.pkl        # train.py 산출물. 현재 서빙은 사용하지 않음
│   ├── requirements.txt
│   ├── serving/
│   │   └── app.py        # FastAPI 추론 서버. sentinel-fraud-dag@champion 모델 + 같은 run 의 scaler.json 로드
│   └── data/             # git 제외
│       ├── creditcard.csv      # Credit Card Fraud 데이터셋
│       ├── creditcard_s1.csv   # S1 Data drift 시나리오 (make_drift_files.py 생성)
│       └── creditcard_s2.csv   # S2 Concept drift 시나리오, 합성 (make_drift_files.py 생성)
├── simulator/            # 트랜잭션 시뮬레이터 (개발/테스트 전용)
│   └── src/main/
│       ├── java/com/sentinelml/simulator/
│       │   ├── SimulatorApplication.java  # Spring Boot 진입점
│       │   ├── SimulatorRunner.java       # produce 루프. 사용자 1000명 풀, 고빈도 사기 패턴만 user-001 고정
│       │   ├── producer/
│       │   │   └── TransactionProducer.java
│       │   ├── loader/
│       │   │   └── CreditCardFeatureLoader.java  # csv의 holdout 구간(파일 순서 뒤 20%)만 로드, Class별 풀 분리
│       │   └── config/
│       │       └── SimulatorProperties.java      # rate, fraud-ratio, csv-path 파라미터
│       └── resources/
│           └── application.yml
├── docs/
│   ├── erd.md                         # PostgreSQL ERD 설계 문서
│   ├── kafka-topic-design.md          # Kafka 토픽 설계 문서
│   └── sql/
│       └── schema.sql                 # PostgreSQL DDL
├── infra/
│   └── docker/
│       ├── docker-compose.yml         # 기반 5개 + 모니터링 3개 (prometheus, pushgateway, grafana)
│       ├── Dockerfile.mlflow
│       ├── check-ports.ps1            # 예약 포트 범위 검사
│       ├── prometheus/
│       │   └── prometheus.yml         # scrape 대상: pushgateway, Spring Actuator
│       ├── grafana/
│       │   └── dashboards/
│       │       └── drift.json         # PSI·판정·Recall/Precision 패널
│       ├── .env                       # git 제외
│       ├── .env.example
│       └── airflow/                   # Airflow 별도 compose
│           ├── docker-compose.yaml
│           ├── Dockerfile
│           ├── requirements-airflow.txt
│           ├── config/
│           │   └── airflow.cfg
│           ├── dags/
│           │   ├── retrain_dag.py            # sentinel_retrain: Time 3분할 누적 재학습 (수동)
│           │   ├── sentinel_drift_monitor.py # 판정 DAG (@hourly): PSI 경보, Recall 판정, 쿨다운, 재학습 트리거
│           │   └── retrain_drift_dag.py      # sentinel_retrain_drift: 드리프트 대응 재학습 (판정 DAG가 호출)
│           ├── .env                   # git 제외 (FERNET_KEY)
│           └── .env.example
```

---

## 실행 방법

### 사전 조건

- Docker Desktop
- Java 17 (백엔드 빌드·실행 시)
- Python 3.11 (ML 파이프라인 실행 시)

> 아래 명령은 **PowerShell 기준**입니다. Git Bash를 쓰신다면 `Copy-Item` → `cp`,
> `Get-Content` → `cat`, 경로 구분자 `\` → `/` 로 바꿔 실행하세요.

### 1) 환경변수 준비

포트·자격증명은 `infra/docker/.env`로 관리합니다. 포트 충돌 시 이 파일만 수정하면 인프라(Docker) 계층은 모두 반영됩니다.

```powershell
cd infra\docker
Copy-Item .env.example .env
Copy-Item airflow\.env.example airflow\.env
```

`airflow\.env` 의 `FERNET_KEY` 는 비어 있으므로 직접 생성해 채웁니다.

```powershell
docker run --rm apache/airflow:3.3.1 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

> Docker 이미지로 생성하므로 별도 패키지 설치가 필요 없습니다.
>
> **Spring은 `.env` 파일을 읽지 않습니다.** `application.yml` 의 기본값으로 동작하며,
> `.env` 의 포트나 자격증명을 기본값과 다르게 바꿨다면 실행 전 환경변수로 전달해야 합니다.
>
> ```powershell
> $env:POSTGRES_USER="sentinel"
> $env:POSTGRES_PASSWORD="sentinel123"
> $env:SPRING_API_PORT="8490"
> ```
>
> `api/src/main/resources/application-local.yml` 은 git에서 제외됩니다.
> 로컬 설정을 덮어쓰려면 직접 생성하세요.

### 2) 포트 확인

```powershell
cd infra/docker
powershell -ExecutionPolicy Bypass -File .\check-ports.ps1
```

> Windows 예약 포트 범위를 검사합니다. PowerShell 실행 정책이 제한된 환경에서도
> 위 명령으로 바로 실행됩니다. Git Bash에서도 동일하게 동작합니다.
>
> **한계**: 예약 범위만 검사하며 다른 프로세스의 실제 점유는 확인하지 않습니다.
> `netstat -ano | findstr :<포트>` 로 별도 확인이 필요합니다.
>
> Windows 외 환경에서는 이 스크립트가 동작하지 않습니다. 포트 충돌 시
> `infra/docker/.env`의 값을 직접 조정하세요.
>
> 예약 범위는 PC마다, 시점에 따라 달라집니다.
> (실측: 같은 PC에서 8/22 8004~8203 → 9/13 오후 8182~8281 → 9/13 저녁 8207~8406)

### 3) 인프라 실행

```bash
cd infra/docker
docker compose up -d
docker compose ps      # 기반 5개 + 모니터링 3개, 합계 8개 Up 확인
```

### 4) DB 스키마 적용

```powershell
Get-Content ..\..\docs\sql\schema.sql | docker exec -i sentinel-postgres psql -U sentinel -d sentineldb
```

> `infra\docker` 에서 실행하는 기준입니다.
> `postgres_data` 볼륨은 PC 간 공유되지 않으므로 새 환경에서는 반드시 실행해야 합니다.
> 누락 시 `Schema-validation: missing table [fraud_results]` 로 API 기동이 실패합니다.

### 5) Kafka 토픽 생성

컨테이너 재시작 시 토픽이 초기화될 수 있으므로, 기동 후 존재 여부를 확인합니다.

```bash
docker exec -it sentinel-kafka kafka-topics --list --bootstrap-server localhost:9192

# 없으면 생성 (KAFKA_PORT 변경 시 아래 포트도 함께 변경)
docker exec -it sentinel-kafka kafka-topics --create --topic transactions  --bootstrap-server localhost:9192 --partitions 3 --replication-factor 1
docker exec -it sentinel-kafka kafka-topics --create --topic fraud-alerts  --bootstrap-server localhost:9192 --partitions 3 --replication-factor 1
docker exec -it sentinel-kafka kafka-topics --create --topic dlq           --bootstrap-server localhost:9192 --partitions 1 --replication-factor 1
```

### 6) Airflow 실행 (MLOps 파이프라인)

sentinel 인프라와 **별도 compose**입니다.

```bash
cd infra/docker/airflow
docker compose build      # 최초 1회. 소요 큼 (실측 약 18분, 노트북 기준)
docker compose up -d      # 최초 init 15분 이상 소요될 수 있음
```

> 이미지와 메타DB 볼륨은 PC 간 공유되지 않습니다. 다른 PC에서 처음 실행하면 build와 init을 다시 거칩니다. 멈춘 것처럼 보여도 `docker stats`의 BLOCK I/O가 증가 중이면 정상 진행입니다.
>
> 최초 기동 시 apiserver가 수 분간 unhealthy로 표시될 수 있습니다. docker compose logs airflow-apiserver에 오류가 없다면 부팅 중이므로 기다리면 됩니다. (실측: 노트북에서 기동 완료까지 약 30분)
>
> 새 DAG는 기본 일시정지 상태로 등록됩니다. `sentinel_drift_monitor`, `sentinel_retrain_drift` 는 일시정지를 해제해야 스케줄과 트리거가 동작합니다. 트리거 대상 DAG가 일시정지 상태면 트리거 시 오류가 납니다.
>
> ```powershell
> docker exec airflow-airflow-worker-1 airflow dags unpause sentinel_drift_monitor
> docker exec airflow-airflow-worker-1 airflow dags unpause sentinel_retrain_drift
> docker exec airflow-airflow-worker-1 airflow dags list   # is_paused 확인
> ```

접속: `http://localhost:8491` (`AIRFLOW_PORT`)

### 7) 빌드 및 서비스 실행

```bash
./gradlew build
./gradlew :api:bootRun
./gradlew :simulator:bootRun --args='--simulator.rate=10 --simulator.fraud-ratio=0.2'
```

> 드리프트 시나리오로 보내려면 csv 경로를 지정합니다 (먼저 `ml/make_drift_files.py` 로 생성).
> 경로는 시뮬레이터 모듈 기준 상대 경로이며 기본값은 `../ml/data/creditcard.csv` 입니다.
>
> ```bash
> ./gradlew :simulator:bootRun --args='--simulator.rate=10 --simulator.fraud-ratio=0.5 --simulator.csv-path=../ml/data/creditcard_s2.csv'
> ```

### 8) FastAPI 추론 서버

```bash
cd ml
.venv\Scripts\Activate.ps1
uvicorn serving.app:app --host 0.0.0.0 --port 8495
curl http://localhost:8495/model-info    # 로드한 모델 버전·run 확인
```

> MLflow 컨테이너가 먼저 떠 있어야 모델 로드에 성공합니다.
> 서빙은 기동 시 `sentinel-fraud-dag@champion` 을 1회 조회해, 그 버전의 모델과 같은 run 의 `scaler/scaler.json` 을 함께 로드합니다.
> **champion 이 바뀌면 재기동해야 반영됩니다.** (환경변수 `SERVING_MODEL_NAME`, `SERVING_MODEL_ALIAS` 로 대상 변경 가능)

### 서비스 주소

| 서비스 | 기본 포트 | `.env` 키 |
|---|---|---|
| MLflow UI | 5000 | `MLFLOW_PORT` |
| Airflow UI | 8491 | `AIRFLOW_PORT` |
| Spring API | 8490 | `SPRING_API_PORT` |
| FastAPI | 8495 | `FASTAPI_PORT` |
| Kafka | 9192 | `KAFKA_PORT` |
| Zookeeper | 4500 | `ZOOKEEPER_PORT` |
| PostgreSQL | 5800 | `POSTGRES_PORT` |
| Redis | 6379 | `REDIS_PORT` |
| Prometheus | 9290 | `PROMETHEUS_PORT` |
| Pushgateway | 9291 | `PUSHGATEWAY_PORT` |
| Grafana | 3900 | `GRAFANA_PORT` |

> 기본 포트는 `infra/docker/.env`에서 변경할 수 있습니다. Windows 예약 범위는 PC마다,
> 그리고 시점에 따라 달라지므로 `check-ports.ps1`로 먼저 확인하세요.
> (실측: 같은 PC에서 8/22 8004~8203 → 9/13 오후 8182~8281 → 9/13 저녁 8207~8406)

### ML 파이프라인 (Python)

[Kaggle Credit Card Fraud 데이터셋](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud)을 `ml/data/creditcard.csv`로 배치한 뒤:

```bash
cd ml
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

| 스크립트 | 역할 |
|---|---|
| `explore.py` | 데이터 탐색(EDA) |
| `train.py` | 6월 XGBoost 베이스라인 학습 + MLflow 기록 (랜덤 분할) |
| `check_split.py` | Time 기준 배치 분할 시 배치별 사기 건수 확인 |
| `retrain_sim.py` | 누적 재학습 시뮬레이션 |
| `check_drift.py` | 재학습 3단계 시나리오별 PSI 검증 |
| `compare_serving_models.py` | 이전 서빙 모델과 champion 의 holdout 성능 비교 |
| `drift_experiment.py` | 드리프트 시나리오(S1·S2) 오프라인 실험 |
| `make_drift_files.py` | 드리프트 시나리오 csv 생성 + 자체 검증 (MLflow 필요) |
| `backfill_champion_scaler.py` | champion run 에 scaler.json 보충 (기본 dry-run, `--apply` 시 업로드) |
| `serving/app.py` | FastAPI 추론 서버 |

---

## 설계 결정 기록

### MDC 기반 분산 추적 로깅

모든 로그에 `txId` 필드를 포함해 하나의 거래 흐름을 처음부터 끝까지 추적할 수 있도록 설계했습니다.

- `X-Transaction-ID` 헤더가 있으면 해당 값 사용, 없으면 UUID 신규 생성
- Kafka Consumer에서 메시지 수신 시 `transactionId`를 MDC에 등록
- `finally { MDC.clear() }` 로 스레드 반납 전 반드시 초기화 (스레드 풀 오염 방지)
- `logback-spring.xml` 사용으로 `local` / `prod` 환경별 로그 포맷 분리

```
# 로그 출력 예시 (local)
[INFO] txId=a1b2c3d4-xxxx-xxxx-xxxx-xxxxxxxxxxxx c.s.a.consumer.TransactionConsumer - Transaction received | userId=user-001 | amount=262449
```

### ERD 설계 핵심 결정

- `TRANSACTION_ID`: Auto Increment 대신 UUID — Kafka 메시지 키 및 MDC txId와 동일 값으로 분산 추적 연결
- `FRAUD_RESULTS → TRANSACTIONS`: N:1 관계 — Shadow Deployment에서 Champion/Challenger 두 모델이 동일 거래에 각각 판단을 내리므로 거래 1건에 판단 결과 2건 생성 가능
- `AMOUNT`: DECIMAL(15,2) — Double 부동소수점 오차 방지, 금융 정밀도 필수
- `IP_ADDRESS`: VARCHAR(45) — IPv4-mapped IPv6 최대 길이(45자) 수용

### Kafka 토픽 설계 핵심 결정

- 파티션 키 `user_id`: 동일 사용자 거래를 같은 파티션으로 라우팅 — 고빈도 패턴 탐지 정확도 확보
- `dlq` 파티션 1개: 실패 메시지 순서 보장 및 장애 복구 후 재처리를 위한 원본 메시지 보존

### 트랜잭션 시뮬레이터 이상 패턴

| 패턴 | 구현 방식 | 탐지 목적 |
|---|---|---|
| 단시간 고빈도 | 동일 userId 고정으로 짧은 간격 반복 | 카드 도용 후 연속 결제 |
| 비정상 고액 | 정상 상한(50만)의 10배 이상 | 비정상 단건 거래 |
| 이상 위치 | country를 해외(US, JP, CN, RU)로 설정 | 물리적 이동 불가 위치 변화 |

### ML 베이스라인 — 불균형 데이터 전략

**클래스 불균형 (정상:사기 ≈ 578:1)**

사기가 전체의 0.17%에 불과해, 모델이 모든 거래를 "정상"으로 찍어도 정확도(accuracy)가 99.8%로 나옵니다. 따라서 accuracy는 평가 지표로 무의미하며, Precision / Recall / F1 / ROC-AUC로 평가합니다. 사기 탐지에서는 놓친 사기(False Negative)를 줄이는 **Recall**이 특히 중요합니다.

불균형 보정은 XGBoost의 `scale_pos_weight = 정상 수 / 사기 수 ≈ 577`로 사기 클래스에 가중치를 부여하는 방식을 택했습니다. SMOTE 오버샘플링 대신 가중치 방식을 쓴 이유는 간단하고 XGBoost가 네이티브로 지원하기 때문입니다.

**초기 베이스라인 (6월, 랜덤 분할 기준)**
- 결과: ROC-AUC 0.98, 사기 Recall 0.87 (테스트셋 사기 98건 중 85건 탐지)
- Precision 0.33은 trade-off입니다 — 가중치를 최대로 줘 Recall을 끌어올린 대가이며, 임계값·가중치 튜닝으로 조정 가능한 베이스라인 수준입니다

**시간 분할 기준 (재학습 DAG champion v12)**
- Time 순서 뒤 20% holdout 기준: ROC-AUC 0.9824, 사기 Recall 0.81 (75건 중 61건, 임계값 0.5)
- 시간 분할은 미래 데이터로 평가하므로 랜덤 분할보다 엄격합니다. 두 수치는 평가 방식이 달라 직접 비교하지 않습니다. 사기 표본이 75건으로 작습니다

**Data Leakage 방지 스케일링**

`StandardScaler`를 train 데이터에만 `fit`하고 test에는 `transform`만 적용합니다. test 정보가 전처리 단계에 새어들면 평가 점수가 거짓으로 부풀려지기 때문입니다. V1~V28은 이미 PCA로 스케일된 피처라, 원본 값인 `Amount`·`Time`만 스케일링합니다.

### 서빙 모델 — 재학습 champion + 전처리 묶음

- **변경 전**: 서빙이 `train.py` 의 랜덤 분할 모델(`sentinel-fraud/latest`)을 로드했다. 시뮬레이터가 보내는 holdout 행의 약 80%가 이 모델의 학습에 포함되어 있어, 시뮬레이터 기준 Recall(약 0.98~0.99)은 **누수로 부풀려진 값**이었다. 재학습 DAG가 champion 을 바꿔도 서빙에는 반영되지 않았다.
- **변경 후**: 서빙이 재학습 DAG의 `sentinel-fraud-dag@champion` 을 로드한다. 재학습 DAG는 학습 run 에 scaler 를 `scaler/scaler.json`(mean, scale)으로 함께 저장하고, 서빙은 같은 run 에서 읽는다.
    - pickle 대신 JSON: DAG(Airflow 컨테이너)와 서빙(로컬 venv)의 scikit-learn 버전이 다를 수 있어 버전과 무관한 형식을 택했다. 변환식 `(x - mean) / scale` 은 StandardScaler.transform 과 같다.
    - scaler 저장 이전에 만든 champion(v12)에는 재현한 scaler 를 보충했다. 재현값으로 계산한 holdout AUC 가 기록값과 전체 자릿수까지 일치(차이 0)했고, 기존 `scaler.pkl` 은 2.8e-05 달랐다.
- **서빙 기준선 (v12, 시뮬레이터 5분 창)**: Recall 0.8198 (표본 870.57). 오프라인 holdout 0.8133(61/75)과 같은 수준. 서빙만 바꿨을 때 Recall 이 약 0.17 내려가 누수 설명과 일치했다.

### Drift 모니터링 — 지표 정의와 한계

**Data Drift (PSI)**
- 기준 분포는 champion 모델의 학습 데이터, 비교 대상은 신규 배치다. 피처는 importance 상위 5개와 Amount를 쓴다.
- 운영 DB에는 V1~V28이 저장되지 않아, 재학습 DAG와 같은 creditcard.csv 시간 순 배치로 계산한다. 실서비스라면 운영 데이터에서 계산해야 한다.
- 판정 DAG의 `current_csv` 파라미터로 비교 대상을 시나리오 파일의 holdout 으로 바꿀 수 있다 (기본값은 원본).

**Concept Drift (Precision / Recall)**
- 시뮬레이터가 정답(csv의 Class)을 메시지에 함께 싣고, Consumer가 예측값(임계값 0.5)과 비교해 TP/FP/FN/TN을 집계한다. 실서비스에서는 확정 레이블이 며칠 뒤 도착하므로 지연 평가 구조가 필요하다.
- Rule을 통과해 모델이 판단한 거래만 집계한다. Rule 차단과 ML Fallback은 제외한다.
- 시뮬레이터는 재학습 DAG(champion) 학습에 쓰지 않은 holdout 구간(Time 순서 뒤 20%) 행만 사용한다.
- **Precision 주의**: 시뮬레이터 사기 비율(예: 0.5)이 실제(약 0.13%)보다 훨씬 높아 Grafana 의 Precision 은 현실보다 높게 나온다 (예: 시뮬레이터 0.997, 오프라인 전체 holdout 0.314). 시뮬레이터 설정과 함께만 인용한다.

**판정 DAG (`sentinel_drift_monitor`, @hourly)**
- **PSI 는 경보만**: alert(0.2 이상) 피처가 있으면 로그만 남기고 재학습하지 않는다. 오프라인 실험에서 PSI 0.904 의 데이터 드리프트에도 champion 성능이 변하지 않아, PSI 단독 재학습은 불필요한 학습·교체를 낳는다고 판단했다.
- **재학습 트리거 = Recall < 0.60** (판정 창 1h, 최소 표본 tp+fn 100). 0.60 은 서빙 기준선(Wilson 95% 하한 0.71, n=75)과 드리프트 시나리오(상한 0.47)의 중간으로 잡은 설계 선택이다. 사기 75행 반복 추출이라 구간은 낙관적이다.
- 표본이 부족하면 실패가 아니라 "판정 보류"로 처리한다.
- **쿨다운(보류 조건)**: ① 서빙 버전(`/model-info`)이 `@champion` 과 다르면 "서빙 반영 대기" ② 서빙·MLflow 조회 실패 ③ 마지막 드리프트 재학습 후 2시간(`cooldown_hours`) 미만. 서빙이 기동 시 1회만 로드하기 때문에, 재기동 전까지 옛 모델의 낮은 Recall 로 재학습이 반복되는 것을 막는다.
- 트리거 대상은 드리프트 재학습 DAG(`sentinel_retrain_drift`).

**드리프트 재학습 DAG (`sentinel_retrain_drift`)**
- 기존 `sentinel_retrain`(3단계 누적 재연)은 그대로 두고, 목적이 다른 재학습을 별도 DAG로 분리했다.
- 학습 = 원본 학습 풀 + 드리프트 파일 학습 구간의 사기 행, 평가 = 드리프트 파일 holdout. 원본 holdout 성능도 함께 기록한다.
- 게이트: champion 도 **같은 평가 데이터로 다시 평가**해 비교한다 (champion run 에 기록된 옛 평가셋 점수와 비교하면 서로 다른 시험지 점수를 비교하게 됨).

**드리프트 시나리오 (`make_drift_files.py`)**

| 시나리오 | 변형 | PSI | champion holdout Recall |
|---|---|---|---|
| S1 Data drift | holdout 전체 행의 V14 를 +1.0σ 이동 | V14 0.904 (alert) | 0.8133 (변화 없음) |
| S2 Concept drift (**합성**) | 사기 행의 V14·V12·V10·V4 를 정상 평균 쪽으로 75% 이동 + V2 에 +3σ 신호 | 거의 변화 없음 (V12 0.154) | 0.36 (27/75) |

- S2 는 실제 사기 패턴 변화가 아니라 루프 검증용 합성 시나리오다. 사기 행만 바뀌는 변화는 전체 분포 기반 PSI 에 거의 잡히지 않고 Recall 에서 드러난다.

**루프 검증 (S2, 시뮬레이터 rate=10·fraud-ratio=0.5, 5분 창)**

| 단계 | 결과 |
|---|---|
| 드리프트 주입 후 (서빙 v12) | Recall 0.3737 |
| 판정 DAG | Recall < 0.60 → `sentinel_retrain_drift` 트리거 (임계값 조정·강제 실행 없음) |
| 재학습·게이트 | challenger 평가분 AUC 0.9986 (Recall 0.96) vs champion 재평가 0.9408 → 통과, 승격 |
| 서빙 재기동 후 | Recall 0.9542 |

- 새 모델의 원본 holdout Recall 은 0.8267 로, 새 패턴을 배우면서 옛 패턴을 잃지 않았다 (n=75 소표본).
- 한계: 라벨 즉시 도착 가정, 재학습용 "새 데이터"가 파일로 고정된 시뮬레이션, 서빙 반영은 수동 재기동.

**알아둘 점**
- 기본 상태에서 PSI 는 csv 배치로, Recall 은 시뮬레이터 거래로 계산하므로 두 값은 서로 다른 데이터에서 나온다. 드리프트 시나리오에서는 같은 파일을 `current_csv` 와 시뮬레이터 `csv-path` 로 지정해 맞춘다.
- PSI baseline 은 원본 학습 풀로 고정되어 있어, champion 이 바뀌어도 baseline 은 따라가지 않는다.

---

## 설계 문서

- [ERD 설계](docs/erd.md)
- [Kafka 토픽 설계](docs/kafka-topic-design.md)
- [PostgreSQL DDL](docs/sql/schema.sql)

---

## 진행 현황

### ✅ 3월 — 기반 설계 및 환경 구축
- Docker Compose 5개 서비스(Kafka, Zookeeper, Redis, PostgreSQL, MLflow) 구축
- Spring Boot 멀티모듈 구조 + MDC 기반 분산 추적 로깅(`logback-spring.xml`, `MDCFilter`)
- PostgreSQL ERD 3테이블 + Kafka 토픽 3개 설계
- 트랜잭션 시뮬레이터 → Kafka → Consumer → PostgreSQL E2E 파이프라인 검증

### ✅ 4월 — 백엔드 코어 구현
- Kafka Consumer 처리 실패 시 DLQ 자동 라우팅
- Redis 캐싱 레이어 — Sorted Set 거래 이력, Sliding Window Counter
- Rule Engine — 규칙 기반 1차 필터 (10ms 이내 처리 목표)

### ✅ 5월 — ML 클라이언트 연동
- FastAPI 추론 서버를 호출할 WebClient (`MlScoreClient`) 구현
- Resilience4j Circuit Breaker + Fallback — ML 서버 장애 시 Rule 결과로 처리
- `MlScoreRequest` / `MlScoreResponse` DTO 및 단위 테스트

### ✅ 6월 — ML 모델 개발 및 추론 통합
- FastAPI 추론 서버 구현, Java ↔ Python E2E 연동 검증
- MDC txId 전 구간 추적 확인 (Simulator → Kafka → Consumer → FastAPI → DB)
- 실제 점수 확인: 정상 거래 0.005 / 사기 거래 0.999 (당시 서빙 모델 `train.py` 랜덤 분할 모델 기준)

### ✅ 8월 — MLOps 재학습 파이프라인
- 재학습 데이터 전략: Time 순서 3분할 + 누적 학습, 뒤 20% 고정 holdout
- 성능 게이트: ROC-AUC 기준, tolerance 0.0 (프로덕션 학습 설정이 동일 환경에서 결정론적임을 실측으로 확인)
- Airflow 설치 및 재학습 DAG 구현 — 배치 추출 → 학습 → MLflow 등록 → 게이트 판정
- 게이트 동작 실증: challenger 0.9661 < champion 0.9812 → 승격 차단 / 통과 시 @champion alias 승격 (v12)
- 누적 재학습 ROC-AUC: 0.9661 → 0.9700 → 0.9824

### 🔄 9월~10월 — Drift 모니터링 (진행 중)
- Data Drift: PSI 계산 모듈, Pushgateway → Prometheus → Grafana 대시보드
- Concept Drift: Precision/Recall 실시간 집계 (Micrometer)
- 판정 DAG(`sentinel_drift_monitor`, @hourly) — PSI 경보, Recall 판정, 쿨다운
- 서빙을 재학습 champion + 전처리 묶음 로드로 전환 (누수 모델 교체)
- 드리프트 재학습 DAG(`sentinel_retrain_drift`) — champion 재평가 게이트
- 합성 드리프트(S2)로 감지 → 재학습 → 승격 → 서빙 반영 → Recall 회복 루프 검증 (0.37 → 0.95)

### 🔜 예정
- Shadow Deployment — Champion/Challenger 병행 추론
- 서빙 자동 재로드 (현재 champion 변경 시 재기동)
- PSI baseline 을 champion 학습 데이터와 연동
- k6 부하 테스트 — P95 < 100ms 검증
- 장애 시나리오 테스트 (Circuit Breaker / DLQ / Fallback)