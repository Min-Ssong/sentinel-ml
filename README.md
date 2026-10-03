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
- MLOps 자동화 — Airflow 재학습 DAG + 성능 게이트 기반 모델 승격 (Drift 감지·Shadow Deployment 구현 예정)
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
| 모니터링 | Prometheus, Grafana (예정) |
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
│       │   └── TransactionMessage.java       # Kafka 메시지 계약 (Producer/Consumer 공유)
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
│       │   │   └── TransactionConsumer.java      # Kafka 메시지 수신 → DB 저장
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
│           ├── application.yml
│           ├── application-local.yml         # git 제외
│           └── application-local.yml.example
├── ml-client/            # FastAPI 추론 서버 WebClient + Circuit Breaker
│   └── src/main/java/com/sentinel/mlclient/
│       ├── MlScoreClient.java                     # WebClient + Resilience4j Circuit Breaker
│       └── dto/
│           ├── MlScoreRequest.java
│           └── MlScoreResponse.java
├── ml/                   # Python ML 파이프라인
│   ├── explore.py        # 데이터 탐색 (EDA)
│   ├── train.py          # XGBoost 학습 + MLflow 실험 기록
│   ├── check_split.py    # Time 분할 시 배치별 사기 건수 확인
│   ├── retrain_sim.py    # 누적 재학습 시뮬레이션
│   ├── measure_noise.py  # seed 변동에 따른 성능 편차 측정
│   ├── requirements.txt
│   ├── serving/
│   │   └── app.py        # FastAPI 추론 서버
│   └── data/
│       └── creditcard.csv  # Credit Card Fraud 데이터셋 (git 제외)
├── simulator/            # 트랜잭션 시뮬레이터 (개발/테스트 전용)
│   └── src/main/
│       ├── java/com/sentinelml/simulator/
│       │   ├── SimulatorApplication.java  # Spring Boot 진입점
│       │   ├── SimulatorRunner.java       # CommandLineRunner 기반 produce 루프
│       │   ├── producer/
│       │   │   └── TransactionProducer.java
│       │   ├── loader/
│       │   │   └── CreditCardFeatureLoader.java  # creditcard.csv 피처 로딩
│       │   └── config/
│       │       └── SimulatorProperties.java      # --rate, --fraud-ratio 파라미터
│       └── resources/
│           └── application.yml
├── docs/
│   ├── erd.md                         # PostgreSQL ERD 설계 문서
│   ├── kafka-topic-design.md          # Kafka 토픽 설계 문서
│   └── sql/
│       └── schema.sql                 # PostgreSQL DDL
├── infra/
│   └── docker/
│       ├── docker-compose.yml
│       ├── Dockerfile.mlflow
│       ├── check-ports.ps1            # 예약 포트 범위 검사
│       ├── .env                       # git 제외
│       ├── .env.example
│       └── airflow/                   # Airflow 별도 compose
│           ├── docker-compose.yaml
│           ├── dags/
│           │   └── retrain_dag.py     # 재학습 DAG
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
docker compose ps      # 5개 서비스 Up 확인
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

### 6) Airflow 실행 (MLOps 재학습 파이프라인)

sentinel 인프라와 **별도 compose**입니다.

```bash
cd infra/docker/airflow
docker compose build      # 최초 1회. 소요 큼 (실측 약 18분, 노트북 기준)
docker compose up -d      # 최초 init 15분 이상 소요될 수 있음
```

> 이미지와 메타DB 볼륨은 PC 간 공유되지 않습니다. 다른 PC에서 처음 실행하면 build와 init을 다시 거칩니다. 멈춘 것처럼 보여도 `docker stats`의 BLOCK I/O가 증가 중이면 정상 진행입니다.
>
> 최초 기동 시 apiserver가 수 분간 unhealthy로 표시될 수 있습니다. docker compose logs airflow-apiserver에 오류가 없다면 부팅 중이므로 기다리면 됩니다. (실측: 노트북에서 기동 완료까지 약 30분)

접속: `http://localhost:8491` (`AIRFLOW_PORT`)

### 7) 빌드 및 서비스 실행

```bash
./gradlew build
./gradlew :api:bootRun
./gradlew :simulator:bootRun --args='--simulator.rate=10 --simulator.fraud-ratio=0.2'
```

### 8) FastAPI 추론 서버

```bash
cd ml
.venv\Scripts\Activate.ps1
uvicorn serving.app:app --host 0.0.0.0 --port 8495
```

> MLflow 컨테이너가 먼저 떠 있어야 모델 로드에 성공합니다.

### 서비스 주소

| 서비스 | 기본 포트 | `.env` 키 |
|---|---|---|
| MLflow UI | 5000 | `MLFLOW_PORT` |
| Airflow UI | 8491 | `AIRFLOW_PORT` |
| Spring API | 8490 | `SPRING_API_PORT` |
| FastAPI | 8495 | `FASTAPI_PORT` |
| Kafka | 9192 | `KAFKA_PORT` |
| Zookeeper | 4500 | `ZOOKEEPER_PORT` |
| PostgreSQL | 5432 | `POSTGRES_PORT` |
| Redis | 6379 | `REDIS_PORT` |

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
| `train.py` | XGBoost 베이스라인 학습 + MLflow 기록 |
| `check_split.py` | Time 기준 배치 분할 시 배치별 사기 건수 확인 |
| `retrain_sim.py` | 누적 재학습 시뮬레이션 |
| `measure_noise.py` | seed 변동에 따른 성능 편차 측정 |
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

**초기 베이스라인(6월, 랜덤 분할 기준)**
- 결과: ROC-AUC 0.98, 사기 Recall 0.87 (98건 중 85건 탐지)
- Precision 0.33은 trade-off입니다 — 가중치를 최대로 줘 Recall을 끌어올린 대가이며, 임계값·가중치 튜닝으로 조정 가능한 베이스라인 수준입니다

**Data Leakage 방지 스케일링**

`StandardScaler`를 train 데이터에만 `fit`하고 test에는 `transform`만 적용합니다. test 정보가 전처리 단계에 새어들면 평가 점수가 거짓으로 부풀려지기 때문입니다. V1~V28은 이미 PCA로 스케일된 피처라, 원본 값인 `Amount`·`Time`만 스케일링합니다.

### Drift 모니터링 — 지표 정의와 한계

**Data Drift (PSI)**
- 기준 분포는 champion 모델의 학습 데이터, 비교 대상은 신규 배치다. 피처는 importance 상위 5개와 Amount를 쓴다.
- 운영 DB에는 V1~V28이 저장되지 않아, 재학습 DAG와 같은 creditcard.csv 시간 순 배치로 계산한다. 실서비스라면 운영 데이터에서 계산해야 한다.

**Concept Drift (Precision / Recall)**
- 시뮬레이터가 정답(csv의 Class)을 메시지에 함께 싣고, Consumer가 예측값과 비교해 TP/FP/FN/TN을 집계한다. 실서비스에서는 확정 레이블이 며칠 뒤 도착하므로 지연 평가 구조가 필요하다.
- Rule을 통과해 모델이 판단한 거래만 집계한다. Rule 차단과 ML Fallback은 제외한다.
- 시뮬레이터는 학습에 쓰지 않은 holdout 구간(뒤 20%) 행만 사용한다.

**알아둘 점**: PSI는 csv 배치로, Recall은 시뮬레이터 거래로 계산하므로 두 값은 서로 다른 데이터에서 나온다.
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
- 실제 점수 확인: 정상 거래 0.005 / 사기 거래 0.999

### ✅ 8월 — MLOps 재학습 파이프라인
- 재학습 데이터 전략: Time 순서 3분할 + 누적 학습, 뒤 20% 고정 holdout
- 성능 게이트: ROC-AUC 기준, tolerance 0.0 (프로덕션 학습 설정이 결정론적임을 실측으로 확인)
- Airflow 설치 및 재학습 DAG 구현 — 배치 추출 → 학습 → MLflow 등록 → 게이트 판정
- 게이트 동작 실증: challenger 0.9661 < champion 0.9812 → 승격 차단 / 통과 시 @champion alias 승격 (v12)
- 누적 재학습 ROC-AUC: 0.9661 → 0.9700 → 0.9824

### 🔄 9월~10월 — Drift 모니터링 (진행 중)
- Data Drift: PSI 계산 모듈, Pushgateway → Prometheus → Grafana 대시보드
- Concept Drift: Precision/Recall 실시간 집계 (Micrometer)

### 🔜 예정
- Drift 모니터링 (Prometheus + Grafana)
- Shadow Deployment — Champion/Challenger 병행 추론
- k6 부하 테스트 — P95 < 100ms 검증
- 장애 시나리오 테스트 (Circuit Breaker / DLQ / Fallback)
