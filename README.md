# Sentinel-ML
실시간 금융 사기 탐지 시스템

---

## 기술 스택

| 영역 | 기술 |
|---|---|
| Backend | Java 17, Spring Boot 3.4.0 |
| Streaming | Apache Kafka |
| Cache | Redis |
| ML | Python 3.11, FastAPI, XGBoost |
| MLOps | MLflow, Apache Airflow |
| Infra | Docker Compose |
| DB | PostgreSQL |
| Test | JUnit 5, Mockito, k6 |

---

## 프로젝트 구조

```
sentinel-ml/
├── common/           # 공통 유틸, 예외, 응답 포맷
├── domain/           # JPA Entity, Repository
├── api/              # REST Controller, Kafka Consumer/Producer
│   └── src/main/
│       ├── java/com/sentinel/api/
│       │   └── filter/
│       │       └── MDCFilter.java      # Transaction ID MDC 등록/해제
│       └── resources/
│           ├── logback-spring.xml      # 환경별 JSON/텍스트 로그 설정
│           ├── application.yml
│           └── application-local.yml
├── ml-client/        # FastAPI 추론 서버 WebClient
├── infra/
│   └── docker/
│       ├── docker-compose.yml
│       ├── Dockerfile.mlflow
│       └── .env      # git 제외
└── .env.example      # 환경변수 템플릿
```

---

## 실행 방법

### 사전 조건
- Docker Desktop 설치
- `.env` 파일 생성 (`.env.example` 참고)

```bash
# .env 파일 생성
cp .env.example infra/docker/.env
# 값 직접 입력 후 저장
```

### 환경 실행

```bash
cd infra/docker
docker compose up -d
```

### 서비스 확인

| 서비스 | 주소 |
|---|---|
| MLflow UI | http://localhost:5000 |
| Kafka | localhost:9092 |
| PostgreSQL | localhost:5432 |
| Redis | localhost:6379 |

### 빌드

```bash
./gradlew build
```

---

## 설계 결정 기록

### MDC 기반 분산 추적 로깅
모든 로그에 `txId` 필드를 포함해 하나의 거래 흐름을 처음부터 끝까지 추적할 수 있도록 설계했습니다.

- `X-Transaction-ID` 헤더가 있으면 해당 값 사용, 없으면 UUID 신규 생성
- `finally { MDC.clear() }` 로 스레드 반납 전 반드시 초기화 (스레드 풀 오염 방지)
- `logback-spring.xml` 사용으로 `local` / `prod` 환경별 로그 포맷 분리

```
# 로그 출력 예시 (local)
[INFO] txId=a1b2c3d4-xxxx-xxxx-xxxx-xxxxxxxxxxxx c.s.a.filter.MDCFilter - Request incoming - method=GET uri=/api/transactions
```

---

## 진행 현황

### ✅ 3월 1주차 (3/6 ~ 3/7)
- Docker Compose 환경 구축 (5개 서비스 정상 기동)
- Spring Boot 멀티모듈 구조 생성 및 빌드 성공

### ✅ 3월 2주차 (3/10 ~ 3/14)
- `logback-spring.xml` 작성 — 환경별 로그 포맷 분리 (local: 텍스트 / prod: JSON)
- `MDCFilter` 구현 — 요청별 Transaction ID 생성 및 MDC 등록/해제
- `build.gradle` 의존성 정리 — 모듈별 책임 분리, 불필요한 의존성 제거
- JUnit 5 테스트 환경 세팅 및 샘플 단위 테스트 작성/통과

### 🔜 3월 3주차 (3/15 ~ 3/21)
- PostgreSQL ERD 확정 및 JPA Entity 코드 작성
- Kafka 토픽 설계 문서 작성 (`transactions` / `fraud-alerts` / `dlq`)
