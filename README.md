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
| ML Ops | MLflow, Apache Airflow |
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

## 진행 현황

### 3월 1주차 (3/6 ~ 3/8) ✅
- Docker Compose 환경 구축 (5개 서비스 정상 기동)
- Spring Boot 멀티모듈 구조 생성 및 빌드 성공