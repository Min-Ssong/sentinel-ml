# Sentinel-ML ERD

## 테이블 관계

- FRAUD_RESULTS.TRANSACTION_ID → TRANSACTIONS.TRANSACTION_ID (N:1)
- FRAUD_RESULTS.MODEL_ID → MODEL_METADATA.ID (N:1)
- MODEL_METADATA는 독립 테이블

---

## TRANSACTIONS

| 컬럼명 | 타입 | 제약조건 | 설명 |
|---|---|---|---|
| TRANSACTION_ID | VARCHAR(36) | PK, NOT NULL | UUID |
| USER_ID | VARCHAR(36) | NOT NULL, INDEX | 사용자 식별자 |
| AMOUNT | DECIMAL(15,2) | NOT NULL | 거래 금액 |
| MERCHANT_CATEGORY | VARCHAR(50) | NOT NULL | 가맹점 카테고리 |
| DEVICE_ID | VARCHAR(100) | | 기기 식별자 |
| IP_ADDRESS | VARCHAR(45) | | IPv4/IPv6 대응 |
| LOCATION | VARCHAR(100) | | 거래 위치 (국가_도시) |
| TIMESTAMP | TIMESTAMP | NOT NULL | 거래 발생 시각 (UTC) |
| STATUS | VARCHAR(20) | NOT NULL, DEFAULT 'PENDING' | PENDING / APPROVED / BLOCKED |
| CREATED_AT | TIMESTAMP | NOT NULL, DEFAULT NOW() | 레코드 생성 시각 |

---

## FRAUD_RESULTS

| 컬럼명 | 타입 | 제약조건 | 설명 |
|---|---|---|---|
| ID | BIGSERIAL | PK, NOT NULL | |
| TRANSACTION_ID | VARCHAR(36) | FK → TRANSACTIONS, NOT NULL | |
| MODEL_ID | BIGINT | FK → MODEL_METADATA, NOT NULL | 판단한 모델 식별 |
| RULE_TRIGGERED | VARCHAR(100) | | 발동된 규칙명 (없으면 NULL) |
| ML_SCORE | DECIMAL(5,4) | | ML 모델 사기 확률 (0.0~1.0) |
| FINAL_DECISION | VARCHAR(20) | NOT NULL | APPROVED / REVIEW / BLOCKED |
| CREATED_AT | TIMESTAMP | NOT NULL, DEFAULT NOW() | |

---

## MODEL_METADATA

| 컬럼명 | 타입 | 제약조건 | 설명 |
|---|---|---|---|
| ID | BIGSERIAL | PK, NOT NULL | |
| MODEL_NAME | VARCHAR(100) | NOT NULL | 모델 이름 |
| VERSION | VARCHAR(20) | NOT NULL | 예: v1.0.0 |
| MLFLOW_RUN_ID | VARCHAR(36) | NOT NULL | MLflow 실험 run ID |
| DEPLOYED_AT | TIMESTAMP | | 배포 시각 |
| IS_ACTIVE | BOOLEAN | NOT NULL, DEFAULT FALSE | 현재 서빙 중인 모델 여부 |
| CREATED_AT | TIMESTAMP | NOT NULL, DEFAULT NOW() | |

---

## 설계 결정 근거

- `TRANSACTION_ID`: Auto Increment 대신 UUID 사용 — Kafka 메시지 키 및 MDC txId와 동일 값으로 분산 추적 연결
- `IP_ADDRESS`: VARCHAR(45) — IPv4-mapped IPv6 표기 최대 길이(45자) 수용
- `LOCATION`: 별도 테이블 정규화 없이 문자열 보관 — 포트폴리오 수준에서 조회 빈도 낮음
- `ML_SCORE`: RULE_TRIGGERED가 있으면 ML 미호출이므로 NULL 허용
- `IS_ACTIVE`: Champion 모델 교체 시 단일 행만 TRUE로 관리
- `MODEL_ID`: Shadow Deployment 시 Champion/Challenger 판단 결과를 동일 테이블에서 모델별로 구분하기 위해 추가
- `ID (BIGSERIAL)`: PostgreSQL 네이티브 자동 증가 타입 사용 — MySQL의 AUTO_INCREMENT 대신 적용
