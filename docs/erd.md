# Sentinel-ML ERD

## 테이블 관계

fraud_results.transaction_id → transactions.transaction_id (N:1)
fraud_results.model_id → model_metadata.id (N:1)
model_metadata는 독립 테이블

---

## transactions

| 컬럼명 | 타입 | 제약조건 | 설명 |
|---|---|---|---|
| transaction_id | VARCHAR(36) | PK, NOT NULL | UUID |
| user_id | VARCHAR(36) | NOT NULL, INDEX | 사용자 식별자 |
| amount | DECIMAL(15,2) | NOT NULL | 거래 금액 |
| merchant_category | VARCHAR(50) | NOT NULL | 가맹점 카테고리 |
| device_id | VARCHAR(100) | | 기기 식별자 |
| ip_address | VARCHAR(45) | | IPv4/IPv6 대응 |
| location | VARCHAR(100) | | 거래 위치 (국가_도시) |
| timestamp | TIMESTAMP | NOT NULL | 거래 발생 시각 (UTC) |
| status | VARCHAR(20) | NOT NULL, DEFAULT 'PENDING' | PENDING / APPROVED / BLOCKED |
| created_at | TIMESTAMP | NOT NULL, DEFAULT NOW() | 레코드 생성 시각 |

---

## fraud_results

| 컬럼명 | 타입 | 제약조건 | 설명 |
|---|---|---|---|
| id | BIGINT | PK, AUTO_INCREMENT | |
| transaction_id | VARCHAR(36) | FK → transactions, NOT NULL | |
| model_id | BIGINT | FK → model_metadata, NULL 허용 | 판단한 모델 식별 (Rule Engine 차단 시 NULL) |
| rule_triggered | VARCHAR(100) | | 발동된 규칙명 (없으면 NULL) |
| ml_score | DECIMAL(5,4) | | ML 모델 사기 확률 (0.0~1.0) |
| final_decision | VARCHAR(20) | NOT NULL | APPROVED / REVIEW / BLOCKED |
| model_role | VARCHAR(20) | | CHAMPION / CHALLENGER (Shadow 배포 시 구분) |
| created_at | TIMESTAMP | NOT NULL, DEFAULT NOW() | |

---

## model_metadata

| 컬럼명 | 타입 | 제약조건 | 설명 |
|---|---|---|---|
| id | BIGINT | PK, AUTO_INCREMENT | |
| model_name | VARCHAR(100) | NOT NULL | 모델 이름 |
| version | VARCHAR(20) | NOT NULL | 예: v1.0.0 |
| mlflow_run_id | VARCHAR(36) | NOT NULL | MLflow 실험 run ID |
| deployed_at | TIMESTAMP | | 배포 시각 |
| is_active | BOOLEAN | NOT NULL, DEFAULT FALSE | 현재 서빙 중인 모델 여부 |
| created_at | TIMESTAMP | NOT NULL, DEFAULT NOW() | |

---

## 설계 결정 근거

- `transaction_id`: Auto Increment 대신 UUID 사용 — Kafka 메시지 키 및 MDC txId와 동일 값으로 분산 추적 연결
- `ip_address`: VARCHAR(45) — IPv6 최대 길이(39) + 여유
- `location`: 별도 테이블 정규화 없이 문자열 보관 — 포트폴리오 수준에서 조회 빈도 낮음
- `ml_score`: rule_triggered가 있으면 ML 미호출이므로 NULL 허용
- `model_id`: Rule Engine만으로 차단된 경우 ML 미호출이므로 NULL 허용
- `model_role`: Shadow Deployment 시 Champion/Challenger 판단 결과를 동일 테이블에서 구분하기 위해 추가
- `is_active`: Champion 모델 교체 시 단일 행만 TRUE로 관리
