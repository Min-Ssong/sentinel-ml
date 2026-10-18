# Sentinel-ML ERD

> 기준: `docs/sql/schema.sql` 과 `domain` 모듈 엔티티 (2026-10-19 대조). 둘이 다른 곳은 표 아래에 적었다.

## 테이블 관계

- FRAUD_RESULTS.TRANSACTION_ID → TRANSACTIONS.TRANSACTION_ID (N:1) — 거래 1건에 CHAMPION 1행 + (Shadow 활성 시) CHALLENGER 1행
- FRAUD_RESULTS.MODEL_ID → MODEL_METADATA.ID (N:1)
- TRANSACTION_LABELS.TRANSACTION_ID → TRANSACTIONS.TRANSACTION_ID (1:1, 공유 PK)
- MODEL_METADATA는 독립 테이블

---

## TRANSACTIONS

| 컬럼명 | 타입 | 제약조건 | 설명 |
|---|---|---|---|
| TRANSACTION_ID | VARCHAR(36) | PK, NOT NULL | UUID |
| USER_ID | VARCHAR(36) | NOT NULL | 사용자 식별자 |
| AMOUNT | DECIMAL(15,2) | NOT NULL | 거래 금액 |
| MERCHANT_CATEGORY | VARCHAR(50) | NOT NULL | 가맹점 카테고리 |
| DEVICE_ID | VARCHAR(100) | | 기기 식별자 |
| IP_ADDRESS | VARCHAR(45) | | IPv4/IPv6 대응 |
| COUNTRY | VARCHAR(100) | | 거래 국가 코드 (시뮬레이터 값: KR / US / JP) |
| TIMESTAMP | TIMESTAMPTZ | NOT NULL | 거래 발생 시각 (2026-10-18 TIMESTAMPTZ 전환. 이전 TIMESTAMP 컬럼에는 문서와 달리 KST 값이 저장됐음) |
| STATUS | VARCHAR(20) | NOT NULL, DEFAULT 'PENDING' | BLOCKED(룰 차단 또는 ML 점수) / REVIEW / APPROVED / PENDING(ML Fallback) |
| CREATED_AT | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | 레코드 생성 시각 |

- 이전 문서의 `LOCATION`(국가_도시) 컬럼은 구현에서 `COUNTRY` 로 바뀌었다
- 엔티티는 `COUNTRY` 길이를 10 으로 선언한다 (스키마 100). `ddl-auto=validate` 는 길이를 비교하지 않아 동작에는 영향 없음
- 이전 문서의 `USER_ID` INDEX 는 `schema.sql` 에 없다 (실제 DB 인덱스 여부는 확인하지 않음)

---

## FRAUD_RESULTS

ML 이 점수를 낸 거래만 저장한다. 룰 차단·Fallback 거래는 판단한 모델이 없어 `MODEL_ID`(NOT NULL)를 채울 수 없으므로 저장하지 않는다.

| 컬럼명 | 타입 | 제약조건 | 설명 |
|---|---|---|---|
| ID | BIGSERIAL | PK, NOT NULL | |
| TRANSACTION_ID | VARCHAR(36) | FK → TRANSACTIONS, NOT NULL, INDEX | |
| MODEL_ID | BIGINT | FK → MODEL_METADATA, NOT NULL | 판단한 모델 (추론 서버 응답의 이름·버전·run_id 로 조회 또는 생성) |
| RULE_TRIGGERED | VARCHAR(100) | | 현재 코드에서는 채우지 않음 (항상 NULL, 위 저장 범위 때문) |
| ML_SCORE | DECIMAL(5,4) | | ML 모델 사기 확률 (0.0~1.0, 소수 4자리 반올림) |
| FINAL_DECISION | VARCHAR(20) | NOT NULL | APPROVED / REVIEW / BLOCKED |
| ROLE | VARCHAR(20) | NOT NULL, CHECK IN ('CHAMPION', 'CHALLENGER') | CHAMPION = 실제 판정 / CHALLENGER = Shadow (판정 미사용) |
| CREATED_AT | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | |

---

## MODEL_METADATA

| 컬럼명 | 타입 | 제약조건 | 설명 |
|---|---|---|---|
| ID | BIGSERIAL | PK, NOT NULL | |
| MODEL_NAME | VARCHAR(100) | NOT NULL, UNIQUE(MODEL_NAME, VERSION) | 모델 이름 |
| VERSION | VARCHAR(20) | NOT NULL, UNIQUE(MODEL_NAME, VERSION) | 추론 서버 응답의 모델 버전 |
| MLFLOW_RUN_ID | VARCHAR(36) | NOT NULL | MLflow 실험 run ID |
| DEPLOYED_AT | TIMESTAMPTZ | | 엔티티에 매핑되지 않아 현재 채우지 않음 |
| IS_ACTIVE | BOOLEAN | NOT NULL, DEFAULT FALSE | 현재 코드는 항상 FALSE 로 저장 (아래 설계 결정 참고) |
| CREATED_AT | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | |

---

## TRANSACTION_LABELS

거래의 정답 라벨 (Champion/Challenger 비교 리포트 기준값, 2026-10-17 추가).

| 컬럼명 | 타입 | 제약조건 | 설명 |
|---|---|---|---|
| TRANSACTION_ID | VARCHAR(36) | PK, FK → TRANSACTIONS, NOT NULL | 거래와 공유 키 |
| IS_FRAUD | BOOLEAN | NOT NULL | 사기 여부 |
| LABEL_SOURCE | VARCHAR(20) | NOT NULL | 현재 SIMULATOR 뿐 |
| LABELED_AT | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | 라벨 저장 시각 |

- 룰 차단·Fallback 거래도 저장한다 (모델 판단 여부와 무관한 거래 단위 정답)

---

## 설계 결정 근거

- `TRANSACTION_ID`: Auto Increment 대신 UUID 사용 — Kafka 메시지 키 및 MDC txId와 동일 값으로 분산 추적 연결
- `IP_ADDRESS`: VARCHAR(45) — IPv4-mapped IPv6 표기 최대 길이(45자) 수용
- `COUNTRY`: 별도 테이블 정규화 없이 문자열 보관 — 포트폴리오 수준에서 조회 빈도 낮음
- `FRAUD_RESULTS` 저장 범위: 판단한 모델이 있는 거래만. 룰 차단·Fallback 결과는 `TRANSACTIONS.STATUS` 로 남는다
- `MODEL_ID` + `ROLE`: Shadow Deployment 시 Champion/Challenger 판단 결과를 같은 테이블에서 모델별·역할별로 구분. 같은 모델 버전도 시점에 따라 역할이 바뀌므로(챌린저 → 승격 후 챔피언) 역할은 모델이 아니라 결과 행에 기록한다
- `IS_ACTIVE`: 처음 설계는 "Champion 교체 시 단일 행만 TRUE"였으나, 현재 Champion 은 MLflow Registry alias(`@champion`)로 관리하고 역할은 `FRAUD_RESULTS.ROLE` 에 기록한다. 컬럼은 남아 있지만 사용하지 않는다
- `TRANSACTION_LABELS` 분리: 실제 금융권에서는 확정 라벨(차지백·고객 신고 등)이 거래보다 늦게 도착한다. 운영 거래 테이블에 시뮬레이션 전용 값을 섞지 않는다
- 시각 컬럼 `TIMESTAMPTZ` + Java `Instant`: 시간대 없는 값 때문에 `now()` 기준 조회가 9시간 어긋난 문제를 2026-10-18 에 전환 (`docs/sql/migration_2026-10-18_timestamptz.sql`)
- `ID (BIGSERIAL)`: PostgreSQL 네이티브 자동 증가 타입 사용 — MySQL의 AUTO_INCREMENT 대신 적용
