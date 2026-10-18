# Sentinel-ML Kafka 토픽 설계

> 2026-10-19 코드와 대조해 갱신. 기준: `common` 모듈 `TransactionMessage`, 시뮬레이터 `TransactionProducer`, API `KafkaConfig`·`TransactionConsumer`·`ShadowConsumer`.

## 토픽 목록

| 토픽명 | 역할 | 상태 |
|---|---|---|
| `transactions` | 결제 요청 수신 | 구현 |
| `dlq` | 처리 실패 메시지 보관 | 구현 |
| `fraud-alerts` | 사기 탐지 결과 발송 | **미구현** (설계만 있음, 생산자 없음) |

- 브로커 설정: `KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'`, `KAFKA_NUM_PARTITIONS: '3'` (`infra/docker/docker-compose.yml`)

---

## transactions

| 항목 | 값 | 근거 |
|---|---|---|
| 파티션 수 | 3 | Consumer 확장 여지 확보 (브로커 기본값으로 생성) |
| 파티션 키 | userId | 동일 사용자 거래를 같은 파티션으로 라우팅 → 사용자별 순서 유지 |
| 메시지 형식 | JSON (`TransactionMessage` record) | |
| Consumer group | `sentinel-group`(운영 판정), `sentinel-shadow-group`(Shadow, `shadow.enabled=true` 일 때) | 같은 이벤트를 두 group 이 독립 소비 |

### 필드

| 필드 | 타입 | 설명 |
|---|---|---|
| transactionId | String | UUID |
| userId | String | 파티션 키 |
| amount | BigDecimal | 거래 금액 |
| merchantCategory | String | 가맹점 카테고리 |
| deviceId | String | 기기 식별자 |
| ipAddress | String | IP |
| country | String | 국가 코드 (시뮬레이터: KR / US / JP) |
| timestamp | Instant | 거래 발생 시각. 실제 메시지에서 epoch 초 숫자(소수부 나노초)로 직렬화됨을 확인 (2026-10-18) |
| isFraud | boolean | 정답 라벨 — **시뮬레이션 전용**. 판정에는 쓰지 않고 `TRANSACTION_LABELS` 저장·지표 집계에만 사용 |
| features | List&lt;Double&gt; | ML 입력 30개: Time, V1~V28, Amount |

- 2026-10-18 계약 변경: `timestamp` 가 `LocalDateTime` → `Instant`. 생산자(시뮬레이터)와 소비자(API)를 함께 배포해야 한다 (두 group lag 0 확인 후 배포)

### 메시지 예시 (형식 예시, 값은 임의. 키 이름은 record 컴포넌트명 기준이며 실제 메시지에서 확인한 것은 `timestamp` 형식뿐)
```json
{
  "transactionId": "uuid-1234",
  "userId": "user-5678",
  "amount": 150000.00,
  "merchantCategory": "OVERSEAS",
  "deviceId": "device-abcd",
  "ipAddress": "192.168.0.1",
  "country": "KR",
  "timestamp": 1792312980.231707500,
  "isFraud": false,
  "features": [0.0, -1.3598, -0.0728]
}
```

- `features` 는 실제로 30개 (예시는 앞 3개만)

---

## dlq

| 항목 | 값 | 근거 |
|---|---|---|
| 라우팅 | `DeadLetterPublishingRecoverer` 가 항상 파티션 0 으로 발행 | 실패 메시지를 발생 순서대로 보관 (원인 분석·재처리 순서) |
| 파티션 수 | 실제 토픽 설정은 확인하지 않음 (자동 생성이면 3) | 코드가 파티션 0 만 쓰므로 순서 보장 목적은 유지 |
| 재시도 | 3초 간격 3회 (`FixedBackOff(3000, 3)`) 후 dlq | 일시 장애 흡수 |
| 대상 | 운영 group(`sentinel-group`)만 | Shadow 는 재시도·DLQ 없는 전용 팩토리 (챌린저 장애가 운영 DLQ 를 오염시키지 않게) |
| 메시지 형식 | 원본 메시지(키·값) 그대로 + 오류 정보는 Kafka 헤더 | Spring Kafka 기본 동작 |

- 오류 정보 헤더 (Spring Kafka 기본): `kafka_dlt-original-topic`, `kafka_dlt-original-partition`, `kafka_dlt-original-offset`, `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-message` 등
- 이전 문서의 `{originalTopic, errorMessage, failedAt, payload}` 래핑 형식은 구현되지 않았다
- 실제 dlq 메시지를 열어 헤더를 확인한 기록은 없다

---

## fraud-alerts (미구현)

판정 결과를 다른 시스템(알림·보상 처리 등)에 전달하는 토픽으로 설계했으나, 현재 생산자가 없다. 판정 결과는 DB(`TRANSACTIONS.STATUS`, `FRAUD_RESULTS`)와 Prometheus 지표로만 남는다.

설계 당시 필드: transactionId, userId, finalDecision, mlScore, ruleTriggered, createdAt (파티션 키 userId)

---

## 설계 결정 근거

- `파티션 키 userId`: 동일 사용자의 거래를 항상 같은 파티션으로 라우팅해 사용자 단위 순서를 유지. 고빈도 사용자가 있으면 파티션 편중이 생길 수 있음 (편중 원인은 미확인)
- `transactions 파티션 3개`: 리스너 concurrency 와 함께 확장 여지 확보
- `dlq 파티션 0 고정`: 실패 메시지는 발생 순서대로 봐야 원인 분석이 정확하므로 순서 보장 우선
- `원본 메시지 보관`: 장애 복구 후 같은 메시지로 재처리할 수 있게 함 (재처리 도구는 미구현)
