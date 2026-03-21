# Sentinel-ML Kafka 토픽 설계

## 토픽 목록

| 토픽명 | 역할 | 파티션 수 |
|---|---|---|
| `transactions` | 결제 요청 수신 | 3 |
| `fraud-alerts` | 사기 탐지 결과 발송 | 3 |
| `dlq` | 처리 실패 메시지 보관 | 1 |

---

## transactions

| 항목 | 값 | 근거 |
|---|---|---|
| 파티션 수 | 3 | Consumer 확장 여지 확보 |
| 파티션 키 | user_id | 동일 사용자 거래를 같은 파티션으로 라우팅, 고빈도 패턴 탐지 정확도 확보 |
| 메시지 형식 | JSON | |
| 주요 필드 | transaction_id, user_id, amount, merchant_category, device_id, ip_address, location, timestamp | |

### 메시지 예시
```json
{
  "transactionId": "uuid-1234",
  "userId": "user-5678",
  "amount": 150000.00,
  "merchantCategory": "OVERSEAS",
  "deviceId": "device-abcd",
  "ipAddress": "192.168.0.1",
  "location": "KR_SEOUL",
  "timestamp": "2026-03-15T09:00:00"
}
```
## fraud-alerts

| 항목 | 값 | 근거 |
|---|---|---|
| 파티션 수 | 3 | transactions 토픽과 동일 기준 |
| 파티션 키 | user_id | 동일 사용자의 탐지 결과를 같은 파티션으로 라우팅 |
| 메시지 형식 | JSON | |
| 주요 필드 | transaction_id, user_id, final_decision, ml_score, rule_triggered, created_at | |

### 메시지 예시
```json
{
  "transactionId": "uuid-1234",
  "userId": "user-5678",
  "finalDecision": "BLOCKED",
  "mlScore": null,
  "ruleTriggered": "HIGH_FREQUENCY",
  "createdAt": "2026-03-15T09:00:01"
}
```

---

## dlq

| 항목 | 값 | 근거 |
|---|---|---|
| 파티션 수 | 1 | 실패 메시지 순서 보장, 트래픽 적음 |
| 파티션 키 | 없음 | 파티션이 1개이므로 키 불필요 |
| 메시지 형식 | JSON | |
| 주요 필드 | original_topic, transaction_id, error_message, failed_at, payload | |

### 메시지 예시
```json
{
  "originalTopic": "transactions",
  "transactionId": "uuid-1234",
  "errorMessage": "ML server timeout",
  "failedAt": "2026-03-15T09:00:02",
  "payload": {
    "transactionId": "uuid-1234",
    "userId": "user-5678",
    "amount": 150000.00
  }
}
```

---

## 설계 결정 근거

- `파티션 키 user_id`: 동일 사용자의 거래를 항상 같은 파티션으로 라우팅하여 고빈도 패턴 탐지 정확도 확보
- `transactions / fraud-alerts 파티션 3개`: 현재 Consumer 1개이나 향후 확장 여지 확보
- `dlq 파티션 1개`: 실패 메시지는 발생 순서대로 재처리해야 원인 분석이 정확하므로 순서 보장 우선
- `payload 필드`: DLQ에 원본 메시지를 그대로 보관하여 장애 복구 후 재처리 가능하도록 설계