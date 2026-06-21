import pandas as pd
import joblib
import mlflow
from fastapi import FastAPI, Header
from pydantic import BaseModel

# --- 서버 시작 시 1회 로드 (매 요청마다 로드하면 느림) ---
mlflow.set_tracking_uri("http://localhost:5000")
model = mlflow.xgboost.load_model("models:/sentinel-fraud/latest")
scaler = joblib.load("scaler.pkl")

# train.py와 동일한 컬럼 순서
FEATURES = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]

app = FastAPI(title="Sentinel-ML Inference Server")

class PredictRequest(BaseModel):
    transaction_id: str
    features: list[float]    # 30개, FEATURES 순서

class PredictResponse(BaseModel):
    transaction_id: str
    fraud_score: float

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest, x_transaction_id: str | None = Header(default=None)):
    print(f"[predict] txId={x_transaction_id} transaction_id={req.transaction_id}")
    df = pd.DataFrame([req.features], columns=FEATURES)
    df[["Amount", "Time"]] = scaler.transform(df[["Amount", "Time"]])   # 학습과 동일 스케일링
    score = float(model.predict_proba(df)[:, 1][0])                      # 사기 확률(클래스 1)
    return PredictResponse(transaction_id=req.transaction_id, fraud_score=score)