import json
import os

import mlflow
import mlflow.artifacts
import pandas as pd
from fastapi import FastAPI, Header
from mlflow.tracking import MlflowClient
from pydantic import BaseModel, ConfigDict

# --- 서버 시작 시 1회 로드 (매 요청마다 로드하면 느림) ---
# 모델과 전처리(scaler)를 같은 alias 버전에서 함께 로드한다.
# 재학습 DAG가 alias 를 바꿔도 이 서버는 재기동 전까지 반영하지 않는다.
MODEL_NAME = os.getenv("SERVING_MODEL_NAME", "sentinel-fraud-dag")
MODEL_ALIAS = os.getenv("SERVING_MODEL_ALIAS", "champion")

mlflow.set_tracking_uri(os.getenv("MLFLOW_URI", "http://localhost:5000"))
_mv = MlflowClient().get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
# alias 를 한 번만 조회해 그 버전 번호로 모델·scaler 를 모두 로드 (조회 사이에 alias 가 바뀌어도 어긋나지 않음)
model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}/{_mv.version}")
_scaler_path = mlflow.artifacts.download_artifacts(
    run_id=_mv.run_id, artifact_path="scaler/scaler.json"
)
with open(_scaler_path, encoding="utf-8") as f:
    SCALER = json.load(f)  # {"columns": [...], "mean": [...], "scale": [...]}

MODEL_INFO = {
    "name": MODEL_NAME,
    "alias": MODEL_ALIAS,
    "version": str(_mv.version),
    "run_id": _mv.run_id,
}
print(f"[serving] loaded {MODEL_INFO}")

# 재학습 DAG(train_batch)와 동일한 컬럼 순서
FEATURES = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]

app = FastAPI(title="Sentinel-ML Inference Server")


def _scale(df: pd.DataFrame) -> pd.DataFrame:
    """StandardScaler.transform 과 같은 수식: (x - mean) / scale"""
    for col, mean, scale in zip(SCALER["columns"], SCALER["mean"], SCALER["scale"]):
        df[col] = (df[col] - mean) / scale
    return df


class PredictRequest(BaseModel):
    transaction_id: str
    features: list[float]    # 30개, FEATURES 순서

class PredictResponse(BaseModel):
    # pydantic v2 는 "model_" 로 시작하는 필드에 경고를 내므로 보호 네임스페이스를 해제
    model_config = ConfigDict(protected_namespaces=())

    transaction_id: str
    fraud_score: float
    # 이 점수를 실제로 낸 모델 — API 가 FRAUD_RESULTS.MODEL_ID 를 정할 때 사용 (DB2)
    model_name: str
    model_version: str
    run_id: str

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/model-info")
def model_info():
    return MODEL_INFO

@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest, x_transaction_id: str | None = Header(default=None)):
    print(f"[predict] txId={x_transaction_id} transaction_id={req.transaction_id}")
    df = _scale(pd.DataFrame([req.features], columns=FEATURES))   # 학습과 동일 스케일링
    score = float(model.predict_proba(df)[:, 1][0])               # 사기 확률(클래스 1)
    return PredictResponse(
        transaction_id=req.transaction_id,
        fraud_score=score,
        model_name=MODEL_INFO["name"],
        model_version=MODEL_INFO["version"],
        run_id=MODEL_INFO["run_id"],
    )
