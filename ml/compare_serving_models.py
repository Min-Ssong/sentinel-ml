"""
서빙 모델(sentinel-fraud/latest = v4)과 DAG champion(sentinel-fraud-dag@champion)의
holdout(Time 정렬 후 마지막 20%) 성능 비교 — 읽기 전용 (MLflow·파일에 아무것도 쓰지 않음)

실행 위치: ml/  (serving/app.py 와 같이 scaler.pkl 을 상대 경로로 읽음)
  cd D:\\DevStudy\\projects\\sentinel-ml\\ml
  .venv\\Scripts\\Activate.ps1
  python compare_serving_models.py

자기 검증: champion(+재현 scaler)의 AUC가 DAG 기록값 0.9824 와 같으면 scaler 재현이 맞은 것
"""
import os

import joblib
import mlflow
import pandas as pd
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler

mlflow.set_tracking_uri(os.getenv("MLFLOW_URI", "http://localhost:5000"))

df = pd.read_csv(os.path.join("data", "creditcard.csv")).sort_values("Time").reset_index(drop=True)
holdout_start = int(len(df) * 0.8)
train_pool = df.iloc[:holdout_start]            # DAG 최종 단계(batch1~3) 학습 구간
holdout = df.iloc[holdout_start:]
y = holdout["Class"].values
X_raw = holdout.drop("Class", axis=1)


def scaled(X, scaler):
    X = X.copy()
    X[["Amount", "Time"]] = scaler.transform(X[["Amount", "Time"]])
    return X


# 서빙과 같은 조합: sentinel-fraud/latest + scaler.pkl (train.py 랜덤 분할에서 fit)
m_serving = mlflow.xgboost.load_model("models:/sentinel-fraud/latest")
s_serving = joblib.load("scaler.pkl")

# DAG champion + DAG 방식으로 재현한 scaler (학습 풀 전체에 fit). DAG는 scaler를 저장하지 않으므로 재현값
m_champ = mlflow.xgboost.load_model("models:/sentinel-fraud-dag@champion")
s_champ = StandardScaler().fit(train_pool[["Amount", "Time"]])

cases = [
    ("serving v4 + scaler.pkl", m_serving, s_serving),
    ("champion + 재현 scaler", m_champ, s_champ),
    ("champion + scaler.pkl (서빙 경로만 바꿨을 때)", m_champ, s_serving),
]

print(f"holdout rows={len(holdout)}, fraud={int(y.sum())}")
for name, model, scaler in cases:
    p = model.predict_proba(scaled(X_raw, scaler))[:, 1]
    parts = [f"AUC={roc_auc_score(y, p):.4f}"]
    for t in (0.5, 0.8):
        pred = p >= t
        tp = int((pred & (y == 1)).sum())
        fn = int((~pred & (y == 1)).sum())
        fp = int((pred & (y == 0)).sum())
        parts.append(f"t={t}: recall={tp / (tp + fn):.4f} (tp={tp}, fn={fn}, fp={fp})")
    print(f"{name} | " + " | ".join(parts))
