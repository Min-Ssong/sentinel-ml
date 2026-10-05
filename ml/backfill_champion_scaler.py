"""
champion run 에 scaler(JSON) 보충 — 1회성

배경: retrain_dag.py 는 학습 때마다 StandardScaler 를 fit 하지만 저장하지 않아
      @champion(v12) 의 run 에는 scaler 가 없다. 서빙이 모델과 같은 전처리를 쓰려면 필요.

방법: DAG 최종 단계(batch1-2-3)와 같은 학습 구간(Time 정렬 후 앞 80%)에 scaler 를 다시 fit 하고,
      champion run 에 scaler/scaler.json (mean, scale) 으로 저장한다.

검증: 재현 scaler 로 champion 을 holdout 에서 평가한 AUC 가
      champion run 에 기록된 roc_auc 와 같아야 한다 (허용 오차 1e-6).
      ※ scaler.pkl(train.py) 로 평가한 AUC 도 함께 출력 — 두 값 차이로 재현이 맞는지 판별한다.

실행 위치: ml/
  python backfill_champion_scaler.py            # dry-run (검증만, 아무것도 쓰지 않음)
  python backfill_champion_scaler.py --apply    # 검증 통과 시 champion run 에 업로드
"""
import os
import sys

import joblib
import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.metrics import roc_auc_score

MLFLOW_URI = os.getenv("MLFLOW_URI", "http://localhost:5000")
MODEL_NAME = "sentinel-fraud-dag"
TOL = 1e-6
COLS = ["Amount", "Time"]
APPLY = "--apply" in sys.argv

mlflow.set_tracking_uri(MLFLOW_URI)
client = MlflowClient()

mv = client.get_model_version_by_alias(MODEL_NAME, "champion")
run = client.get_run(mv.run_id)
stage = run.data.tags.get("stage")
train_rows = int(run.data.metrics["train_rows"])
logged_auc = run.data.metrics["roc_auc"]
print(f"champion: v{mv.version} run_id={mv.run_id} stage={stage} train_rows={train_rows} logged_roc_auc={logged_auc!r}")

df = pd.read_csv(os.path.join("data", "creditcard.csv")).sort_values("Time").reset_index(drop=True)
holdout_start = int(len(df) * 0.8)
if stage != "batch1-2-3" or train_rows != holdout_start:
    sys.exit(f"[중단] 최종 단계가 아님: stage={stage}, train_rows={train_rows}, holdout_start={holdout_start}")

train = df.iloc[:train_rows]
holdout = df.iloc[holdout_start:]
mean = train[COLS].mean()          # StandardScaler 와 동일: 모집단 표준편차(ddof=0)
scale = train[COLS].std(ddof=0)
params = {"columns": COLS, "mean": [float(mean[c]) for c in COLS], "scale": [float(scale[c]) for c in COLS]}
print(f"재현 scaler: {params}")

model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}@champion")
y = holdout["Class"].values


def auc_with(mean_vals, scale_vals):
    X = holdout.drop("Class", axis=1).copy()
    for i, c in enumerate(COLS):
        X[c] = (X[c] - mean_vals[i]) / scale_vals[i]
    return roc_auc_score(y, model.predict_proba(X)[:, 1])


auc_new = auc_with(params["mean"], params["scale"])
old = joblib.load("scaler.pkl")  # train.py 가 랜덤 80% 에 fit 한 서빙용 scaler
auc_old = auc_with(list(old.mean_), list(old.scale_))
diff_new, diff_old = abs(auc_new - logged_auc), abs(auc_old - logged_auc)
print(f"재현 scaler AUC={auc_new!r}  (기록값과 차이 {diff_new:.3e})")
print(f"scaler.pkl AUC={auc_old!r}  (기록값과 차이 {diff_old:.3e})")

if diff_new > TOL:
    sys.exit(f"[중단] 재현 scaler 의 AUC 가 기록값과 {TOL} 보다 크게 다름 — 업로드하지 않음")
if diff_old <= TOL:
    print("[주의] scaler.pkl 도 기록값과 같아 이 검증은 두 scaler 를 구분하지 못함 (재현 근거는 학습 구간 코드뿐)")
print("검증 통과")

if not APPLY:
    print("dry-run 종료 — 업로드하려면 --apply")
    sys.exit(0)

existing = [a.path for a in client.list_artifacts(mv.run_id, "scaler")]
if existing:
    sys.exit(f"[중단] 이미 scaler 아티팩트가 있음: {existing}")
client.log_dict(mv.run_id, params, "scaler/scaler.json")
print("업로드 후 확인:", [a.path for a in client.list_artifacts(mv.run_id, "scaler")])
