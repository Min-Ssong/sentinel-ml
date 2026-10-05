"""
드리프트 시나리오 csv 생성 (D1) — 원본 csv 는 수정하지 않고 새 파일 2개를 만든다

  data/creditcard_s1.csv   S1 Data drift : holdout(Time 정렬 후 뒤 20%) 전체 행의 V14 를
                           학습 풀 정상 표준편차의 +1.0σ 만큼 이동
  data/creditcard_s2.csv   S2 Concept drift (합성): 모든 사기 행의 V14·V12·V10·V4 를
                           학습 풀 정상 평균 쪽으로 75% 이동하고, V2 에 정상 표준편차의 +3σ 를 더함
                           ※ 실제 사기 패턴 변화가 아닌 합성 시나리오

공통 설계
  - 행 순서·컬럼·Time 값은 원본과 같다 → 시뮬레이터(파일 순서로 앞 80% 건너뜀)와
    DAG(Time 정렬 후 앞 80%)가 같은 holdout 구간을 읽는다.
  - 형식은 Kaggle 원본과 같게 쓴다: 헤더와 Class 값은 따옴표, 숫자는 따옴표 없음
    (시뮬레이터 로더가 Class 토큰을 unquote 하므로)
  - 기준 통계(정상 평균·표준편차)는 원본 학습 풀(앞 80%)의 정상 행에서 계산 (drift_experiment.py 와 동일)

자체 검증 (MLflow 가 떠 있어야 함): 파일을 다시 읽어 drift_experiment.py 의 결과와 같은지 확인
  S1: V14 PSI ≈ 0.904, champion AUC ≈ 0.9838, Recall 61/75
  S2: champion AUC ≈ 0.9408, Recall 27/75 (0.36), PSI(V12) ≈ 0.154

실행 위치: ml/
  python make_drift_files.py
"""
import csv
import os

import mlflow
import mlflow.artifacts
import json
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.metrics import roc_auc_score

from drift import build_baseline, calculate_psi

MLFLOW_URI = os.getenv("MLFLOW_URI", "http://localhost:5000")
MODEL_NAME = "sentinel-fraud-dag"
SRC = os.path.join("data", "creditcard.csv")
OUT_S1 = os.path.join("data", "creditcard_s1.csv")
OUT_S2 = os.path.join("data", "creditcard_s2.csv")

S1_FEATURE, S1_SIGMA = "V14", 1.0
S2_SHIFT_FEATURES = ["V14", "V12", "V10", "V4"]
S2_ALPHA = 0.75
S2_SIGNAL, S2_SIGNAL_STD = "V2", 3.0
PSI_FEATURES = ["V14", "V10", "V4", "V12", "V8", "Amount"]
THRESH = 0.5

# 시간 정렬 후 같은 순서로 쓴다 (원본이 이미 정렬돼 있으면 순서 변화 없음)
orig = pd.read_csv(SRC).sort_values("Time", kind="stable").reset_index(drop=True)
cut = int(len(orig) * 0.8)
train_pool = orig.iloc[:cut]
normals = train_pool[train_pool["Class"] == 0]
normal_mean = normals.mean(numeric_only=True)
normal_std = normals.std(numeric_only=True)   # drift_experiment.py 와 같은 표준편차(ddof=1)

# ---------------- S1 ----------------
s1 = orig.copy()
s1.loc[cut:, S1_FEATURE] = s1.loc[cut:, S1_FEATURE] + S1_SIGMA * normal_std[S1_FEATURE]

# ---------------- S2 ----------------
s2 = orig.copy()
fraud = s2["Class"] == 1
for f in S2_SHIFT_FEATURES:
    s2.loc[fraud, f] = s2.loc[fraud, f] + S2_ALPHA * (normal_mean[f] - s2.loc[fraud, f])
s2.loc[fraud, S2_SIGNAL] = s2.loc[fraud, S2_SIGNAL] + S2_SIGNAL_STD * normal_std[S2_SIGNAL]


def write(df, path):
    out = df.copy()
    out["Class"] = out["Class"].astype(int).astype(str)   # Kaggle 원본처럼 따옴표로 감싸지게 문자열로
    out.to_csv(path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    print(f"쓰기 완료: {path} ({len(out)}행)")


write(s1, OUT_S1)
write(s2, OUT_S2)

# ---------------- 자체 검증 ----------------
print("\n=== 자체 검증 (파일을 다시 읽어서 확인) ===")
mlflow.set_tracking_uri(MLFLOW_URI)
mv = MlflowClient().get_model_version_by_alias(MODEL_NAME, "champion")
model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}/{mv.version}")
with open(mlflow.artifacts.download_artifacts(run_id=mv.run_id, artifact_path="scaler/scaler.json"),
          encoding="utf-8") as f:
    sc = json.load(f)
print(f"champion v{mv.version}, scaler 로드 완료")

baselines = {f: build_baseline(train_pool[f].values) for f in PSI_FEATURES}


def evaluate(path, label, expected):
    d = pd.read_csv(path).sort_values("Time", kind="stable").reset_index(drop=True)
    assert len(d) == len(orig), "행 수가 다름"
    # 의도한 컬럼 외에는 원본과 같아야 함
    changed = [c for c in orig.columns if not (d[c].values == orig[c].values).all()]
    h = d.iloc[cut:]
    X = h.drop("Class", axis=1).copy()
    for col, m, s in zip(sc["columns"], sc["mean"], sc["scale"]):
        X[col] = (X[col] - m) / s
    p = model.predict_proba(X)[:, 1]
    y = h["Class"].values
    pred = p >= THRESH
    tp, fn = int((pred & (y == 1)).sum()), int((~pred & (y == 1)).sum())
    psi = {f: calculate_psi(baselines[f], h[f].values) for f in PSI_FEATURES}
    print(f"\n[{label}] 원본과 달라진 컬럼: {changed}")
    print(f"  holdout 행={len(h)}, 사기={int(y.sum())}")
    print(f"  champion AUC={roc_auc_score(y, p):.4f} (기대 {expected['auc']}), Recall={tp / (tp + fn):.4f} ({tp}/{tp + fn}, 기대 {expected['tp']}/75)")
    print("  PSI: " + " ".join(f"{k}={v:.3f}" for k, v in psi.items()) + f"   (기대 {expected['psi']})")


evaluate(OUT_S1, "S1", {"auc": 0.9838, "tp": 61, "psi": "V14≈0.904"})
evaluate(OUT_S2, "S2", {"auc": 0.9408, "tp": 27, "psi": "V12≈0.154, 나머지 기준과 비슷"})
