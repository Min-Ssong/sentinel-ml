"""
드리프트 주입 오프라인 실험 — 읽기 전용 (MLflow·파일에 아무것도 쓰지 않음)

실행 위치: ml/  (drift.py 를 import 하고 data/creditcard.csv 를 읽음)
  cd D:\\DevStudy\\projects\\sentinel-ml\\ml
  .venv\\Scripts\\Activate.ps1
  python drift_experiment.py

S1 Data drift : holdout 전체 행의 피처 하나를 변형 → PSI 와 champion 성능 변화
S2 Concept drift (a, 합성): 사기 행의 주요 피처를 정상 평균 쪽으로 alpha 만큼 이동 + 새 신호 1개 추가
   → champion Recall 하락 여부 / 변형 데이터로 재학습한 challenger 의 회복 여부 /
     게이트(같은 평가셋에서 AUC 비교) 통과 여부 / PSI 에 잡히는지
"""
import os

import mlflow
import pandas as pd
from sklearn.base import clone
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler

from drift import build_baseline, calculate_psi

mlflow.set_tracking_uri(os.getenv("MLFLOW_URI", "http://localhost:5000"))

PSI_FEATURES = ["V14", "V10", "V4", "V12", "V8", "Amount"]  # 판정 DAG 와 동일
S2_SHIFT_FEATURES = ["V14", "V12", "V10", "V4"]
S2_ALPHAS = [0.5, 0.75, 0.9]      # 정상 평균 쪽 이동 비율
S2_SIGNAL_STD = 3.0               # 새 신호 크기 (정상 표준편차 배수)
S1_CASES = [("Amount", "scale", 1.5), ("Amount", "scale", 2.0),
            ("V14", "shift_std", 0.5), ("V14", "shift_std", 1.0)]
THRESH = 0.5                      # TransactionConsumer.PREDICTION_THRESHOLD

df = pd.read_csv(os.path.join("data", "creditcard.csv")).sort_values("Time").reset_index(drop=True)
cut = int(len(df) * 0.8)
train_pool = df.iloc[:cut].reset_index(drop=True)   # champion(v12) 학습 구간
holdout = df.iloc[cut:].reset_index(drop=True)
normals = train_pool[train_pool["Class"] == 0]
normal_mean = normals.mean(numeric_only=True)
normal_std = normals.std(numeric_only=True)

champ = mlflow.xgboost.load_model("models:/sentinel-fraud-dag@champion")
champ_scaler = StandardScaler().fit(train_pool[["Amount", "Time"]])  # DAG 방식 재현
baselines = {f: build_baseline(train_pool[f].values) for f in PSI_FEATURES}
cols = list(holdout.drop("Class", axis=1).columns)


def prep(d, scaler):
    X = d.drop("Class", axis=1).copy()
    X[["Amount", "Time"]] = scaler.transform(X[["Amount", "Time"]])
    return X


def evaluate(model, scaler, d):
    p = model.predict_proba(prep(d, scaler))[:, 1]
    y = d["Class"].values
    pred = p >= THRESH
    tp = int((pred & (y == 1)).sum())
    fn = int((~pred & (y == 1)).sum())
    return roc_auc_score(y, p), tp / (tp + fn), tp, fn


def psi_str(current):
    return " ".join(f"{f}={calculate_psi(baselines[f], current[f].values):.3f}" for f in PSI_FEATURES)


auc, rec, tp, fn = evaluate(champ, champ_scaler, holdout)
print(f"[기준] champion holdout: AUC={auc:.4f} Recall@{THRESH}={rec:.4f} ({tp}/{tp + fn})")
print(f"[기준] PSI: {psi_str(holdout)}")

# ---------------- S1 ----------------
print("\n=== S1 Data drift (holdout 전체 행 변형) ===")
for feat, kind, k in S1_CASES:
    d = holdout.copy()
    if kind == "scale":
        d[feat] = d[feat] * k
    else:
        d[feat] = d[feat] + k * normal_std[feat]
    auc, rec, tp, fn = evaluate(champ, champ_scaler, d)
    psi = calculate_psi(baselines[feat], d[feat].values)
    print(f"{feat} {kind} {k}: PSI[{feat}]={psi:.3f} | champion AUC={auc:.4f} Recall={rec:.4f} ({tp}/{tp + fn})")

# ---------------- S2 ----------------
imp = pd.Series(champ.feature_importances_, index=cols)
cand = imp[[c for c in cols if c.startswith("V") and c not in PSI_FEATURES]]
signal = cand.idxmin()
print(f"\n=== S2 Concept drift (a, 합성) — 신호 피처 {signal} (champion 중요도 {cand.min():.4f}, 후보 중 최저) ===")

params = champ.get_params()
shown = {k: params.get(k) for k in ("n_estimators", "max_depth", "learning_rate", "scale_pos_weight", "random_state")}
print(f"challenger 파라미터 (champion 에서 복제, DAG 값과 같은지 확인 필요): {shown}")


def s2(d, alpha):
    d = d.copy()
    m = d["Class"] == 1
    for f in S2_SHIFT_FEATURES:
        d.loc[m, f] = d.loc[m, f] + alpha * (normal_mean[f] - d.loc[m, f])
    d.loc[m, signal] = d.loc[m, signal] + S2_SIGNAL_STD * normal_std[signal]
    return d


for a in S2_ALPHAS:
    ev = s2(holdout, a)                                    # 평가분: holdout 사기 75건 변형
    tr_new = s2(train_pool[train_pool["Class"] == 1], a)   # 학습분: 학습 풀 사기 변형
    train = pd.concat([train_pool, tr_new], ignore_index=True)
    sc = StandardScaler().fit(train[["Amount", "Time"]])
    ch = clone(champ)
    ch.fit(prep(train, sc), train["Class"].values)

    c_auc, c_rec, c_tp, c_fn = evaluate(champ, champ_scaler, ev)
    n_auc, n_rec, n_tp, n_fn = evaluate(ch, sc, ev)
    o_auc, o_rec, o_tp, o_fn = evaluate(ch, sc, holdout)
    gate = "통과" if n_auc >= c_auc else "차단"
    print(f"\nalpha={a} (학습분 사기 {len(tr_new)}건 추가)")
    print(f"  champion  @평가분: AUC={c_auc:.4f} Recall={c_rec:.4f} ({c_tp}/{c_tp + c_fn})")
    print(f"  challenger@평가분: AUC={n_auc:.4f} Recall={n_rec:.4f} ({n_tp}/{n_tp + n_fn})  → 게이트(AUC, tol 0) {gate}")
    print(f"  challenger@원본 holdout: AUC={o_auc:.4f} Recall={o_rec:.4f} ({o_tp}/{o_tp + o_fn})  (옛 패턴 유지 확인)")
    print(f"  PSI(평가분 전체 행): {psi_str(ev)}")
