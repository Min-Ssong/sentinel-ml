import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score
from xgboost import XGBClassifier
import mlflow
import mlflow.xgboost

# === 1. 데이터 준비: Time 정렬 → holdout 분리 → 학습분 3분할 ===
df = pd.read_csv("data/creditcard.csv").sort_values("Time").reset_index(drop=True)
n = len(df)

# 뒤쪽 20%를 고정 평가셋(holdout)으로 분리 — 모든 배치를 같은 잣대로 평가
holdout_start = int(n * 0.8)
train_pool = df.iloc[:holdout_start].reset_index(drop=True)   # 앞 80%
holdout    = df.iloc[holdout_start:].reset_index(drop=True)   # 뒤 20% 고정

# 학습분(앞 80%)을 Time 순서로 3등분
m = len(train_pool)
b1 = train_pool.iloc[: m // 3]
b2 = train_pool.iloc[m // 3 : 2 * m // 3]
b3 = train_pool.iloc[2 * m // 3 :]

print(f"holdout: {len(holdout)}행, 사기 {(holdout['Class']==1).sum()}건")
for name, b in [("배치1", b1), ("배치2", b2), ("배치3", b3)]:
    print(f"{name}: {len(b)}행, 사기 {(b['Class']==1).sum()}건")

# holdout X/y 고정
X_hold = holdout.drop("Class", axis=1)
y_hold = holdout["Class"]

# === 2. MLflow 설정 ===
mlflow.set_tracking_uri("http://localhost:5000")
exp_name = "credit-card-fraud-retrain"
if mlflow.get_experiment_by_name(exp_name) is None:
    mlflow.create_experiment(exp_name, artifact_location="mlflow-artifacts:/")
mlflow.set_experiment(exp_name)


def train_and_eval(train_df, run_name):
    """train_df로 학습, 고정 holdout으로 평가, MLflow 기록."""
    X_tr = train_df.drop("Class", axis=1).copy()
    y_tr = train_df["Class"]

    # 스케일러는 매 배치의 학습분에 fit (data leakage 방지: holdout은 transform만)
    scaler = StandardScaler()
    X_tr[["Amount", "Time"]] = scaler.fit_transform(X_tr[["Amount", "Time"]])
    X_ev = X_hold.copy()
    X_ev[["Amount", "Time"]] = scaler.transform(X_ev[["Amount", "Time"]])

    spw = (y_tr == 0).sum() / (y_tr == 1).sum()
    params = {
        "n_estimators": 100, "max_depth": 4, "learning_rate": 0.1,
        "scale_pos_weight": spw, "eval_metric": "logloss", "random_state": 42,
    }

    with mlflow.start_run(run_name=run_name):
        model = XGBClassifier(**params)
        model.fit(X_tr, y_tr)

        y_pred = model.predict(X_ev)
        y_proba = model.predict_proba(X_ev)[:, 1]
        metrics = {
            "precision": precision_score(y_hold, y_pred, zero_division=0),
            "recall":    recall_score(y_hold, y_pred),
            "f1":        f1_score(y_hold, y_pred),
            "roc_auc":   roc_auc_score(y_hold, y_proba),
            "train_rows":  len(train_df),
            "train_fraud": int((y_tr == 1).sum()),
        }
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)

        print(f"\n[{run_name}] 학습 {len(train_df)}행(사기 {int((y_tr==1).sum())}) "
              f"→ holdout 평가")
        print(f"  ROC-AUC={metrics['roc_auc']:.4f}  Recall={metrics['recall']:.4f}  "
              f"Precision={metrics['precision']:.4f}  F1={metrics['f1']:.4f}")
        return metrics


# === 3. 누적 재학습 3회 ===
print("\n" + "=" * 50)
m1 = train_and_eval(b1,                       "initial-batch1")       # 초기학습 (champion 후보)
m2 = train_and_eval(pd.concat([b1, b2]),      "retrain-batch1-2")     # 재학습1 (누적)
m3 = train_and_eval(pd.concat([b1, b2, b3]),  "retrain-batch1-2-3")   # 재학습2 (누적)

# === 4. 배치별 성능 비교 요약 ===
print("\n" + "=" * 50)
print("누적 재학습 성능 추이 (고정 holdout 기준)")
print(f"{'단계':<20}{'ROC-AUC':>10}{'Recall':>10}{'Precision':>10}")
for name, m in [("초기(b1)", m1), ("재학습1(b1-2)", m2), ("재학습2(b1-3)", m3)]:
    print(f"{name:<20}{m['roc_auc']:>10.4f}{m['recall']:>10.4f}{m['precision']:>10.4f}")

print("\nMLflow UI: http://localhost:5000  (실험: credit-card-fraud-retrain)")