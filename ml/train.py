import pandas as pd
import joblib
import os
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (precision_score, recall_score, f1_score,
                             roc_auc_score, classification_report, confusion_matrix)
from xgboost import XGBClassifier
import mlflow
import mlflow.xgboost

# === 1. 데이터 준비 ===
df = pd.read_csv("data/creditcard.csv")
X = df.drop("Class", axis=1)
y = df["Class"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)
X_train, X_test = X_train.copy(), X_test.copy()

scaler = StandardScaler()
X_train[["Amount", "Time"]] = scaler.fit_transform(X_train[["Amount", "Time"]])
X_test[["Amount", "Time"]]  = scaler.transform(X_test[["Amount", "Time"]])
joblib.dump(scaler, "scaler.pkl")
scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()

# === 2. MLflow 설정 ===
mlflow.set_tracking_uri(os.getenv("MLFLOW_URI", "http://localhost:5000"))
exp_name = "credit-card-fraud-final"
if mlflow.get_experiment_by_name(exp_name) is None:
    mlflow.create_experiment(exp_name, artifact_location="mlflow-artifacts:/")
mlflow.set_experiment(exp_name)

params = {
    "n_estimators": 100,
    "max_depth": 4,
    "learning_rate": 0.1,
    "scale_pos_weight": scale_pos_weight,
    "eval_metric": "logloss",
    "random_state": 42,
}

# === 3. 학습 + 기록 ===
with mlflow.start_run(run_name="xgboost-baseline"):
    model = XGBClassifier(**params)
    model.fit(X_train, y_train)

    y_pred  = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    metrics = {
        "precision": precision_score(y_test, y_pred),
        "recall":    recall_score(y_test, y_pred),
        "f1":        f1_score(y_test, y_pred),
        "roc_auc":   roc_auc_score(y_test, y_proba),
    }

    mlflow.log_params(params)
    mlflow.log_metrics(metrics)
    mlflow.xgboost.log_model(model, "model", registered_model_name="sentinel-fraud")

    print(confusion_matrix(y_test, y_pred))
    print(classification_report(y_test, y_pred, digits=4))
    print("기록된 지표:", {k: round(v, 4) for k, v in metrics.items()})

print("MLflow UI 확인: os.getenv(MLFLOW_URI, http://localhost:5000)")