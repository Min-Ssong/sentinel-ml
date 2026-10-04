"""
Sentinel-ML 재학습 파이프라인 (3단계 누적)

Time 순서 3분할 → 누적 재학습(b1 → b1+b2 → b1+b2+b3).
각 단계마다 게이트 판정, 통과 시 모델 등록 후 다음 단계로.

설계 근거 (2026-08-07 확정):
- Time 순서 분할, 뒤 20%는 고정 holdout (모든 단계 공통 평가셋)
- 게이트 지표 ROC-AUC, tolerance 0.0
- 태스크 간 전달은 run_id만 XCom, 지표는 MLflow에서 조회
"""
import os
import sys
from datetime import datetime

from airflow.sdk import dag, task

sys.path.append("/opt/airflow/ml")

ML_DIR = "/opt/airflow/ml"
CSV_PATH = f"{ML_DIR}/data/creditcard.csv"
MLFLOW_URI = os.getenv("MLFLOW_URI", "http://host.docker.internal:5000")
EXPERIMENT = "sentinel-retrain-dag-v2"
MODEL_NAME = "sentinel-fraud-dag"


@dag(
    dag_id="sentinel_retrain",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["sentinel-ml", "mlops"],
)
def sentinel_retrain():

    @task
    def prepare_data() -> dict:
        """Time 순서 분할 → 배치 경계와 holdout 시작점 계산."""
        import pandas as pd

        df = pd.read_csv(CSV_PATH).sort_values("Time").reset_index(drop=True)
        n = len(df)
        holdout_start = int(n * 0.8)
        m = holdout_start

        info = {
            "total_rows": n,
            "holdout_start": holdout_start,
            "b1_end": m // 3,
            "b2_end": 2 * m // 3,
            "b3_end": m,
            "holdout_fraud": int(df.iloc[holdout_start:]["Class"].sum()),
        }
        print(f"[prepare_data] {info}")
        return info

    @task
    def train_batch(info: dict, train_end: int, stage: str) -> str:
        """0 ~ train_end 행으로 누적 학습, 고정 holdout 평가, MLflow 기록."""
        import pandas as pd
        import mlflow
        from sklearn.preprocessing import StandardScaler
        from sklearn.metrics import roc_auc_score, recall_score, precision_score
        from xgboost import XGBClassifier

        df = pd.read_csv(CSV_PATH).sort_values("Time").reset_index(drop=True)
        train_df = df.iloc[0:train_end]
        holdout = df.iloc[info["holdout_start"]:]

        X_tr = train_df.drop("Class", axis=1).copy()
        y_tr = train_df["Class"]
        X_ev = holdout.drop("Class", axis=1).copy()
        y_ev = holdout["Class"]

        scaler = StandardScaler()
        X_tr[["Amount", "Time"]] = scaler.fit_transform(X_tr[["Amount", "Time"]])
        X_ev[["Amount", "Time"]] = scaler.transform(X_ev[["Amount", "Time"]])

        spw = (y_tr == 0).sum() / (y_tr == 1).sum()
        params = {
            "n_estimators": 100, "max_depth": 4, "learning_rate": 0.1,
            "scale_pos_weight": spw, "eval_metric": "logloss", "random_state": 42,
        }

        mlflow.set_tracking_uri(MLFLOW_URI)
        if mlflow.get_experiment_by_name(EXPERIMENT) is None:
            mlflow.create_experiment(EXPERIMENT, artifact_location="mlflow-artifacts:/")
        mlflow.set_experiment(EXPERIMENT)

        with mlflow.start_run(run_name=f"dag-{stage}") as run:
            model = XGBClassifier(**params)
            model.fit(X_tr, y_tr)

            proba = model.predict_proba(X_ev)[:, 1]
            pred = model.predict(X_ev)
            metrics = {
                "roc_auc": roc_auc_score(y_ev, proba),
                "recall": recall_score(y_ev, pred),
                "precision": precision_score(y_ev, pred, zero_division=0),
                "train_rows": len(train_df),
                "train_fraud": int((y_tr == 1).sum()),
            }
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            mlflow.set_tag("stage", stage)
            mlflow.xgboost.log_model(model, name="model")

            print(f"[train_batch:{stage}] rows={len(train_df)} "
                  f"fraud={metrics['train_fraud']} "
                  f"ROC-AUC={metrics['roc_auc']:.4f} Recall={metrics['recall']:.4f}")
            return run.info.run_id

    @task
    def gate_check(mlflow_run_id: str, stage: str) -> dict:
        """@champion alias가 가리키는 버전과 비교. 미달 시 예외로 중단."""
        import mlflow
        from mlflow.tracking import MlflowClient
        from gate import evaluate_gate

        mlflow.set_tracking_uri(MLFLOW_URI)
        client = MlflowClient()

        challenger = client.get_run(mlflow_run_id).data.metrics["roc_auc"]

        champion = None
        try:
            champ_mv = client.get_model_version_by_alias(MODEL_NAME, "champion")
            champion = client.get_run(champ_mv.run_id).data.metrics.get("roc_auc")
            print(f"[gate_check:{stage}] champion=v{champ_mv.version} roc_auc={champion}")
        except Exception as e:
            print(f"[gate_check:{stage}] @champion 없음(최초 등록으로 간주): {e}")

        passed, reason = evaluate_gate(challenger, champion, tolerance=0.0)
        print(f"[gate_check:{stage}] {reason}")

        if not passed:
            raise ValueError(f"게이트 차단({stage}): {reason}")

        return {"mlflow_run_id": mlflow_run_id, "roc_auc": challenger, "stage": stage}

    @task
    def register_model(gate_result: dict) -> str:
        """게이트 통과 모델을 등록하고 @champion alias를 새 버전으로 승격."""
        import mlflow
        from mlflow.tracking import MlflowClient

        mlflow.set_tracking_uri(MLFLOW_URI)
        client = MlflowClient()

        model_uri = f"runs:/{gate_result['mlflow_run_id']}/model"
        mv = mlflow.register_model(model_uri, MODEL_NAME)

        # 게이트를 통과했으므로 새 champion으로 승격
        client.set_registered_model_alias(MODEL_NAME, "champion", mv.version)

        print(f"[register_model:{gate_result['stage']}] {MODEL_NAME} v{mv.version} 등록 "
              f"→ @champion 승격 (roc_auc={gate_result['roc_auc']:.4f})")
        return f"v{mv.version}"

    info = prepare_data()

    # 1단계: b1
    r1 = train_batch(info, info["b1_end"], "batch1")
    g1 = gate_check(r1, "batch1")
    reg1 = register_model(g1)

    # 2단계: b1+b2 (1단계 등록 완료 후)
    r2 = train_batch(info, info["b2_end"], "batch1-2")
    r2.set_upstream(reg1)
    g2 = gate_check(r2, "batch1-2")
    reg2 = register_model(g2)

    # 3단계: b1+b2+b3 (2단계 등록 완료 후)
    r3 = train_batch(info, info["b3_end"], "batch1-2-3")
    r3.set_upstream(reg2)
    g3 = gate_check(r3, "batch1-2-3")
    register_model(g3)


dag_instance = sentinel_retrain()