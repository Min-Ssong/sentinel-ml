"""
sentinel_retrain_drift — 드리프트 대응 재학습 DAG (1단계)

판정 DAG(sentinel_drift_monitor)가 Recall < 임계값일 때 호출한다.
기존 sentinel_retrain(3단계 누적 재연, 8/22 검증)은 그대로 두고, 목적이 다른 재학습을 별도 DAG로 분리했다 (2026-10-05).

데이터 (시뮬레이션 — 라벨 즉시 도착 가정)
  - 학습 = 원본 학습 풀(Time 정렬 후 앞 80%) + 드리프트 파일 학습 풀 구간의 사기 행(변형본)
  - 평가 = 드리프트 파일의 holdout(뒤 20%) — 시뮬레이터가 보내는 구간과 같음
  - 드리프트 파일은 ml/make_drift_files.py 가 만든 것 (기본 creditcard_s2.csv, 합성 시나리오)
게이트
  - champion 도 같은 평가 데이터로 다시 평가해 ROC-AUC 비교 (tolerance 0.0)
    (기존 DAG처럼 champion run 의 기록값과 비교하면 서로 다른 평가셋 점수를 비교하게 됨)
  - champion 전처리는 champion run 의 scaler/scaler.json 사용. 없으면 실패 처리
등록
  - 통과 시 sentinel-fraud-dag 에 등록하고 @champion 승격, scaler 는 학습 run 에 함께 저장
  - 서빙(FastAPI)은 기동 시 1회 로드하므로 재기동해야 반영된다
"""
import os
import sys
from datetime import datetime

from airflow.sdk import dag, task

sys.path.append("/opt/airflow/ml")

ML_DIR = "/opt/airflow/ml"
CSV_PATH = f"{ML_DIR}/data/creditcard.csv"
MLFLOW_URI = os.getenv("MLFLOW_URI", "http://host.docker.internal:5000")
EXPERIMENT = "sentinel-retrain-drift"
MODEL_NAME = "sentinel-fraud-dag"
COLS = ["Amount", "Time"]


def _load_data(drift_name):
    """(학습 데이터, 드리프트 평가 데이터, 원본 holdout, 추가한 사기 수) 반환."""
    import pandas as pd

    orig = pd.read_csv(CSV_PATH).sort_values("Time").reset_index(drop=True)
    cut = int(len(orig) * 0.8)
    drift = (pd.read_csv(os.path.join(ML_DIR, "data", drift_name))
             .sort_values("Time", kind="stable").reset_index(drop=True))
    if len(drift) != len(orig):
        raise ValueError(f"드리프트 파일 행 수가 원본과 다름: {len(drift)} != {len(orig)}")

    drift_train = drift.iloc[:cut]
    new_fraud = drift_train[drift_train["Class"] == 1]
    train = pd.concat([orig.iloc[:cut], new_fraud], ignore_index=True)
    return (train, drift.iloc[cut:].reset_index(drop=True),
            orig.iloc[cut:].reset_index(drop=True), len(new_fraud))


def _scale(df, sc):
    """StandardScaler.transform 과 같은 수식 (x - mean) / scale. 서빙과 같은 방식."""
    X = df.drop("Class", axis=1).copy()
    for col, m, s in zip(sc["columns"], sc["mean"], sc["scale"]):
        X[col] = (X[col] - m) / s
    return X


@dag(
    dag_id="sentinel_retrain_drift",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"drift_csv": "creditcard_s2.csv"},  # ml/data/ 아래 파일명
    tags=["sentinel-ml", "mlops", "drift"],
)
def sentinel_retrain_drift():

    @task
    def train_drift(**context) -> dict:
        import mlflow
        from sklearn.metrics import precision_score, recall_score, roc_auc_score
        from sklearn.preprocessing import StandardScaler
        from xgboost import XGBClassifier

        drift_name = os.path.basename(str(context["params"]["drift_csv"]))
        train, ev, orig_ho, n_new = _load_data(drift_name)

        scaler = StandardScaler().fit(train[COLS])
        sc = {"columns": COLS, "mean": scaler.mean_.tolist(), "scale": scaler.scale_.tolist()}
        y_tr = train["Class"]
        spw = (y_tr == 0).sum() / (y_tr == 1).sum()
        params = {  # retrain_dag.py 와 같은 설정
            "n_estimators": 100, "max_depth": 4, "learning_rate": 0.1,
            "scale_pos_weight": spw, "eval_metric": "logloss", "random_state": 42,
        }

        mlflow.set_tracking_uri(MLFLOW_URI)
        if mlflow.get_experiment_by_name(EXPERIMENT) is None:
            mlflow.create_experiment(EXPERIMENT, artifact_location="mlflow-artifacts:/")
        mlflow.set_experiment(EXPERIMENT)

        with mlflow.start_run(run_name=f"drift-{drift_name}") as run:
            model = XGBClassifier(**params)
            model.fit(_scale(train, sc), y_tr)

            def scores(d):
                y = d["Class"]
                proba = model.predict_proba(_scale(d, sc))[:, 1]
                pred = (proba >= 0.5).astype(int)
                return (roc_auc_score(y, proba), recall_score(y, pred),
                        precision_score(y, pred, zero_division=0))

            auc, rec, prec = scores(ev)
            o_auc, o_rec, _ = scores(orig_ho)
            metrics = {
                "roc_auc": auc, "recall": rec, "precision": prec,          # 드리프트 평가분 기준
                "orig_holdout_roc_auc": o_auc, "orig_holdout_recall": o_rec,  # 옛 패턴 유지 확인
                "train_rows": len(train), "train_fraud": int((y_tr == 1).sum()),
                "added_fraud": n_new,
            }
            mlflow.log_params({**params, "drift_csv": drift_name})
            mlflow.log_metrics(metrics)
            mlflow.set_tag("stage", "drift")
            mlflow.set_tag("drift_csv", drift_name)
            mlflow.xgboost.log_model(model, name="model")
            mlflow.log_dict(sc, "scaler/scaler.json")

            print(f"[train_drift] drift_csv={drift_name} rows={len(train)} added_fraud={n_new} "
                  f"평가분 ROC-AUC={auc:.4f} Recall={rec:.4f} | 원본 holdout ROC-AUC={o_auc:.4f} Recall={o_rec:.4f}")
            return {"run_id": run.info.run_id, "drift_csv": drift_name}

    @task
    def gate_drift(train_result: dict) -> dict:
        import json

        import mlflow
        import mlflow.artifacts
        from mlflow.tracking import MlflowClient
        from sklearn.metrics import roc_auc_score

        from gate import evaluate_gate

        mlflow.set_tracking_uri(MLFLOW_URI)
        client = MlflowClient()
        challenger = client.get_run(train_result["run_id"]).data.metrics["roc_auc"]

        champion = None
        try:
            mv = client.get_model_version_by_alias(MODEL_NAME, "champion")
        except Exception as e:  # noqa: BLE001
            mv = None
            print(f"[gate_drift] @champion 없음(최초 등록으로 간주): {e}")

        if mv is not None:
            _, ev, _, _ = _load_data(train_result["drift_csv"])
            champ_model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}/{mv.version}")
            path = mlflow.artifacts.download_artifacts(run_id=mv.run_id, artifact_path="scaler/scaler.json")
            with open(path, encoding="utf-8") as f:
                champ_sc = json.load(f)
            champion = roc_auc_score(ev["Class"], champ_model.predict_proba(_scale(ev, champ_sc))[:, 1])
            print(f"[gate_drift] champion=v{mv.version} 같은 평가분 재평가 roc_auc={champion:.4f}")
            try:  # 추적용 기록. 실패해도 판정은 계속
                client.log_metric(train_result["run_id"], "champion_roc_auc_same_eval", champion)
            except Exception as e:  # noqa: BLE001
                print(f"[gate_drift] champion 점수 기록 실패(판정은 계속): {e}")

        passed, reason = evaluate_gate(challenger, champion, tolerance=0.0)
        print(f"[gate_drift] {reason}")
        if not passed:
            raise ValueError(f"게이트 차단(drift): {reason}")
        return {"run_id": train_result["run_id"], "roc_auc": challenger}

    @task
    def register_drift(gate_result: dict) -> str:
        import mlflow
        from mlflow.tracking import MlflowClient

        mlflow.set_tracking_uri(MLFLOW_URI)
        client = MlflowClient()
        mv = mlflow.register_model(f"runs:/{gate_result['run_id']}/model", MODEL_NAME)
        client.set_registered_model_alias(MODEL_NAME, "champion", mv.version)
        print(f"[register_drift] {MODEL_NAME} v{mv.version} 등록 → @champion 승격 "
              f"(roc_auc={gate_result['roc_auc']:.4f}). 서빙 반영은 FastAPI 재기동 필요")
        return f"v{mv.version}"

    register_drift(gate_drift(train_drift()))


dag_instance = sentinel_retrain_drift()