"""
sentinel_drift_monitor — Drift 판정 DAG

흐름
  compute_psi    ─┐
  fetch_recall   ─┼─> decide ──(True)──> trigger_retrain (sentinel_retrain_drift)
  check_cooldown ─┘

판정 규칙 (2026-10-05 변경)
  - PSI: 경보만. 피처 중 하나라도 level == "alert"(PSI_ALERT 이상)이면 로그([psi-alert])만 남기고
         재학습은 트리거하지 않는다. (S1 실험: PSI 0.904 에서도 champion 성능 변화 없음)
  - Recall: 판정 창(window) 안의 tp+fn >= min_samples 일 때만 판정,
            recall < recall_threshold(0.60) 이면 트리거 후보. 표본 부족은 "보류"(스킵)
  - 보류 조건(쿨다운) — 하나라도 해당하면 트리거 후보여도 트리거하지 않는다
      1) 서빙 버전(FastAPI /model-info) != registry @champion  → 서빙 반영 대기 (재기동 필요)
         측정 중인 모델과 champion 이 다르면 Recall 판정이 의미 없음
      2) 서빙 또는 MLflow 조회 실패 → 판정 근거 없음
      3) 마지막 드리프트 재학습 run 시작 후 cooldown_hours(기본 2) 미만
         재기동 직후 판정 창(1h)에 이전 트래픽이 섞여 재트리거되는 것을 방지 (창의 2배로 잡은 설계 선택)
  - force_trigger=True 는 트리거 경로 검증용 스위치 (보류 조건 무시)

PSI 계산 데이터
  - baseline = 원본 csv 의 학습 풀(Time 정렬 후 앞 80%), current = params.current_csv 의 holdout(뒤 20%)
  - current_csv 기본값은 원본(creditcard.csv). 드리프트 시나리오 파일은 data/ 아래 파일명으로 지정
    (예: creditcard_s1.csv). 수동 실행으로 바꾼 값은 다음 스케줄 실행에서 기본값으로 되돌아간다.

스케줄
  - @hourly, 판정 창 1h (10/4 표본 수집 속도 측정 기준)
"""
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task

SCHEDULE = "@hourly"

# 컨테이너 -> 호스트 서비스 주소 (retrain_dag.py 의 MLFLOW_URI 와 같은 방식)
MLFLOW_URI = os.getenv("MLFLOW_URI", "http://host.docker.internal:5000")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://host.docker.internal:9090")
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "host.docker.internal:9091")
SERVING_URL = os.getenv("SERVING_URL", "http://host.docker.internal:8495")

# champion 조회용. retrain_dag.py 의 MODEL_NAME 과 같아야 함
MODEL_NAME = os.getenv("MODEL_NAME", "sentinel-fraud-dag")
DRIFT_EXPERIMENT = "sentinel-retrain-drift"  # retrain_drift_dag.py 의 EXPERIMENT

ML_DIR = "/opt/airflow/ml"  # compose 볼륨 마운트 (../../../ml)
DATA_PATH = os.path.join(ML_DIR, "data", "creditcard.csv")

# check_drift.py 와 동일한 감시 피처
FEATURES = ["V14", "V10", "V4", "V12", "V8", "Amount"]


def _champion_version():
    """@champion alias 의 버전을 'v12' 형태로 반환. 실패 시 'unknown'."""
    try:
        import mlflow
        from mlflow.tracking import MlflowClient

        mlflow.set_tracking_uri(MLFLOW_URI)
        mv = MlflowClient().get_model_version_by_alias(MODEL_NAME, "champion")
        return f"v{mv.version}"
    except Exception as e:  # noqa: BLE001 - 라벨 조회 실패가 판정을 막지 않게 함
        print(f"[warn] champion 버전 조회 실패 -> 'unknown': {e}")
        return "unknown"


def _prom_scalar(promql):
    """Prometheus 즉시 쿼리 결과의 첫 값을 float 으로 반환. 결과가 비면 0.0."""
    url = f"{PROMETHEUS_URL}/api/v1/query?" + urllib.parse.urlencode({"query": promql})
    with urllib.request.urlopen(url, timeout=10) as resp:
        body = json.load(resp)
    if body.get("status") != "success":
        raise RuntimeError(f"Prometheus 쿼리 실패: {body}")
    result = body["data"]["result"]
    if not result:
        return 0.0
    return float(result[0]["value"][1])


def _hold_reasons(cd, cooldown_hours):
    """check_cooldown 결과로 보류 사유 목록을 만든다. 비어 있으면 보류 없음."""
    holds = []
    if cd["serving_version"] is None:
        holds.append("서빙 버전 조회 실패")
    elif cd["champion_version"] is None:
        holds.append("champion 조회 실패")
    elif cd["serving_version"] != cd["champion_version"]:
        holds.append(f"서빙 반영 대기: 서빙 v{cd['serving_version']} != champion v{cd['champion_version']} "
                     f"(FastAPI 재기동 필요)")
    age = cd["last_drift_age_h"]
    if age is not None and age < cooldown_hours:
        holds.append(f"쿨다운: 직전 드리프트 재학습 {age}h 전 < {cooldown_hours}h")
    return holds


@task
def compute_psi(**context):
    """baseline = 원본 csv 학습 풀, current = params.current_csv 의 holdout (기본: 원본 csv)."""
    import pandas as pd

    if ML_DIR not in sys.path:
        sys.path.insert(0, ML_DIR)
    from drift import build_baseline, calculate_psi_batch, push_psi_metrics

    # data/ 아래 파일명만 허용 (경로 문자열을 그대로 쓰지 않음)
    current_name = os.path.basename(str(context["params"]["current_csv"]))
    current_path = os.path.join(ML_DIR, "data", current_name)

    df = pd.read_csv(DATA_PATH).sort_values("Time").reset_index(drop=True)
    train_pool = df.iloc[: int(len(df) * 0.8)]  # b1+b2+b3 와 동일

    if current_name == os.path.basename(DATA_PATH):
        cur = df
    else:  # 시나리오 파일은 make_drift_files.py 가 Time 안정 정렬 순서로 씀
        cur = pd.read_csv(current_path).sort_values("Time", kind="stable").reset_index(drop=True)
    holdout = cur.iloc[int(len(cur) * 0.8):].reset_index(drop=True)

    baselines = {f: build_baseline(train_pool[f].values) for f in FEATURES}
    results = calculate_psi_batch(baselines, holdout, FEATURES)
    model_version = _champion_version()

    # 모니터링 기록(push) 실패가 drift 판정을 막지 않도록 분리
    pushed = True
    try:
        push_psi_metrics(results, gateway_url=PUSHGATEWAY_URL, model_version=model_version)
    except Exception as e:  # noqa: BLE001
        pushed = False
        print(f"[warn] Pushgateway push 실패 (판정은 계속): {e}")

    summary = {
        f: {"psi": round(float(r["psi"]), 4), "level": r["level"]}
        for f, r in results.items()
    }
    print(f"[psi] current_csv={current_name} model_version={model_version} pushed={pushed} {summary}")
    return {"current_csv": current_name, "model_version": model_version,
            "pushed": pushed, "features": summary}


@task
def fetch_recall(**context):
    """판정 창 안의 tp/fn 증가량으로 Recall 계산. 표본 0이면 recall=None."""
    window = str(context["params"]["window"])
    if not re.fullmatch(r"\d+[smhd]", window):
        raise ValueError(f"window 형식 오류: {window!r} (예: 1h, 30m)")

    def q(outcome):
        return _prom_scalar(
            f'sum(increase(sentinel_ml_predictions_total{{outcome="{outcome}"}}[{window}])) or vector(0)'
        )

    tp, fn = q("tp"), q("fn")
    samples = tp + fn
    recall = (tp / samples) if samples > 0 else None
    out = {"window": window, "tp": round(tp, 2), "fn": round(fn, 2),
           "samples": round(samples, 2), "recall": recall}
    print(f"[recall] {out}")
    return out


@task
def check_cooldown():
    """보류 조건 판단용 상태 수집: 서빙 버전, champion 버전, 마지막 드리프트 재학습 경과 시간."""
    import time

    out = {"serving_version": None, "champion_version": None, "last_drift_age_h": None, "errors": []}

    try:
        with urllib.request.urlopen(f"{SERVING_URL}/model-info", timeout=5) as resp:
            out["serving_version"] = str(json.load(resp).get("version"))
    except Exception as e:  # noqa: BLE001 - 조회 실패는 보류 사유로 처리
        out["errors"].append(f"서빙 조회 실패: {e}")

    try:
        import mlflow
        from mlflow.tracking import MlflowClient

        mlflow.set_tracking_uri(MLFLOW_URI)
        client = MlflowClient()
        out["champion_version"] = str(client.get_model_version_by_alias(MODEL_NAME, "champion").version)
        exp = client.get_experiment_by_name(DRIFT_EXPERIMENT)
        if exp is not None:
            runs = client.search_runs([exp.experiment_id],
                                      order_by=["attributes.start_time DESC"], max_results=1)
            if runs:
                out["last_drift_age_h"] = round((time.time() * 1000 - runs[0].info.start_time) / 3.6e6, 2)
    except Exception as e:  # noqa: BLE001
        out["errors"].append(f"MLflow 조회 실패: {e}")

    print(f"[cooldown] {out}")
    return out


@task.short_circuit
def decide(psi, recall, cooldown, **context):
    """트리거 여부 판정. False 이면 하위 trigger_retrain 이 스킵된다."""
    params = context["params"]
    reasons = []

    # PSI 는 경보만 — 재학습 사유로 쓰지 않는다
    alerts = [f for f, v in psi["features"].items() if v["level"] == "alert"]
    if alerts:
        print(f"[psi-alert] 경보만 (재학습 트리거 아님): {alerts}")

    min_samples = float(params["min_samples"])
    threshold = float(params["recall_threshold"])
    if recall["recall"] is not None and recall["samples"] >= min_samples:
        if recall["recall"] < threshold:
            reasons.append(f"Recall {recall['recall']:.4f} < {threshold} (표본 {recall['samples']})")
    else:
        print(f"[hold] Recall 판정 보류: 표본 {recall['samples']} < 최소 {min_samples}")

    holds = _hold_reasons(cooldown, float(params["cooldown_hours"]))
    if reasons and holds:
        print(f"[hold] 트리거 조건 충족했으나 보류: {holds} (조건: {reasons})")
        reasons = []
    elif holds:
        print(f"[cooldown-state] 현재 보류 상태(트리거 조건은 미충족): {holds}")

    if params["force_trigger"]:
        reasons.append("force_trigger=True (경로 검증용, 보류 조건 무시)")

    print(f"[decide] trigger={bool(reasons)} reasons={reasons}")
    return bool(reasons)


@dag(
    dag_id="sentinel_drift_monitor",
    schedule=SCHEDULE,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    params={
        # 서빙 기준선 약 0.82(10/5 측정, v12)와 S2 시나리오(Recall 0.36) 사이.
        # 근거: Wilson 95%(n=75) 기준 하한 0.71 / S2 상한 0.47 의 중간
        "recall_threshold": 0.60,
        "min_samples": 100,        # tp+fn 최소 표본
        "window": "1h",            # Recall 판정 창 (@hourly 주기와 일치)
        "cooldown_hours": 2,       # 직전 드리프트 재학습 후 보류 시간 (판정 창의 2배)
        "force_trigger": False,    # 트리거 경로 검증용 (보류 조건 무시)
        "current_csv": "creditcard.csv",  # PSI 의 현재 데이터 (ml/data/ 아래 파일명)
    },
    tags=["sentinel", "drift"],
)
def sentinel_drift_monitor():
    psi = compute_psi()
    recall = fetch_recall()
    cooldown = check_cooldown()
    go = decide(psi, recall, cooldown)

    trigger_retrain = TriggerDagRunOperator(
        task_id="trigger_retrain",
        trigger_dag_id="sentinel_retrain_drift",
        conf={"trigger_source": "sentinel_drift_monitor"},
    )
    go >> trigger_retrain


dag_instance = sentinel_drift_monitor()