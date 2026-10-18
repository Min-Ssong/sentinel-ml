"""
챌린저 승격·폐기·되돌리기 (Shadow 비교 후 수동 판단, SD7)

사용법 (ml/ 에서, venv 활성)
  python promote_challenger.py status                 # 현재 alias + Shadow 비교 지표 (아무것도 바꾸지 않음)
  python promote_challenger.py promote                # 승격 계획만 출력 (dry-run)
  python promote_challenger.py promote --apply        # @champion -> 챌린저 버전, 이전 챔피언은 @previous_champion
  python promote_challenger.py reject  --apply        # @challenger 제거 (버전은 남기고 태그만 기록)
  python promote_challenger.py rollback --apply       # @champion -> @previous_champion

옵션: --window 1h (Prometheus 비교 창)

비교 지표 해석 주의
  - 챔피언 지표(sentinel_ml_predictions)는 룰 통과 거래만, Shadow 지표(sentinel_ml_shadow_predictions)는
    룰 차단 거래까지 포함한다 (모집단이 다름). 룰은 피처와 무관하게 사용자·금액 기준이라 비교에 큰 편향은 없다고
    보지만(10/7 SD6 추론, 표본 비율로 확인), 같은 거래끼리의 비교는 FRAUD_RESULTS 대조로 한다.
  - Precision 은 시뮬레이터 사기 비율에 좌우되므로 실제 수준으로 해석하지 않는다.
  - 승격 결정은 이 스크립트가 하지 않는다. 숫자를 보고 사람이 --apply 한다.

변경 후 반영: 서빙은 기동 시 alias 를 1회 조회하므로 재기동해야 한다 (출력에 안내).
"""
import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from mlflow.tracking import MlflowClient

MODEL_NAME = os.getenv("SERVING_MODEL_NAME", "sentinel-fraud-dag")
MLFLOW_URI = os.getenv("MLFLOW_URI", "http://localhost:5000")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9290")


def alias_version(client, alias):
    try:
        return client.get_model_version_by_alias(MODEL_NAME, alias).version
    except Exception:  # noqa: BLE001 - alias 없음
        return None


def prom_outcomes(metric, window):
    q = f"sum by (outcome)(increase({metric}[{window}]))"
    url = f"{PROMETHEUS_URL}/api/v1/query?" + urllib.parse.urlencode({"query": q})
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            body = json.load(r)
    except Exception as e:  # noqa: BLE001
        return None, f"Prometheus 조회 실패: {e}"
    m = {x["metric"].get("outcome"): float(x["value"][1]) for x in body["data"]["result"]}
    return m, None


def describe(name, m):
    tp, fn, fp = m.get("tp", 0.0), m.get("fn", 0.0), m.get("fp", 0.0)
    samples = tp + fn
    recall = tp / samples if samples > 0 else None
    precision = tp / (tp + fp) if (tp + fp) > 0 else None
    r = f"{recall:.4f}" if recall is not None else "N/A"
    p = f"{precision:.4f}" if precision is not None else "N/A"
    return f"  {name:<10} tp={tp:9.2f} fn={fn:9.2f} fp={fp:7.2f} 표본(tp+fn)={samples:9.2f}  Recall={r}  Precision={p}"


def status(client, window):
    champ, chall, prev = (alias_version(client, a) for a in ("champion", "challenger", "previous_champion"))
    print(f"[alias] champion=v{champ} challenger=v{chall} previous_champion=v{prev}")
    for name, metric in (("champion", "sentinel_ml_predictions_total"),
                         ("challenger", "sentinel_ml_shadow_predictions_total")):
        m, err = prom_outcomes(metric, window)
        print(err if err else describe(name, m))
    print(f"  (창={window}. 챔피언은 룰 통과 거래만, 챌린저는 룰 차단 포함 — 모집단 다름)")
    return champ, chall, prev


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["status", "promote", "reject", "rollback"])
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--window", default="1h")
    args = ap.parse_args()

    os.environ.setdefault("MLFLOW_TRACKING_URI", MLFLOW_URI)
    client = MlflowClient(tracking_uri=MLFLOW_URI)
    champ, chall, prev = status(client, args.window)
    if args.action == "status":
        return

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if args.action == "promote":
        if chall is None:
            sys.exit("[중단] @challenger 없음")
        plan = [f"@champion: v{champ} -> v{chall}", f"@previous_champion: -> v{champ}", "@challenger 제거"]
    elif args.action == "reject":
        if chall is None:
            sys.exit("[중단] @challenger 없음")
        plan = [f"@challenger(v{chall}) 제거, 버전에 rejected 태그 기록 (버전 자체는 삭제하지 않음)"]
    else:  # rollback
        if prev is None:
            sys.exit("[중단] @previous_champion 없음")
        plan = [f"@champion: v{champ} -> v{prev}", f"@previous_champion: -> v{champ}"]

    print("\n[계획] " + " / ".join(plan))
    if not args.apply:
        print("dry-run — 실행하려면 --apply")
        return

    if args.action == "promote":
        client.set_registered_model_alias(MODEL_NAME, "previous_champion", champ)
        client.set_registered_model_alias(MODEL_NAME, "champion", chall)
        client.delete_registered_model_alias(MODEL_NAME, "challenger")
        client.set_model_version_tag(MODEL_NAME, chall, "promoted_at", now)
    elif args.action == "reject":
        client.delete_registered_model_alias(MODEL_NAME, "challenger")
        client.set_model_version_tag(MODEL_NAME, chall, "rejected_at", now)
    else:
        client.set_registered_model_alias(MODEL_NAME, "previous_champion", champ)
        client.set_registered_model_alias(MODEL_NAME, "champion", prev)
        # 판정 DAG 보류 조건 5(alias 변경 직후)가 읽는 시각 (promoted_at / rejected_at 와 같은 형식)
        client.set_model_version_tag(MODEL_NAME, prev, "rolled_back_at", now)

    print("\n[적용 후]")
    status(client, args.window)
    print("\n[다음]\n" + "\n".join(f"  - {s}" for s in next_steps(args.action)))


def next_steps(action):
    """적용 후 사람이 할 일. 서빙은 기동 시 alias 를 1회만 읽으므로 alias 가 바뀐 쪽만 재기동한다."""
    restart_champion = ("챔피언 서빙(8495) 재기동 → /model-info 버전 확인 "
                        "(창에 SERVING_MODEL_ALIAS 가 남아 있으면 Remove-Item Env:SERVING_MODEL_ALIAS)")
    stop_challenger = ("챌린저 서빙(8496) 종료 — @challenger 가 없어 다시 기동할 수 없음. "
                       "API 는 --shadow.enabled=false 로 재기동 (켜 두면 sentinel_ml_shadow_fallback 만 증가)")
    if action == "promote":
        return [restart_champion, stop_challenger,
                "판정 DAG 는 재기동 전까지 '서빙 반영 대기'로 보류, 재기동 후 재개"]
    if action == "reject":
        return ["챔피언은 바뀌지 않음 → 챔피언 서빙 재기동 불필요", stop_challenger,
                "판정 DAG 의 '챌린저 검증 중' 보류가 풀림 → 다음 판정에서 Recall < 임계값이면 재학습이 다시 트리거될 수 있음"]
    return [restart_champion,  # rollback
            "판정 DAG 는 재기동 전까지 '서빙 반영 대기'로 보류, 재기동 후 재개",
            "드리프트 트래픽(S2 등)이 계속 흐르면 되돌린 챔피언의 Recall 이 낮아 재학습이 트리거될 수 있음 → 시험이면 시뮬레이터 먼저 종료"]


if __name__ == "__main__":
    main()
