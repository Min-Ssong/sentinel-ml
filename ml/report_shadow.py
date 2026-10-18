"""
Champion/Challenger 비교 리포트 (6월 산출물 4번)

같은 거래에 대해 두 모델이 낸 점수(FRAUD_RESULTS)를 정답 라벨(TRANSACTION_LABELS)과 맞춰 비교한다.
Prometheus 지표(집계값)와 달리 "같은 거래끼리" 비교하므로 모집단 차이(SD6)가 없다.

사용법 (ml/ 에서, venv 활성, pip install psycopg2-binary)
  python report_shadow.py \
      --phase "원본=2026-10-18T11:00:00/2026-10-18T11:07:00" \
      --phase "S2=2026-10-18T11:08:00/2026-10-18T11:15:00" \
      --out ../docs/reports/shadow_2026-10-18.md

  - 구간 시각은 KST(+09:00)로 해석한다 (시간대를 붙여 쓰면 그대로 사용)
  - 구간 선택 기준은 TRANSACTION_LABELS.LABELED_AT (TIMESTAMPTZ). 다른 테이블의 CREATED_AT 은
    시간대 없는 KST 값이라 쓰지 않는다 (R2 전환 전)

판정 기준
  - 예측 사기 = ML_SCORE >= 0.5 (ScoreDecision.outcome 과 같은 기준). ML_SCORE 는 소수 4자리 반올림 저장
  - 챔피언 결과가 없는 거래: 거래 STATUS 가 PENDING 이면 ML Fallback, 그 외는 룰 차단으로 분류

주의 (리포트에도 출력)
  - 사기 행은 holdout 사기 75행을 반복 추출 → 표본이 독립이 아니라 Wilson 구간은 낙관적
  - 시뮬레이터 사기 비율(fraud-ratio)이 실제보다 훨씬 높아 Precision 은 실제 수준이 아님
  - S2 는 합성 드리프트 시나리오
"""
import argparse
import math
import os
from datetime import datetime, timedelta, timezone

import pandas as pd

KST = timezone(timedelta(hours=9))
THRESHOLD = 0.5

QUERY = """
SELECT l.transaction_id, l.is_fraud, l.labeled_at, t.status,
       c.ml_score AS champ_score, cm.version AS champ_ver, cm.mlflow_run_id AS champ_run,
       h.ml_score AS chall_score, hm.version AS chall_ver, hm.mlflow_run_id AS chall_run
FROM transaction_labels l
JOIN transactions t ON t.transaction_id = l.transaction_id
LEFT JOIN fraud_results c ON c.transaction_id = l.transaction_id AND c.role = 'CHAMPION'
LEFT JOIN model_metadata cm ON cm.id = c.model_id
LEFT JOIN fraud_results h ON h.transaction_id = l.transaction_id AND h.role = 'CHALLENGER'
LEFT JOIN model_metadata hm ON hm.id = h.model_id
WHERE l.labeled_at >= %s AND l.labeled_at < %s
"""


def parse_time(s):
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=KST)


def parse_phase(arg):
    name, rng = arg.split("=", 1)
    start, end = rng.split("/", 1)
    return name.strip(), parse_time(start.strip()), parse_time(end.strip())


def wilson(k, n, z=1.96):
    if n == 0:
        return None
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return c - h, c + h


def metrics(y, pred):
    tp = int((pred & y).sum()); fn = int((~pred & y).sum())
    fp = int((pred & ~y).sum()); tn = int((~pred & ~y).sum())
    recall = tp / (tp + fn) if tp + fn else None
    precision = tp / (tp + fp) if tp + fp else None
    f1 = (2 * precision * recall / (precision + recall)
          if recall is not None and precision is not None and precision + recall else None)
    return {"tp": tp, "fn": fn, "fp": fp, "tn": tn, "recall": recall,
            "precision": precision, "f1": f1, "recall_ci": wilson(tp, tp + fn)}


def fmt(x, nd=4):
    return "N/A" if x is None else f"{x:.{nd}f}"


def fmt_ci(ci):
    return "N/A" if ci is None else f"{ci[0]:.4f} ~ {ci[1]:.4f}"


def versions(series, runs):
    pairs = sorted({(v, r) for v, r in zip(series, runs) if pd.notna(v)})
    return ", ".join(f"v{v} (run {str(r)[:8]}…)" for v, r in pairs) or "없음"


def analyze(df):
    """구간 데이터프레임 → 결과 dict. DB 와 분리해 단위 확인이 가능하게 둔다."""
    out = {}
    dup = int(df["transaction_id"].duplicated().sum())
    df = df.drop_duplicates("transaction_id").copy()
    df["y"] = df["is_fraud"].astype(bool)
    has_c, has_h = df["champ_score"].notna(), df["chall_score"].notna()

    out["n_total"] = len(df)
    out["n_fraud"] = int(df["y"].sum())
    out["dup_rows"] = dup
    out["champ_versions"] = versions(df["champ_ver"], df["champ_run"])
    out["chall_versions"] = versions(df["chall_ver"], df["chall_run"])
    out["n_rule_blocked"] = int((~has_c & (df["status"] != "PENDING")).sum())
    out["n_fallback"] = int((~has_c & (df["status"] == "PENDING")).sum())
    out["n_no_chall"] = int((~has_h).sum())

    # A. 같은 거래 비교: 두 모델 모두 점수를 낸 거래
    paired = df[has_c & has_h]
    y = paired["y"].to_numpy()
    pc = (paired["champ_score"].astype(float) >= THRESHOLD).to_numpy()
    ph = (paired["chall_score"].astype(float) >= THRESHOLD).to_numpy()
    out["n_paired"] = len(paired)
    out["champ"] = metrics(y, pc)
    out["chall"] = metrics(y, ph)

    # B. 불일치 거래: 누가 맞았나
    dis = pc != ph
    out["n_disagree"] = int(dis.sum())
    out["dis_champ_right"] = int((dis & (pc == y)).sum())
    out["dis_chall_right"] = int((dis & (ph == y)).sum())
    out["dis_table"] = (pd.DataFrame({"챔피언 예측": pc[dis], "챌린저 예측": ph[dis], "정답": y[dis]})
                        .replace({True: "사기", False: "정상"})
                        .value_counts().rename("건수").reset_index())

    # C. 룰 차단 거래(챔피언 미판단)에서 챌린저 성능 — Shadow 만 볼 수 있는 모집단
    rb = df[~has_c & (df["status"] != "PENDING") & has_h]
    out["n_rule_blocked_scored"] = len(rb)
    out["rb_chall"] = metrics(rb["y"].to_numpy(), (rb["chall_score"].astype(float) >= THRESHOLD).to_numpy())
    return out


def render(name, start, end, r):
    L = [f"## 구간: {name}",
         f"- 기간 (KST): {start.astimezone(KST):%Y-%m-%d %H:%M:%S} ~ {end.astimezone(KST):%H:%M:%S}",
         f"- 챔피언: {r['champ_versions']} / 챌린저: {r['chall_versions']}",
         f"- 라벨 거래 {r['n_total']}건 (사기 {r['n_fraud']}), 두 모델 모두 판단 {r['n_paired']}건, "
         f"룰 차단 {r['n_rule_blocked']}건, ML Fallback {r['n_fallback']}건, 챌린저 결과 없음 {r['n_no_chall']}건"
         + (f", 중복 행 {r['dup_rows']}건 제외" if r["dup_rows"] else ""),
         "",
         f"### A. 같은 거래 비교 ({r['n_paired']}건, 임계값 {THRESHOLD})",
         "| 모델 | TP | FN | FP | TN | Recall | Recall 95% 구간 (Wilson) | Precision | F1 |",
         "|---|---|---|---|---|---|---|---|---|"]
    for label, m in (("챔피언", r["champ"]), ("챌린저", r["chall"])):
        L.append(f"| {label} | {m['tp']} | {m['fn']} | {m['fp']} | {m['tn']} | {fmt(m['recall'])} | "
                 f"{fmt_ci(m['recall_ci'])} | {fmt(m['precision'])} | {fmt(m['f1'])} |")
    L += ["", f"### B. 판단 불일치 {r['n_disagree']}건 — 챔피언이 맞음 {r['dis_champ_right']} / 챌린저가 맞음 {r['dis_chall_right']}"]
    if r["n_disagree"]:
        L += ["| 챔피언 예측 | 챌린저 예측 | 정답 | 건수 |", "|---|---|---|---|"]
        L += [f"| {a} | {b} | {c} | {n} |" for a, b, c, n in r["dis_table"].itertuples(index=False)]
    m = r["rb_chall"]
    L += ["", f"### C. 룰 차단 거래에서 챌린저 ({r['n_rule_blocked_scored']}건, 챔피언은 판단하지 않은 거래)",
          f"- TP {m['tp']} / FN {m['fn']} / FP {m['fp']} / TN {m['tn']}, Recall {fmt(m['recall'])} "
          f"({fmt_ci(m['recall_ci'])})", ""]
    return L


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", action="append", required=True, help='"이름=시작/끝" (KST)')
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    import psycopg2
    conn = psycopg2.connect(host=os.getenv("POSTGRES_HOST", "localhost"),
                            port=int(os.getenv("POSTGRES_PORT", "5800")),
                            dbname=os.getenv("POSTGRES_DB", "sentineldb"),
                            user=os.getenv("POSTGRES_USER", "sentinel"),
                            password=os.getenv("POSTGRES_PASSWORD", "sentinel123"))
    lines = ["# Champion/Challenger 비교 리포트",
             f"생성: {datetime.now(KST):%Y-%m-%d %H:%M} KST · `ml/report_shadow.py`", "",
             "> 기준: 정답 라벨(`TRANSACTION_LABELS`)과 거래별 판단 결과(`FRAUD_RESULTS`)를 같은 거래끼리 맞춰 계산.",
             "> 주의: 사기 행은 holdout 사기 75행의 반복 추출이라 표본이 독립이 아니며 Wilson 구간은 낙관적이다.",
             "> 시뮬레이터 사기 비율이 실제(약 0.17%)보다 훨씬 높아 Precision 은 실제 수준이 아니다. S2 는 합성 드리프트 시나리오다.", ""]
    for arg in args.phase:
        name, start, end = parse_phase(arg)
        df = pd.read_sql(QUERY, conn, params=(start, end))
        if df.empty:
            lines += [f"## 구간: {name}", "- 해당 기간 라벨 거래 없음", ""]
            print(f"[{name}] 데이터 없음")
            continue
        r = analyze(df)
        lines += render(name, start, end, r)
        print(f"[{name}] paired={r['n_paired']} champ recall={fmt(r['champ']['recall'])} "
              f"chall recall={fmt(r['chall']['recall'])} disagree={r['n_disagree']}")
    conn.close()
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"→ {args.out}")


if __name__ == "__main__":
    main()
