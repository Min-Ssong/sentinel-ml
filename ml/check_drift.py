import os
import pandas as pd
from drift import build_baseline, calculate_psi, judge_psi
from drift import calculate_psi_batch, push_psi_metrics

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
df = pd.read_csv(os.path.join(BASE_DIR, "data", "creditcard.csv"))
df = df.sort_values("Time").reset_index(drop=True)

n = len(df)
holdout_start = int(n * 0.8)
train_pool = df.iloc[:holdout_start].reset_index(drop=True)
holdout = df.iloc[holdout_start:].reset_index(drop=True)

m = len(train_pool)
b1 = train_pool.iloc[: m // 3]
b2 = train_pool.iloc[m // 3 : 2 * m // 3]
b3 = train_pool.iloc[2 * m // 3 :]

stages = [
    ("champion=b1     → 신규 b2",      b1,                         b2),
    ("champion=b1+2   → 신규 b3",      pd.concat([b1, b2]),        b3),
    ("champion=b1+2+3 → 신규 holdout", pd.concat([b1, b2, b3]),    holdout),
]

FEATURES = ["V14", "V10", "V4", "V12", "V8", "Amount"]

for label, base_df, curr_df in stages:
    print(f"\n{label}")
    for feat in FEATURES:
        baseline = build_baseline(base_df[feat].values)
        psi = calculate_psi(baseline, curr_df[feat].values)
        level, _ = judge_psi(psi)
        print(f"  {feat:8s} {psi:.4f}  {level}")

# ... 3단계 루프 끝난 뒤
base_df = pd.concat([b1, b2, b3])
baselines = {f: build_baseline(base_df[f].values) for f in FEATURES}
results = calculate_psi_batch(baselines, holdout, FEATURES)

push_psi_metrics(
    results,
    gateway_url=os.getenv("PUSHGATEWAY_URL", "localhost:9091"),
    model_version="v12",
)
print("\nPushgateway 전송 완료")