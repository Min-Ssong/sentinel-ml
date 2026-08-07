# check_split.py
import pandas as pd

df = pd.read_csv("data/creditcard.csv").sort_values("Time").reset_index(drop=True)
n = len(df)
print(f"전체: {n}행, 사기: {(df['Class']==1).sum()}건, Time범위: {df['Time'].min()}~{df['Time'].max()}")

for k in [2, 3, 4]:
    print(f"\n=== {k}분할 (Time 순서 균등) ===")
    size = n // k
    for i in range(k):
        s = i * size
        e = n if i == k-1 else (i+1) * size
        batch = df.iloc[s:e]
        fraud = (batch['Class']==1).sum()
        print(f"  배치{i+1}: {len(batch)}행, 사기 {fraud}건 "
              f"(비율 1:{(len(batch)-fraud)//max(fraud,1)})")