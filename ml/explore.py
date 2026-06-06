import pandas as pd

df = pd.read_csv("data/creditcard.csv")

print("행/열:", df.shape)
print("컬럼:", df.columns.tolist())
print("결측치 총합:", df.isnull().sum().sum())
print(df["Class"].value_counts())                # 정상(0) / 사기(1) 개수
print(df["Class"].value_counts(normalize=True))  # 비율