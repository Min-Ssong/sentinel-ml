# ml/check_importance.py (임시)
import os
import mlflow

mlflow.set_tracking_uri(os.getenv("MLFLOW_URI", "http://localhost:5000"))
model = mlflow.xgboost.load_model("models:/sentinel-fraud-dag@champion")

try:
    booster = model.get_booster()
except AttributeError:
    booster = model

imp = booster.get_score(importance_type="gain")
for k, v in sorted(imp.items(), key=lambda x: -x[1])[:10]:
    print(f"{k:10s} {v:.2f}")