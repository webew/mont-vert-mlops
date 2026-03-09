"""
train_model.py
--------------
Entraîne deux modèles (RandomForest et GradientBoosting),
les compare et les loggue dans MLflow.
"""
import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration MLflow
mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
mlflow.set_experiment("mont-vert-repas")

# Lecture des données depuis MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

bucket = os.getenv("S3_BUCKET")

# Télécharger le premier fichier Parquet disponible
response = s3.list_objects_v2(Bucket=bucket, Prefix="processed/admissions/")
parquet_files = [o["Key"] for o in response.get("Contents", []) if o["Key"].endswith(".parquet")]

if not parquet_files:
    raise FileNotFoundError("Aucun fichier Parquet trouvé dans processed/admissions/")

s3.download_file(bucket, parquet_files[0], "/tmp/admissions.parquet")
df = pd.read_parquet("/tmp/admissions.parquet")
print(f"✅ {len(df)} lignes chargées depuis MinIO")

FEATURES = [
    "nb_patients_hospitalises",
    "nb_admissions_jour",
    "nb_sorties_jour",
    "jour_semaine",
    "est_weekend",
    "mois",
    "taux_occupation",
    "est_periode_hivernale",
    "ratio_entrees_sorties",
]

X = df[FEATURES]
y = df["nb_repas"]
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)


def train_and_log(model, run_name, params):
    with mlflow.start_run(run_name=run_name):
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)

        mae = mean_absolute_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)

        mlflow.log_params(params)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)
        mlflow.sklearn.log_model(
            model, "model",
            registered_model_name="mont-vert-repas"
        )

        print(f"  {run_name} — MAE: {mae:.2f} repas | R²: {r2:.3f}")
        return mae, r2


print("\n🔁 Entraînement en cours...")

# Modèle 1 — Random Forest
rf_params = {"n_estimators": 100, "max_depth": 5}
rf_mae, rf_r2 = train_and_log(
    RandomForestRegressor(**rf_params, random_state=42),
    run_name="random-forest-v1",
    params=rf_params
)

# Modèle 2 — Gradient Boosting
gb_params = {"n_estimators": 100, "max_depth": 3, "learning_rate": 0.1}
gb_mae, gb_r2 = train_and_log(
    GradientBoostingRegressor(**gb_params, random_state=42),
    run_name="gradient-boosting-v1",
    params=gb_params
)

# Comparaison
print("\n=== Comparaison finale ===")
print(f"  Random Forest      — MAE: {rf_mae:.2f} | R²: {rf_r2:.3f}")
print(f"  Gradient Boosting  — MAE: {gb_mae:.2f} | R²: {gb_r2:.3f}")
winner = "Random Forest" if rf_mae < gb_mae else "Gradient Boosting"
print(f"\n🏆 Meilleur modèle : {winner}")
print("✅ Modèles enregistrés dans MLflow")
