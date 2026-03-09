"""
check_drift.py
--------------
Détecte le data drift avec Evidently 0.7.x et exporte les métriques
vers Prometheus pour visualisation dans Grafana.
"""
import pandas as pd
import numpy as np
from evidently import Dataset, DataDefinition
from evidently.presets import DataDriftPreset
from evidently import Report
from prometheus_client import Gauge, start_http_server
import boto3
import os
import time
from dotenv import load_dotenv

load_dotenv()

drift_score = Gauge("mont_vert_drift_score", "Part des features en drift (0 à 1)")
features_drifted = Gauge("mont_vert_features_drifted", "Nombre de features en drift")

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


def load_reference():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    bucket = os.getenv("S3_BUCKET")
    response = s3.list_objects_v2(Bucket=bucket, Prefix="processed/admissions/")
    parquet_files = [o["Key"] for o in response.get("Contents", []) if o["Key"].endswith(".parquet")]
    if not parquet_files:
        raise FileNotFoundError("Aucun fichier Parquet trouvé")
    s3.download_file(bucket, parquet_files[0], "/tmp/reference.parquet")
    return pd.read_parquet("/tmp/reference.parquet")


# def simulate_current_old(reference, drift_factor=1.0):
#     current = reference.copy()
#     current["nb_patients_hospitalises"] = (
#         current["nb_patients_hospitalises"] * drift_factor
#         + np.random.normal(0, 5, len(current))
#     ).astype(int).clip(0, 250)
#     current["mois"] = 1
#     return current

def simulate_current(reference, drift_factor=1.0):
    current = reference.copy()
    current["nb_patients_hospitalises"] = (
        current["nb_patients_hospitalises"] * drift_factor
        + np.random.normal(0, 5, len(current))
    ).astype(int).clip(0, 250)
    current["nb_admissions_jour"] = (
        current["nb_admissions_jour"] * drift_factor
    ).astype(int)
    current["taux_occupation"] = (
        current["taux_occupation"] * drift_factor
    ).clip(0, 100)
    current["mois"] = 1
    return current

def run_monitoring(reference, drift_factor):
    available = [f for f in FEATURES if f in reference.columns]
    ref_df = reference[available].copy()
    cur_df = simulate_current(reference, drift_factor)[available].copy()

    definition = DataDefinition(numerical_columns=available)
    ref_dataset = Dataset.from_pandas(ref_df, data_definition=definition)
    cur_dataset = Dataset.from_pandas(cur_df, data_definition=definition)

    report = Report(metrics=[DataDriftPreset()])
    result = report.run(reference_data=ref_dataset, current_data=cur_dataset)
    result_dict = result.dict()

    # La structure Evidently 0.7 : metrics[0]["value"] contient count et share
    first_metric = result_dict["metrics"][0]["value"]
    n_drifted = int(first_metric.get("count", 0))
    share = float(first_metric.get("share", 0.0))

    drift_score.set(share)
    features_drifted.set(n_drifted)

    print(f"  drift_factor={drift_factor:.1f} | score={share:.2f} | features_en_drift={n_drifted}")

    if share > 0.5:
        print("  ⚠️  ALERTE : drift majeur détecté ! Réentraîner le modèle.")

    return share


if __name__ == "__main__":
    start_http_server(8001)
    print("✅ Serveur Prometheus démarré sur :8001")
    print("   Grafana peut scraper http://localhost:8001/metrics\n")

    reference = load_reference()
    print(f"✅ Données de référence chargées ({len(reference)} lignes)\n")

    drift_factor = 1.0

    while True:
        run_monitoring(reference, drift_factor)
        drift_factor = min(drift_factor + 0.1, 2.0)
        time.sleep(30)
