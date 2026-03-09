"""
generate_data.py
----------------
Simule 1 an d'admissions patients à la Clinique du Mont Vert.
Génère un CSV et l'uploade dans la couche raw/ de MinIO.
"""
import pandas as pd
import numpy as np
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

np.random.seed(42)
n_jours = 365
dates = pd.date_range(start="2024-01-01", periods=n_jours)
nb_patients = np.random.randint(120, 180, size=n_jours)

df = pd.DataFrame({
    "date": dates,
    "nb_patients_hospitalises": nb_patients,
    "nb_admissions_jour": np.random.randint(5, 20, size=n_jours),
    "nb_sorties_jour": np.random.randint(5, 20, size=n_jours),
    "jour_semaine": dates.dayofweek,
    "est_weekend": (dates.dayofweek >= 5).astype(int),
    "mois": dates.month,
})

# La cible : nb_repas dépend principalement du nombre de patients
# Le bruit gaussien simule les imperfections de la réalité
df["nb_repas"] = (
    df["nb_patients_hospitalises"] * 2.8
    + df["nb_admissions_jour"] * 0.5
    - df["nb_sorties_jour"] * 0.3
    + np.random.normal(0, 5, size=n_jours)
).astype(int)

print(f"✅ {len(df)} jours générés")
print(df.head())

# Sauvegarde locale temporaire
df.to_csv("/tmp/admissions.csv", index=False)

# Upload vers MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

s3.upload_file(
    "/tmp/admissions.csv",
    os.getenv("S3_BUCKET"),
    "raw/admissions.csv"
)
print(f"✅ Fichier uploadé : s3://{os.getenv('S3_BUCKET')}/raw/admissions.csv")
