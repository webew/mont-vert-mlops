"""
process_data.py
---------------
Lit les données brutes depuis MinIO, les enrichit avec Pandas,
et les sauvegarde au format Parquet dans la couche processed/.
"""
import pandas as pd
import boto3
import os
import io
from dotenv import load_dotenv

load_dotenv()

bucket = os.getenv("S3_BUCKET")
endpoint = os.getenv("MINIO_ENDPOINT")

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

# Lecture depuis MinIO
response = s3.get_object(Bucket=bucket, Key="raw/admissions.csv")
df = pd.read_csv(response["Body"])
print(f"✅ {len(df)} lignes lues depuis MinIO")

# Enrichissement : nouvelles features
df["taux_occupation"] = (df["nb_patients_hospitalises"] / 180 * 100).round(2)
df["est_periode_hivernale"] = df["mois"].isin([11, 12, 1, 2]).astype(int)
df["ratio_entrees_sorties"] = (df["nb_admissions_jour"] / (df["nb_sorties_jour"] + 1)).round(2)
df = df.dropna()

print(f"✅ Features ajoutées : taux_occupation, est_periode_hivernale, ratio_entrees_sorties")

# Export Parquet vers MinIO
buffer = io.BytesIO()
df.to_parquet(buffer, index=False)
buffer.seek(0)

s3.put_object(
    Bucket=bucket,
    Key="processed/admissions/part-00000.parquet",
    Body=buffer.getvalue()
)

print(f"✅ Données exportées : s3://{bucket}/processed/admissions/ (format Parquet)")
