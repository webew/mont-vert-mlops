"""
setup_minio.py
--------------
A lancer UNE SEULE FOIS après docker compose up -d
Crée le bucket MinIO et les dossiers de base du Data Lake.
"""
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

bucket = os.getenv("S3_BUCKET")

# Création du bucket
try:
    s3.create_bucket(Bucket=bucket)
    print(f"✅ Bucket '{bucket}' créé")
except s3.exceptions.BucketAlreadyOwnedByYou:
    print(f"ℹ️  Bucket '{bucket}' existe déjà")

# Création des dossiers virtuels (objets vides)
for prefix in ["raw/", "processed/", "mlflow/"]:
    s3.put_object(Bucket=bucket, Key=prefix)
    print(f"   📁 Dossier {prefix} créé")

print("\n✅ MinIO prêt. Accès : http://localhost:9001 (minioadmin/minioadmin)")
