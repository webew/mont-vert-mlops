"""
process_data.py
---------------
Lit les données brutes depuis MinIO, les enrichit avec PySpark,
et les sauvegarde au format Parquet dans la couche processed/.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("MontVert - Processing") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.0") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bucket = os.getenv("S3_BUCKET")

# Lecture depuis MinIO
df = spark.read.csv(
    f"s3a://{bucket}/raw/admissions.csv",
    header=True,
    inferSchema=True
)
print(f"✅ {df.count()} lignes lues depuis MinIO")

# Enrichissement : nouvelles features
df_processed = df \
    .withColumn("taux_occupation",
        F.round(F.col("nb_patients_hospitalises") / 180 * 100, 2)) \
    .withColumn("est_periode_hivernale",
        F.when(F.col("mois").isin([11, 12, 1, 2]), 1).otherwise(0)) \
    .withColumn("ratio_entrees_sorties",
        F.round(F.col("nb_admissions_jour") / (F.col("nb_sorties_jour") + 1), 2)) \
    .dropna()

print(f"✅ Features ajoutées : taux_occupation, est_periode_hivernale, ratio_entrees_sorties")

# Export Parquet vers MinIO
df_processed.write \
    .mode("overwrite") \
    .parquet(f"s3a://{bucket}/processed/admissions/")

print(f"✅ Données exportées : s3://{bucket}/processed/admissions/ (format Parquet)")
spark.stop()