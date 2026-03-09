"""
validate_data.py
----------------
Valide la qualité des données brutes avec Great Expectations 1.x.
Stoppe le pipeline si les règles métier ne sont pas respectées.
"""
import great_expectations as gx
import pandas as pd
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

# Téléchargement depuis MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
s3.download_file(os.getenv("S3_BUCKET"), "raw/admissions.csv", "/tmp/admissions.csv")
df = pd.read_csv("/tmp/admissions.csv")

print(f"📋 Validation de {len(df)} lignes...")

# --- Pour simuler des données corrompues (décommenter) ---
# df.loc[0, "nb_patients_hospitalises"] = -5
# df.loc[1, "mois"] = 15
# ---------------------------------------------------------

# Initialisation Great Expectations 1.x
context = gx.get_context()
data_source = context.data_sources.add_pandas(name="admissions")
data_asset = data_source.add_dataframe_asset(name="admissions_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

# Définition des règles métier
suite = context.suites.add(gx.ExpectationSuite(name="mont_vert_suite"))

suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="nb_patients_hospitalises"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="nb_repas"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="nb_patients_hospitalises", min_value=0, max_value=180))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="nb_repas", min_value=0, max_value=600))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="jour_semaine", min_value=0, max_value=6))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="mois", min_value=1, max_value=12))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="nb_admissions_jour", min_value=0, max_value=50))

# Validation
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="mont_vert_validation",
        data=batch_definition,
        suite=suite,
    )
)
results = validation_definition.run(batch_parameters={"dataframe": df})

if results.success:
    print(f"✅ Toutes les validations sont passées — données conformes")
else:
    failed = [
        r for r in results.results if not r.success
    ]
    print(f"❌ {len(failed)} validation(s) échouée(s) :")
    for r in failed:
        col = r.expectation_config.kwargs.get("column", "?")
        exp = r.expectation_config.type
        print(f"   → {col} : {exp}")
    raise ValueError("Qualité des données insuffisante — pipeline arrêté")