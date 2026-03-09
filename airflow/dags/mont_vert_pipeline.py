"""
mont_vert_pipeline.py
---------------------
DAG Airflow orchestrant le coeur du pipeline MLOps de la Clinique du Mont Vert.
S'exécute automatiquement chaque jour à 6h00.

Etapes manuelles (hors Airflow) :
  - python3 spark/process_data.py      → avant de lancer le DAG
  - python3 monitoring/check_drift.py  → en continu dans un terminal dédié

Pourquoi ?
  - PySpark nécessite Java 17 et tourne en local, pas dans le conteneur Airflow
  - check_drift.py est une boucle infinie — Airflow ne peut pas gérer ça
    En production, le monitoring serait un service séparé (Kubernetes, systemd...)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

BASE_PATH = "/opt/airflow/project"

default_args = {
    "owner": "mont-vert",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "depends_on_past": False,
}


def run_script(script_path):
    env = os.environ.copy()
    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True,
        env=env
    )
    print("STDOUT:", result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        raise Exception(f"Script échoué (code {result.returncode}) :\n{result.stderr}")


with DAG(
    dag_id="mont_vert_pipeline",
    default_args=default_args,
    description="Pipeline MLOps — Clinique du Mont Vert",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlops", "mont-vert"],
) as dag:

    t1_generate = PythonOperator(
        task_id="generate_data",
        python_callable=run_script,
        op_args=[f"{BASE_PATH}/data/generate_data.py"],
        doc="Génère 365 jours d'admissions simulées et uploade sur MinIO"
    )

    t2_validate = PythonOperator(
        task_id="validate_data",
        python_callable=run_script,
        op_args=[f"{BASE_PATH}/data/validate_data.py"],
        doc="Valide la qualité des données avec Great Expectations"
    )

    t3_train = PythonOperator(
        task_id="train_models",
        python_callable=run_script,
        op_args=[f"{BASE_PATH}/training/train_model.py"],
        doc="Entraîne RandomForest et GradientBoosting, compare dans MLflow"
    )

    # Pipeline : générer → valider → entraîner
    t1_generate >> t2_validate >> t3_train
