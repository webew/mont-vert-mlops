# TP Big Data & MLOps — Clinique du Mont Vert

Prédiction du nombre de repas journaliers.

## Stack technique

| Outil | Rôle | Port |
|---|---|---|
| MinIO | Stockage objet (Data Lake) | 9001 |
| MLflow | Tracking des expériences ML | 5000 |
| FastAPI | API REST de prédiction | 8000 |
| Airflow | Orchestration du pipeline | 8080 |
| Grafana | Dashboard de monitoring | 3000 |
| Prometheus | Collecte des métriques | 9090 |

## Démarrage rapide

```bash
# 1. Copier le fichier de configuration
cp .env.example .env

# 2. Lancer tous les services Docker
docker compose up -d

# 3. Attendre ~60 secondes, puis vérifier
docker compose ps

# 4. Créer le bucket MinIO
python setup_minio.py

# 5. Installer les dépendances Python
pip install -r requirements.txt
```

## Exécution du pipeline

```bash
# Etape 1 — Générer les données
python data/generate_data.py

# Etape 2 — Valider la qualité
python data/validate_data.py

# Etape 3 — Traiter avec Spark
python spark/process_data.py

# Etape 4 — Entraîner les modèles
python training/train_model.py

# Etape 5 — Monitorer le drift
python monitoring/check_drift.py
```

Ou laisser Airflow orchestrer automatiquement via http://localhost:8080.

## Interfaces

- **MinIO** : http://localhost:9001 — `minioadmin` / `minioadmin`
- **MLflow** : http://localhost:5000
- **FastAPI** : http://localhost:8000/docs
- **Airflow** : http://localhost:8080 — `admin` / `admin`
- **Grafana** : http://localhost:3000 — `admin` / `admin`

## Structure du projet

```
mont-vert-mlops/
├── docker-compose.yml
├── .env.example
├── setup_minio.py
├── requirements.txt
├── data/
│   ├── generate_data.py
│   └── validate_data.py
├── spark/
│   └── process_data.py
├── training/
│   └── train_model.py
├── api/
│   ├── main.py
│   └── Dockerfile
├── monitoring/
│   ├── check_drift.py
│   ├── prometheus.yml
│   └── grafana/
│       └── provisioning/
└── airflow/
    └── dags/
        └── mont_vert_pipeline.py
```
