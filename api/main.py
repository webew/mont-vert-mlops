"""
main.py
-------
API REST FastAPI pour la prédiction du nombre de repas.
Charge le meilleur modèle depuis le registry MLflow.
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import mlflow.sklearn
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Mont Vert — Prédiction Repas",
    description="API de prédiction du nombre de repas journaliers pour la Clinique du Mont Vert",
    version="1.0.0"
)

# Chargement du modèle au démarrage
mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))

try:
    model = mlflow.sklearn.load_model("models:/mont-vert-repas/latest")
    print("✅ Modèle chargé depuis MLflow")
except Exception as e:
    print(f"⚠️  Impossible de charger le modèle : {e}")
    model = None


class AdmissionData(BaseModel):
    nb_patients_hospitalises: int = Field(..., ge=0, le=180, description="Nombre de patients hospitalisés")
    nb_admissions_jour: int = Field(..., ge=0, description="Admissions du jour")
    nb_sorties_jour: int = Field(..., ge=0, description="Sorties du jour")
    jour_semaine: int = Field(..., ge=0, le=6, description="Jour de la semaine (0=Lundi, 6=Dimanche)")
    est_weekend: int = Field(..., ge=0, le=1, description="1 si weekend, 0 sinon")
    mois: int = Field(..., ge=1, le=12, description="Mois (1-12)")
    taux_occupation: float = Field(..., ge=0.0, le=100.0, description="Taux d'occupation en %")
    est_periode_hivernale: int = Field(..., ge=0, le=1, description="1 si nov-fév, 0 sinon")
    ratio_entrees_sorties: float = Field(default=1.0, description="Ratio entrées/sorties")


class PredictionResponse(BaseModel):
    nb_repas_predit: int
    message: str
    modele: str = "mont-vert-repas/latest"


@app.get("/health")
def health():
    return {
        "status": "ok",
        "modele_charge": model is not None
    }


@app.post("/predict", response_model=PredictionResponse)
def predict(data: AdmissionData):
    if model is None:
        raise HTTPException(
            status_code=503,
            detail="Modèle non disponible. Lancez train_model.py d'abord."
        )

    features = [[
        data.nb_patients_hospitalises,
        data.nb_admissions_jour,
        data.nb_sorties_jour,
        data.jour_semaine,
        data.est_weekend,
        data.mois,
        data.taux_occupation,
        data.est_periode_hivernale,
        data.ratio_entrees_sorties,
    ]]

    prediction = model.predict(features)[0]
    nb_repas = max(0, round(prediction))

    return PredictionResponse(
        nb_repas_predit=nb_repas,
        message=f"Préparer {nb_repas} repas pour demain"
    )
