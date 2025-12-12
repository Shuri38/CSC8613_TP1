from fastapi import FastAPI
from feast import FeatureStore

app = FastAPI()

# Initialisation du Feature Store global (le repo est monté dans /repo)
store = FeatureStore(repo_path="/repo")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/features/{user_id}")
def get_features(user_id: str):
    # Liste des features à récupérer pour l'utilisateur
    features = [
        "subs_profile_fv:months_active",
        "subs_profile_fv:monthly_fee",
        "subs_profile_fv:paperless_billing",
    ]

    # Récupération des features en ligne
    feature_dict = store.get_online_features(
        features=features,
        entity_rows=[{"user_id": user_id}],
    ).to_dict()

    # Conversion en dictionnaire simple {nom_feature: valeur}
    simple_features = {name: values[0] for name, values in feature_dict.items()}

    return {
        "user_id": user_id,
        "features": simple_features,
    }
