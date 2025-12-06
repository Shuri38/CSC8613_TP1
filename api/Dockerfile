# Dockerfile

# Choisir une image de base Python
FROM python:3.11-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier le fichier app.py
COPY app.py /app/

# Installer FastAPI et Uvicorn
RUN pip install fastapi uvicorn

# Lancer le serveur au démarrage du conteneur
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
