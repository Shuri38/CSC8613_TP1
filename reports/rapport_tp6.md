# Exercice 1 : Mise en place du rapport et vérifications de départ


Question 1.c: 

![alt text](images_tp6/image1.png)

![alt text](images_tp6/image2.png)

# Exercice 2 : Ajouter une logique de décision testable (unit test)

Question 2.d :

![alt text](images_tp6/image3.png)


On extrait une fonction pure pour pouvoir tester la logique métier indépendamment de Prefect, MLflow ou de l’environnement d’exécution. Cela permet d’avoir des tests simples, rapides et déterministes, qui valident uniquement la règle de décision sans dépendances externes ni effets de bord.

# Exercice 3 : Créer le flow Prefect train_and_compare_flow (train → eval → compare → promote)

Question 3.d :

[COMPARE] candidate_auc < prod_auc + delta (delta=0.01)
[SUMMARY] as_of=2024-02-29 cand_v=3 prod_v=1 -> skipped

![alt text](images_tp6/image3.png)

Le paramètre delta permet d’éviter la promotion d’un nouveau modèle pour des gains de performance marginaux pouvant être dus au bruit statistique ou à la variabilité du jeu de validation. Il garantit que seules des améliorations significatives justifient un changement en Production.


# Exercice 4 : Connecter drift → retraining automatique (monitor_flow.py)

Question 4.c:

![alt text](images_tp6/image4.png)


![alt text](images_tp6/image5.png)

Même si le drift global n’atteint pas le seuil (0.3), nous avons forcé l’exécution du flow avec threshold=0.0 pour démonstration. La colonne driftée représente 6,25 % des colonnes du dataset (1 sur 16).

# Exercice 5 : Redémarrage API pour charger le nouveau modèle Production + test /predict

Question 5.c : 

(base) robin.slesinski@macbookair projet-docker %  
curl -s -X POST "http://localhost:8000/predict" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"9763-GRSKD"}' | jq  
user_id,signup_date,user_gender,user_is_senior,has_family,has_dependents
7590-VHVEG,2022-02-05,Female,False,True,False
{
  "user_id": "9763-GRSKD",
  "prediction": 0,
  "features_used": {
    "months_active": 13,
    "plan_stream_movies": false,
    "plan_stream_tv": false,
    "net_service": "DSL",
    "monthly_fee": 49.95000076293945,
    "paperless_billing": true,
    "rebuffer_events_7d": 3,
    "unique_devices_30d": 2,
    "watch_hours_30d": 29.941709518432617,
    "avg_session_mins_7d": 29.14104461669922,
    "skips_7d": 6,
    "failed_payments_90d": 0,
    "support_tickets_90d": 0,
    "ticket_avg_resolution_hrs_90d": 8.300000190734863
  }
}
(base) robin.slesinski@macbookair projet-docker % 

L’API doit être redémarrée après la promotion d’un nouveau modèle car le modèle est chargé en mémoire uniquement au démarrage. Sans redémarrage, l’API continuerait à utiliser l’ancienne version pour les prédictions.
