from feast import Entity

# Définition de l'entité principale "user"
user = Entity(
    name="user",
    join_keys=["user_id"],
    description="Représente un utilisateur de StreamFlow, identifié de manière unique par son user_id.",
)
