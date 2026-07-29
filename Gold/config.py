import os
from dotenv import load_dotenv

# 1. Trouver et charger le .env situé à la racine du projet
dossier_actuel = os.path.dirname(os.path.abspath(__file__))
dossier_racine = os.path.dirname(dossier_actuel)
chemin_env = os.path.join(dossier_racine, ".env")
load_dotenv(dotenv_path=chemin_env)

# 2. Dictionnaire du Data Warehouse
data_warehouse = {
    "host":     os.getenv("DW_HOST_local"),
    "port":     int(os.getenv("DW_PORT_local", 5432)),
    "database": os.getenv("DW_DATABASE_local"),
    "user":     os.getenv("DW_USER_local"),
    "password": os.getenv("DW_PASSWORD_local"),
    "sslmode":  os.getenv("DW_SSLMODE", "prefer")  # prefer = SSL si disponible, sinon non
}

# 3. Liste des bases sources
source_a = {
    "host":     os.getenv("SOURCE_A_HOST_local"),
    "port":     int(os.getenv("SOURCE_A_PORT_local", 5432)),
    "database": os.getenv("SOURCE_A_DATABASE_local"),
    "user":     os.getenv("SOURCE_A_USER_local"),
    "password": os.getenv("SOURCE_A_PASSWORD_local"),
    "sslmode":  os.getenv("SOURCE_A_SSLMODE", os.getenv("DW_SSLMODE", "prefer"))
}

source_b = {
    "host":     os.getenv("SOURCE_B_HOST_local"),
    "port":     int(os.getenv("SOURCE_B_PORT_local", 5432)),
    "database": os.getenv("SOURCE_B_DATABASE_local"),
    "user":     os.getenv("SOURCE_B_USER_local"),
    "password": os.getenv("SOURCE_B_PASSWORD_local"),
    "sslmode":  os.getenv("SOURCE_B_SSLMODE", os.getenv("DW_SSLMODE", "prefer"))
}

base_source = [source_a, source_b]


# ==============================================================================
# FONCTIONS UTILITAIRES POUR LA GESTION DES CONNEXIONS
# ==============================================================================

def construire_uri_postgresql(config):
    """
    Construit une URI PostgreSQL pour Polars (connectorx)
    en gérant correctement le paramètre SSL selon le mode.
    """
    host = config['host']
    port = config['port']
    database = config['database']
    user = config['user']
    password = config['password']
    sslmode = config.get('sslmode', 'prefer')
    
    # Construction de l'URI de base
    uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    # Ajout des paramètres SSL selon le mode
    if sslmode and sslmode.lower() != 'prefer':
        # Pour require, verify-ca, verify-full : on ajoute sslmode
        uri += f"?sslmode={sslmode}"
    # Pour 'prefer', on ne met rien car connectorx gère par défaut
    
    return uri


def construire_uri_psycopg2(config):
    """
    Construit une URI pour psycopg2 (utilisée uniquement pour la création du schéma)
    """
    host = config['host']
    port = config['port']
    database = config['database']
    user = config['user']
    password = config['password']
    sslmode = config.get('sslmode', 'prefer')
    
    # Construction de l'URI de base
    uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    # Ajout des paramètres SSL si nécessaire
    if sslmode and sslmode.lower() != 'prefer':
        uri += f"?sslmode={sslmode}"
    
    return uri