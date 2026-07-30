import os
from pathlib import Path
from dotenv import load_dotenv

# 1. Localiser dynamiquement la racine du projet qui contient le fichier .env
dossier_actuel = Path(__file__).resolve().parent

# On remonte l'arborescence jusqu'à trouver le fichier .env
chemin_env = None
for parent in [dossier_actuel, *dossier_actuel.parents]:
    potential_env = parent / ".env"
    if potential_env.exists():
        chemin_env = potential_env
        break

if chemin_env:
    load_dotenv(dotenv_path=chemin_env)
else:
    # Si .env n'est pas trouvé, charge par défaut les variables d'environnement système
    load_dotenv()

# 2. Dictionnaire du Data Warehouse
data_warehouse = {
    "host":     os.getenv("DW_HOST_local"),
    "port":     int(os.getenv("DW_PORT_local", 5432)),
    "database": os.getenv("DW_DATABASE_local"),
    "user":     os.getenv("DW_USER_local"),
    "password": os.getenv("DW_PASSWORD_local"),
    "sslmode":  os.getenv("DW_SSLMODE", "prefer")
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
    """
    host = config['host']
    port = config['port']
    database = config['database']
    user = config['user']
    password = config['password']
    sslmode = config.get('sslmode', 'prefer')
    
    uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    if sslmode and sslmode.lower() != 'prefer':
        uri += f"?sslmode={sslmode}"
    return uri


def construire_uri_psycopg2(config):
    """
    Construit une URI pour psycopg2
    """
    host = config['host']
    port = config['port']
    database = config['database']
    user = config['user']
    password = config['password']
    sslmode = config.get('sslmode', 'prefer')
    
    uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    if sslmode and sslmode.lower() != 'prefer':
        uri += f"?sslmode={sslmode}"
    return uri