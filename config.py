import os
from pathlib import Path
from dotenv import load_dotenv

# 1. Chargement intelligent du .env
dossier_actuel = Path(__file__).resolve().parent

chemin_env = None
for parent in [dossier_actuel, *dossier_actuel.parents]:
    potential_env = parent / ".env"
    if potential_env.exists():
        chemin_env = potential_env
        break

if chemin_env:
    load_dotenv(dotenv_path=chemin_env)
else:
    load_dotenv()

# Helper sécurisé pour convertir les ports en entier sans faire crasher le script
def extraire_port(nom_var: str, port_defaut: int = 5432) -> int:
    valeur = os.getenv(nom_var)
    if not valeur or not str(valeur).isdigit():
        return port_defaut
    return int(valeur)

# 2. Dictionnaire du Data Warehouse
data_warehouse = {
    "host":     os.getenv("DW_HOST_local"),
    "port":     extraire_port("DW_PORT_local", 5432),
    "database": os.getenv("DW_DATABASE_local"),
    "user":     os.getenv("DW_USER_local"),
    "password": os.getenv("DW_PASSWORD_local"),
    "sslmode":  os.getenv("DW_SSLMODE", "require")
}

# 3. Liste des bases sources
source_a = {
    "host":     os.getenv("SOURCE_A_HOST_local"),
    "port":     extraire_port("SOURCE_A_PORT_local", 5432),
    "database": os.getenv("SOURCE_A_DATABASE_local"),
    "user":     os.getenv("SOURCE_A_USER_local"),
    "password": os.getenv("SOURCE_A_PASSWORD_local"),
    "sslmode":  os.getenv("SOURCE_A_SSLMODE", os.getenv("DW_SSLMODE", "require"))
}

source_b = {
    "host":     os.getenv("SOURCE_B_HOST_local"),
    "port":     extraire_port("SOURCE_B_PORT_local", 5432),
    "database": os.getenv("SOURCE_B_DATABASE_local"),
    "user":     os.getenv("SOURCE_B_USER_local"),
    "password": os.getenv("SOURCE_B_PASSWORD_local"),
    "sslmode":  os.getenv("SOURCE_B_SSLMODE", os.getenv("DW_SSLMODE", "require"))
}

base_source = [source_a, source_b]

# ==============================================================================
# FONCTIONS UTILITAIRES POUR LA GESTION DES CONNEXIONS
# ==============================================================================

def construire_uri_postgresql(config):
    """
    Construit une URI PostgreSQL universelle (compatible Polars / ConnectorX / Psycopg2)
    """
    host = config['host']
    port = config['port']
    database = config['database']
    user = config['user']
    password = config['password']
    sslmode = config.get('sslmode', 'require')
    
    uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    if sslmode and sslmode.lower() != 'require':
        uri += f"?sslmode={sslmode}"
    return uri

# Alias pour garder la compatibilité avec tes modules existants
construire_uri_psycopg2 = construire_uri_postgresql