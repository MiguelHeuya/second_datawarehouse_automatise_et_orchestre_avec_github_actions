import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# 1. CHARGEMENT DU FICHIER .ENV
# ------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH)

# ------------------------------------------------------------------------------
# 2. CONFIGURATION GLOBALE & LOGS
# ------------------------------------------------------------------------------
ENV = os.getenv("ENV", "development").lower()
LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_PATH = Path(os.getenv("LOG_PATH", "./logs/"))

LOG_PATH.mkdir(parents=True, exist_ok=True)

numeric_log_level = getattr(logging, LOG_LEVEL_STR, logging.INFO)
logging.basicConfig(
    level=numeric_log_level,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH / "etl_execution.log", encoding="utf-8")
    ]
)

logger = logging.getLogger("ETL_Config")
logger.info(f"Environnement chargé : [{ENV.upper()}] | Niveau de Log : [{LOG_LEVEL_STR}]")

# ------------------------------------------------------------------------------
# 3. IDENTIFIANTS DES BASES DE DONNÉES (Format Dictionnaire)
# ------------------------------------------------------------------------------

SOURCE_A_DB = {
    "host": os.getenv("SOURCE_A_DB_HOST", "localhost"),
    "port": int(os.getenv("SOURCE_A_DB_PORT", 5432)),
    "database": os.getenv("SOURCE_A_DB_NAME"),
    "user": os.getenv("SOURCE_A_DB_USER"),
    "password": os.getenv("SOURCE_A_DB_PASSWORD"),
}

SOURCE_B_DB = {
    "host": os.getenv("SOURCE_B_DB_HOST", "localhost"),
    "port": int(os.getenv("SOURCE_B_DB_PORT", 5432)),
    "database": os.getenv("SOURCE_B_DB_NAME"),
    "user": os.getenv("SOURCE_B_DB_USER"),
    "password": os.getenv("SOURCE_B_DB_PASSWORD"),
}

DW_DB = {
    "host": os.getenv("DW_DB_HOST", "localhost"),
    "port": int(os.getenv("DW_DB_PORT", 5432)),
    "database": os.getenv("DW_DB_NAME"),
    "user": os.getenv("DW_DB_USER"),
    "password": os.getenv("DW_DB_PASSWORD"),
    "schemas": {
        "bronze": os.getenv("DW_SCHEMA_BRONZE", "bronze"),
        "silver": os.getenv("DW_SCHEMA_SILVER", "silver"),
        "gold": os.getenv("DW_SCHEMA_GOLD", "gold"),
    }
}

# ------------------------------------------------------------------------------
# 4. GÉNÉRATEURS DE CONNEXION GENERIQUES
# ------------------------------------------------------------------------------

def get_db_uri(db_config: dict) -> str:
    """
    Génère une URI PostgreSQL standard (libpq / RFC 3986).
    Format : postgresql://user:password@host:port/database
    
    Compatible avec : psycopg2, psycopg3, asyncpg, pg8000, Polars, DuckDB, etc.
    """
    return (
        f"postgresql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )

def get_db_dsn(db_config: dict) -> str:
    """
    Génère une chaîne DSN classique (Key-Value).
    Format : host=localhost port=5432 dbname=my_db user=usr password=pwd
    
    Très courante avec psycopg2 ou les outils d'administration libpq.
    """
    return (
        f"host={db_config['host']} "
        f"port={db_config['port']} "
        f"dbname={db_config['database']} "
        f"user={db_config['user']} "
        f"password={db_config['password']}"
    )