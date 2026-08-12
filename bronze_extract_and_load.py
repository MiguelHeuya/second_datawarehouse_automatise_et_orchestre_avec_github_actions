import sys
import logging
import polars as pl
import adbc_driver_postgresql.dbapi as adbc_dbapi
from config import SOURCE_A_DB, SOURCE_B_DB, DW_DB, get_db_uri

# Logger dédié à ce module
logger = logging.getLogger("ETL_Bronze")

# Configurations par défaut issues de config.py
DEFAULT_DW_URI = get_db_uri(DW_DB)
DEFAULT_BRONZE_SCHEMA = DW_DB["schemas"]["bronze"]

DEFAULT_SOURCES = [
    {"alias": "sourcea", "config": SOURCE_A_DB},
    {"alias": "sourceb", "config": SOURCE_B_DB},
]


def create_schema_if_not_exists(dw_uri: str = DEFAULT_DW_URI, schema_name: str = DEFAULT_BRONZE_SCHEMA):
    """
    S'assure que le schéma destination existe dans le Data Warehouse via ADBC.
    """
    logger.info(f"🏗️  Vérification / Création du schéma : [{schema_name}]")
    ddl_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    
    with adbc_dbapi.connect(dw_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_query)
        conn.commit()


def fetch_source_tables(source_uri: str) -> list[tuple[str, str]]:
    """
    Récupère la liste des tables utilisateur d'une base source.
    Note : Pas de point-virgule final pour assurer la compatibilité ConnectorX.
    """
    query = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          AND table_type = 'BASE TABLE'
    """
    df_tables = pl.read_database_uri(query=query, uri=source_uri)
    return list(zip(df_tables["table_schema"], df_tables["table_name"]))


def run_bronze_extraction(
    sources: list[dict] = None,
    dw_uri: str = DEFAULT_DW_URI,
    bronze_schema: str = DEFAULT_BRONZE_SCHEMA
) -> bool:
    """
    Exécute le pipeline d'ingestion complète des sources vers le schéma Bronze.
    
    Returns:
        bool: True si toutes les tables ont été transférées, False en cas d'erreur.
    """
    if sources is None:
        sources = DEFAULT_SOURCES

    logger.info("=" * 80)
    logger.info("🚀 DÉMARRAGE DU PIPELINE D'INGESTION - COUCHE BRONZE")
    logger.info("=" * 80)

    try:
        # Étape 1 : Création / Validation du schéma Bronze
        create_schema_if_not_exists(dw_uri=dw_uri, schema_name=bronze_schema)

        # Étape 2 : Parcours et extraction des bases sources
        for source in sources:
            alias = source["alias"].lower()
            config = source["config"]
            source_uri = get_db_uri(config)
            db_name = config["database"]

            logger.info("-" * 80)
            logger.info(f"📂 SOURCE EN COURS : [{alias.upper()}] (Base : '{db_name}')")
            logger.info("-" * 80)

            tables = fetch_source_tables(source_uri)
            total_tables = len(tables)

            if total_tables == 0:
                logger.warning(f"⚠️  Aucune table trouvée dans la source [{alias.upper()}].")
                continue

            logger.info(f"📊 {total_tables} table(s) identifiée(s) pour ingestion.")

            for idx, (schema_name, table_name) in enumerate(tables, 1):
                target_table_name = f"{alias}_{schema_name}_{table_name}"
                full_target_table = f"{bronze_schema}.{target_table_name}"

                # Lecture depuis la source (sans point-virgule final)
                read_query = f'SELECT * FROM "{schema_name}"."{table_name}"'
                df = pl.read_database_uri(query=read_query, uri=source_uri)
                nb_rows = df.height

                # Écriture / Remplacement dans le Data Warehouse
                df.write_database(
                    table_name=full_target_table,
                    connection=dw_uri,
                    if_table_exists="replace",
                    engine="adbc"
                )

                # Affichage formaté lisible
                formatted_rows = f"{nb_rows:,}".replace(",", " ")
                logger.info(
                    f"  ├─ [{idx}/{total_tables}] {schema_name}.{table_name:<30} "
                    f"➔  {full_target_table:<45} | ✅ {formatted_rows:>8} lignes"
                )

        logger.info("=" * 80)
        logger.info("🎉 PIPELINE BRONZE TERMINÉ AVEC SUCCÈS")
        logger.info("=" * 80)
        return True

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ ERREUR CRITIQUE DURANT L'INGESTION BRONZE : {e}", exc_info=True)
        logger.error("=" * 80)
        return False


# ------------------------------------------------------------------------------
# POINT D'ENTRÉE AUTONOME
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("🔔 Exécution autonome lancée depuis le terminal.")
    success = run_bronze_extraction()
    sys.exit(0 if success else 1)