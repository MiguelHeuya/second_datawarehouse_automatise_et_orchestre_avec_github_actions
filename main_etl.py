import sys
import logging
from datetime import datetime
import adbc_driver_postgresql.dbapi as adbc_dbapi

# Imports des fonctions d'exécution des 3 couches
from config import DW_DB, get_db_uri
from bronze_extract_and_load import run_bronze_extraction
from silver_data_cleaning import run_silver_transformation as run_silver_pipeline
from gold_business_logic import run_gold_transformation

DW_URI = get_db_uri(DW_DB)
SCHEMAS_TO_PROTECT = ["bronze", "silver", "gold"]

# ------------------------------------------------------------------------------
# CONFIGURATION DU LOGGING (Fichier Horodaté + Terminal)
# ------------------------------------------------------------------------------
log_filename = f"etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("ETL_Orchestrator")


# ------------------------------------------------------------------------------
# LOGIQUE D'ATOMICITÉ & SÉCURISATION (ROLLBACK EN CAS D'ÉCHEC)
# ------------------------------------------------------------------------------

def drop_all_schemas(dw_uri: str = DW_URI):
    """
    Supprime totalement les schémas bronze, silver et gold en cas d'échec.
    Garantit qu'aucune donnée partielle ou corrompue ne persiste en base.
    """
    logger.warning("🚨 [ROLLBACK] Nettoyage d'urgence : Suppression des schémas modifiés...")
    try:
        with adbc_dbapi.connect(dw_uri) as conn:
            with conn.cursor() as cur:
                for schema in SCHEMAS_TO_PROTECT:
                    cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            conn.commit()
        logger.info("🧹 [ROLLBACK] Réinitialisation de la base de données effectuée avec succès.")
    except Exception as rollback_err:
        logger.error(f"❌ Échec lors du nettoyage de la base de données : {rollback_err}")


def run_full_etl_pipeline() -> bool:
    """
    Orchestre les 3 pipelines (Bronze -> Silver -> Gold).
    Applique une logique "Tout ou Rien" (Atomicité globale).
    """
    start_time = datetime.now()
    
    logger.info("=" * 90)
    logger.info("🚀 DÉMARRAGE DE L'ORCHESTRATION DU PIPELINE ETL COMPLET")
    logger.info(f"📅 Horodatage : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 90)

    # Définition de la séquence d'exécution
    pipeline_steps = [
        ("1/3 - BRONZE (Extraction & Chargement Brut)", run_bronze_extraction),
        ("2/3 - SILVER (Nettoyage & Typpage)", run_silver_pipeline),
        ("3/3 - GOLD   (Modélisation & Indexation)", run_gold_transformation),
    ]

    for step_label, step_func in pipeline_steps:
        logger.info("\n" + "-" * 90)
        logger.info(f"⏳ ÉTAPE EN COURS : {step_label}")
        logger.info("-" * 90)

        try:
            success = step_func()
            if not success:
                raise RuntimeError(f"L'étape [{step_label}] a renvoyé un statut d'échec (False).")
            
            logger.info(f"✅ ÉTAPE RÉUSSIE : {step_label}")

        except Exception as e:
            logger.error("\n" + "=" * 90)
            logger.error(f"💥 ERREUR CRITIQUE DÉTECTÉE LORS DE L'ÉTAPE : [{step_label}]")
            logger.error(f"Détail : {e}", exc_info=True)
            logger.error("=" * 90)

            # Rollback complet pour ne laisser aucun enregistrement partiel
            drop_all_schemas()

            duration = datetime.now() - start_time
            logger.error("=" * 90)
            logger.error(f"❌ PIPELINE ETL ANNULÉ APPRÈS {duration.total_seconds():.2f}s")
            logger.error(f"📄 Consultez le journal détaillé dans : {log_filename}")
            logger.error("=" * 90)
            return False

    # Confirmation de succès global
    duration = datetime.now() - start_time
    logger.info("\n" + "=" * 90)
    logger.info(f"🎉 PIPELINE ETL EXÉCUTÉ ET VALIDÉ AVEC SUCCÈS EN {duration.total_seconds():.2f}s")
    logger.info("💾 Les données des couches Bronze, Silver et Gold sont synchronisées dans PostgreSQL.")
    logger.info(f"📄 Rapport d'exécution enregistré dans : {log_filename}")
    logger.info("=" * 90)
    return True


# ------------------------------------------------------------------------------
# POINT D'ENTRÉE DU SCRIPT
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    success = run_full_etl_pipeline()
    sys.exit(0 if success else 1)