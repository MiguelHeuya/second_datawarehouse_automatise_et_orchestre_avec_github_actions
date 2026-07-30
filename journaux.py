# ==============================================================================
# MODULE DE JOURNALISATION ET SUIVI QUALITÉ DATA PIPELINE (LOGGING)
# ==============================================================================
import logging
import os
from datetime import datetime

# Nom du dossier où seront enregistrés les fichiers de logs
LOG_DIR = "logs"

def configurer_journaux():
    """
    Initialise et configure les deux loggers principaux :
    1. logger (execution_YYYY-MM-DD.log) : Suit le déroulement technique du pipeline.
    2. quality_logger (qualite_donnees_YYYY-MM-DD.log) : Suit le bilan qualité (lignes, tables, erreurs).
    
    Retourne:
        tuple: (logger, quality_logger)
    """
    # Création du répertoire de logs s'il n'existe pas
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    # Date du jour pour nommer les fichiers de logs
    date_jour = datetime.now().strftime("%Y-%m-%d")
    
    # --------------------------------------------------------------------------
    # 1. LOGGER D'EXÉCUTION TECHNIQUE
    # --------------------------------------------------------------------------
    logger = logging.getLogger("execution_logger")
    logger.setLevel(logging.DEBUG)
    
    # Éviter la duplication d'handlers si la fonction est appelée plusieurs fois
    if not logger.handlers:
        fichier_execution = os.path.join(LOG_DIR, f"execution_{date_jour}.log")
        fh_exec = logging.FileHandler(fichier_execution, encoding="utf-8")
        fh_exec.setLevel(logging.DEBUG)
        
        formatter_exec = logging.Formatter(
            "[%(asctime)s] [%(levelname)-8s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        fh_exec.setFormatter(formatter_exec)
        logger.addHandler(fh_exec)

    # --------------------------------------------------------------------------
    # 2. LOGGER DE QUALITÉ DES DONNÉES
    # --------------------------------------------------------------------------
    quality_logger = logging.getLogger("quality_logger")
    quality_logger.setLevel(logging.INFO)
    
    if not quality_logger.handlers:
        fichier_qualite = os.path.join(LOG_DIR, f"qualite_donnees_{date_jour}.log")
        fh_qualite = logging.FileHandler(fichier_qualite, encoding="utf-8")
        fh_qualite.setLevel(logging.INFO)
        
        formatter_qualite = logging.Formatter(
            "[%(asctime)s] [QUALITÉ] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        fh_qualite.setFormatter(formatter_qualite)
        quality_logger.addHandler(fh_qualite)

    return logger, quality_logger


# ==============================================================================
# FONCTIONS UTILITAIRES DE LOGS D'EXÉCUTION
# ==============================================================================

def log_demarrage(logger, nom_pipeline: str):
    """Signale le début d'un pipeline."""
    logger.info("=" * 80)
    logger.info(f"🚀 DÉMARRAGE DU PIPELINE : {nom_pipeline}")
    logger.info("=" * 80)

def log_arret(logger, succes: bool = True, message: str = ""):
    """Signale la fin d'un pipeline."""
    statut = "SUCCÈS" if succes else "ÉCHEC"
    logger.info("=" * 80)
    logger.info(f"🏁 FIN DE PIPELINE [{statut}] : {message}")
    logger.info("=" * 80 + "\n")

def log_etape(logger, description_etape: str, numero_etape: int = None, total_etapes: int = None):
    """Enregistre le passage d'une étape importante."""
    prefixe = f"[{numero_etape}/{total_etapes}] " if numero_etape and total_etapes else ""
    logger.info(f"📌 ÉTAPE {prefixe}: {description_etape}")

def log_succes(logger, message: str, details: str = ""):
    """Enregistre un événement réussi."""
    msg = f"✅ {message}" + (f" | Details: {details}" if details else "")
    logger.info(msg)

def log_avertissement(logger, message: str, details: str = ""):
    """Enregistre un avertissement."""
    msg = f"⚠️ {message}" + (f" | Details: {details}" if details else "")
    logger.warning(msg)

def log_erreur(logger, message: str, erreur: str = ""):
    """Enregistre une erreur non critique."""
    msg = f"❌ {message}" + (f" | Erreur: {erreur}" if erreur else "")
    logger.error(msg)

def log_critique(logger, message: str, erreur: str = ""):
    """Enregistre une erreur critique provoquant un arrêt."""
    msg = f"💥 ERREUR CRITIQUE: {message}" + (f" | Erreur: {erreur}" if erreur else "")
    logger.critical(msg)

def log_statistique(logger, label: str, valeur):
    """Loggue une métrique numérique ou statistique."""
    if isinstance(valeur, (int, float)):
        logger.info(f"📊 STATISTIQUE | {label} : {valeur:,}")
    else:
        logger.info(f"📊 STATISTIQUE | {label} : {valeur}")


# ==============================================================================
# FONCTIONS UTILITAIRES DE LOGS DE QUALITÉ / TRANSFERT TABLES
# ==============================================================================

def log_qualite_table(quality_logger, table_dest: str, schema_src: str, base_src: str, nombre_lignes: int, statut: str = "SUCCES"):
    """Enregistre le bilan individuel d'une table transférée."""
    quality_logger.info(
        f"TABLE: {table_dest} | SOURCE: {base_src}.{schema_src} | LIGNES: {nombre_lignes:,} | STATUT: {statut}"
    )

def log_erreur_table(quality_logger, table_src: str, schema_src: str, base_src: str, erreur: str):
    """Enregistre l'échec d'une table dans le log de qualité."""
    quality_logger.error(
        f"TABLE: {schema_src}.{table_src} | SOURCE: {base_src} | STATUT: ÉCHEC | ERREUR: {erreur}"
    )

def log_qualite_passe(quality_logger, numero_passe: int, stats: dict):
    """Enregistre les métriques résumées d'une passe de réplication."""
    quality_logger.info(
        f"PASSE {numero_passe} TERMINEE | Tables copiées: {stats.get('tables', 0)} | Lignes: {stats.get('lignes', 0):,} | Ignorées: {stats.get('ignorees', 0)}"
    )

def log_qualite_bilan(quality_logger, stats_p1: dict, stats_p2: dict, stats_final: dict):
    """Enregistre le bilan final consolidé du pipeline."""
    quality_logger.info("-" * 65)
    quality_logger.info("BILAN QUALITÉ FINAL DU PIPELINE")
    quality_logger.info(f"Total Tables Transférées : {stats_final.get('tables', 0):,}")
    quality_logger.info(f"Total Lignes Injectées    : {stats_final.get('lignes', 0):,}")
    quality_logger.info("-" * 65)