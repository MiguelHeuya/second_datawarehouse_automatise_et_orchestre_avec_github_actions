"""
Fichier de configuration et de gestion des journaux (logs)
Pour le pipeline de réplication Bronze
"""

import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

def configurer_journaux():
    """
    Configure le système de journalisation pour le pipeline ETL.
    
    Retourne:
        tuple: (logger, quality_logger) - Les deux loggers configurés
    """
    
    # 1. Créer le dossier logs s'il n'existe pas
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # 2. Nom du fichier de logs principal avec la date
    date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    nom_fichier_log = f'logs/pipeline_replication_{date_str}.log'
    nom_fichier_erreurs = f'logs/erreurs_{date_str}.log'
    
    # 3. Configuration du logger racine
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # On capture tout
    
    # 4. Format des messages
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 5. Handler pour la console (affichage à l'écran)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # On voit INFO et plus à l'écran
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 6. Handler pour le fichier général (tout)
    file_handler = RotatingFileHandler(
        nom_fichier_log,
        maxBytes=10*1024*1024,  # 10 Mo
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)  # Tout dans le fichier
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 7. Handler pour les erreurs uniquement
    error_handler = RotatingFileHandler(
        nom_fichier_erreurs,
        maxBytes=5*1024*1024,  # 5 Mo
        backupCount=3,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)  # Seulement les erreurs
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)
    
    # 8. Logger spécifique pour la qualité (pour les stats)
    quality_logger = logging.getLogger('qualite')
    quality_logger.setLevel(logging.INFO)
    quality_logger.propagate = False  # Ne pas propager vers le root
    
    # Handler pour les logs de qualité
    quality_handler = RotatingFileHandler(
        f'logs/qualite_{date_str}.log',
        maxBytes=5*1024*1024,
        backupCount=3,
        encoding='utf-8'
    )
    quality_formatter = logging.Formatter(
        '%(asctime)s - QUALITE - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    quality_handler.setFormatter(quality_formatter)
    quality_logger.addHandler(quality_handler)
    
    return logger, quality_logger


def get_logger(name):
    """
    Récupère un logger nommé.
    
    Args:
        name (str): Nom du logger
    
    Returns:
        logging.Logger: Logger configuré
    """
    return logging.getLogger(name)


def formater_message(message, niveau='INFO'):
    """
    Formate un message pour les logs de qualité.
    
    Args:
        message (str): Le message à formater
        niveau (str): Le niveau du message
    
    Returns:
        str: Message formaté
    """
    return f"[{niveau}] {message}"


class LoggerContext:
    """
    Context manager pour ajouter des informations de contexte aux logs.
    """
    
    def __init__(self, logger, contexte):
        self.logger = logger
        self.contexte = contexte
        self.original_handlers = []
    
    def __enter__(self):
        # Ajouter un filtre pour ajouter le contexte
        class ContexteFilter(logging.Filter):
            def filter(self, record):
                record.contexte = self.contexte
                return True
        
        self.filter = ContexteFilter()
        for handler in self.logger.handlers:
            handler.addFilter(self.filter)
        
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Retirer le filtre
        for handler in self.logger.handlers:
            handler.removeFilter(self.filter)


# Fonctions utilitaires pour différents types de logs

def log_demarrage(logger, nom_pipeline):
    """
    Log le démarrage du pipeline.
    """
    logger.info("=" * 80)
    logger.info(f"🚀 DÉMARRAGE DU PIPELINE : {nom_pipeline}")
    logger.info(f"📅 Date et heure : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)


def log_arret(logger, succes=True, message=None):
    """
    Log l'arrêt du pipeline.
    """
    logger.info("=" * 80)
    if succes:
        logger.info("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
    else:
        logger.info("❌ PIPELINE TERMINÉ AVEC ERREURS")
    if message:
        logger.info(f"📝 {message}")
    logger.info(f"📅 Date et heure : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)


def log_etape(logger, nom_etape, numero=None, total=None):
    """
    Log le début d'une étape.
    """
    if numero and total:
        logger.info(f"\n📌 ÉTAPE {numero}/{total} : {nom_etape}")
    else:
        logger.info(f"\n📌 ÉTAPE : {nom_etape}")
    logger.info("-" * 60)


def log_succes(logger, message, details=None):
    """
    Log un succès.
    """
    if details:
        logger.info(f"✅ {message} : {details}")
    else:
        logger.info(f"✅ {message}")


def log_avertissement(logger, message, details=None):
    """
    Log un avertissement.
    """
    if details:
        logger.warning(f"⚠️ {message} : {details}")
    else:
        logger.warning(f"⚠️ {message}")


def log_erreur(logger, message, details=None, exception=None):
    """
    Log une erreur.
    """
    if exception:
        logger.error(f"❌ {message} : {details}", exc_info=True)
    elif details:
        logger.error(f"❌ {message} : {details}")
    else:
        logger.error(f"❌ {message}")


def log_critique(logger, message, details=None):
    """
    Log une erreur critique.
    """
    if details:
        logger.critical(f"💥 {message} : {details}")
    else:
        logger.critical(f"💥 {message}")


def log_statistique(logger, titre, donnees):
    """
    Log des statistiques formatées.
    
    Args:
        logger: Le logger à utiliser
        titre (str): Titre des statistiques
        donnees (dict): Dictionnaire des statistiques
    """
    logger.info(f"\n📊 {titre}")
    logger.info("-" * 40)
    for cle, valeur in donnees.items():
        if isinstance(valeur, (int, float)):
            logger.info(f"   {cle} : {valeur:,}")
        else:
            logger.info(f"   {cle} : {valeur}")


def log_progression(logger, actuel, total, message="Progression"):
    """
    Log la progression.
    """
    pourcentage = (actuel / total) * 100 if total > 0 else 0
    logger.debug(f"   {message} : {actuel}/{total} ({pourcentage:.1f}%)")


# ==============================================================================
# FONCTIONS POUR LES LOGS DE QUALITÉ SPÉCIFIQUES
# ==============================================================================

def log_qualite_table(quality_logger, nom_table, schema, source, lignes, statut):
    """
    Log la qualité d'une table répliquée.
    
    Args:
        quality_logger: Le logger de qualité
        nom_table (str): Nom de la table
        schema (str): Schéma source
        source (str): Base source
        lignes (int): Nombre de lignes
        statut (str): 'SUCCES' ou 'ECHEC'
    """
    message = f"TABLE | {source} | {schema}.{nom_table} | {lignes} lignes | {statut}"
    if statut == 'SUCCES':
        quality_logger.info(message)
    else:
        quality_logger.warning(message)


def log_qualite_passe(quality_logger, passe, statistiques):
    """
    Log la qualité d'une passe complète.
    
    Args:
        quality_logger: Le logger de qualité
        passe (int): Numéro de la passe
        statistiques (dict): Statistiques de la passe
    """
    message = f"PASSE {passe} | Tables: {statistiques.get('tables', 0)} | "
    message += f"Lignes: {statistiques.get('lignes', 0)} | "
    message += f"Ignorées: {statistiques.get('ignorees', 0)}"
    quality_logger.info(message)


def log_qualite_bilan(quality_logger, stats_p1, stats_p2, stats_final):
    """
    Log le bilan final de qualité.
    
    Args:
        quality_logger: Le logger de qualité
        stats_p1 (dict): Statistiques passe 1
        stats_p2 (dict): Statistiques passe 2
        stats_final (dict): Statistiques finales
    """
    quality_logger.info("=" * 60)
    quality_logger.info("BILAN QUALITÉ DU PIPELINE")
    quality_logger.info("=" * 60)
    
    quality_logger.info(f"PASSE 1 : {stats_p1.get('tables', 0)} tables, {stats_p1.get('lignes', 0)} lignes")
    quality_logger.info(f"PASSE 2 : {stats_p2.get('tables', 0)} tables, {stats_p2.get('lignes', 0)} lignes")
    quality_logger.info(f"TOTAL   : {stats_final.get('tables', 0)} tables, {stats_final.get('lignes', 0)} lignes")
    quality_logger.info("=" * 60)


def log_erreur_table(quality_logger, nom_table, schema, source, erreur):
    """
    Log une erreur sur une table.
    
    Args:
        quality_logger: Le logger de qualité
        nom_table (str): Nom de la table
        schema (str): Schéma source
        source (str): Base source
        erreur (str): Message d'erreur
    """
    quality_logger.error(f"ERREUR TABLE | {source} | {schema}.{nom_table} | {erreur}")