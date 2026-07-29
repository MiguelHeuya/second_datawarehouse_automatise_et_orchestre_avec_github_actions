"""
Script de contrôle qualité pour la couche Bronze
Vérifie l'intégrité des données répliquées
"""

import psycopg2
from config import base_source, data_warehouse

# ==============================================================================
# IMPORT DE LA JOURNALISATION
# ==============================================================================
from journaux import (
    configurer_journaux,
    log_demarrage,
    log_arret,
    log_etape,
    log_succes,
    log_avertissement,
    log_erreur,
    log_critique,
    log_statistique
)

# ==============================================================================
# CONFIGURATION DE LA JOURNALISATION
# ==============================================================================
logger, quality_logger = configurer_journaux()


def executer_controle_qualite_bronze():
    """
    Exécute le contrôle qualité complet de la couche Bronze.
    Vérifie l'intégrité des données entre les sources et le Data Warehouse.
    """
    
    # ==========================================================================
    # DÉMARRAGE DU CONTRÔLE QUALITÉ
    # ==========================================================================
    log_demarrage(logger, "Contrôle de Qualité - Couche Bronze")
    
    logger.info("=" * 80)
    logger.info("🔍  DEMARRAGE DU CONTROLE DE QUALITE DES DONNEES : COUCHE [BRONZE]  🔍")
    logger.info("=" * 80)
    
    # Variables pour le rapport final
    volumes_bronze = {}
    tables_manquantes = []
    tables_corrompues = []
    total_tables_sources = 0
    
    # ==========================================================================
    # ÉTAPE 1 : ANALYSE DU DATA WAREHOUSE
    # ==========================================================================
    log_etape(logger, "Analyse du Data Warehouse - Couche Bronze", 1, 3)
    
    logger.info("📡 Analyse de l'etat du stockage dans le Data Warehouse...")
    
    try:
        conn_dw = psycopg2.connect(**data_warehouse)
        curseur_dw = conn_dw.cursor()
        
        logger.debug("Connexion au Data Warehouse établie")
        
        # Récupérer toutes les tables du schéma bronze
        curseur_dw.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'bronze';
        """)
        tables_bronze = [ligne[0] for ligne in curseur_dw.fetchall()]
        
        logger.info(f"   Tables trouvées dans bronze : {len(tables_bronze)}")
        logger.debug(f"   Liste des tables : {tables_bronze}")
        
        # Compter les lignes pour chaque table
        for table in tables_bronze:
            curseur_dw.execute(f'SELECT COUNT(*) FROM "bronze"."{table}";')
            volumes_bronze[table] = curseur_dw.fetchone()[0]
            logger.debug(f"   Table {table} : {volumes_bronze[table]:,} lignes")
            
        curseur_dw.close()
        conn_dw.close()
        
        log_succes(logger, "Analyse DW terminée", f"{len(volumes_bronze)} table(s) inventoriée(s) dans [bronze]")
        logger.info(f"✨ Analyse DW terminee. {len(volumes_bronze)} table(s) inventoriee(s) dans [bronze].")
        
    except Exception as e:
        log_critique(logger, "Impossible de lire le Data Warehouse", str(e))
        logger.error(f"💥 ERREUR IMPOSSIBLE DE LIRE LE DATA WAREHOUSE : {e}")
        return

    # ==========================================================================
    # ÉTAPE 2 : AUDIT CROISÉ AVEC LES SOURCES
    # ==========================================================================
    log_etape(logger, "Audit croisé avec les bases sources", 2, 3)
    
    logger.info("🔬 Phase d'audit croise en cours avec les bases de donnees sources...")
    
    for source in base_source:
        nom_base = source['database']
        logger.info(f"📂 Verification de l'integrite de la source : [{nom_base.upper()}]")
        
        try:
            conn_src = psycopg2.connect(**source)
            curseur_src = conn_src.cursor()
            
            logger.debug(f"Connexion établie à la source {nom_base}")
            
            # Récupérer toutes les tables de la source
            requete_tables = """
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                  AND table_schema NOT LIKE 'pg_%%'
                  AND table_schema != 'information_schema';
            """
            curseur_src.execute(requete_tables)
            tables_sources = curseur_src.fetchall()
            
            logger.info(f"   Tables trouvées dans {nom_base} : {len(tables_sources)}")
            logger.debug(f"   Liste des tables : {[f'{s}.{t}' for s,t in tables_sources]}")
            
            tables_source_ok = 0
            tables_source_ko = 0
            
            for schema_src, table_src in tables_sources:
                total_tables_sources += 1
                nom_attendu_dw = f"{nom_base}_{schema_src}_{table_src}".lower()
                
                # Compter les lignes dans la source
                curseur_src.execute(f'SELECT COUNT(*) FROM "{schema_src}"."{table_src}";')
                lignes_source = curseur_src.fetchone()[0]
                
                # Vérifier si la table existe dans bronze
                if nom_attendu_dw not in volumes_bronze:
                    logger.warning(f"   ❌ RETARD / ABSENCE : [{schema_src}.{table_src}] non detectee dans la couche bronze.")
                    tables_manquantes.append({
                        "source": nom_base,
                        "table": f"{schema_src}.{table_src}",
                        "lignes_source": lignes_source
                    })
                    tables_source_ko += 1
                    continue
                
                lignes_bronze = volumes_bronze[nom_attendu_dw]
                
                # Vérifier l'alignement des volumes
                if lignes_source == lignes_bronze:
                    logger.info(f"   ✅ ALIGNEMENT PARFAIT : [{schema_src}.{table_src}] -> {lignes_bronze:,} / {lignes_source:,} lignes.")
                    tables_source_ok += 1
                else:
                    logger.warning(f"   🚨 ANOMALIE DE VOLUMETRIE : [{schema_src}.{table_src}] -> Bronze: {lignes_bronze:,} vs Source: {lignes_source:,}")
                    tables_corrompues.append({
                        "source": nom_base,
                        "table_attendu": nom_attendu_dw,
                        "table_origine": f"{schema_src}.{table_src}",
                        "lignes_source": lignes_source,
                        "lignes_bronze": lignes_bronze,
                        "ecart": lignes_source - lignes_bronze
                    })
                    tables_source_ko += 1
            
            # Statistiques pour la source
            logger.info(f"   Source {nom_base.upper()} : {tables_source_ok} OK, {tables_source_ko} KO")
            
            curseur_src.close()
            conn_src.close()
            
        except Exception as e:
            log_erreur(logger, f"Échec de l'audit pour la source {nom_base}", str(e))
            logger.error(f"   💥 Echec critique de la liaison d'audit avec {nom_base} : {e}")

    # ==========================================================================
    # ÉTAPE 3 : RAPPORT STRUCTURAL DE QUALITÉ
    # ==========================================================================
    log_etape(logger, "Génération du rapport de qualité", 3, 3)
    
    logger.info("=" * 80)
    logger.info("📊  RAPPORT DE SYNTHESE DU CONTROLE DE QUALITE [BRONZE]  📊")
    logger.info("=" * 80)
    
    total_erreurs = len(tables_manquantes) + len(tables_corrompues)
    
    # Statistiques générales
    stats = {
        "Tables sources inspectées": total_tables_sources,
        "Tables dans Bronze": len(volumes_bronze),
        "Tables manquantes": len(tables_manquantes),
        "Tables corrompues": len(tables_corrompues),
        "Total erreurs": total_erreurs
    }
    
    log_statistique(logger, "Statistiques du contrôle qualité", stats)
    
    logger.info(f"📝 Volume total de tables sources inspectees : {total_tables_sources}")
    logger.info(f"📝 Volume total de tables reperees dans Bronze : {len(volumes_bronze)}")
    logger.info("-" * 80)
    
    # Si tout est parfait
    if total_erreurs == 0:
        logger.info("🏆  SITUATION NOMINALE : 100% des donnees sources valides dans le schema [bronze] !")
        logger.info("🎉  L'integrite moleculaire de la structure est parfaite. Transition autorisee vers Silver.")
        
        # Log qualité
        quality_logger.info(f"CONTRÔLE QUALITÉ RÉUSSI | {total_tables_sources} tables | {len(volumes_bronze)} dans Bronze")
        
    else:
        logger.warning(f"⚠️  VULNERABILITES DECLAREES : {total_erreurs} point(s) de rupture detecte(s).")
        
        # Log qualité des problèmes
        quality_logger.warning(f"CONTRÔLE QUALITÉ ÉCHEC | {total_erreurs} problèmes détectés")
        
        # Détail des tables manquantes
        if tables_manquantes:
            logger.warning(f"❌ COMPOSANTS MANQUANTS ({len(tables_manquantes)}) :")
            for tm in tables_manquantes:
                logger.warning(f"   • [{tm['source'].upper()}] Table: {tm['table']} ({tm['lignes_source']:,} lignes manquantes)")
                
                # Log qualité détaillé
                quality_logger.warning(
                    f"TABLE MANQUANTE | {tm['source']} | {tm['table']} | {tm['lignes_source']:,} lignes"
                )
        
        # Détail des tables corrompues
        if tables_corrompues:
            logger.warning(f"🚨 DISCORDANCES NUMERIQUES ({len(tables_corrompues)}) :")
            for tc in tables_corrompues:
                logger.warning(
                    f"   • Table DW: bronze.{tc['table_attendu']} | "
                    f"Source: {tc['lignes_source']:,} l. | "
                    f"Bronze: {tc['lignes_bronze']:,} l. "
                    f"(Ecart: {tc['ecart']:,})"
                )
                
                # Log qualité détaillé
                quality_logger.warning(
                    f"TABLE CORROMPUE | {tc['source']} | {tc['table_origine']} | "
                    f"Bronze: {tc['lignes_bronze']:,} | "
                    f"Source: {tc['lignes_source']:,} | "
                    f"Écart: {tc['ecart']:,}"
                )
    
    logger.info("=" * 80)
    
    # ==========================================================================
    # FIN DU CONTRÔLE QUALITÉ
    # ==========================================================================
    succes = (total_erreurs == 0)
    log_arret(logger, succes=succes, message="Contrôle de qualité Bronze terminé")
    
    # Retourner les résultats pour une éventuelle utilisation
    return {
        'succes': succes,
        'total_tables_sources': total_tables_sources,
        'total_tables_bronze': len(volumes_bronze),
        'tables_manquantes': tables_manquantes,
        'tables_corrompues': tables_corrompues,
        'total_erreurs': total_erreurs
    }


# ==============================================================================
# POINT D'ENTRÉE PRINCIPAL
# ==============================================================================
if __name__ == "__main__":
    try:
        resultats = executer_controle_qualite_bronze()
        
        # Afficher un résumé dans la console
        if resultats and resultats['succes']:
            print("\n" + "=" * 80)
            print("✅ CONTRÔLE QUALITÉ RÉUSSI - 100% d'intégrité")
            print("=" * 80)
        elif resultats:
            print("\n" + "=" * 80)
            print("⚠️ CONTRÔLE QUALITÉ ÉCHEC - Des problèmes ont été détectés")
            print(f"   {resultats['total_erreurs']} erreur(s) trouvée(s)")
            print("=" * 80)
            
    except Exception as e:
        logger.critical(f"💥 Erreur inattendue dans le contrôle qualité : {e}", exc_info=True)
        print(f"\n💥 ERREUR CRITIQUE : {e}")
        exit(1)