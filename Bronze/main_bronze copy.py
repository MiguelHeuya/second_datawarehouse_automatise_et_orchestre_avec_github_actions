import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import traceback

# On importe vos configurations
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
    log_statistique,
    log_qualite_passe,
    log_qualite_bilan,
    log_qualite_table,
    log_erreur_table
)

# ==============================================================================
# CONFIGURATION DE LA JOURNALISATION
# ==============================================================================
logger, quality_logger = configurer_journaux()

# Aligner le titre principal
print("\n" + "="*80)
print("🚀  PIPELINE DE RÉPLICATION BRONZE : COMPROMIS DOUBLE PASSE AUTOMATIQUE  🚀")
print("="*80 + "\n")

# Log du démarrage du pipeline
log_demarrage(logger, "Pipeline de Réplication Bronze")
logger.info("📋 Configuration :")
logger.info(f"   - Nombre de sources : {len(base_source)}")
logger.info(f"   - Bases sources : {', '.join([s['database'] for s in base_source])}")
logger.info(f"   - Data Warehouse : {data_warehouse['database']}")
logger.info("")

# ==============================================================================
# ÉTAPE 1 : CONNEXION ET PRÉPARATION DU DATA WAREHOUSE
# ==============================================================================
log_etape(logger, "Connexion et préparation du Data Warehouse", 1, 3)
print("📡 Connexion au Data Warehouse de destination...")
logger.info("Connexion au Data Warehouse de destination...")

try:
    url_dw = f"postgresql://{data_warehouse['user']}:{data_warehouse['password']}@{data_warehouse['host']}/{data_warehouse['database']}?sslmode={data_warehouse['sslmode']}"
    moteur_dw = create_engine(url_dw)
    logger.debug("Moteur SQLAlchemy créé avec succès")

    # Initialisation du schéma Bronze
    conn_dw_brut = psycopg2.connect(**data_warehouse)
    curseur_dw = conn_dw_brut.cursor()
    curseur_dw.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    conn_dw_brut.commit() 
    curseur_dw.close()
    conn_dw_brut.close()
    
    log_succes(logger, "Schéma [bronze] prêt")
    print("✨  SUCCESS: Le schéma [bronze] est prêt.")
    print("-" * 60 + "\n")
    
except Exception as e:
    log_critique(logger, "Erreur critique lors de l'initialisation du Data Warehouse", str(e))
    print(f"💥  ERREUR CRITIQUE initialisation DW : {e}")
    exit(1)


def recuperer_tables_existantes_bronze():
    """Interroge le DW pour lister les tables présentes dans bronze."""
    logger.debug("Récupération des tables existantes dans bronze...")
    try:
        conn = psycopg2.connect(**data_warehouse)
        curseur = conn.cursor()
        curseur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze';")
        tables = {ligne[0] for ligne in curseur.fetchall()}
        curseur.close()
        conn.close()
        logger.debug(f"   Tables existantes : {len(tables)}")
        return tables
    except Exception as e:
        log_erreur(logger, "Erreur lors de la récupération des tables existantes", str(e))
        return set()


# Structuration des rapports par passe
stats_passe_1 = {s['database']: {'tables': 0, 'lignes': 0, 'ignorees': 0} for s in base_source}
stats_passe_2 = {s['database']: {'tables': 0, 'lignes': 0, 'ignorees': 0} for s in base_source}

logger.info("Statistiques initialisées")


# ==============================================================================
# FONCTION MAÎTRESSE DE RÉPLICATION
# ==============================================================================
def executer_pipeline_replication(numero_passe, dictionnaire_stats, activer_traceback=False):
    logger.info("")
    log_etape(logger, f"Exécution de la Passe {numero_passe}", 2, 3)
    
    print(f"\n" + "═"*80)
    print(f"🔄  DÉMARRAGE DE LA PASSE N°{numero_passe}  🔄")
    print("═"*80)
    
    logger.info(f"Démarrage de la passe {numero_passe}")
    
    # Étape critique : Rafraîchir l'état des tables existantes dans bronze avant la passe
    tables_deja_dans_bronze = recuperer_tables_existantes_bronze()
    logger.info(f"Tables déjà présentes : {len(tables_deja_dans_bronze)}")
    
    for source in base_source:
        nom_base_source = source['database']
        print(f"\n📂 Source active : [{nom_base_source.upper()}]")
        logger.info(f"Traitement de la source : {nom_base_source.upper()}")
        
        try:
            conn_source = psycopg2.connect(**source)
            curseur_source = conn_source.cursor()
            logger.debug(f"Connexion établie à {nom_base_source}")
            
            # Lister toutes les tables de la source
            requete_liste_tables = """
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                  AND table_schema NOT LIKE 'pg_%%'
                  AND table_schema != 'information_schema';
            """
            curseur_source.execute(requete_liste_tables)
            liste_tables = [(ligne[0], ligne[1]) for ligne in curseur_source.fetchall()]
            
            logger.info(f"   Tables trouvées dans {nom_base_source} : {len(liste_tables)}")
            logger.debug(f"   Liste des tables : {[f'{s}.{t}' for s,t in liste_tables]}")
            
            for schema_origine, nom_table in liste_tables:
                nom_table_destination = f"{nom_base_source}_{schema_origine}_{nom_table}"
                
                # LOGIQUE D'IGNORANCE : Si la table existe déjà dans Bronze, on zappe
                if nom_table_destination in tables_deja_dans_bronze:
                    dictionnaire_stats[nom_base_source]['ignorees'] += 1
                    if numero_passe == 2:
                        print(f"    ⏭️  [COUCHE EXISTANTE] : [{schema_origine}.{nom_table}] déjà copiée au run 1. Ignorée.")
                    logger.debug(f"   Table ignorée (déjà présente) : {nom_table_destination}")
                    continue
                
                print(f"    ⏳ [{schema_origine}.{nom_table}] ==> [bronze.{nom_table_destination}]")
                logger.info(f"   Copie de {schema_origine}.{nom_table} vers bronze.{nom_table_destination}")
                
                try:
                    requete_extraction = f'SELECT * FROM "{schema_origine}"."{nom_table}"'
                    taille_paquet = 200000
                    generateur_df = pd.read_sql(requete_extraction, conn_source, chunksize=taille_paquet)
                    
                    est_premier_paquet = True
                    total_lignes_table = 0
                    
                    for df_paquet in generateur_df:
                        mode_insertion = 'replace' if est_premier_paquet else 'append'
                        total_lignes_table += len(df_paquet)
                        
                        logger.debug(f"      Paquet de {len(df_paquet)} lignes - mode {mode_insertion}")
                        df_paquet.to_sql(
                            name=nom_table_destination,
                            con=moteur_dw,
                            schema='bronze',
                            if_exists=mode_insertion, 
                            index=False          
                        )
                        est_premier_paquet = False
                    
                    print(f"    ✅ SUCCÈS : Transférée ({total_lignes_table:,} lignes).")
                    log_succes(logger, f"Table {schema_origine}.{nom_table} transférée", f"{total_lignes_table:,} lignes")
                    
                    # Log qualité
                    log_qualite_table(
                        quality_logger,
                        nom_table_destination,
                        schema_origine,
                        nom_base_source,
                        total_lignes_table,
                        'SUCCES'
                    )
                    
                    dictionnaire_stats[nom_base_source]['tables'] += 1
                    dictionnaire_stats[nom_base_source]['lignes'] += total_lignes_table
                    
                except Exception as table_error:
                    print(f"    ⚠️  [ÉCHEC TABLE] Erreur rencontrée sur [{schema_origine}.{nom_table}]")
                    log_erreur(logger, f"Échec sur {schema_origine}.{nom_table}", str(table_error))
                    
                    # Log qualité d'erreur
                    log_erreur_table(
                        quality_logger,
                        nom_table,
                        schema_origine,
                        nom_base_source,
                        str(table_error)
                    )
                    
                    if activer_traceback:
                        print("\n💥 --- EXPANSION TRACEBACK (DÉBOGAGE PASSE 2) ---")
                        traceback.print_exc()
                        print("--------------------------------------------------\n")
                        logger.debug("Traceback activé pour débogage", exc_info=True)
                    else:
                        print(f"        👉 Motif abrégé : {table_error}")
                    continue
                    
            curseur_source.close()
            conn_source.close()
            logger.info(f"Source {nom_base_source} traitée avec succès")
            
        except Exception as base_error:
            log_erreur(logger, f"Erreur critique sur la source {nom_base_source}", str(base_error))
            print(f"💥 Erreur critique d'accès à la source {nom_base_source} : {base_error}")
    
    # Log qualité pour la passe
    total_tables = sum(d['tables'] for d in dictionnaire_stats.values())
    total_lignes = sum(d['lignes'] for d in dictionnaire_stats.values())
    total_ignorees = sum(d['ignorees'] for d in dictionnaire_stats.values())
    
    stats_passe = {
        'tables': total_tables,
        'lignes': total_lignes,
        'ignorees': total_ignorees
    }
    log_qualite_passe(quality_logger, numero_passe, stats_passe)
    logger.info(f"Passe {numero_passe} terminée : {total_tables} tables, {total_lignes} lignes")


# ==============================================================================
# EXÉCUTION ENCHAÎNÉE DES DEUX PASSES
# ==============================================================================
logger.info("")
logger.info("=" * 80)
logger.info("DÉBUT DE L'EXÉCUTION ENCHAÎNÉE DES DEUX PASSES")
logger.info("=" * 80)

# Passe 1 : Extraction globale aveugle de masse
executer_pipeline_replication(numero_passe=1, dictionnaire_stats=stats_passe_1, activer_traceback=False)

# Passe 2 : Extraction de rattrapage ciblée avec affichage des erreurs complexes
executer_pipeline_replication(numero_passe=2, dictionnaire_stats=stats_passe_2, activer_traceback=True)


# ==============================================================================
# ÉTAPE 3 : ENGINE DE RAPPORT STATISTIQUE MULTI-NIVEAUX
# ==============================================================================
log_etape(logger, "Génération du rapport final", 3, 3)

print("\n" + "="*80)
print("🏁🏁🏁                    BILAN FINAL DU PIPELINE MULTI-PASSE              🏁🏁🏁")
print("="*80)

logger.info("Génération du bilan final")
logger.info("=" * 80)

# Variables pour cumuler les totaux généraux (toutes sources confondues)
global_p1_tables, global_p1_lignes = 0, 0
global_p2_tables, global_p2_lignes = 0, 0

print("\n📊 1. ANALYSE DÉTAILLÉE PAR SOURCE DE DONNÉES :")
logger.info("ANALYSE DÉTAILLÉE PAR SOURCE DE DONNÉES")

for source in base_source:
    db = source['database']
    p1 = stats_passe_1[db]
    p2 = stats_passe_2[db]
    
    # Calculs locaux combinés pour cette base précise
    total_tables_base = p1['tables'] + p2['tables']
    total_lignes_base = p1['lignes'] + p2['lignes']
    
    # Accumulation globale
    global_p1_tables += p1['tables']
    global_p1_lignes += p1['lignes']
    global_p2_tables += p2['tables']
    global_p2_lignes += p2['lignes']
    
    print(f"\n🔹 [SOURCE : {db.upper()}]")
    print(f"   ├─ PASSE 1 (Initiale)  : {p1['tables']:,} table(s) copiée(s) | {p1['lignes']:,} ligne(s)")
    print(f"   ├─ PASSE 2 (Rattrapage): {p2['tables']:,} table(s) sauvée(s) | {p2['lignes']:,} ligne(s)")
    print(f"   └─ TOTAL COMBINÉ       : {total_tables_base:,} table(s) intégrée(s) | {total_lignes_base:,} ligne(s)")
    print("   " + "-"*40)
    
    # Log des statistiques par source
    logger.info(f"Source {db.upper()} : P1={p1['tables']} tables, P2={p2['tables']} tables, Total={total_tables_base} tables")

print("\n🏆 2. STATISTIQUES SYNTHÉTIQUES COMBINÉES (TOUTES SOURCES RÉUNIES) :")
print("-" * 65)
print(f"📈 TOTAL PASSE 1 : {global_p1_tables:,} table(s) insérée(s) au premier passage ({global_p1_lignes:,} lignes).")
print(f"📈 TOTAL PASSE 2 : {global_p2_tables:,} table(s) repêchée(s) au second passage  ({global_p2_lignes:,} lignes).")
print("-" * 65)
print(f"🔥 BILAN GLOBAL  : {global_p1_tables + global_p2_tables:,} table(s) au total injectée(s) dans [BRONZE].")
print(f"🔥 LINES TOTALS  : {global_p1_lignes + global_p2_lignes:,} lignes chargées au total dans l'infrastructure.")
print("\n🎉 Opération incrémentale à double niveau exécutée avec succès ! 🎉\n")
print("="*80 + "\n")

# Log du bilan final
logger.info("=" * 80)
logger.info("BILAN FINAL DU PIPELINE")
logger.info("=" * 80)
logger.info(f"📈 TOTAL PASSE 1 : {global_p1_tables:,} tables, {global_p1_lignes:,} lignes")
logger.info(f"📈 TOTAL PASSE 2 : {global_p2_tables:,} tables, {global_p2_lignes:,} lignes")
logger.info(f"🔥 BILAN GLOBAL  : {global_p1_tables + global_p2_tables:,} tables, {global_p1_lignes + global_p2_lignes:,} lignes")

# Log qualité du bilan final
stats_final = {
    'tables': global_p1_tables + global_p2_tables,
    'lignes': global_p1_lignes + global_p2_lignes
}
stats_p1_total = {'tables': global_p1_tables, 'lignes': global_p1_lignes}
stats_p2_total = {'tables': global_p2_tables, 'lignes': global_p2_lignes}

log_qualite_bilan(quality_logger, stats_p1_total, stats_p2_total, stats_final)

# Log de fin
log_arret(logger, succes=True, message="Pipeline de réplication Bronze terminé avec succès")