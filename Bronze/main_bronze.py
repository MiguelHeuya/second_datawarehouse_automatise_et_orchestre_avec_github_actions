import polars as pl
from config import base_source, data_warehouse, construire_uri_postgresql, construire_uri_psycopg2
import traceback
import psycopg2
from psycopg2.extras import execute_values
import io

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
print("🚀  PIPELINE DE RÉPLICATION BRONZE - POLARS ULTRA-RAPIDE  🚀")
print("="*80 + "\n")

# Log du démarrage du pipeline
log_demarrage(logger, "Pipeline de Réplication Bronze (Polars)")
logger.info("📋 Configuration :")
logger.info(f"   - Nombre de sources : {len(base_source)}")
logger.info(f"   - Bases sources : {', '.join([s['database'] for s in base_source])}")
logger.info(f"   - Data Warehouse : {data_warehouse['database']}")
logger.info("")

# ==============================================================================
# ÉTAPE 1 : CONNEXION ET PRÉPARATION DU DATA WAREHOUSE
# ==============================================================================
log_etape(logger, "Connexion et préparation du Data Warehouse", 1, 2)
print("📡 Connexion au Data Warehouse de destination...")

try:
    # Construction de l'URI pour Polars (connectorx)
    dw_uri = construire_uri_postgresql(data_warehouse)
    logger.debug(f"URI DW (masquée) : postgresql://{data_warehouse['user']}:****@{data_warehouse['host']}:{data_warehouse['port']}/{data_warehouse['database']}")
    
    # Connexion pour les opérations DDL avec psycopg2
    dw_uri_psycopg2 = construire_uri_psycopg2(data_warehouse)
    conn_dw_brut = psycopg2.connect(dw_uri_psycopg2)
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
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'"
        df = pl.read_database_uri(query, dw_uri)
        tables = set(df['table_name'].to_list()) if not df.is_empty() else set()
        logger.debug(f"   Tables existantes : {len(tables)}")
        return tables
    except Exception as e:
        log_erreur(logger, "Erreur lors de la récupération des tables existantes", str(e))
        return set()


def ecrire_table_avec_psycopg2(df, nom_table, schema, uri):
    """
    Écrit un DataFrame Polars dans PostgreSQL en utilisant psycopg2 avec COPY pour des performances optimales.
    """
    conn = psycopg2.connect(uri)
    conn.autocommit = False
    curseur = conn.cursor()
    
    try:
        # Récupérer les noms de colonnes
        colonnes = df.columns
        types = df.schema
        
        # Créer la table si elle n'existe pas
        # On détermine les types PostgreSQL à partir des types Polars
        type_mapping = {
            pl.Int64: 'BIGINT',
            pl.Int32: 'INTEGER',
            pl.Int16: 'SMALLINT',
            pl.Int8: 'SMALLINT',
            pl.Float64: 'DOUBLE PRECISION',
            pl.Float32: 'REAL',
            pl.Utf8: 'TEXT',
            pl.Boolean: 'BOOLEAN',
            pl.Date: 'DATE',
            pl.Datetime: 'TIMESTAMP',
            pl.Time: 'TIME',
            pl.Binary: 'BYTEA',
            pl.Decimal: 'DECIMAL',
        }
        
        # Construction de la requête CREATE TABLE
        colonnes_sql = []
        for col in colonnes:
            polars_type = types[col]
            # Récupérer le type PostgreSQL correspondant
            pg_type = 'TEXT'  # Par défaut
            for ptype, pgtype in type_mapping.items():
                if isinstance(polars_type, ptype):
                    pg_type = pgtype
                    break
            
            # Nettoyer le nom de colonne (remplacer les espaces par des underscores)
            col_propre = col.replace(' ', '_').replace('"', '')
            colonnes_sql.append(f'"{col_propre}" {pg_type}')
        
        create_table_sql = f'CREATE TABLE IF NOT EXISTS {schema}."{nom_table}" ({", ".join(colonnes_sql)});'
        curseur.execute(create_table_sql)
        
        # Vider la table avant d'insérer (comportement 'replace')
        curseur.execute(f'TRUNCATE TABLE {schema}."{nom_table}";')
        
        # Si le DataFrame est vide, on s'arrête là
        if df.is_empty():
            conn.commit()
            return
        
        # Utilisation de COPY pour une insertion massive
        # Convertir le DataFrame en CSV en mémoire
        output = io.StringIO()
        # Écrire le CSV sans en-tête
        df.write_csv(output, include_header=False)
        output.seek(0)
        
        # COPY FROM avec CSV
        curseur.copy_expert(
            f'COPY {schema}."{nom_table}" FROM STDIN WITH CSV',
            output
        )
        
        conn.commit()
        logger.debug(f"   {len(df)} lignes insérées dans {nom_table} via COPY")
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        curseur.close()
        conn.close()


def ecrire_table_polars_write_database(df, nom_table, schema, uri):
    """
    Tente d'utiliser la méthode write_database de Polars (si disponible).
    Fallback sur la méthode COPY avec psycopg2.
    """
    try:
        # Essayer d'utiliser la méthode native de Polars
        df.write_database(
            table_name=f'{schema}."{nom_table}"',
            connection=uri,
            if_table_exists='replace',
            engine='postgresql'
        )
        logger.debug(f"   Écriture avec Polars.write_database réussie")
        return True
    except AttributeError:
        # La méthode n'existe pas, utiliser psycopg2
        logger.debug(f"   Polars.write_database non disponible, utilisation de psycopg2")
        ecrire_table_avec_psycopg2(df, nom_table, schema, uri)
        return True
    except Exception as e:
        # Si l'écriture Polars échoue, essayer psycopg2
        logger.warning(f"   Écriture Polars échouée, fallback sur psycopg2 : {e}")
        ecrire_table_avec_psycopg2(df, nom_table, schema, uri)
        return True


# Structuration des statistiques
stats_globales = {s['database']: {'tables': 0, 'lignes': 0, 'ignorees': 0} for s in base_source}

logger.info("Statistiques initialisées")


# ==============================================================================
# FONCTION MAÎTRESSE DE RÉPLICATION (MONO-PASSE)
# ==============================================================================
def executer_pipeline_replication(dictionnaire_stats, activer_traceback=False):
    logger.info("")
    log_etape(logger, "Exécution de la réplication unique", 2, 2)
    
    print(f"\n" + "═"*80)
    print(f"🔄  DÉMARRAGE DE LA RÉPLICATION UNIQUE  🔄")
    print("═"*80)
    
    logger.info("Démarrage de la réplication unique")
    
    # Rafraîchir l'état des tables existantes dans bronze
    tables_deja_dans_bronze = recuperer_tables_existantes_bronze()
    logger.info(f"Tables déjà présentes : {len(tables_deja_dans_bronze)}")
    
    for source in base_source:
        nom_base_source = source['database']
        print(f"\n📂 Source active : [{nom_base_source.upper()}]")
        logger.info(f"Traitement de la source : {nom_base_source.upper()}")
        
        # Construction de l'URI source
        source_uri = construire_uri_postgresql(source)
        dw_uri_psycopg2 = construire_uri_psycopg2(data_warehouse)
        logger.debug(f"URI source (masquée) : postgresql://{source['user']}:****@{source['host']}:{source['port']}/{source['database']}")
        
        try:
            # Lister toutes les tables de la source avec Polars
            requete_liste_tables = """
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                  AND table_schema NOT LIKE 'pg_%%'
                  AND table_schema != 'information_schema'
            """
            
            df_tables = pl.read_database_uri(requete_liste_tables, source_uri)
            
            # Vérifier si le DataFrame contient des données
            if df_tables.is_empty():
                logger.warning(f"Aucune table trouvée dans {nom_base_source}")
                print(f"    ⚠️  Aucune table trouvée dans la source.")
                continue
            
            liste_tables = [(row['table_schema'], row['table_name']) for row in df_tables.to_dicts()]
            
            logger.info(f"   Tables trouvées dans {nom_base_source} : {len(liste_tables)}")
            logger.debug(f"   Liste des tables : {[f'{s}.{t}' for s,t in liste_tables]}")
            
            for schema_origine, nom_table in liste_tables:
                nom_table_destination = f"{nom_base_source}_{schema_origine}_{nom_table}"
                
                # LOGIQUE D'IGNORANCE : Si la table existe déjà dans Bronze, on zappe
                if nom_table_destination in tables_deja_dans_bronze:
                    dictionnaire_stats[nom_base_source]['ignorees'] += 1
                    print(f"    ⏭️  [COUCHE EXISTANTE] : [{schema_origine}.{nom_table}] déjà copiée. Ignorée.")
                    logger.debug(f"   Table ignorée (déjà présente) : {nom_table_destination}")
                    continue
                
                print(f"    ⏳ [{schema_origine}.{nom_table}] ==> [bronze.{nom_table_destination}]")
                logger.info(f"   Copie de {schema_origine}.{nom_table} vers bronze.{nom_table_destination}")
                
                try:
                    # Extraction complète avec Polars
                    requete_extraction = f'SELECT * FROM "{schema_origine}"."{nom_table}"'
                    
                    # Lecture directe avec Polars
                    df = pl.read_database_uri(requete_extraction, source_uri)
                    total_lignes_table = df.height
                    
                    if total_lignes_table == 0:
                        print(f"    ⚠️  Table vide : aucune ligne à transférer.")
                        logger.warning(f"Table {schema_origine}.{nom_table} est vide")
                        
                        # Créer la table vide
                        ecrire_table_avec_psycopg2(
                            df,
                            nom_table_destination,
                            'bronze',
                            dw_uri_psycopg2
                        )
                    else:
                        # Écriture avec fallback automatique
                        ecrire_table_polars_write_database(
                            df,
                            nom_table_destination,
                            'bronze',
                            dw_uri_psycopg2
                        )
                    
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
                        print("\n💥 --- EXPANSION TRACEBACK (DÉBOGAGE) ---")
                        traceback.print_exc()
                        print("--------------------------------------------------\n")
                        logger.debug("Traceback activé pour débogage", exc_info=True)
                    else:
                        print(f"        👉 Motif abrégé : {table_error}")
                    continue
                    
            logger.info(f"Source {nom_base_source} traitée avec succès")
            
        except Exception as base_error:
            log_erreur(logger, f"Erreur critique sur la source {nom_base_source}", str(base_error))
            print(f"💥 Erreur critique d'accès à la source {nom_base_source} : {base_error}")
            if activer_traceback:
                print("\n💥 --- EXPANSION TRACEBACK (DÉBOGAGE) ---")
                traceback.print_exc()
                print("--------------------------------------------------\n")
    
    # Log qualité pour la passe
    total_tables = sum(d['tables'] for d in dictionnaire_stats.values())
    total_lignes = sum(d['lignes'] for d in dictionnaire_stats.values())
    total_ignorees = sum(d['ignorees'] for d in dictionnaire_stats.values())
    
    stats_passe = {
        'tables': total_tables,
        'lignes': total_lignes,
        'ignorees': total_ignorees
    }
    log_qualite_passe(quality_logger, 1, stats_passe)
    logger.info(f"Réplication terminée : {total_tables} tables, {total_lignes} lignes")
    
    return total_tables, total_lignes


# ==============================================================================
# EXÉCUTION DE LA RÉPLICATION UNIQUE
# ==============================================================================
logger.info("")
logger.info("=" * 80)
logger.info("DÉBUT DE L'EXÉCUTION DE LA RÉPLICATION UNIQUE")
logger.info("=" * 80)

total_tables, total_lignes = executer_pipeline_replication(
    dictionnaire_stats=stats_globales,
    activer_traceback=True
)


# ==============================================================================
# ÉTAPE FINALE : RAPPORT STATISTIQUE
# ==============================================================================
log_etape(logger, "Génération du rapport final", 2, 2)

print("\n" + "="*80)
print("🏁🏁🏁                    BILAN FINAL DU PIPELINE MONO-PASSE              🏁🏁🏁")
print("="*80)

logger.info("Génération du bilan final")
logger.info("=" * 80)

print("\n📊 1. ANALYSE DÉTAILLÉE PAR SOURCE DE DONNÉES :")
logger.info("ANALYSE DÉTAILLÉE PAR SOURCE DE DONNÉES")

global_tables, global_lignes = 0, 0

for source in base_source:
    db = source['database']
    stats = stats_globales[db]
    
    global_tables += stats['tables']
    global_lignes += stats['lignes']
    
    print(f"\n🔹 [SOURCE : {db.upper()}]")
    print(f"   ├─ TABLES COPIÉES : {stats['tables']:,} table(s)")
    print(f"   ├─ LIGNES CHARGÉES: {stats['lignes']:,} ligne(s)")
    print(f"   └─ TABLES IGNORÉES: {stats['ignorees']:,} (déjà présentes)")
    print("   " + "-"*40)
    
    logger.info(f"Source {db.upper()} : {stats['tables']} tables, {stats['lignes']} lignes, {stats['ignorees']} ignorées")

print("\n🏆 2. STATISTIQUES SYNTHÉTIQUES COMBINÉES (TOUTES SOURCES RÉUNIES) :")
print("-" * 65)
print(f"📈 TOTAL TABLES : {global_tables:,} table(s) copiée(s)")
print(f"📈 TOTAL LIGNES : {global_lignes:,} ligne(s) chargées")
print("-" * 65)
print(f"🔥 BILAN GLOBAL : {global_tables:,} table(s) au total injectée(s) dans [BRONZE].")
print(f"🔥 LINES TOTAL  : {global_lignes:,} lignes chargées au total dans l'infrastructure.")
print("\n🎉 Opération de réplication exécutée avec succès ! 🎉\n")
print("="*80 + "\n")

# Log du bilan final
logger.info("=" * 80)
logger.info("BILAN FINAL DU PIPELINE")
logger.info("=" * 80)
logger.info(f"📈 TOTAL TABLES : {global_tables:,} tables")
logger.info(f"📈 TOTAL LIGNES : {global_lignes:,} lignes")

# Log qualité du bilan final
stats_final = {
    'tables': global_tables,
    'lignes': global_lignes
}
stats_p1_total = {'tables': global_tables, 'lignes': global_lignes}
stats_p2_total = {'tables': 0, 'lignes': 0}

log_qualite_bilan(quality_logger, stats_p1_total, stats_p2_total, stats_final)

# Log de fin
log_arret(logger, succes=True, message="Pipeline de réplication Bronze terminé avec succès")