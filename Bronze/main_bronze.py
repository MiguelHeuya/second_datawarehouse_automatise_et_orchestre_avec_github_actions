import io
import time
import traceback
import psycopg2
import polars as pl
from config import base_source, data_warehouse, construire_uri_postgresql, construire_uri_psycopg2

# ==============================================================================
# IMPORT DE LA JOURNALISATION
# ==============================================================================
try:
    from journaux import (
        configurer_journaux,
        log_demarrage,
        log_arret,
        log_etape,
        log_succes,
        log_avertissement,
        log_erreur,
        log_critique,
        log_qualite_passe,
        log_qualite_bilan,
        log_qualite_table,
        log_erreur_table
    )
    logger, quality_logger = configurer_journaux()
except ImportError:
    logger, quality_logger = None, None


def nettoyer_schema_bronze_sur_echec(conn):
    """Purge le schéma bronze en cas d'erreur sans commiter la transaction."""
    print("🧹 Nettoyage d'urgence du schéma [bronze]...")
    try:
        with conn.cursor() as curseur:
            curseur.execute("DROP SCHEMA IF EXISTS bronze CASCADE; CREATE SCHEMA bronze;")
        print("✨ Schéma [bronze] réinitialisé à vide après échec.")
        if logger:
            logger.info("Schéma bronze purgé suite à une erreur d'exécution.")
    except Exception as e:
        if logger:
            logger.error(f"Échec de la purge d'urgence du schéma bronze : {e}")
        print(f"💥 Échec de la purge d'urgence du schéma bronze : {e}")


def mapper_type_polars_vers_pg(polars_type: pl.DataType) -> str:
    """Mappe un type Polars vers le type PostgreSQL équivalent."""
    if polars_type.is_integer():
        if polars_type in (pl.Int8, pl.Int16, pl.UInt8, pl.UInt16):
            return 'SMALLINT'
        elif polars_type in (pl.Int32, pl.UInt32):
            return 'INTEGER'
        else:
            return 'BIGINT'
    elif polars_type.is_float():
        return 'REAL' if polars_type == pl.Float32 else 'DOUBLE PRECISION'
    elif polars_type in (pl.String, pl.Utf8, pl.Categorical, pl.Enum):
        return 'TEXT'
    elif polars_type == pl.Boolean:
        return 'BOOLEAN'
    elif polars_type == pl.Date:
        return 'DATE'
    elif polars_type == pl.Datetime:
        return 'TIMESTAMP'
    elif polars_type == pl.Time:
        return 'TIME'
    elif polars_type == pl.Binary:
        return 'BYTEA'
    elif polars_type.is_decimal():
        return 'NUMERIC'
    else:
        return 'TEXT'


def ecrire_table_bronze(df: pl.DataFrame, nom_table: str, schema: str, conn):
    """
    Écrit un DataFrame Polars dans PostgreSQL via COPY STDIN sans effectuer de commit.
    Le commit est entièrement délégué à l'orchestrateur principal.
    """
    table_complete = f'{schema}."{nom_table}"'
    try:
        with conn.cursor() as curseur:
            colonnes_sql = []
            for col_name, col_type in df.schema.items():
                col_propre = col_name.replace(' ', '_').replace('"', '')
                pg_type = mapper_type_polars_vers_pg(col_type)
                colonnes_sql.append(f'"{col_propre}" {pg_type}')
            
            create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_complete} ({", ".join(colonnes_sql)});'
            curseur.execute(create_table_sql)
            curseur.execute(f'TRUNCATE TABLE {table_complete};')
            
            if not df.is_empty():
                output = io.StringIO()
                df.write_csv(output, include_header=False)
                output.seek(0)
                
                curseur.copy_expert(
                    f"COPY {table_complete} FROM STDIN WITH CSV NULL AS ''",
                    output
                )
    except Exception as e:
        print(f"💥 Erreur lors de l'écriture dans {table_complete} : {e}")
        raise RuntimeError(f"Échec d'écriture dans {table_complete}") from e


def run_bronze(conn=None):
    """
    Fonction principale de la couche Bronze.
    Extrait les données brutes des sources et les réplique dans la couche Bronze.
    
    :param conn: Connexion psycopg2 active transmise par le script principal.
                 Si None (exécution autonome), aucun commit ne sera appliqué.
    """
    print("\n" + "="*80)
    print("🚀  PIPELINE DE RÉPLICATION BRONZE - POLARS ULTRA-RAPIDE  🚀")
    print("="*80 + "\n")

    if logger:
        log_demarrage(logger, "Pipeline de Réplication Bronze (Polars)")

    dw_uri_psycopg2 = construire_uri_psycopg2(data_warehouse)

    # Gestion de la connexion globale (déléguée ou locale sans autocommit)
    conn_externe = conn is not None
    if not conn_externe:
        print("⚠️ Exécution autonome détectée : AUCUN COMMIT ne sera effectué à la fin du sous-script.")
        conn = psycopg2.connect(dw_uri_psycopg2)

    # ==============================================================================
    # ÉTAPE 1 : PRÉPARATION DU DATA WAREHOUSE (SANS COMMIT LOCAL)
    # ==============================================================================
    if logger:
        log_etape(logger, "Connexion et préparation du Data Warehouse", 1, 2)
    print("📡 Vérification de l'infrastructure du Data Warehouse...")

    try:
        with conn.cursor() as curseur:
            curseur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        if logger:
            log_succes(logger, "Schéma [bronze] prêt")
        print("✨ SUCCESS: Le schéma [bronze] est prêt.")
        print("-" * 60 + "\n")
    except Exception as e:
        if logger:
            log_critique(logger, "Erreur critique lors de l'initialisation du Data Warehouse", str(e))
        print(f"💥 ERREUR CRITIQUE initialisation DW : {e}")
        if not conn_externe:
            conn.rollback()
            conn.close()
        raise e

    # ==============================================================================
    # ÉTAPE 2 : EXECUTION DU PIPELINE BRONZE
    # ==============================================================================
    stats_globales = {s['database']: {'tables': 0, 'lignes': 0, 'ignorees': 0} for s in base_source}
    
    if logger:
        log_etape(logger, "Exécution de la réplication Bronze", 2, 2)
    print("═"*80)
    print("🔄  DÉMARRAGE DE LA RÉPLICATION BRONZE  🔄")
    print("═"*80)
    
    try:
        for source in base_source:
            nom_base_source = source['database']
            print(f"\n📂 Source active : [{nom_base_source.upper()}]")
            source_uri = construire_uri_postgresql(source)
            
            requete_liste_tables = """
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                  AND table_schema NOT LIKE 'pg_%%'
                  AND table_schema != 'information_schema'
            """
            
            df_tables = pl.read_database_uri(requete_liste_tables, source_uri)
            
            if df_tables.is_empty():
                if logger:
                    logger.warning(f"Aucune table trouvée dans {nom_base_source}")
                continue
            
            liste_tables = [(row['table_schema'], row['table_name']) for row in df_tables.to_dicts()]
            
            for schema_origine, nom_table in liste_tables:
                nom_table_destination = f"{nom_base_source}_{schema_origine}_{nom_table}"
                print(f"    ⏳ [{schema_origine}.{nom_table}] ==> [bronze.{nom_table_destination}]")
                
                # Extraction depuis la source
                requete_extraction = f'SELECT * FROM "{schema_origine}"."{nom_table}"'
                df = pl.read_database_uri(requete_extraction, source_uri)
                total_lignes_table = df.height
                
                # Écriture dans le DWH (sans commit)
                ecrire_table_bronze(
                    df=df,
                    nom_table=nom_table_destination,
                    schema='bronze',
                    conn=conn
                )
                
                print(f"    ✅ SUCCÈS : Transférée ({total_lignes_table:,} lignes).")
                stats_globales[nom_base_source]['tables'] += 1
                stats_globales[nom_base_source]['lignes'] += total_lignes_table

        print("\n🎉 Réplication Bronze exécutée intégralement (en attente de commit) !\n")
        if logger:
            log_arret(logger, succes=True, message="Pipeline Bronze terminé avec succès")

    except Exception as err_critique:
        print(f"\n💥 [ÉCHEC CRITIQUE BRONZE] : {err_critique}")
        if logger:
            log_erreur(logger, "Échec bloquant de la réplication Bronze", str(err_critique))
        
        # Nettoyage immédiat sur la transaction courante
        nettoyer_schema_bronze_sur_echec(conn)
        
        if not conn_externe:
            conn.rollback()
            conn.close()

        raise RuntimeError(f"Échec de la couche Bronze : {err_critique}") from err_critique

    finally:
        # En mode autonome, on s'assure de ne jamais commiter et de tout annuler
        if not conn_externe and conn:
            conn.rollback()
            conn.close()
            print("🔒 Connexion locale fermée (mode autonome : aucun commit appliqué).")


if __name__ == "__main__":
    run_bronze()