import io
import time
import sys
import traceback
from datetime import datetime
import polars as pl
import psycopg2
from config import data_warehouse, construire_uri_psycopg2

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
        log_erreur,
        log_critique
    )
    logger, quality_logger = configurer_journaux()
except ImportError:
    logger = None


def nettoyer_schema_silver_sur_echec(conn):
    """Purge le schéma silver en cas d'erreur sans commiter la transaction."""
    print("\n🧹 Nettoyage d'urgence du schéma [silver]...")
    try:
        with conn.cursor() as curseur:
            curseur.execute("DROP SCHEMA IF EXISTS silver CASCADE; CREATE SCHEMA silver;")
        print("✨ Schéma [silver] réinitialisé à vide après échec.")
        if logger:
            logger.info("Schéma silver purgé suite à une erreur d'exécution.")
    except Exception as e:
        print(f"💥 Échec de la purge d'urgence du schéma silver : {e}")


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


def run_silver(conn=None):
    """
    Fonction principale de la couche Silver.
    Chargée d'extraire depuis Bronze, transformer et écrire dans Silver.
    
    :param conn: Connexion psycopg2 active transmise par le script principal.
                 Si None (exécution autonome), aucun commit ne sera appliqué.
    """
    print("\n" + "="*80)
    print("🚀  PIPELINE DE TRANSFORMATION SILVER - POLARS  🚀")
    print("="*80 + "\n")

    # Connecteurs DWH
    uri_psycopg2 = construire_uri_psycopg2(data_warehouse)

    # Gestion de la connexion globale (déléguée ou locale)
    conn_externe = conn is not None
    if not conn_externe:
        print("⚠️ Exécution autonome détectée : AUCUN COMMIT ne sera effectué à la fin du sous-script.")
        conn = psycopg2.connect(uri_psycopg2)

    # ==============================================================================
    # 1. PRÉPARATION DU SCHÉMA SILVER (SANS COMMIT LOCAL)
    # ==============================================================================
    print("📡 Vérification de l'infrastructure du Data Warehouse...")
    try:
        with conn.cursor() as curseur:
            curseur.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        print("✨ SUCCESS: Le schéma [silver] est prêt pour l'ingestion.")
        print("-" * 60 + "\n")
    except Exception as infra_err:
        print(f"💥 ERREUR CRITIQUE lors de la création du schéma Silver : {infra_err}")
        if not conn_externe:
            conn.rollback()
            conn.close()
        raise infra_err

    # ==============================================================================
    # 2. FONCTIONS AUXILIAIRES DE LECTURE/ÉCRITURE SANS COMMIT
    # ==============================================================================
    def query_avec_retry(nom_table, max_retries=3, delay=3):
        """Exécute l'extraction depuis Bronze en réutilisant la connexion active (conn)."""
        requete = f'SELECT * FROM bronze."{nom_table}";'
        for tentative in range(1, max_retries + 1):
            try:
                # Utilisation de conn (psycopg2) au lieu de l'URI pour lire les données non commitées
                df = pl.read_database(query=requete, connection=conn)
                return df
            except Exception as e:
                print(f"⚠️ [Tentative {tentative}/{max_retries}] Erreur de lecture sur bronze.{nom_table} : {e}")
                if tentative < max_retries:
                    time.sleep(delay)
                else:
                    raise RuntimeError(f"Échec définitif d'extraction de la table bronze.{nom_table}") from e

    def ecrire_table_silver(df: pl.DataFrame, nom_table_destination: str):
        """
        Écrit un DataFrame Polars dans le schéma Silver via COPY STDIN sans effectuer de commit.
        Le commit est entièrement délégué à l'orchestrateur principal.
        """
        table_complete = f'silver."{nom_table_destination}"'
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
            print(f"    ✅ [SILVER] Ingestion réussie : {table_complete} ({len(df):,} lignes).")
        except Exception as e:
            print(f"💥 Problème d'écriture dans {table_complete} : {e}")
            raise RuntimeError(f"Échec définitif d'écriture dans {table_complete}") from e

    # ==============================================================================
    # 3. CORE PIPELINE : TRANSFORMATIONS & ROUTAGES MÉTIERS
    # ==============================================================================
    print("🚀 Lancement des transformations et de la bascule vers la couche Silver...\n")

    try:
        correspondance_etats = {
            "AC": "Acre", "AL": "Alagoas", "AM": "Amazonas", "AP": "Amapá", "BA": "Bahia",
            "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo", "GO": "Goiás", 
            "MA": "Maranhão", "MG": "Minas Gerais", "MS": "Mato Grosso do Sul", "MT": "Mato Grosso", 
            "PA": "Pará", "PB": "Paraíba", "PE": "Pernambuco", "PI": "Piauí", "PR": "Paraná", 
            "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte", "RO": "Rondônia", "RR": "Roraima", 
            "RS": "Rio Grande do Sul", "SC": "Santa Catarina", "SE": "Sergipe", "SP": "São Paulo", 
            "TO": "Tocantins"
        }
        
        # --- 1. CUSTOMERS ---
        df_cust = query_avec_retry("sourcea_source_olist_customers_dataset")
        sourcea_source_olist_customers_dataset = df_cust.select([
            pl.col("customer_id"),
            pl.col("customer_zip_code_prefix"),
            pl.col("customer_city"),
            pl.col("customer_state"),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
        ]).with_columns(
            pl.col("customer_state").replace(correspondance_etats, default=pl.col("customer_state")),
            pl.col("customer_city").replace({"são paulo": "sao paulo"}, default=pl.col("customer_city"))
        )
        ecrire_table_silver(sourcea_source_olist_customers_dataset, "sourcea_source_olist_customers_dataset")

        # --- 2. GEOLOCATION ---
        df_geo = query_avec_retry("sourcea_source_olist_geolocation_dataset")
        sourcea_source_olist_geolocation_dataset = df_geo.select([
            pl.col("geolocation_zip_code_prefix"),
            pl.col("geolocation_city"),
            pl.col("geolocation_state"),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
        ]).with_columns(
            pl.col("geolocation_state").replace(correspondance_etats, default=pl.col("geolocation_state"))
        )
        ecrire_table_silver(sourcea_source_olist_geolocation_dataset, "sourcea_source_olist_geolocation_dataset")

        # --- 3. ORDER ITEMS ---
        df_order_items = query_avec_retry("sourcea_source_olist_order_items_dataset")
        sourcea_source_olist_order_items_dataset = df_order_items.select([
            pl.col("order_id"),
            pl.col("order_item_id"),
            pl.col("product_id"),
            pl.col("seller_id"),
            pl.col("shipping_limit_date").str.to_datetime(strict=False),
            pl.col("price").cast(pl.Float64, strict=False),
            pl.col("freight_value").cast(pl.Float64, strict=False),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
        ])
        ecrire_table_silver(sourcea_source_olist_order_items_dataset, "sourcea_source_olist_order_items_dataset")

        # --- 4. ORDER PAYMENTS ---
        df_order_payments = query_avec_retry("sourcea_source_olist_order_payments_dataset")
        sourcea_source_olist_order_payments_dataset = df_order_payments.select([
            pl.col("order_id"),
            pl.col("payment_sequential").cast(pl.Float64, strict=False),
            pl.col("payment_type"),
            pl.col("payment_installments").cast(pl.Float64, strict=False),
            pl.col("payment_value").cast(pl.Float64, strict=False),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
        ])
        ecrire_table_silver(sourcea_source_olist_order_payments_dataset, "sourcea_source_olist_order_payments_dataset")

        # --- 5. ORDER REVIEWS ---
        df_reviews = query_avec_retry("sourceb_source_olist_order_reviews_dataset")
        sourceb_source_olist_order_reviews_dataset = df_reviews.select([
            pl.col("review_id"),
            pl.col("order_id"),
            pl.col("review_score").cast(pl.Float64, strict=False),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
        ])
        ecrire_table_silver(sourceb_source_olist_order_reviews_dataset, "sourceb_source_olist_order_reviews_dataset")

        # --- 6. ORDERS ---
        df_orders = query_avec_retry("sourceb_source_olist_orders_dataset")
        sourceb_source_olist_orders_dataset = df_orders.select([
            pl.col("order_id"),
            pl.col("customer_id"),
            pl.col("order_status"),
            pl.col("order_purchase_timestamp").str.to_datetime(strict=False),
            pl.col("order_approved_at").str.to_datetime(strict=False),
            pl.col("order_delivered_carrier_date").str.to_datetime(strict=False),
            pl.col("order_delivered_customer_date").str.to_datetime(strict=False),
            pl.col("order_estimated_delivery_date").str.to_datetime(strict=False),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
        ])
        ecrire_table_silver(sourceb_source_olist_orders_dataset, "sourceb_source_olist_orders_dataset")

        # --- 7. PRODUCTS ---
        df_prod = query_avec_retry("sourceb_source_olist_products_dataset")
        sourceb_source_olist_products_dataset = df_prod.select([
            pl.col("product_id"),
            pl.col("product_category_name"),
            pl.col("product_name_lenght").cast(pl.Float64, strict=False),
            pl.col("product_description_lenght").cast(pl.Float64, strict=False),
            pl.col("product_photos_qty").cast(pl.Float64, strict=False),
            pl.col("product_weight_g").cast(pl.Float64, strict=False),
            pl.col("product_length_cm").cast(pl.Float64, strict=False),
            pl.col("product_height_cm").cast(pl.Float64, strict=False),
            pl.col("product_width_cm").cast(pl.Float64, strict=False),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
        ])
        ecrire_table_silver(sourceb_source_olist_products_dataset, "sourceb_source_olist_products_dataset")

        # --- 8. SELLERS ---
        df_sellers = query_avec_retry("sourceb_source_olist_sellers_dataset")
        sourceb_source_olist_sellers_dataset = df_sellers.select([
            pl.col("seller_id"),
            pl.col("seller_zip_code_prefix"),
            pl.col("seller_city"),
            pl.col("seller_state"),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
        ]).with_columns(
            pl.col("seller_state").replace(correspondance_etats, default=pl.col("seller_state")),
            pl.col("seller_city").replace({"são paulo": "sao paulo"}, default=pl.col("seller_city"))
        )
        ecrire_table_silver(sourceb_source_olist_sellers_dataset, "sourceb_source_olist_sellers_dataset")

        # --- 9. TRANSLATION ---
        df_trans = query_avec_retry("sourceb_source_product_category_name_translation")
        # Gestion propre du BOM UTF-8 au cas où le nom de colonne contient ï»¿
        cols_actuelles = df_trans.columns
        for col in cols_actuelles:
            if "product_category_name" in col and col != "product_category_name_english":
                df_trans = df_trans.rename({col: "product_category_name"})

        sourceb_source_product_category_name_translation = df_trans.select([
            pl.col("product_category_name"),
            pl.col("product_category_name_english"),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
        ])
        ecrire_table_silver(sourceb_source_product_category_name_translation, "sourceb_source_product_category_name_translation")

        print("\n🎉 TOUTES LES TABLES ONT ÉTÉ TRANSFORMÉES ET ENVOYÉES DANS LE SCHÉMA [SILVER] (EN ATTENTE DE COMMIT) !")

    except Exception as global_err:
        print(f"\n💥 PIPELINE SILVER ARRÊTÉ : Une erreur bloquante est survenue : {global_err}")
        traceback.print_exc()
        
        # Purge immédiate du schéma silver
        nettoyer_schema_silver_sur_echec(conn)
        
        if not conn_externe:
            conn.rollback()
            conn.close()
            
        raise RuntimeError(f"Échec de la couche Silver : {global_err}") from global_err

    finally:
        # Si le script tourne en solo, on ferme proprement la connexion locale
        if not conn_externe and conn:
            conn.rollback()
            conn.close()
            print("🔒 Connexion locale fermée (mode autonome : aucun commit appliqué).")


if __name__ == "__main__":
    run_silver()