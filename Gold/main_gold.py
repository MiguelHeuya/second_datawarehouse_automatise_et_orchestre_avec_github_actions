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


def nettoyer_schema_gold_sur_echec(conn):
    """Purge le schéma gold en cas d'erreur sans commiter la transaction."""
    print("\n🧹 Nettoyage d'urgence du schéma [gold]...")
    try:
        with conn.cursor() as curseur:
            curseur.execute("DROP SCHEMA IF EXISTS gold CASCADE; CREATE SCHEMA gold;")
        print("✨ Schéma [gold] réinitialisé à vide après échec.")
        if logger:
            logger.info("Schéma gold purgé suite à une erreur d'exécution.")
    except Exception as e:
        print(f"💥 Échec de la purge d'urgence du schéma gold : {e}")


def run_gold(conn=None):
    """
    Fonction principale de la couche Gold (Modélisation en étoiles / Datamart).
    Consomme les tables de la couche Silver, applique la modélisation exacte 
    définie dans les scripts SQL et alimente Gold.
    
    :param conn: Connexion psycopg2 active transmise par le script principal. 
                 Si aucune connexion n'est fournie (exécution autonome), 
                 le script tourne en mode lecture/simulation SANS commit final.
    """
    print("\n" + "="*80)
    print("🚀  PIPELINE DE MODELISATION GOLD - POLARS & SQL (EXACT MATCH)  🚀")
    print("="*80 + "\n")

    uri_psycopg2 = construire_uri_psycopg2(data_warehouse)
    
    # Gestion de la connexion globale (déléguée ou locale sans autocommit)
    conn_externe = conn is not None
    if not conn_externe:
        print("⚠️ Exécution autonome détectée : AUCUN COMMIT ne sera effectué à la fin du sous-script.")
        conn = psycopg2.connect(uri_psycopg2)

    # ==============================================================================
    # 1. PRÉPARATION DU SCHÉMA GOLD (SANS COMMIT LOCAL)
    # ==============================================================================
    print("📡 Vérification du schéma [gold] dans le Data Warehouse...")
    try:
        with conn.cursor() as curseur:
            curseur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        print("✨ SUCCESS: Le schéma [gold] est prêt.")
        print("-" * 60 + "\n")
    except Exception as infra_err:
        print(f"💥 ERREUR CRITIQUE lors de la création du schéma Gold : {infra_err}")
        if not conn_externe:
            conn.rollback()
            conn.close()
        raise infra_err

    # ==============================================================================
    # 2. FONCTIONS AUXILIAIRES DE LECTURE ET ÉCRITURE SANS COMMIT
    # ==============================================================================
    def lire_table_silver(nom_table, max_retries=3, delay=3):
        """Lit une table depuis la couche Silver au sein de la transaction active (conn)."""
        requete = f'SELECT * FROM silver."{nom_table}";'
        for tentative in range(1, max_retries + 1):
            try:
                return pl.read_database(query=requete, connection=conn)
            except Exception as e:
                print(f"⚠️ [Tentative {tentative}/{max_retries}] Erreur de lecture sur silver.{nom_table} : {e}")
                if tentative < max_retries:
                    time.sleep(delay)
                else:
                    raise RuntimeError(f"Échec définitif d'extraction de la table silver.{nom_table}") from e

    def ecrire_table_gold(df: pl.DataFrame, nom_table_destination: str):
        """
        Écrit un DataFrame Polars dans le schéma Gold via copy_expert sans effectuer de commit.
        """
        table_complete = f'gold."{nom_table_destination}"'
        try:
            with conn.cursor() as curseur:
                # 1. Création de la table si elle n'existe pas
                colonnes_sql = []
                for col_name, col_type in df.schema.items():
                    col_propre = col_name.replace(' ', '_').replace('"', '')
                    
                    if col_type.is_integer():
                        pg_type = 'BIGINT'
                    elif col_type.is_float():
                        pg_type = 'DOUBLE PRECISION'
                    elif col_type == pl.Boolean:
                        pg_type = 'BOOLEAN'
                    elif col_type == pl.Date:
                        pg_type = 'DATE'
                    elif col_type == pl.Datetime:
                        pg_type = 'TIMESTAMP'
                    else:
                        pg_type = 'TEXT'
                        
                    colonnes_sql.append(f'"{col_propre}" {pg_type}')

                create_sql = f'CREATE TABLE IF NOT EXISTS {table_complete} ({", ".join(colonnes_sql)});'
                curseur.execute(create_sql)
                curseur.execute(f'TRUNCATE TABLE {table_complete} CASCADE;')

                # 2. Ingestion massive par flux CSV en mémoire
                if not df.is_empty():
                    output = io.StringIO()
                    df.write_csv(output, include_header=False)
                    output.seek(0)
                    curseur.copy_expert(
                        f"COPY {table_complete} FROM STDIN WITH CSV NULL AS ''",
                        output
                    )
                    output.close()
            print(f"    ✅ [GOLD] Ingestion réussie : {table_complete} ({len(df):,} lignes).")
        except Exception as e:
            print(f"💥 Erreur lors de l'écriture dans {table_complete} : {e}")
            raise RuntimeError(f"Échec d'écriture dans {table_complete}") from e

    # ==============================================================================
    # 3. TRANSFORMATION & MODÉLISATION GOLD (EXACTE CONFORMITÉ SQL)
    # ==============================================================================
    print("🚀 Démarrage de la construction du Datamart Gold...\n")

    try:
        # Chargement des tables Silver nécessaires
        df_orders = lire_table_silver("sourceb_source_olist_orders_dataset")
        df_order_items = lire_table_silver("sourcea_source_olist_order_items_dataset")
        df_payments = lire_table_silver("sourcea_source_olist_order_payments_dataset")
        df_reviews = lire_table_silver("sourceb_source_olist_order_reviews_dataset")
        df_customers = lire_table_silver("sourcea_source_olist_customers_dataset")
        df_geolocation = lire_table_silver("sourcea_source_olist_geolocation_dataset")
        df_sellers = lire_table_silver("sourceb_source_olist_sellers_dataset")
        df_products = lire_table_silver("sourceb_source_olist_products_dataset")
        df_translation = lire_table_silver("sourceb_source_product_category_name_translation")

        # --- A. DIMENSION CUSTOMERS ---
        dim_customers = df_customers.select([
            "customer_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state"
        ])
        ecrire_table_gold(dim_customers, "dim_customers")

        # --- B. DIMENSION GEOLOCATION ---
        dim_geolocation = df_geolocation.select([
            "geolocation_zip_code_prefix",
            "geolocation_city",
            "geolocation_state"
        ])
        ecrire_table_gold(dim_geolocation, "dim_geolocation")

        # --- C. DIMENSION SELLERS ---
        dim_sellers = df_sellers.select([
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        ])
        ecrire_table_gold(dim_sellers, "dim_sellers")

        # --- D. DIMENSION PRODUCTS ---
        dim_products = (
            df_products.join(df_translation, on="product_category_name", how="left")
            .select([
                "product_id",
                pl.col("product_category_name")
                  .fill_null("Uncategorized")
                  .alias("product_category_name_original"),
                pl.coalesce([
                    pl.col("product_category_name_english"),
                    pl.col("product_category_name"),
                    pl.lit("Uncategorized")
                ]).alias("product_category_name_english_translated")
            ])
        )
        ecrire_table_gold(dim_products, "dim_products")

        # --- E. TABLE DE FAITS : fact_orders ---
        # Nettoyage préalable des métadonnées d'audit Silver pour éviter le conflit "date_silver_added_right"
        df_orders_clean = df_orders.drop("date_silver_added", strict=False)
        df_order_items_clean = df_order_items.drop("date_silver_added", strict=False)
        df_payments_clean = df_payments.drop("date_silver_added", strict=False)
        df_reviews_clean = df_reviews.drop("date_silver_added", strict=False)

        # Étape 0 : Jointures
        etape0_jointure = (
            df_orders_clean.join(df_order_items_clean, on="order_id", how="left")
                           .join(df_payments_clean, on="order_id", how="left")
                           .join(df_reviews_clean, on="order_id", how="left")
        )

        canceled_statuses = ["unavailable", "canceled", "invoiced", "created"]
        is_canceled_no_product = pl.col("product_id").is_null() & pl.col("order_status").is_in(canceled_statuses)

        # Étape 1 : Correction des paiements pour les produits manquants
        etape1_correction = etape0_jointure.with_columns([
            pl.col("product_id").fill_null("n/a"),
            pl.col("seller_id").fill_null("n/a"),
            pl.when(is_canceled_no_product).then(pl.lit("canceled_no_product")).otherwise(pl.col("payment_type")).alias("payment_type"),
            pl.when(is_canceled_no_product).then(0).otherwise(pl.col("payment_sequential")).alias("payment_sequential"),
            pl.when(is_canceled_no_product).then(0).otherwise(pl.col("payment_installments")).alias("payment_installments"),
            pl.when(is_canceled_no_product).then(0.0).otherwise(pl.col("payment_value")).alias("payment_value")
        ])

        # Étape 2 : Analyse des doublons d'order_id (Window Functions équivalentes)
        etape2_analyse = etape1_correction.with_columns([
            pl.len().over("order_id").alias("nb_occurrences"),
            (pl.col("payment_value").min().over("order_id") == pl.col("payment_value").max().over("order_id")).alias("paiements_sont_identiques"),
            pl.col("payment_value").sum().over("order_id").alias("somme_totale_paiements"),
            pl.col("order_purchase_timestamp").rank(method="ordinal", descending=True).over("order_id").alias("rang_ligne")
        ])

        # Étape 3 : Application des règles métiers finales et déduplication (rang_ligne == 1)
        fact_orders = (
            etape2_analyse
            .filter(pl.col("rang_ligne") == 1)
            .select([
                "order_id",
                "customer_id",
                "product_id",
                "seller_id",
                "order_status",
                "payment_type",
                "payment_sequential",
                "payment_installments",
                "review_score",
                "order_approved_at",
                "shipping_limit_date",
                "order_purchase_timestamp",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
                
                # RÈGLE SUR PRICE
                pl.when((pl.col("nb_occurrences") > 1) & (pl.col("paiements_sont_identiques") == True))
                  .then(pl.col("price") * pl.col("nb_occurrences"))
                  .otherwise(pl.col("price"))
                  .alias("price"),
                  
                # RÈGLE SUR FREIGHT
                pl.when((pl.col("nb_occurrences") > 1) & (pl.col("paiements_sont_identiques") == True))
                  .then(pl.col("freight_value") * pl.col("nb_occurrences"))
                  .otherwise(pl.col("freight_value"))
                  .alias("freight_value"),
                  
                # RÈGLE SUR PAYMENT_VALUE
                pl.when(pl.col("paiements_sont_identiques") == False)
                  .then(pl.col("somme_totale_paiements"))
                  .otherwise(pl.col("payment_value"))
                  .alias("payment_value")
            ])
        )
        ecrire_table_gold(fact_orders, "fact_orders")

        print("\n🎉 TOUTES LES TABLES DU DATAMART GOLD ONT ÉTÉ GÉNÉRÉES ET ÉCRITES STRICTEMENT SELON LA LOGIQUE SQL !")

    except Exception as global_err:
        print(f"\n💥 PIPELINE GOLD ARRÊTÉ : Erreur bloquante : {global_err}")
        traceback.print_exc()
        
        # Annulation et purge d'urgence
        nettoyer_schema_gold_sur_echec(conn)
        
        if not conn_externe:
            conn.rollback()
            conn.close()
            
        raise RuntimeError(f"Échec de la couche Gold : {global_err}") from global_err

    finally:
        if not conn_externe and conn:
            conn.rollback()
            conn.close()
            print("🔒 Connexion locale fermée (mode autonome : aucun commit appliqué).")


if __name__ == "__main__":
    run_gold()