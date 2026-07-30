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
    Consomme les tables de la couche Silver, applique la modélisation et alimente Gold.
    
    :param conn: Connexion psycopg2 active transmise par le script principal. 
                 Si aucune connexion n'est fournie (exécution autonome), 
                 le script tourne en mode lecture/simulation SANS commit final.
    """
    print("\n" + "="*80)
    print("🚀  PIPELINE DE MODELISATION GOLD - POLARS & SQL  🚀")
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
                # Utilisation de conn (psycopg2) au lieu de l'URI pour lire les données non commitées
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
        Le commit est délégué à l'orchestrateur principal.
        """
        table_complete = f'gold."{nom_table_destination}"'
        try:
            with conn.cursor() as curseur:
                # 1. Création de la table si elle n'existe pas
                colonnes_sql = []
                for col_name, col_type in df.schema.items():
                    col_propre = col_name.replace(' ', '_').replace('"', '')
                    
                    # Mapping simplifié Polars -> Postgres
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
                curseur.execute(f'TRUNCATE TABLE {table_complete};')

                # 2. Ingestion massive par flux CSV en mémoire
                if not df.is_empty():
                    output = io.StringIO()
                    df.write_csv(output, include_header=False)
                    output.seek(0)
                    curseur.copy_expert(
                        f"COPY {table_complete} FROM STDIN WITH CSV NULL AS ''",
                        output
                    )
            print(f"    ✅ [GOLD] Ingestion réussie : {table_complete} ({len(df):,} lignes).")
        except Exception as e:
            print(f"💥 Erreur lors de l'écriture dans {table_complete} : {e}")
            raise RuntimeError(f"Échec d'écriture dans {table_complete}") from e

    # ==============================================================================
    # 3. TRANSFORMATION & MODÉLISATION EN ÉTOILE (DATAMART)
    # ==============================================================================
    print("🚀 Démarrage de la construction du Datamart Gold...\n")

    try:
        # Chargement des tables Silver nécessaires via la connexion active
        df_orders = lire_table_silver("sourceb_source_olist_orders_dataset")
        df_order_items = lire_table_silver("sourcea_source_olist_order_items_dataset")
        df_payments = lire_table_silver("sourcea_source_olist_order_payments_dataset")
        df_reviews = lire_table_silver("sourceb_source_olist_order_reviews_dataset")
        df_customers = lire_table_silver("sourcea_source_olist_customers_dataset")
        df_products = lire_table_silver("sourceb_source_olist_products_dataset")
        df_sellers = lire_table_silver("sourceb_source_olist_sellers_dataset")

        # --- A. DIMENSION CLIENTS ---
        dim_customers = df_customers.select([
            pl.col("customer_id"),
            pl.col("customer_city").alias("ville"),
            pl.col("customer_state").alias("etat"),
            pl.col("customer_zip_code_prefix").alias("code_postal"),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_gold_added")
        ])
        ecrire_table_gold(dim_customers, "dim_clients")

        # --- B. DIMENSION PRODUITS ---
        dim_products = df_products.select([
            pl.col("product_id"),
            pl.col("product_category_name").alias("categorie"),
            pl.col("product_weight_g").alias("poids_g"),
            pl.col("product_length_cm").alias("longueur_cm"),
            pl.col("product_height_cm").alias("hauteur_cm"),
            pl.col("product_width_cm").alias("largeur_cm"),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_gold_added")
        ])
        ecrire_table_gold(dim_products, "dim_produits")

        # --- C. DIMENSION VENDEURS ---
        dim_sellers = df_sellers.select([
            pl.col("seller_id"),
            pl.col("seller_city").alias("ville"),
            pl.col("seller_state").alias("etat"),
            pl.col("seller_zip_code_prefix").alias("code_postal"),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_gold_added")
        ])
        ecrire_table_gold(dim_sellers, "dim_vendeurs")

        # --- D. TABLE DE FAITS : VENTES ---
        # Agrégation des paiements par commande
        df_payments_agg = df_payments.group_by("order_id").agg([
            pl.col("payment_value").sum().alias("total_paiement"),
            pl.col("payment_type").first().alias("mode_paiement_principal")
        ])

        # Agrégation des avis par commande
        df_reviews_agg = df_reviews.group_by("order_id").agg([
            pl.col("review_score").mean().alias("note_moyenne_review")
        ])

        # Jointure pour former le fait principal
        fact_sales = df_order_items.join(
            df_orders, on="order_id", how="inner"
        ).join(
            df_payments_agg, on="order_id", how="left"
        ).join(
            df_reviews_agg, on="order_id", how="left"
        ).select([
            pl.col("order_id"),
            pl.col("order_item_id"),
            pl.col("customer_id"),
            pl.col("product_id"),
            pl.col("seller_id"),
            pl.col("order_status").alias("statut_commande"),
            pl.col("order_purchase_timestamp").alias("date_achat"),
            pl.col("shipping_limit_date").alias("date_limite_expedition"),
            pl.col("price").alias("prix_unitaire"),
            pl.col("freight_value").alias("frais_port"),
            pl.col("total_paiement"),
            pl.col("mode_paiement_principal"),
            pl.col("note_moyenne_review"),
            pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_gold_added")
        ])
        ecrire_table_gold(fact_sales, "fait_ventes")

        print("\n🎉 TOUTES LES TABLES DU DATAMART GOLD ONT ÉTÉ GÉNÉRÉES ET ÉCRITES (EN ATTENTE DE COMMIT) !")

    except Exception as global_err:
        print(f"\n💥 PIPELINE GOLD ARRÊTÉ : Erreur bloquante : {global_err}")
        traceback.print_exc()
        
        # Annulation et purge
        nettoyer_schema_gold_sur_echec(conn)
        
        if not conn_externe:
            conn.rollback()
            conn.close()
            
        raise RuntimeError(f"Échec de la couche Gold : {global_err}") from global_err

    finally:
        # Fermeture propre si le script a été lancé en autonomie
        if not conn_externe and conn:
            conn.rollback()  # Pas de commit automatique lors d'un test isolé
            conn.close()
            print("🔒 Connexion locale fermée (mode autonome : aucun commit appliqué).")


if __name__ == "__main__":
    run_gold()