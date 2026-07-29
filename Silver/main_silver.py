import polars as pl
import sqlalchemy as sa
import traceback
import time
from datetime import datetime
from config import data_warehouse, construire_uri_postgresql

# ==============================================================================
# LEVIER CONFIGURATION & SÉCURITÉ RESEAU LOCAL VS PROD
# ==============================================================================
def obtenir_uri_securisee(config_dict):
    """
    Génère l'URI de connexion PostgreSQL adaptée.
    Force le débogage du sslmode pour éviter les timeouts r2d2 en local.
    """
    host = config_dict.get('host', '')
    if 'localhost' in host or '127.0.0.1' in host:
        sslmode = 'disable'
    else:
        sslmode = config_dict.get('sslmode', 'prefer')
        
    return f"postgresql://{config_dict['user']}:{config_dict['password']}@{config_dict['host']}:{config_dict.get('port', 5432)}/{config_dict['database']}?sslmode={sslmode}"

# Base d'infrastructure réseau résiliente
uri = obtenir_uri_securisee(data_warehouse)

# ==============================================================================
# SÉCURISATION ET INITIALISATION DU SCHÉMA SILVER
# ==============================================================================
print("📡 Vérification de l'infrastructure du Data Warehouse...")
try:
    engine_init = sa.create_engine(uri)
    with engine_init.begin() as conn_init:
        conn_init.execute(sa.text("CREATE SCHEMA IF NOT EXISTS silver;"))
    print("✨ SUCCESS: Le schéma [silver] est prêt pour l'ingestion.")
    print("-" * 60 + "\n")
except Exception as infra_err:
    print(f"💥 ERREUR CRITIQUE lors de la création du schéma Silver : {infra_err}")
    exit(1)

# ==============================================================================
# GESTIONNAIRE RÉSILIENT DE LECTURE/ÉCRITURE (NETWORK FAILSAFE)
# ==============================================================================
def query_avec_retry(nom_table, max_retries=3, delay=5):
    """Exécute l'extraction depuis Bronze avec reconnexion automatique."""
    requete = f"SELECT * FROM bronze.{nom_table}"
    for tentative in range(1, max_retries + 1):
        try:
            df = pl.read_database_uri(requete, uri, engine="connectorx")
            return df
        except Exception as e:
            print(f"⚠️ [Tentative {tentative}/{max_retries}] Erreur de connexion ou coupure réseau sur Bronze : {e}")
            if tentative < max_retries:
                time.sleep(delay)
            else:
                print(f"💥 Échec définitif d'extraction pour la table bronze.{nom_table}")
                raise e

def ecrire_table_silver(df, nom_table_destination, max_retries=3, delay=5):
    """Exécute l'ingestion vers Silver à l'aide d'ADBC avec gestion d'erreurs."""
    table_complete = f"silver.{nom_table_destination}"
    
    for tentative in range(1, max_retries + 1):
        try:
            df.write_database(
                table_name=table_complete,
                connection=uri,
                engine='adbc',
                if_table_exists='replace'
            )
            print(f"   ✅ [SILVER] Ingestion validée : {table_complete} ({len(df):,} lignes).")
            return
        except Exception as e:
            print(f"⚠️ [Tentative {tentative}/{max_retries}] Problème réseau à l'écriture dans Silver : {e}")
            if tentative < max_retries:
                time.sleep(delay)
            else:
                print(f"💥 Échec critique d'écriture pour la table {table_complete}")
                raise e

# ==============================================================================
# CORE PIPELINE : TRANSFORMATIONS & ROUTAGES MÈTIERS
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
    ])
    
    sourcea_source_olist_customers_dataset = df_cust.with_columns(
        pl.col("customer_state").replace(correspondance_etats, default = pl.col("customer_state"),),
        pl.col("customer_city").replace({"são paulo" : "sao paulo"}, default = pl.col("customer_city"))
    )
    
    ecrire_table_silver(sourcea_source_olist_customers_dataset, "sourcea_source_olist_customers_dataset")

    # --- 2. GEOLOCATION ---
    df_geo = query_avec_retry("sourcea_source_olist_geolocation_dataset")
    df_geo = df_geo.select([
        pl.col("geolocation_zip_code_prefix"),
        pl.col("geolocation_city"),
        pl.col("geolocation_state"),
        pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
    ])
    
    sourcea_source_olist_geolocation_dataset = df_geo.with_columns(
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
         pl.col("payment_sequential").cast(pl.Float64, strict = False),
         pl.col("payment_type"),
         pl.col("payment_installments").cast(pl.Float64, strict = False),
         pl.col("payment_value").cast(pl.Float64, strict = False),
         pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
    ])
    ecrire_table_silver(sourcea_source_olist_order_payments_dataset, "sourcea_source_olist_order_payments_dataset")

    # --- 5. ORDER REVIEWS ---
    df_reviews = query_avec_retry("sourceb_source_olist_order_reviews_dataset")
    sourceb_source_olist_order_reviews_dataset = df_reviews.select([
        pl.col("review_id"),
        pl.col("order_id"),
        pl.col("review_score").cast(pl.Float64, strict = False),
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
        pl.col("product_name_lenght").cast(pl.Float64, strict = False),
        pl.col("product_description_lenght").cast(pl.Float64, strict = False),
        pl.col("product_photos_qty").cast(pl.Float64, strict = False),
        pl.col("product_weight_g").cast(pl.Float64, strict = False),
        pl.col("product_length_cm").cast(pl.Float64, strict = False),
        pl.col("product_height_cm").cast(pl.Float64, strict = False),
        pl.col("product_width_cm").cast(pl.Float64, strict = False),
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
    ])
    sourceb_source_olist_sellers_dataset = sourceb_source_olist_sellers_dataset.with_columns(
        pl.col("seller_state").replace(correspondance_etats, default=pl.col("seller_state"))
    )
    
    sourceb_source_olist_sellers_dataset = sourceb_source_olist_sellers_dataset.with_columns(
        pl.col("seller_city").replace({"são paulo":"sao paulo"}, default=pl.col("seller_city"))
    )
    
    ecrire_table_silver(sourceb_source_olist_sellers_dataset, "sourceb_source_olist_sellers_dataset")

    # --- 9. TRANSLATION (WITH FIX UTF8 BOM) ---
    df_trans = query_avec_retry("sourceb_source_product_category_name_translation")
# 1. Renommage pour nettoyer le BOM UTF-8
    df_trans = df_trans.rename(
        {"ï»¿product_category_name": "product_category_name"}
    )
    
    # 2. Sélection des colonnes et ajout de la date avec .select()
    sourceb_source_product_category_name_translation = df_trans.select([
        pl.col("product_category_name"),          # Correction de la faute de frappe (c au lieu de d)
        pl.col("product_category_name_english"),
        pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("date_silver_added")
    ])
    ecrire_table_silver(sourceb_source_product_category_name_translation, "sourceb_source_product_category_name_translation")

    print("\n🏁 Head de contrôle (Translation) :")
    print(sourceb_source_product_category_name_translation.head(5))

    print("\n🎉 TOUTES LES TABLES ONT ÉTÉ ENVOYÉES AVEC SUCCÈS DANS LE SCHÉMA [SILVER] ! 🎉")

except Exception as global_err:
    print(f"\n💥 PIPELINE TRANSFORMATION ARRÊTÉ : Une erreur bloquante est survenue.")
    traceback.print_exc()
    exit(1)