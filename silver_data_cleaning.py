import sys
import logging
import polars as pl
import adbc_driver_postgresql.dbapi as adbc_dbapi
from config import DW_DB, get_db_uri

# Logger dédié au module Silver
logger = logging.getLogger("ETL_Silver")

# Configs de base
DW_URI = get_db_uri(DW_DB)
BRONZE_SCHEMA = DW_DB["schemas"]["bronze"]
SILVER_SCHEMA = DW_DB["schemas"].get("silver", "silver")

# Dictionnaire de mapping des états du Brésil
STATE_MAPPING = {
    "AC": "Acre", "AL": "Alagoas", "AM": "Amazonas", "AP": "Amapá",
    "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo",
    "GO": "Goiás", "MA": "Maranhão", "MG": "Minas Gerais", "MS": "Mato Grosso do Sul",
    "MT": "Mato Grosso", "PA": "Pará", "PB": "Paraíba", "PE": "Pernambuco",
    "PI": "Piauí", "PR": "Paraná", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
    "RO": "Rondônia", "RR": "Roraima", "RS": "Rio Grande do Sul", "SC": "Santa Catarina",
    "SE": "Sergipe", "SP": "São Paulo", "TO": "Tocantins"
}


def create_schema_if_not_exists(dw_uri: str = DW_URI, schema_name: str = SILVER_SCHEMA):
    """
    S'assure que le schéma destination (Silver) existe via ADBC.
    """
    logger.info(f"🏗️  Vérification / Création du schéma : [{schema_name}]")
    ddl_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    
    with adbc_dbapi.connect(dw_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_query)
        conn.commit()


def drop_table_if_exists(dw_uri: str, schema_name: str, table_name: str):
    """
    Supprime la table cible si elle existe (avec CASCADE).
    """
    ddl_query = f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;"
    with adbc_dbapi.connect(dw_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_query)
        conn.commit()


def read_bronze_table(table_name: str) -> pl.DataFrame:
    """
    Lit une table source du schéma Bronze dans un DataFrame Polars.
    """
    query = f'SELECT * FROM "{BRONZE_SCHEMA}"."{table_name}"'
    return pl.read_database_uri(query=query, uri=DW_URI)


def parse_to_date(col_name: str) -> pl.Expr:
    """
    Convertit en toute sécurité une colonne texte/timestamp au format Date Polars.
    Gère aussi bien les chaînes ISO "YYYY-MM-DD HH:MM:SS" que les types Date/Datetime natifs.
    """
    return (
        pl.col(col_name)
        .cast(pl.Utf8)
        .str.to_datetime(strict=False)
        .dt.date()
        .alias(col_name)
    )


# ------------------------------------------------------------------------------
# LOGIQUES DE TRANSFORMATION POLARS
# ------------------------------------------------------------------------------

def transform_customers() -> pl.DataFrame:
    df = read_bronze_table("sourcea_source_olist_customers_dataset")
    return df.select([
        pl.col("customer_id"),
        pl.col("customer_unique_id"),
        pl.col("customer_zip_code_prefix"),
        pl.col("customer_city"),
        pl.col("customer_state").replace(STATE_MAPPING).alias("customer_state")
    ])


def transform_geolocation() -> pl.DataFrame:
    df = read_bronze_table("sourcea_source_olist_geolocation_dataset")
    return df.select([
        pl.col("geolocation_zip_code_prefix"),
        pl.col("geolocation_lat"),
        pl.col("geolocation_lng"),
        pl.col("geolocation_city"),
        pl.col("geolocation_state").replace(STATE_MAPPING).alias("geolocation_state")
    ]).unique()


def transform_order_items() -> pl.DataFrame:
    df = read_bronze_table("sourcea_source_olist_order_items_dataset")
    
    # Castings des colonnes requises
    df = df.with_columns([
        pl.col("price").cast(pl.Float64),
        pl.col("freight_value").cast(pl.Float64)
    ])
    
    # Calcul du nombre de produits distincts par commande
    counts = (
        df.group_by("order_id")
        .agg(pl.col("product_id").n_unique().alias("nb_distinct_products"))
    )
    
    df_joined = df.join(counts, on="order_id")
    
    # CASE 1 : Single-product orders (nb_distinct_products == 1)
    df_single = (
        df_joined.filter(pl.col("nb_distinct_products") == 1)
        .group_by("order_id")
        .agg([
            pl.col("order_item_id").min().alias("order_item_id"),
            pl.col("product_id").max().alias("product_id"),
            pl.col("seller_id").max().alias("seller_id"),
            pl.col("shipping_limit_date").max().alias("shipping_limit_date"),
            pl.col("price").sum().alias("price"),
            pl.col("freight_value").sum().alias("freight_value")
        ])
    )
    
    # CASE 2 : Multi-product orders (nb_distinct_products > 1)
    df_multi = (
        df_joined.filter(pl.col("nb_distinct_products") > 1)
        .select([
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value"
        ])
    )
    
    return pl.concat([df_single, df_multi])


def transform_order_payments() -> pl.DataFrame:
    df = read_bronze_table("sourcea_source_olist_order_payments_dataset")
    
    df = df.with_columns([
        pl.col("payment_sequential").cast(pl.Float64),
        pl.col("payment_installments").cast(pl.Float64),
        pl.col("payment_value").cast(pl.Float64)
    ])
    
    return (
        df.group_by("order_id")
        .agg([
            pl.col("payment_sequential").sum().alias("payment_sequential"),
            pl.col("payment_installments").sum().alias("payment_installments"),
            pl.when(pl.col("payment_type") == "credit_card")
              .then(pl.col("payment_value"))
              .otherwise(0.0)
              .sum()
              .alias("has_credit_card"),
            pl.when(pl.col("payment_type") == "boleto")
              .then(pl.col("payment_value"))
              .otherwise(0.0)
              .sum()
              .alias("has_boleto"),
            pl.when(pl.col("payment_type") == "voucher")
              .then(pl.col("payment_value"))
              .otherwise(0.0)
              .sum()
              .alias("has_voucher"),
            pl.when(pl.col("payment_type") == "debit_card")
              .then(pl.col("payment_value"))
              .otherwise(0.0)
              .sum()
              .alias("has_debit_card"),
            pl.col("payment_value").sum().alias("total_payment_value")
        ])
    )


def transform_order_reviews() -> pl.DataFrame:
    df = read_bronze_table("sourceb_source_olist_order_reviews_dataset")
    return df.select([
        pl.col("review_id"),
        pl.col("order_id"),
        pl.col("review_score").cast(pl.Float64),
        pl.col("review_comment_title").fill_null("n/a").alias("review_comment_title"),
        pl.col("review_comment_message").fill_null("n/a").alias("review_comment_message"),
        parse_to_date("review_creation_date"),
        parse_to_date("review_answer_timestamp")
    ])


def transform_orders() -> pl.DataFrame:
    df = read_bronze_table("sourceb_source_olist_orders_dataset")
    return df.select([
        pl.col("order_id"),
        pl.col("customer_id"),
        pl.col("order_status"),
        parse_to_date("order_purchase_timestamp"),
        parse_to_date("order_approved_at"),
        parse_to_date("order_delivered_carrier_date"),
        parse_to_date("order_delivered_customer_date"),
        parse_to_date("order_estimated_delivery_date")
    ])


def transform_products() -> pl.DataFrame:
    df = read_bronze_table("sourceb_source_olist_products_dataset")
    return df.select([
        pl.col("product_id"),
        pl.col("product_category_name").fill_null("n/a").alias("product_category_name"),
        pl.col("product_name_lenght").cast(pl.Float64),
        pl.col("product_description_lenght").cast(pl.Float64),
        pl.col("product_photos_qty").cast(pl.Float64),
        pl.col("product_weight_g").cast(pl.Float64),
        pl.col("product_length_cm").cast(pl.Float64),
        pl.col("product_height_cm").cast(pl.Float64),
        pl.col("product_width_cm").cast(pl.Float64)
    ])


def transform_sellers() -> pl.DataFrame:
    df = read_bronze_table("sourceb_source_olist_sellers_dataset")
    return df.select([
        pl.col("seller_id"),
        pl.col("seller_zip_code_prefix"),
        pl.col("seller_city"),
        pl.col("seller_state").replace(STATE_MAPPING).alias("seller_state")
    ])


def transform_product_category_translation() -> pl.DataFrame:
    df = read_bronze_table("sourceb_source_product_category_name_translation")
    
    # Gestion automatique de la présence éventuelle du caractère BOM (ï»¿)
    target_col = None
    for col in df.columns:
        if "product_category_name" in col and "english" not in col:
            target_col = col
            break

    if not target_col:
        target_col = df.columns[0]

    return df.select([
        pl.col(target_col).fill_null("n/a").alias("product_category_name"),
        pl.col("product_category_name_english").fill_null("n/a").alias("product_category_name_english")
    ])


# Registre des transformations (Nom Table -> Fonction associée)
TRANSFORMATIONS = [
    ("sourcea_source_olist_customers_dataset", transform_customers),
    ("sourcea_source_olist_geolocation_dataset", transform_geolocation),
    ("sourcea_source_olist_order_items_dataset", transform_order_items),
    ("sourcea_source_olist_order_payments_dataset", transform_order_payments),
    ("sourceb_source_olist_order_reviews_dataset", transform_order_reviews),
    ("sourceb_source_olist_orders_dataset", transform_orders),
    ("sourceb_source_olist_products_dataset", transform_products),
    ("sourceb_source_olist_sellers_dataset", transform_sellers),
    ("sourceb_source_product_category_name_translation", transform_product_category_translation),
]


def run_silver_transformation() -> bool:
    """
    Exécute l'ensemble du pipeline de transformation Silver.
    """
    logger.info("=" * 80)
    logger.info("🚀 DÉMARRAGE DU PIPELINE DE TRANSFORMATION - COUCHE SILVER")
    logger.info("=" * 80)

    try:
        # Étape 1 : S'assurer que le schéma existe
        create_schema_if_not_exists(dw_uri=DW_URI, schema_name=SILVER_SCHEMA)
        
        total_tables = len(TRANSFORMATIONS)

        # Étape 2 : Boucle de transformation et chargement
        for idx, (table_name, transform_func) in enumerate(TRANSFORMATIONS, 1):
            full_target_table = f"{SILVER_SCHEMA}.{table_name}"
            
            # Message de progression
            logger.info(f"⏳ [{idx}/{total_tables}] Transformation en cours : {table_name}...")

            # 1. Calcul / Transformation Polars
            df_transformed = transform_func()
            nb_rows = df_transformed.height

            # 2. Suppression explicite de la table cible si existante (CASCADE)
            drop_table_if_exists(DW_URI, SILVER_SCHEMA, table_name)

            # 3. Écriture dans la couche Silver via ADBC
            df_transformed.write_database(
                table_name=full_target_table,
                connection=DW_URI,
                if_table_exists="replace",
                engine="adbc"
            )

            formatted_rows = f"{nb_rows:,}".replace(",", " ")
            logger.info(
                f"  └─ ✅ {full_target_table:<50} | {formatted_rows:>8} lignes créées"
            )

        logger.info("=" * 80)
        logger.info("🎉 PIPELINE SILVER TERMINÉ AVEC SUCCÈS")
        logger.info("=" * 80)
        return True

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ ERREUR CRITIQUE DURANT LA TRANSFORMATION SILVER : {e}", exc_info=True)
        logger.error("=" * 80)
        return False


# ------------------------------------------------------------------------------
# POINT D'ENTRÉE AUTONOME
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("🔔 Exécution autonome lancée depuis terminal.")
    success = run_silver_transformation()
    sys.exit(0 if success else 1)