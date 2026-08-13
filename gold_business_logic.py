import sys
import logging
import polars as pl
import adbc_driver_postgresql.dbapi as adbc_dbapi
from config import DW_DB, get_db_uri

# Logger dédié au module Gold
logger = logging.getLogger("ETL_Gold")

# Configs de base
DW_URI = get_db_uri(DW_DB)
SILVER_SCHEMA = DW_DB["schemas"].get("silver", "silver")
GOLD_SCHEMA = DW_DB["schemas"].get("gold", "gold")


def create_schema_if_not_exists(dw_uri: str = DW_URI, schema_name: str = GOLD_SCHEMA):
    """
    S'assure que le schéma destination (Gold) existe via ADBC.
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


def read_silver_table(table_name: str) -> pl.DataFrame:
    """
    Lit une table source du schéma Silver dans un DataFrame Polars.
    """
    query = f'SELECT * FROM "{SILVER_SCHEMA}"."{table_name}"'
    return pl.read_database_uri(query=query, uri=DW_URI)


# ------------------------------------------------------------------------------
# LOGIQUES DE TRANSFORMATION GOLD (MODÉLISATION EN ÉTOILE)
# ------------------------------------------------------------------------------

def build_dim_customers() -> pl.DataFrame:
    df = read_silver_table("sourcea_source_olist_customers_dataset")
    return df.select([
        pl.col("customer_id"),
        pl.col("customer_zip_code_prefix").alias("customer_zip_code"),
        pl.col("customer_city"),
        pl.col("customer_state")
    ])


def build_dim_sellers() -> pl.DataFrame:
    df = read_silver_table("sourceb_source_olist_sellers_dataset")
    return df.select([
        pl.col("seller_id"),
        pl.col("seller_zip_code_prefix").alias("seller_zip_code"),
        pl.col("seller_city"),
        pl.col("seller_state")
    ])


def build_dim_products() -> pl.DataFrame:
    df_products = read_silver_table("sourceb_source_olist_products_dataset")
    df_trans = read_silver_table("sourceb_source_product_category_name_translation")

    df_joined = df_products.join(
        df_trans,
        on="product_category_name",
        how="left"
    )

    # Logique COALESCE / NULLIF pour product_category_name_english
    category_english_expr = (
        pl.when(
            pl.col("product_category_name_english").is_not_null() & 
            (pl.col("product_category_name_english") != "n/a")
        )
        .then(pl.col("product_category_name_english"))
        .when(
            pl.col("product_category_name").is_not_null() & 
            (pl.col("product_category_name") != "n/a")
        )
        .then(pl.col("product_category_name"))
        .otherwise(pl.lit("non spécifié"))
        .alias("product_category_name_english")
    )

    # Calcul du volume produit (longueur * hauteur * largeur)
    volume_expr = (
        (pl.col("product_length_cm") * pl.col("product_height_cm") * pl.col("product_width_cm"))
        .round(2)
        .alias("product_volume_cm3")
    )

    return df_joined.select([
        pl.col("product_id"),
        pl.col("product_category_name"),
        category_english_expr,
        pl.col("product_name_lenght").fill_null(0).cast(pl.Float64).alias("product_name_length"),
        pl.col("product_description_lenght").fill_null(0).cast(pl.Float64).alias("product_description_length"),
        pl.col("product_photos_qty").fill_null(0).cast(pl.Float64).alias("product_photos_qty"),
        pl.col("product_weight_g").cast(pl.Float64),
        pl.col("product_length_cm").cast(pl.Float64),
        pl.col("product_height_cm").cast(pl.Float64),
        pl.col("product_width_cm").cast(pl.Float64),
        volume_expr
    ])


def build_fact_orders() -> pl.DataFrame:
    df_orders = read_silver_table("sourceb_source_olist_orders_dataset")
    df_payments = read_silver_table("sourcea_source_olist_order_payments_dataset")
    df_items = read_silver_table("sourcea_source_olist_order_items_dataset")
    df_reviews = read_silver_table("sourceb_source_olist_order_reviews_dataset")
    df_customers = read_silver_table("sourcea_source_olist_customers_dataset")

    # 1. Calcul du score moyen par commande
    df_avg_reviews = (
        df_reviews.group_by("order_id")
        .agg(pl.col("review_score").mean().alias("avg_score"))
    )

    # Score de secours global si aucune note n'existe
    global_avg_score = df_reviews["review_score"].mean()
    if global_avg_score is None:
        global_avg_score = 0.0

    # 2. Jointures
    df_fact = (
        df_orders
        .join(df_payments, on="order_id", how="inner")
        .join(df_items, on="order_id", how="inner")
        .join(df_avg_reviews, on="order_id", how="left")
        .join(df_customers, on="customer_id", how="left")
    )

    # 3. Calculs des colonnes dérivées
    review_score_expr = (
        pl.col("avg_score")
        .fill_null(global_avg_score)
        .round(2)
        .alias("customer_review_score")
    )

    fee_expr = (
        (pl.col("total_payment_value") - (pl.col("price") + pl.col("freight_value")))
        .round(2)
        .alias("installment_or_tip_fee")
    )

    # 4. Sélection et renommage final
    return df_fact.select([
        # 1. Identifiants & Clés
        pl.col("order_id"),
        pl.col("seller_id"),
        pl.col("product_id"),
        pl.col("customer_id"),

        # 2. Statut & Suivi Temporel
        pl.col("order_status"),
        pl.col("order_purchase_timestamp").alias("order_purchase_date"),
        pl.col("order_approved_at").alias("order_approved_date"),
        pl.col("order_delivered_carrier_date").alias("carrier_delivery_date"),
        pl.col("order_delivered_customer_date").alias("customer_delivery_date"),
        pl.col("order_estimated_delivery_date").alias("estimated_delivery_date"),
        pl.col("shipping_limit_date").alias("seller_shipping_limit_date"),

        # 3. Évaluation Client
        review_score_expr,

        # 4. Détails Article & Frais
        pl.col("price").cast(pl.Float64).alias("item_price"),
        pl.col("freight_value").cast(pl.Float64).alias("item_freight_value"),
        fee_expr,

        # 5. Échéancier, Ajustements & Paiements
        pl.col("payment_sequential").cast(pl.Float64).alias("payment_sequence_number"),
        pl.col("payment_installments").cast(pl.Float64).alias("payment_installments_count"),
        pl.col("has_credit_card").cast(pl.Float64).alias("is_paid_by_credit_card"),
        pl.col("has_boleto").cast(pl.Float64).alias("is_paid_by_boleto"),
        pl.col("has_voucher").cast(pl.Float64).alias("is_paid_by_voucher"),
        pl.col("has_debit_card").cast(pl.Float64).alias("is_paid_by_debit_card"),
        pl.col("total_payment_value").cast(pl.Float64).alias("total_payment_amount")
    ])


# ------------------------------------------------------------------------------
# LOGIQUE D'INDEXATION DE LA COUCHE GOLD
# ------------------------------------------------------------------------------

# Configuration des index par table (Nom table -> Liste d'instructions DDL)
GOLD_INDEXES = {
    "dim_customers": [
        f'CREATE INDEX IF NOT EXISTS idx_dim_cust_pk ON "{GOLD_SCHEMA}"."dim_customers" (customer_id);',
        f'CREATE INDEX IF NOT EXISTS idx_dim_cust_state ON "{GOLD_SCHEMA}"."dim_customers" (customer_state);',
    ],
    "dim_sellers": [
        f'CREATE INDEX IF NOT EXISTS idx_dim_sell_pk ON "{GOLD_SCHEMA}"."dim_sellers" (seller_id);',
        f'CREATE INDEX IF NOT EXISTS idx_dim_sell_state ON "{GOLD_SCHEMA}"."dim_sellers" (seller_state);',
    ],
    "dim_products": [
        f'CREATE INDEX IF NOT EXISTS idx_dim_prod_pk ON "{GOLD_SCHEMA}"."dim_products" (product_id);',
        f'CREATE INDEX IF NOT EXISTS idx_dim_prod_cat_eng ON "{GOLD_SCHEMA}"."dim_products" (product_category_name_english);',
    ],
    "fact_orders": [
        # Clés de jointure FK / PK
        f'CREATE INDEX IF NOT EXISTS idx_fact_ord_order_id ON "{GOLD_SCHEMA}"."fact_orders" (order_id);',
        f'CREATE INDEX IF NOT EXISTS idx_fact_ord_cust_id ON "{GOLD_SCHEMA}"."fact_orders" (customer_id);',
        f'CREATE INDEX IF NOT EXISTS idx_fact_ord_seller_id ON "{GOLD_SCHEMA}"."fact_orders" (seller_id);',
        f'CREATE INDEX IF NOT EXISTS idx_fact_ord_prod_id ON "{GOLD_SCHEMA}"."fact_orders" (product_id);',
        # Axes d'analyses temporelles & statuts
        f'CREATE INDEX IF NOT EXISTS idx_fact_ord_purchase_date ON "{GOLD_SCHEMA}"."fact_orders" (order_purchase_date);',
        f'CREATE INDEX IF NOT EXISTS idx_fact_ord_status ON "{GOLD_SCHEMA}"."fact_orders" (order_status);',
    ]
}


def apply_table_indexes(table_name: str, dw_uri: str = DW_URI):
    """
    Applique la liste des index PostgreSQL associés à une table Gold.
    """
    indexes = GOLD_INDEXES.get(table_name, [])
    if not indexes:
        return

    logger.info(f"  └─ ⚡ Création de {len(indexes)} index sur [{GOLD_SCHEMA}.{table_name}]...")
    with adbc_dbapi.connect(dw_uri) as conn:
        with conn.cursor() as cur:
            for idx_ddl in indexes:
                cur.execute(idx_ddl)
        conn.commit()


# Registre des tables Gold (Nom Table -> Fonction associée)
GOLD_TABLES = [
    ("dim_customers", build_dim_customers),
    ("dim_sellers", build_dim_sellers),
    ("dim_products", build_dim_products),
    ("fact_orders", build_fact_orders),
]


def run_gold_transformation() -> bool:
    """
    Exécute le pipeline de modélisation et d'indexation de la couche Gold.
    """
    logger.info("=" * 80)
    logger.info("🚀 DÉMARRAGE DU PIPELINE DE MODÉLISATION & INDEXATION - COUCHE GOLD")
    logger.info("=" * 80)

    try:
        # Étape 1 : S'assurer que le schéma Gold existe
        create_schema_if_not_exists(dw_uri=DW_URI, schema_name=GOLD_SCHEMA)
        
        total_tables = len(GOLD_TABLES)

        # Étape 2 : Boucle de construction, chargement et indexation
        for idx, (table_name, build_func) in enumerate(GOLD_TABLES, 1):
            full_target_table = f"{GOLD_SCHEMA}.{table_name}"
            
            logger.info(f"⏳ [{idx}/{total_tables}] Construction en cours : {table_name}...")

            # 1. Transformation Polars
            df_gold = build_func()
            nb_rows = df_gold.height

            # 2. Suppression de la table existante (CASCADE)
            drop_table_if_exists(DW_URI, GOLD_SCHEMA, table_name)

            # 3. Écriture dans la couche Gold via ADBC
            df_gold.write_database(
                table_name=full_target_table,
                connection=DW_URI,
                if_table_exists="replace",
                engine="adbc"
            )

            # 4. Application des index de performance
            apply_table_indexes(table_name=table_name, dw_uri=DW_URI)

            formatted_rows = f"{nb_rows:,}".replace(",", " ")
            logger.info(
                f"  └─ ✅ {full_target_table:<50} | {formatted_rows:>8} lignes créées & indexées"
            )

        logger.info("=" * 80)
        logger.info("🎉 PIPELINE GOLD ET INDEXATION TERMINÉS AVEC SUCCÈS")
        logger.info("=" * 80)
        return True

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ ERREUR CRITIQUE DURANT LA MODÉLISATION GOLD : {e}", exc_info=True)
        logger.error("=" * 80)
        return False


# ------------------------------------------------------------------------------
# POINT D'ENTRÉE AUTONOME
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("🔔 Exécution autonome lancée depuis le terminal.")
    success = run_gold_transformation()
    sys.exit(0 if success else 1)