import polars as pl
from sqlalchemy import create_engine
from config import data_warehouse, construire_uri_postgresql

def executer_pipeline_gold():
    print("🚀 Démarrage du pipeline Gold avec Polars...")
    
    # 1. Récupération de l'URI PostgreSQL pour ConnectorX / Polars
    uri_polars = construire_uri_postgresql(data_warehouse)
    
    # 2. Moteur SQLAlchemy pour l'écriture des tables dans la base
    engine = create_engine(uri_polars)

    # =========================================================================
    # A. CRÉATION DE GOLD.FACT_ORDERS
    # =========================================================================
    print("📦 Traitement de gold.fact_orders...")

    # Chargement ciblés des colonnes métiers (évite le conflit 'date_silver_added')
    df_orders = pl.read_database_uri(
        """
        SELECT 
            order_id, customer_id, order_status, order_approved_at, 
            order_purchase_timestamp, order_delivered_carrier_date, 
            order_delivered_customer_date, order_estimated_delivery_date 
        FROM silver.sourceb_source_olist_orders_dataset
        """, 
        uri_polars
    )
    
    df_items = pl.read_database_uri(
        """
        SELECT 
            order_id, product_id, seller_id, 
            shipping_limit_date, price, freight_value 
        FROM silver.sourcea_source_olist_order_items_dataset
        """, 
        uri_polars
    )
    
    df_payments = pl.read_database_uri(
        """
        SELECT 
            order_id, payment_type, payment_sequential, 
            payment_installments, payment_value 
        FROM silver.sourcea_source_olist_order_payments_dataset
        """, 
        uri_polars
    )
    
    df_reviews = pl.read_database_uri(
        "SELECT order_id, review_score FROM silver.sourceb_source_olist_order_reviews_dataset", 
        uri_polars
    )

    # Étape 0 : Jointures
    df_fact = (
        df_orders
        .join(df_items, on="order_id", how="left")
        .join(df_payments, on="order_id", how="left")
        .join(df_reviews, on="order_id", how="left")
    )

    # Étape 1 : Nettoyage et gestion des annulations / produits manquants
    status_annules = ["unavailable", "canceled", "invoiced", "created"]
    
    df_fact = df_fact.with_columns([
        pl.col("product_id").fill_null("n/a"),
        pl.col("seller_id").fill_null("n/a"),
        
        pl.when((pl.col("product_id") == "n/a") & (pl.col("order_status").is_in(status_annules)))
          .then(pl.lit("canceled_no_product"))
          .otherwise(pl.col("payment_type"))
          .alias("payment_type"),
          
        pl.when((pl.col("product_id") == "n/a") & (pl.col("order_status").is_in(status_annules)))
          .then(0)
          .otherwise(pl.col("payment_sequential"))
          .alias("payment_sequential"),
          
        pl.when((pl.col("product_id") == "n/a") & (pl.col("order_status").is_in(status_annules)))
          .then(0)
          .otherwise(pl.col("payment_installments"))
          .alias("payment_installments"),
          
        pl.when((pl.col("product_id") == "n/a") & (pl.col("order_status").is_in(status_annules)))
          .then(0.0)
          .otherwise(pl.col("payment_value"))
          .alias("payment_value")
    ])

    # Étape 2 & 3 : Analyse des métriques par order_id et déduplication
    df_fact_gold = (
        df_fact
        .with_columns([
            pl.len().over("order_id").alias("nb_occurrences"),
            (pl.col("payment_value").min().over("order_id") == pl.col("payment_value").max().over("order_id")).alias("paiements_sont_identiques"),
            pl.col("payment_value").sum().over("order_id").alias("somme_totale_paiements")
        ])
        # Tri pour prendre la ligne la plus récente si doublons
        .sort("order_purchase_timestamp", descending=True)
        .unique(subset=["order_id"], keep="first")
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

    # Écriture dans PostgreSQL
    df_fact_gold.write_database(
        table_name="gold.fact_orders", 
        connection=engine, 
        if_table_exists="replace"
    )
    print("  ✅ Table gold.fact_orders créée avec succès.")

    # =========================================================================
    # B. CRÉATION DES TABLES DE DIMENSIONS
    # =========================================================================
    
    # 1. DIM_CUSTOMERS
    print("👤 Traitement de gold.dim_customers...")
    df_customers = pl.read_database_uri(
        """
        SELECT 
            customer_id, customer_unique_id, customer_zip_code_prefix, 
            customer_city, customer_state 
        FROM silver.sourcea_source_olist_customers_dataset
        """, 
        uri_polars
    )
    df_customers.write_database("gold.dim_customers", engine, if_table_exists="replace")
    print("  ✅ Table gold.dim_customers créée.")

    # 2. DIM_GEOLOCATION
    print("📍 Traitement de gold.dim_geolocation...")
    df_geo = pl.read_database_uri(
        """
        SELECT 
            geolocation_zip_code_prefix, geolocation_city, geolocation_state 
        FROM silver.sourcea_source_olist_geolocation_dataset
        """, 
        uri_polars
    )
    df_geo.write_database("gold.dim_geolocation", engine, if_table_exists="replace")
    print("  ✅ Table gold.dim_geolocation créée.")

    # 3. DIM_SELLERS
    print("🏪 Traitement de gold.dim_sellers...")
    df_sellers = pl.read_database_uri(
        """
        SELECT 
            seller_id, seller_zip_code_prefix, seller_city, seller_state 
        FROM silver.sourceb_source_olist_sellers_dataset
        """, 
        uri_polars
    )
    df_sellers.write_database("gold.dim_sellers", engine, if_table_exists="replace")
    print("  ✅ Table gold.dim_sellers créée.")

    # 4. DIM_PRODUCTS (avec traductions et fallback 'Uncategorized')
    print("🏷️ Traitement de gold.dim_products...")
    df_products = pl.read_database_uri(
        "SELECT product_id, product_category_name FROM silver.sourceb_source_olist_products_dataset", 
        uri_polars
    )
    df_translations = pl.read_database_uri(
        "SELECT product_category_name, product_category_name_english FROM silver.sourceb_source_product_category_name_translation", 
        uri_polars
    )

    df_dim_products = (
        df_products
        .join(df_translations, on="product_category_name", how="left")
        .select([
            pl.col("product_id"),
            
            # Nom d'origine
            pl.col("product_category_name")
              .fill_null("Uncategorized")
              .alias("product_category_name_original"),
            
            # Nom traduit en anglais avec fallback
            pl.coalesce([
                pl.col("product_category_name_english"),
                pl.col("product_category_name"),
                pl.lit("Uncategorized")
            ]).alias("product_category_name_english_translated")
        ])
    )
    df_dim_products.write_database("gold.dim_products", engine, if_table_exists="replace")
    print("  ✅ Table gold.dim_products créée.")

    print("\n🎉 Pipeline Gold exécuté avec succès !")

if __name__ == "__main__":
    executer_pipeline_gold()