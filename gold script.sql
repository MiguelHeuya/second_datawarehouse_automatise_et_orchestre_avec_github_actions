


DROP TABLE IF EXISTS gold.fact_orders CASCADE;
CREATE TABLE gold.fact_orders AS 
(WITH avg_reviews_per_order AS (
    SELECT 
        order_id,
        AVG(review_score) AS avg_score
    FROM silver.sourceb_source_olist_order_reviews_dataset
    GROUP BY order_id
)
SELECT
    -- 1. IDENTIFIANTS & CLÉS
    a.order_id                      AS order_id,
    c.seller_id                     AS seller_id,
    c.product_id                    AS product_id,
    a.customer_id                   AS customer_id,

	
    -- 2. STATUT & SUIVI TEMPOREL
    a.order_status                  AS order_status,
    a.order_purchase_timestamp      AS order_purchase_date,
    a.order_approved_at             AS order_approved_date,
    a.order_delivered_carrier_date  AS carrier_delivery_date,
    a.order_delivered_customer_date AS customer_delivery_date,
    a.order_estimated_delivery_date AS estimated_delivery_date,
    c.shipping_limit_date           AS seller_shipping_limit_date,

    -- 3. ÉVALUATION ET SATISFACTION CLIENT
    ROUND(
        CAST(
            COALESCE(
                r.avg_score, 
                (SELECT AVG(review_score) FROM silver.sourceb_source_olist_order_reviews_dataset)
            ) AS numeric
        ), 2
    )                               AS customer_review_score,

    -- 4. DÉTAILS DE L'ARTICLE & FRAIS ASSOCIÉS
    c.price::numeric                         AS item_price,
    c.freight_value::numeric                 AS item_freight_value,
    ROUND(
        CAST(b.total_payment_value - (c.price + c.freight_value) AS numeric), 
        2
    )                               AS installment_or_tip_fee,

    -- 5. ÉCHÉANCIER, AJUSTEMENTS & MODES DE PAIEMENT
    b.payment_sequential::numeric            AS payment_sequence_number,
    b.payment_installments::numeric          AS payment_installments_count,
    b.has_credit_card::numeric               AS is_paid_by_credit_card,
    b.has_boleto::numeric                    AS is_paid_by_boleto,
    b.has_voucher::numeric                   AS is_paid_by_voucher,
    b.has_debit_card::numeric                AS is_paid_by_debit_card,
    b.total_payment_value::numeric           AS total_payment_amount

FROM silver.sourceb_source_olist_orders_dataset AS a
INNER JOIN silver.sourcea_source_olist_order_payments_dataset AS b
    ON a.order_id = b.order_id
INNER JOIN silver.sourcea_source_olist_order_items_dataset AS c
    ON a.order_id = c.order_id
LEFT JOIN avg_reviews_per_order AS r
    ON a.order_id = r.order_id
LEFT JOIN silver.sourcea_source_olist_customers_dataset AS d
	ON a.customer_id = d.customer_id
);


DROP TABLE IF EXISTS gold.dim_customers CASCADE;
CREATE TABLE gold.dim_customers AS(
	SELECT
		customer_id,
		customer_zip_code_prefix AS customer_zip_code,
		customer_city,
		customer_state
	FROM silver.sourcea_source_olist_customers_dataset
);

DROP TABLE IF EXISTS gold.dim_sellers CASCADE;
CREATE TABLE gold.dim_sellers AS(
	SELECT
		seller_id,
		seller_zip_code_prefix AS seller_zip_code,
		seller_city,
		seller_state
	FROM silver.sourceb_source_olist_sellers_dataset
);

DROP TABLE IF EXISTS gold.dim_products CASCADE;
CREATE TABLE gold.dim_products AS
	(SELECT
	    -- 1. CLÉ PRIMAIRE & IDENTIFIANT
	    p.product_id,
	
	    -- 2. CATÉGORIES (ORIGINALE ET TRADUITE)
	    p.product_category_name,
	    COALESCE(
	        NULLIF(t.product_category_name_english, 'n/a'), 
	        NULLIF(p.product_category_name, 'n/a'), 
	        'non spécifié'
	    ) AS product_category_name_english,
	    -- 3. CARACTÉRISTIQUES DE LA FICHE PRODUIT
	    COALESCE(p.product_name_lenght, 0)::numeric         AS product_name_length,
	    COALESCE(p.product_description_lenght, 0)::numeric  AS product_description_length,
	    COALESCE(p.product_photos_qty, 0)::numeric          AS product_photos_qty,
	    -- 4. PAYSAGE PHYSIQUE & LOGISTIQUE
	    p.product_weight_g::numeric,
	    p.product_length_cm::numeric,
	    p.product_height_cm::numeric,
	    p.product_width_cm::numeric,
	    -- Indicateur dérivé pour l'analyse logistique (Volume en cm3)
	    ROUND(
	        CAST(p.product_length_cm * p.product_height_cm * p.product_width_cm AS numeric), 
	        2
	    ) AS product_volume_cm3
	
	FROM silver.sourceb_source_olist_products_dataset AS p
	LEFT JOIN silver.sourceb_source_product_category_name_translation AS t
	    ON p.product_category_name = t.product_category_name);

