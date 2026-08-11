

DROP TABLE IF EXISTS silver.sourcea_source_olist_customers_dataset CASCADE;
CREATE TABLE silver.sourcea_source_olist_customers_dataset AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        CASE customer_state
            WHEN 'AC' THEN 'Acre'
            WHEN 'AL' THEN 'Alagoas'
            WHEN 'AM' THEN 'Amazonas'
            WHEN 'AP' THEN 'Amapá'
            WHEN 'BA' THEN 'Bahia'
            WHEN 'CE' THEN 'Ceará'
            WHEN 'DF' THEN 'Distrito Federal'
            WHEN 'ES' THEN 'Espírito Santo'
            WHEN 'GO' THEN 'Goiás'
            WHEN 'MA' THEN 'Maranhão'
            WHEN 'MG' THEN 'Minas Gerais'
            WHEN 'MS' THEN 'Mato Grosso do Sul'
            WHEN 'MT' THEN 'Mato Grosso'
            WHEN 'PA' THEN 'Pará'
            WHEN 'PB' THEN 'Paraíba'
            WHEN 'PE' THEN 'Pernambuco'
            WHEN 'PI' THEN 'Piauí'
            WHEN 'PR' THEN 'Paraná'
            WHEN 'RJ' THEN 'Rio de Janeiro'
            WHEN 'RN' THEN 'Rio Grande do Norte'
            WHEN 'RO' THEN 'Rondônia'
            WHEN 'RR' THEN 'Roraima'
            WHEN 'RS' THEN 'Rio Grande do Sul'
            WHEN 'SC' THEN 'Santa Catarina'
            WHEN 'SE' THEN 'Sergipe'
            WHEN 'SP' THEN 'São Paulo'
            WHEN 'TO' THEN 'Tocantins'
            ELSE customer_state
        END AS customer_state
    FROM bronze.sourcea_source_olist_customers_dataset);


DROP TABLE IF EXISTS silver.sourcea_source_olist_geolocation_dataset CASCADE;
CREATE TABLE silver.sourcea_source_olist_geolocation_dataset AS (
    SELECT DISTINCT
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        CASE geolocation_state
            WHEN 'AC' THEN 'Acre'
            WHEN 'AL' THEN 'Alagoas'
            WHEN 'AM' THEN 'Amazonas'
            WHEN 'AP' THEN 'Amapá'
            WHEN 'BA' THEN 'Bahia'
            WHEN 'CE' THEN 'Ceará'
            WHEN 'DF' THEN 'Distrito Federal'
            WHEN 'ES' THEN 'Espírito Santo'
            WHEN 'GO' THEN 'Goiás'
            WHEN 'MA' THEN 'Maranhão'
            WHEN 'MG' THEN 'Minas Gerais'
            WHEN 'MS' THEN 'Mato Grosso do Sul'
            WHEN 'MT' THEN 'Mato Grosso'
            WHEN 'PA' THEN 'Pará'
            WHEN 'PB' THEN 'Paraíba'
            WHEN 'PE' THEN 'Pernambuco'
            WHEN 'PI' THEN 'Piauí'
            WHEN 'PR' THEN 'Paraná'
            WHEN 'RJ' THEN 'Rio de Janeiro'
            WHEN 'RN' THEN 'Rio Grande do Norte'
            WHEN 'RO' THEN 'Rondônia'
            WHEN 'RR' THEN 'Roraima'
            WHEN 'RS' THEN 'Rio Grande do Sul'
            WHEN 'SC' THEN 'Santa Catarina'
            WHEN 'SE' THEN 'Sergipe'
            WHEN 'SP' THEN 'São Paulo'
            WHEN 'TO' THEN 'Tocantins'
            ELSE geolocation_state
        END AS geolocation_state
    FROM bronze.sourcea_source_olist_geolocation_dataset
);


DROP TABLE IF EXISTS silver.sourcea_source_olist_order_items_dataset CASCADE;
CREATE TABLE silver.sourcea_source_olist_order_items_dataset AS
(WITH order_product_counts AS (
	    -- 1. Count distinct products per order
	    SELECT 
	        order_id,
	        COUNT(DISTINCT product_id) AS nb_distinct_products
	    FROM bronze.sourcea_source_olist_order_items_dataset
	    GROUP BY order_id
	)
	-- CASE 1: Single-product orders -> Aggregate with explicit numeric casting
	SELECT 
	    i.order_id,
	    MIN(i.order_item_id)                            AS order_item_id,
	    MAX(i.product_id)                              AS product_id,
	    MAX(i.seller_id)                               AS seller_id,
	    MAX(i.shipping_limit_date)                     AS shipping_limit_date,
	    SUM(i.price::numeric)                          AS price,
	    SUM(i.freight_value::numeric)                  AS freight_value
	FROM bronze.sourcea_source_olist_order_items_dataset AS i
	INNER JOIN order_product_counts AS c 
	    ON i.order_id = c.order_id
	WHERE c.nb_distinct_products = 1
	GROUP BY i.order_id
	UNION ALL
	-- CASE 2: Multi-product orders -> Retain original rows with matching types
	SELECT 
	    i.order_id,
	    i.order_item_id,
	    i.product_id,
	    i.seller_id,
	    i.shipping_limit_date,
	    i.price::numeric                               AS price,
	    i.freight_value::numeric                       AS freight_value
	FROM bronze.sourcea_source_olist_order_items_dataset AS i
	INNER JOIN order_product_counts AS c 
	    ON i.order_id = c.order_id
	WHERE c.nb_distinct_products > 1);




DROP TABLE IF EXISTS silver.sourcea_source_olist_order_payments_dataset CASCADE;
CREATE TABLE silver.sourcea_source_olist_order_payments_dataset AS(
SELECT
    order_id,
    SUM(payment_sequential::double precision) AS payment_sequential,
    SUM(payment_installments::double precision) AS payment_installments,
    COALESCE(SUM(CASE WHEN payment_type = 'credit_card' THEN payment_value::double precision ELSE 0 END), 0) AS has_credit_card,
    COALESCE(SUM(CASE WHEN payment_type = 'boleto' THEN payment_value::double precision ELSE 0 END), 0) AS has_boleto,
    COALESCE(SUM(CASE WHEN payment_type = 'voucher' THEN payment_value::double precision ELSE 0 END), 0) AS has_voucher,
    COALESCE(SUM(CASE WHEN payment_type = 'debit_card' THEN payment_value::double precision ELSE 0 END), 0) AS has_debit_card,
	SUM(payment_value::double precision) AS total_payment_value
FROM bronze.sourcea_source_olist_order_payments_dataset
GROUP BY order_id
);


DROP TABLE IF EXISTS silver.sourceb_source_olist_order_reviews_dataset CASCADE;
CREATE TABLE silver.sourceb_source_olist_order_reviews_dataset AS
	(
	SELECT
		review_id,
		order_id,
		review_score:: double precision,
		COALESCE(review_comment_title, 'n/a') AS review_comment_title,
		COALESCE(review_comment_message, 'n/a') AS review_comment_message,
		review_creation_date::date,
		review_answer_timestamp::date
	FROM bronze.sourceb_source_olist_order_reviews_dataset
);


DROP TABLE IF EXISTS silver.sourceb_source_olist_orders_dataset CASCADE;
CREATE TABLE silver.sourceb_source_olist_orders_dataset AS
(SELECT
	order_id,
	customer_id,
	order_status,
	order_purchase_timestamp::date,
	order_approved_at::date,
	order_delivered_carrier_date::date,
	order_delivered_customer_date::date,
	order_estimated_delivery_date::date
FROM bronze.sourceb_source_olist_orders_dataset
);


DROP TABLE IF EXISTS silver.sourceb_source_olist_products_dataset CASCADE;
CREATE TABLE silver.sourceb_source_olist_products_dataset AS
(
	SELECT
		product_id,
		COALESCE(product_category_name, 'n/a') AS product_category_name,
		product_name_lenght::double precision,
		product_description_lenght::double precision,
		product_photos_qty::double precision,
		product_weight_g::double precision,
		product_length_cm::double precision,
		product_height_cm::double precision,
		product_width_cm::double precision
	FROM bronze.sourceb_source_olist_products_dataset
);

DROP TABLE IF EXISTS silver.sourceb_source_olist_sellers_dataset CASCADE;
CREATE TABLE silver.sourceb_source_olist_sellers_dataset AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        CASE seller_state
            WHEN 'AC' THEN 'Acre'
            WHEN 'AL' THEN 'Alagoas'
            WHEN 'AM' THEN 'Amazonas'
            WHEN 'AP' THEN 'Amapá'
            WHEN 'BA' THEN 'Bahia'
            WHEN 'CE' THEN 'Ceará'
            WHEN 'DF' THEN 'Distrito Federal'
            WHEN 'ES' THEN 'Espírito Santo'
            WHEN 'GO' THEN 'Goiás'
            WHEN 'MA' THEN 'Maranhão'
            WHEN 'MG' THEN 'Minas Gerais'
            WHEN 'MS' THEN 'Mato Grosso do Sul'
            WHEN 'MT' THEN 'Mato Grosso'
            WHEN 'PA' THEN 'Pará'
            WHEN 'PB' THEN 'Paraíba'
            WHEN 'PE' THEN 'Pernambuco'
            WHEN 'PI' THEN 'Piauí'
            WHEN 'PR' THEN 'Paraná'
            WHEN 'RJ' THEN 'Rio de Janeiro'
            WHEN 'RN' THEN 'Rio Grande do Norte'
            WHEN 'RO' THEN 'Rondônia'
            WHEN 'RR' THEN 'Roraima'
            WHEN 'RS' THEN 'Rio Grande do Sul'
            WHEN 'SC' THEN 'Santa Catarina'
            WHEN 'SE' THEN 'Sergipe'
            WHEN 'SP' THEN 'São Paulo'
            WHEN 'TO' THEN 'Tocantins'
            ELSE seller_state
        END AS seller_state
    FROM bronze.sourceb_source_olist_sellers_dataset
);


DROP TABLE IF EXISTS silver.sourceb_source_product_category_name_translation CASCADE;
CREATE TABLE silver.sourceb_source_product_category_name_translation AS (
    SELECT
        COALESCE("ï»¿product_category_name", 'n/a') AS "product_category_name",
        COALESCE(product_category_name_english, 'n/a') AS product_category_name_english
    FROM bronze.sourceb_source_product_category_name_translation
);

