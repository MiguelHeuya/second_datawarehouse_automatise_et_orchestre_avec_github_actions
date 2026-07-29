

-- Dimension customers

SELECT
*
FROM silver.sourcea_source_olist_customers_dataset

-- Dimension customers



-- Dim location

SELECT
geolocation_zip_code_prefix,
geolocation_city,
geolocation_state
FROM silver.sourcea_source_olist_geolocation_dataset


-- Dim location



-- Fact order

SELECT
order_id,
order_item_id,
product_id,
seller_id,
shipping_limit_date,
price,
freight_value
FROM silver.sourcea_source_olist_order_items_dataset

-- Fact order






--- Facts order

SELECT
order_id,
payment_sequential,
payment_type,
payment_installments,
payment_value
FROM silver.sourcea_source_olist_order_payments_dataset

--- Fact order






-- Dimension review

SELECT
review_id,
order_id,
review_score
FROM bronze.sourceb_source_olist_order_reviews_dataset

-- Dimension review





/* Fact Order */

SELECT
order_id,
customer_id,
order_status,
order_purchase_timestamp,
order_approved_at,
order_delivered_carrier_date,
order_delivered_customer_date,
order_estimated_delivery_date
FROM silver.sourceb_source_olist_orders_dataset

/* Fact Order */




-- Dim Product

SELECT
product_id,
product_category_name,
product_weight_g
FROM bronze.sourceb_source_olist_products_dataset

-- Dim Product





-- Dim Seller

SELECT
seller_id,
seller_zip_code_prefix
FROM bronze.sourceb_source_olist_sellers_dataset

-- Dim Seller




-- Dim product

SELECT
product_category_name as product_category_name,
product_category_name_english
FROM silver.sourceb_source_product_category_name_translation

-- Dim product



