
DROP TABLE IF EXISTS gold.dim_customers;
CREATE TABLE gold.dim_customers AS(
	SELECT
		customer_id,
		customer_unique_id,
		customer_zip_code_prefix,
		customer_city,
		customer_state
	FROM silver.sourcea_source_olist_customers_dataset
);




DROP TABLE IF EXISTS gold.dim_geolocation;
CREATE TABLE gold.dim_geolocation AS(
	SELECT
		geolocation_zip_code_prefix,
		geolocation_city,
		geolocation_state
	FROM silver.sourcea_source_olist_geolocation_dataset
);



DROP TABLE IF EXISTS gold.dim_sellers;
CREATE TABLE gold.dim_sellers AS(
	SELECT
		seller_id,
		seller_zip_code_prefix,
		seller_city,
		seller_state
	FROM silver.sourceb_source_olist_sellers_dataset
);



DROP TABLE IF EXISTS gold.dim_products;
CREATE TABLE gold.dim_products AS(
SELECT
    *
FROM (
    SELECT 
        a.product_id,
        COALESCE(a.product_category_name, 'Uncategorized') AS product_category_name_original,
        COALESCE(b.product_category_name_english, a.product_category_name, 'Uncategorized') AS product_category_name_english_translated

    FROM silver.sourceb_source_olist_products_dataset AS a
    LEFT JOIN silver.sourceb_source_product_category_name_translation AS b
        ON a.product_category_name = b.product_category_name
) AS subquery
);

