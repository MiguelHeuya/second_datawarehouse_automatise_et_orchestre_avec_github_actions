
CREATE TABLE facts AS(
SELECT
			a.order_id,
			a.customer_id,
			b.product_id,
			b.seller_id,
			a.order_status,
			c.payment_type,
			c.payment_sequential,
			c.payment_installments,
			d.review_score,
			a.order_approved_at,
			b.shipping_limit_date,
			a.order_purchase_timestamp,
			a.order_delivered_carrier_date,
			a.order_delivered_customer_date,
			a.order_estimated_delivery_date,
			b.price,
			b.freight_value,
			c.payment_value
		FROM silver.sourceb_source_olist_orders_dataset AS a
		LEFT JOIN silver.sourcea_source_olist_order_items_dataset AS b
			ON a.order_id = b.order_id
		LEFT JOIN silver.sourcea_source_olist_order_payments_dataset AS c
			ON a.order_id = c.order_id
		LEFT JOIN silver.sourceb_source_olist_order_reviews_dataset AS d
		ON a.order_id = d.order_id)




/*

Recreer  la colonne payment_value, si les product id sont null, 
--- si le status est a unavailable, canceled, invoiced ou created, la commande est annulé et le payment revient a zero 
--- si l'order_status est a shipped, delivered, approved, processing la commande n'est pas annulée, le payment_value ne revient pas a zéro, 
Pour les deux, il faudra l'indiquer dans les colonnes payment_type, payment_sequential, payment_installments, payment_value.




Ensuite
Je vais garder uniquement une seule occurence de order_id dans cette table.
Je veux faire en sorte que lorsque les payment value sont les memes pour un id, la valeur du payment pour cet unique order_id soit la valeur en question, les colonnes price et freight seront leur valeur initiales multiplié par le nombre d'occcurence de l'id.
Je veux que lorsque les payment value ne sont pas les memes pour un id, la valeur du payment pour l'unique occurence de cette id qui va rester soit la somme des payment relatif a l'id et pour les colonnes price et freight, elle reste inchangée.

*/









-- Le CREATE TABLE doit encapsuler toute la requête WITH

DROP TABLE IF EXISTS gold.fact_orders;


CREATE TABLE gold.fact_orders AS 

WITH 

etape0_jointure AS (
		SELECT
			a.order_id,
			a.customer_id,
			b.product_id,
			b.seller_id,
			a.order_status,
			c.payment_type,
			c.payment_sequential,
			c.payment_installments,
			d.review_score,
			a.order_approved_at,
			b.shipping_limit_date,
			a.order_purchase_timestamp,
			a.order_delivered_carrier_date,
			a.order_delivered_customer_date,
			a.order_estimated_delivery_date,
			b.price,
			b.freight_value,
			c.payment_value
		FROM silver.sourceb_source_olist_orders_dataset AS a
		LEFT JOIN silver.sourcea_source_olist_order_items_dataset AS b
			ON a.order_id = b.order_id
		LEFT JOIN silver.sourcea_source_olist_order_payments_dataset AS c
			ON a.order_id = c.order_id
		LEFT JOIN silver.sourceb_source_olist_order_reviews_dataset AS d
		ON a.order_id = d.order_id
),

-- =========================================================================
-- ÉTAPE 1 : Correction des paiements pour les produits manquants (product_id NULL)
-- =========================================================================
etape1_correction_paiements AS (
    SELECT 
        order_id,
        customer_id,
		
        CASE 
            WHEN product_id IS NULL
                THEN 'n/a'
            ELSE product_id 
        END AS product_id,

        CASE 
            WHEN seller_id IS NULL
                THEN 'n/a'
            ELSE seller_id 
        END AS seller_id,
		
        order_status,
        review_score,
        order_approved_at,
        shipping_limit_date,
        order_purchase_timestamp,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        price,
        freight_value,
        
        -- Si le produit est NULL ET le statut fait partie des annulations :
        CASE 
            WHEN product_id IS NULL AND order_status IN ('unavailable', 'canceled', 'invoiced', 'created') 
                THEN 'canceled_no_product'
            ELSE payment_type 
        END AS payment_type,

        CASE 
            WHEN product_id IS NULL AND order_status IN ('unavailable', 'canceled', 'invoiced', 'created') 
                THEN 0
            ELSE payment_sequential 
        END AS payment_sequential,

        CASE 
            WHEN product_id IS NULL AND order_status IN ('unavailable', 'canceled', 'invoiced', 'created') 
                THEN 0
            ELSE payment_installments 
        END AS payment_installments,

        CASE 
            WHEN product_id IS NULL AND order_status IN ('unavailable', 'canceled', 'invoiced', 'created') 
                THEN 0.0
            ELSE payment_value 
        END AS payment_value

    FROM etape0_jointure
),

-- =========================================================================
-- ÉTAPE 2 : Analyse des doublons d'order_id
-- =========================================================================
etape2_analyse_doublons AS (
    SELECT 
        *,
        -- 1. Compter le nombre de lignes pour cet order_id
        COUNT(*) OVER(PARTITION BY order_id) AS nb_occurrences,

        -- 2. Comparaison MIN/MAX pour savoir si tous les paiements sont identiques
        (MIN(payment_value) OVER(PARTITION BY order_id) = MAX(payment_value) OVER(PARTITION BY order_id)) AS paiements_sont_identiques,

        -- 3. Somme totale des paiements pour cet order_id
        SUM(payment_value) OVER(PARTITION BY order_id) AS somme_totale_paiements,

        -- 4. Numérotation pour dédupliquer à la fin
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY order_purchase_timestamp DESC) AS rang_ligne

    FROM etape1_correction_paiements
)

-- =========================================================================
-- ÉTAPE 3 : Application des règles métiers et déduplication finale
-- =========================================================================
SELECT 
    order_id,
    customer_id,
    product_id,
    seller_id,
    order_status,
    payment_type,
    payment_sequential,
    payment_installments,
    review_score,
    order_approved_at,
    shipping_limit_date,
    order_purchase_timestamp,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    -- RÈGLE SUR PRICE
    CASE 
        WHEN nb_occurrences > 1 AND paiements_sont_identiques = TRUE THEN price * nb_occurrences
        ELSE price
    END AS price,

    -- RÈGLE SUR FREIGHT
    CASE 
        WHEN nb_occurrences > 1 AND paiements_sont_identiques = TRUE THEN freight_value * nb_occurrences
        ELSE freight_value
    END AS freight_value,

    -- RÈGLE SUR PAYMENT_VALUE
    CASE 
        WHEN paiements_sont_identiques = FALSE THEN somme_totale_paiements
        ELSE payment_value
    END AS payment_value

FROM etape2_analyse_doublons
-- Conserver uniquement la première occurrence nettoyée de chaque commande
WHERE rang_ligne = 1;




