{{ config(materialized = 'view') }}

WITH clientes AS (
    SELECT 
        CAST(SUBSTRING(user_id, 5) AS INT) AS id_usuario,  
        joined_date::DATE AS fecha_registro
    FROM {{ ref('ods_s3_l1_v_clientes') }}
),

compras_con_ranking AS (
    SELECT 
        CAST(SUBSTRING(c.transaction_id, 4) AS INT) AS id_transaccion, 
        CAST(SUBSTRING(c.user_id, 5) AS INT) AS id_usuario,  
        c.purchased_date::DATE AS fecha_compra,           
        EXTRACT(HOUR FROM c.purchased_time) AS hora_compra,
        c.purchase_medium::STRING AS medio_compra, 
        c.payment_method::STRING AS metodo_pago,       
        c.product_category::STRING AS categoria_producto,
        c.shipping_method::STRING AS metodo_envio,
        c.shipping_cost::INT AS coste_envio,
        COALESCE(c.tier_discount_percentage, 0) AS descuento_suscripcion,
        COALESCE(c.card_discount_percentage, 0) AS descuento_tarjeta,
        COALESCE(c.coupon_discount_percentage, 0) AS descuento_cupon,
        (COALESCE(c.tier_discount_percentage, 0) + COALESCE(c.card_discount_percentage, 0) + COALESCE(c.coupon_discount_percentage, 0)) AS descuento_total,
        c.total_purchase::FLOAT AS total_compra,
        (c.total_purchase * (COALESCE(c.tier_discount_percentage, 0) + COALESCE(c.card_discount_percentage, 0) + COALESCE(c.coupon_discount_percentage, 0)) / 100) AS descuento,
        (c.total_purchase - (c.total_purchase * (COALESCE(c.tier_discount_percentage, 0) + COALESCE(c.card_discount_percentage, 0) + COALESCE(c.coupon_discount_percentage, 0)) / 100)) AS compra_final,
        ABS(DATEDIFF(DAY, c.received_date::DATE, c.payment_date::DATE)) AS dias_entrega,
        c.customer_exp_rating::FLOAT AS calificacion_cliente,
        cl.fecha_registro AS fecha_registro,
        ROW_NUMBER() OVER (PARTITION BY c.transaction_id ORDER BY cl.fecha_registro DESC) AS row_num
    FROM {{ ref('lnd_s3_sales') }} c
    LEFT JOIN clientes cl
    ON CAST(SUBSTRING(c.user_id, 5) AS INT) = cl.id_usuario  
)

SELECT *
FROM compras_con_ranking
WHERE row_num = 1

