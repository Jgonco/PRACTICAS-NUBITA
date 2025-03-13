{{ config(materialized = 'view') }}

WITH categoria_producto AS (
    SELECT 
        product_category AS categoria_producto,
        ROW_NUMBER() OVER (ORDER BY product_category ASC) + 999 AS id_categproducto
    FROM (
        SELECT DISTINCT product_category  
        FROM {{ ref('lnd_s3_sales') }}  
    ) AS distinct_metodos
)

SELECT * 
FROM categoria_producto