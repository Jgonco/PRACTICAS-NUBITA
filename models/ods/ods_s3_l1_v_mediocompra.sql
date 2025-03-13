{{ config(materialized = 'view') }}

WITH medio_compra AS (
    SELECT 
        purchase_medium AS medio_compra,
        CASE 
            WHEN purchase_medium = 'in-store' THEN 0
            WHEN purchase_medium = 'online' THEN 1
        END AS id_mediocompra
    FROM (
        SELECT DISTINCT purchase_medium  
        FROM {{ ref('lnd_s3_sales') }}  
    ) AS distinct_medios
)

SELECT * 
FROM medio_compra