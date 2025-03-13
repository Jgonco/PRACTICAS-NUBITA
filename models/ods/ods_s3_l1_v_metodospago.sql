{{ config(materialized = 'view') }}

WITH metodos_pago AS (
    SELECT 
        payment_method AS metodo_pago,
        ROW_NUMBER() OVER (ORDER BY payment_method ASC) + 9 AS id_metodopago
    FROM (
        SELECT DISTINCT payment_method  
        FROM {{ ref('lnd_s3_sales') }}  
    ) AS distinct_metodos
)

SELECT * 
FROM metodos_pago
