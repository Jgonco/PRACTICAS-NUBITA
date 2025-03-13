{{ config(materialized = 'table') }}

WITH ventas_online AS (
    SELECT 
        HORA_COMPRA, 
        ID_TRANSACCION, 
        ROUND(COMPRA_FINAL,2) AS GASTO,
        FECHA_COMPRA
    FROM {{ ref('ods_vnt_l2_v_compras') }}
    WHERE MEDIO_COMPRA = 'online'
)
SELECT * FROM ventas_online