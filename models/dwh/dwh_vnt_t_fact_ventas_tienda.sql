{{ config(materialized = 'table') }}

WITH ventas_tienda AS (
    SELECT 
        HORA_COMPRA, 
        ID_TRANSACCION, 
        ROUND(COMPRA_FINAL,2) AS GASTO,
        FECHA_COMPRA
    FROM {{ ref('ods_vnt_l2_v_compras') }}
    WHERE MEDIO_COMPRA = 'in-store'
)
SELECT * FROM ventas_tienda