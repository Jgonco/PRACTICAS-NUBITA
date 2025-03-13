{{ config(materialized='table') }}

WITH ventas_por_metodo AS (
    SELECT 
        m.id_metodopago, 
        m.metodo_pago,
        COUNT(v.ID_TRANSACCION) AS total_compras,
        ROUND(SUM(v.COMPRA_FINAL), 2) AS total_gastado
    FROM {{ ref('ods_vnt_l2_v_compras') }} v
    JOIN {{ ref('ods_s3_l1_v_metodospago') }} m 
        ON v.METODO_PAGO = m.metodo_pago
    GROUP BY m.id_metodopago, m.metodo_pago
    ORDER BY total_compras DESC
)
SELECT * FROM ventas_por_metodo