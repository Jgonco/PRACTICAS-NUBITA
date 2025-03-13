{{ config(materialized='table') }}

WITH ventas_mensuales AS (
    SELECT 
        c.year,
        c.month,
        COUNT(v.ID_TRANSACCION) AS total_ventas, 
        Sv.COMPRA_FINAL) AS total_gastado
    FROM {{ ref('lnd_calendario') }} c
    LEFT JOIN {{ ref('ods_vnt_l2_v_compras') }} v
        ON c.calendar_date = v.FECHA_COMPRA
    GROUP BY c.year, c.month
    ORDER BY c.year, c.month
)
SELECT * FROM ventas_mensuales
