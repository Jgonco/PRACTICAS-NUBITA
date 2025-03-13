{{ config(materialized='table') }}

WITH ventas_envio AS (
    SELECT 
        CASE 
            WHEN dias_entrega BETWEEN 0 AND 5 THEN 'Exprés(0-5)'
            WHEN dias_entrega BETWEEN 6 AND 10 THEN 'Estándar(6-10)'
            ELSE 'Demorado(>10)'
        END AS tipo_envio,
        COUNT(ID_TRANSACCION) AS total_ventas,
        ROUND(AVG(coste_envio), 2) AS gasto_medio_envio,
        ROUND(AVG(calificacion_cliente), 2) AS puntuacion_media
    FROM {{ ref('ods_vnt_l2_v_compras') }}
    GROUP BY tipo_envio
)
SELECT * FROM ventas_envio
ORDER BY 
    CASE tipo_envio
        WHEN 'Exprés(0-5)' THEN 1
        WHEN 'Estándar(6-10)' THEN 2
        WHEN 'Demorado(>10)' THEN 3
    END