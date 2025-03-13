{{ config(materialized = 'table') }}

WITH ventas_categoria AS (
    SELECT 
        cat.ID_CATEGPRODUCTO, 
        cat.CATEGORIA_PRODUCTO, 
        COUNT(v.ID_TRANSACCION) AS total_ventas, 
        SUM(v.COMPRA_FINAL) AS total_gastado, 
        ROUND(AVG(v.CALIFICACION_CLIENTE),2) AS puntuacion_media 
    FROM {{ ref('ods_vnt_l2_v_compras') }} v
    JOIN {{ ref('ods_s3_l1_v_categproducto') }} cat 
        ON v.CATEGORIA_PRODUCTO = cat.CATEGORIA_PRODUCTO 
    GROUP BY cat.ID_CATEGPRODUCTO, cat.CATEGORIA_PRODUCTO
)
SELECT * FROM ventas_categoria 