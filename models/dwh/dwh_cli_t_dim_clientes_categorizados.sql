{{ config(materialized = 'table') }}

WITH compras_por_usuario AS (
    SELECT 
        c.ID_USUARIO,
        COUNT(*) AS total_compras,
        AVG(c.descuento_suscripcion) AS descuento_suscripcion,
        SUM(c.COMPRA_FINAL) AS total_gastado
    FROM ODS_VNT_L2_V_COMPRAS c
    GROUP BY c.ID_USUARIO
),
clasificacion AS (
    SELECT 
        cu.ID_USUARIO,
        cu.total_compras,
        cu.total_gastado,
        cli.FECHA_REGISTRO,
        cli.nivel_suscripcion,
        cu.descuento_suscripcion,

        
        CASE 
            WHEN cu.total_compras = 1 THEN 'Ocasional'
            WHEN cu.total_compras BETWEEN 2 AND 3 THEN 'Regular'
            ELSE 'Frecuente'
        END AS categoria_comprador,

        
        CASE 
            WHEN cu.total_gastado < 5000 THEN 'Bajo'
            WHEN cu.total_gastado BETWEEN 5000 AND 10000 THEN 'Medio'
            ELSE 'Alto'
        END AS categoria_gasto,

        
        DATEDIFF(MONTH, cli.FECHA_REGISTRO, '2024-12-31') AS antiguedad_meses,

        CASE 
            WHEN cli.nivel_suscripcion = 1 THEN 'Basico'
            WHEN cli.nivel_suscripcion = 2 THEN 'Estandar'
            WHEN cli.nivel_suscripcion = 3 THEN 'Premium'
            WHEN cli.nivel_suscripcion = 4 THEN 'Premium-S'
            ELSE 'Sin suscripcion'
        END AS categoria_suscripcion

    FROM compras_por_usuario cu
    JOIN {{ ref('ods_cli_l2_v_clientes') }} cli ON cu.ID_USUARIO = cli.ID_USUARIO
)
SELECT * FROM clasificacion
