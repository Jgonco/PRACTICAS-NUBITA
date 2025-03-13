{{ config(materialized='table') }}

WITH compras_procesadas AS (
    SELECT 
        id_transaccion,
        id_usuario,
        fecha_compra, 
        YEAR(fecha_compra) AS anio,
        MONTH(fecha_compra) AS mes,
        DAY(fecha_compra) AS dia,
        hora_compra,
        medio_compra,
        metodo_pago,
        categoria_producto,
        coste_envio,
        descuento_suscripcion,
        descuento_tarjeta,
        descuento_cupon,
        descuento_total,
        total_compra,
        descuento,
        compra_final,
        dias_entrega,
        calificacion_cliente
    FROM {{ ref('ods_vnt_l2_v_compras') }}  
)

SELECT * FROM compras_procesadas
