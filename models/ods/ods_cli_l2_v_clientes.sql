{{ config(materialized = 'view') }}

WITH clientes AS (
    SELECT 
        CAST(SUBSTRING(user_id, 5) AS INT) AS id_usuario,
        age::INT AS edad,                          
        sex::STRING AS genero,                            
        joined_date::DATE AS fecha_registro,         
        loyalty_program_member::BOOLEAN AS suscripcion, 
        loyalty_tier::FLOAT AS nivel_suscripcion,
        tier_discount::FLOAT AS descuento_suscripcion,
        ROW_NUMBER() OVER (PARTITION BY CAST(SUBSTRING(user_id, 5) AS INT) ORDER BY joined_date DESC) AS num_fila
    FROM {{ ref('ods_s3_l1_v_clientes') }} 
)
SELECT 
    id_usuario, 
    edad, 
    genero, 
    fecha_registro, 
    suscripcion, 
    nivel_suscripcion,
    descuento_suscripcion
FROM clientes
WHERE num_fila = 1
