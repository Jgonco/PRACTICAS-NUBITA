{{ config(materialized = 'view') }}

WITH clientes AS (
    SELECT 
        user_id::STRING AS user_id,              
        age::INT AS age,                          
        sex::STRING AS sex,                           
        joined_date::DATE AS joined_date,         
        loyalty_program_member::BOOLEAN AS loyalty_program_member, 
        loyalty_tier::FLOAT AS loyalty_tier,
        tier_discount_percentage::FLOAT AS tier_discount
    FROM {{ ref('lnd_s3_sales') }} 
)
SELECT * FROM clientes
