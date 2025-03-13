{{ config(materialized = 'view') }}

WITH compras AS (
    SELECT 
        transaction_id::STRING AS transaction_id,
        user_id::STRING AS lk_clienteid,
        purchased_date::DATE AS purchased_date,           
        purchased_time::TIME AS purchased_time,       
        payment_method::STRING AS payment_method,       
        product_category::STRING AS product_category,
        tier_discount_percentage::FLOAT AS tier_discount_percentage,
        card_discount_percentage::FLOAT AS card_discount_percentage,
        coupon_discount_percentage::FLOAT AS coupon_discount_percentage,
        (tier_discount_percentage + card_discount_percentage + coupon_discount_percentage) AS total_discount_percentage,
        total_purchase::FLOAT AS total_purchase,
        (total_purchase * (tier_discount_percentage + card_discount_percentage + coupon_discount_percentage) / 100) AS total_discount,
        (total_purchase - (total_purchase * (tier_discount_percentage + card_discount_percentage + coupon_discount_percentage) / 100)) AS total_purchase_after_discount,
        ABS(DATEDIFF(DAY, received_date::DATE, payment_date::DATE)) AS delivery_days,
        customer_exp_rating::FLOAT AS customer_exp_rating
        
    FROM {{ ref('lnd_s3_sales') }} 
)
SELECT * FROM compras