{{ config(materialized = 'table') }} 
WITH totalsales AS (
    SELECT 
        sales.$1::STRING AS user_id,
        sales.$2::INT AS age,
        sales.$3::STRING AS sex,
        sales.$4::STRING AS phone_number,
        sales.$5::DATE AS joined_date,
        sales.$6::STRING AS country,
        sales.$7::STRING AS payment_method,
        sales.$8::BOOLEAN AS loyalty_program_member,
        sales.$9::INT AS loyalty_points_redeemed,
        sales.$10::STRING AS loyalty_tier,
        sales.$11::INT AS tier_discount_percentage,
        sales.$12::INT AS card_discount_percentage,
        sales.$13::INT AS coupon_discount_percentage,
        sales.$14::INT AS total_discount_percentage,
        sales.$15::DECIMAL(10, 2) AS total_purchase,
        sales.$16::DECIMAL(10, 2) AS total_discount,
        sales.$17::DECIMAL(10, 2) AS total_purchase_after_discount,
        sales.$18::STRING AS transaction_id,
        sales.$19::STRING AS payment_status,
        sales.$20::DATE AS payment_date,
        sales.$21::TIME AS payment_time,
        sales.$22::DATE AS purchased_date,
        sales.$23::TIME AS purchased_time,
        sales.$24::STRING AS product_category,
        sales.$25::STRING AS purchase_medium,
        sales.$26::STRING AS return_status,
        sales.$27::DECIMAL(10, 2) AS refund_amount,
        sales.$28::DATE AS return_date,
        sales.$29::STRING AS order_id,
        sales.$30::DATE AS released_date,
        sales.$31::DATE AS estimated_delivery_date,
        sales.$32::DATE AS received_date,
        sales.$33::INT AS total_delivery_days,
        sales.$34::STRING AS shipping_method,
        sales.$35::DECIMAL(10, 2) AS shipping_cost,
        sales.$36::STRING AS tracking_number,
        sales.$37::DECIMAL(3, 2) AS customer_exp_rating
    FROM 
        @PRACTICA_NUBITA.SALES_COMPANY.STAGE_DATALAKE/sales.csv 
        (FILE_FORMAT => PRACTICA_NUBITA.SALES_COMPANY.csv_format) AS sales
)
SELECT *
FROM totalsales