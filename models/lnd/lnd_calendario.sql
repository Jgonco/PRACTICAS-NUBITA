{{ config(materialized='table') }}

WITH RECURSIVE date_series AS (
    SELECT DATE('2020-01-01') AS calendar_date
    UNION ALL
    SELECT DATEADD(DAY, 1, calendar_date)
    FROM date_series
    WHERE calendar_date < DATE('2024-12-31')
)
SELECT 
    calendar_date,
    YEAR(calendar_date) AS year,
    MONTH(calendar_date) AS month,
    DAY(calendar_date) AS day,
    DAYOFWEEK(calendar_date) AS day_of_week,
    CASE 
        WHEN DAYOFWEEK(calendar_date) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    QUARTER(calendar_date) AS quarter,
    MONTHNAME(calendar_date) AS month_name,
    DAYNAME(calendar_date) AS day_name,
    WEEKOFYEAR(calendar_date) AS week_of_year
FROM date_series