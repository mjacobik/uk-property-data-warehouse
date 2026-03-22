{{ config(
    materialized='table',
    unique_key='date_key'
) }}

WITH date_spine AS (
    SELECT 
        -- Generate dates from 1990-01-01 (oldest PPD data) to 2030-12-31
        GENERATE_SERIES(
            '1990-01-01'::DATE, 
            '2030-12-31'::DATE, 
            '1 day'::interval
        )::DATE AS date_day
)

SELECT
    -- INT key in YYYYMMDD format (Kimball requirement)
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) AS date_key,
    
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    TO_CHAR(date_day, 'Month') AS month_name,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(ISODOW FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_name,
    
    CASE 
        WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN True 
        ELSE False 
    END AS is_weekend

FROM date_spine
