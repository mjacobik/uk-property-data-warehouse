-- M6: Data freshness
-- Fails (returns rows) if the most recent transfer_date in fact_sales
-- is more than 40 days in the past.
-- Pipeline runs on the 25th monthly; PPD is published ~20th working day.
-- Threshold: MAX(transfer_date) >= CURRENT_DATE - INTERVAL '40 days'.

SELECT 1 AS failure_flag
WHERE (
    SELECT MAX(transfer_date)
    FROM {{ ref('fact_sales') }}
) < CURRENT_DATE - INTERVAL '40 days'
