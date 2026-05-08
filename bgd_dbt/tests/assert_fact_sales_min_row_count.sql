-- M5: Row count of fact_sales
-- Fails (returns rows) if fact_sales contains fewer than 29,000,000 records.
-- A drop below this threshold indicates a failed or incomplete ingestion run.
-- Threshold: COUNT(*) >= 29,000,000.

SELECT 1 AS failure_flag
WHERE (
    SELECT COUNT(*)
    FROM {{ ref('fact_sales') }}
) < 29000000
