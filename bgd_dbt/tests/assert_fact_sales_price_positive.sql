-- M3: Validity of sale price
-- Fails (returns rows) if any record in fact_sales has a price <= 0.
-- Every property transaction must have a positive sale price.
-- Threshold: 0 rows with price <= 0 (i.e. 100% validity).

SELECT
    transaction_id,
    price
FROM {{ ref('fact_sales') }}
WHERE price IS NULL
   OR price <= 0
