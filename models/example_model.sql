-- models/example_model.sql

WITH source_data AS (
    SELECT
        id,
        customer_id,
        order_date,
        status,
        amount
    FROM {{ source('jaffle_shop', 'orders') }}
)

SELECT
    id AS order_id,
    customer_id,
    order_date,
    status,
    amount,
    CASE
        WHEN amount > 100 THEN 'high_value'
        WHEN amount > 50 THEN 'medium_value'
        ELSE 'low_value'
    END AS order_value_category
FROM source_data
WHERE status = 'completed'