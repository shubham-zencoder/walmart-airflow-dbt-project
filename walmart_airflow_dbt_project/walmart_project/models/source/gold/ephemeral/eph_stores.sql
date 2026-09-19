SELECT
    DISTINCT
    store_id,
    store_name,
    store_city,
    store_country,
    store_province,
    store_created_timestamp,
    store_updated_timestamp,
    store_is_active,
    store_processed_at,
    CURRENT_TIMESTAMP() AS order_gold_processed_at
FROM
    {{ ref('obt_b') }}