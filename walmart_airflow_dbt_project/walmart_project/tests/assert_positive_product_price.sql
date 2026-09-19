SELECT
    product_id,
    price
FROM {{ ref('products_t') }}
WHERE TRY_CAST(REPLACE(price, '"', '') AS DOUBLE) <= 0
   OR TRY_CAST(REPLACE(price, '"', '') AS DOUBLE) IS NULL