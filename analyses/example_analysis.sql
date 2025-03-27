-- Exemple d'analyse
SELECT
    cip_code,
    AVG(product_price) as avg_price
FROM {{ ref('pharma_products') }}
GROUP BY cip_code
ORDER BY avg_price DESC