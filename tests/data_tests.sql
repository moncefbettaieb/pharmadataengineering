-- Ce test échoue s'il y a des lignes avec product_price < 0
SELECT
    cip_code,
    COUNT(*) as nb
FROM {{ ref('pharma_products') }}
GROUP BY 1
HAVING COUNT(*) > 1