-- Transform: Add risk metrics and categories
CREATE OR REPLACE TABLE direct_lending_transformed AS
SELECT 
    year,
    aum_billions,
    yoy_growth_pct,
    avg_deal_size_m,
    avg_interest_rate_pct,
    default_rate_pct,
    nr_of_deals,
    ROUND(avg_interest_rate_pct / default_rate_pct, 2) AS risk_adjusted_return,
    CASE 
        WHEN default_rate_pct < 2.0 THEN 'Low Risk'
        WHEN default_rate_pct BETWEEN 2.0 AND 3.0 THEN 'Medium Risk'
        ELSE 'High Risk'
    END AS risk_category,
    CASE
        WHEN yoy_growth_pct > 20 THEN 'High Growth'
        WHEN yoy_growth_pct BETWEEN 10 AND 20 THEN 'Moderate Growth'
        WHEN yoy_growth_pct < 10 THEN 'Slow Growth'
        ELSE 'Contraction'
    END AS growth_category
FROM direct_lending_market;

-- Data Quality Check
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN aum_billions IS NULL THEN 1 END) as null_aum,
    COUNT(CASE WHEN default_rate_pct IS NULL THEN 1 END) as null_default_rate,
    MIN(year) as min_year,
    MAX(year) as max_year,
    ROUND(AVG(avg_interest_rate_pct), 2) as avg_interest_rate,
    ROUND(AVG(default_rate_pct), 2) as avg_default_rate
FROM direct_lending_transformed;
