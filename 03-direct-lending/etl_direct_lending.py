# ── EXTRACT & LOAD — Direct Lending Market Data ──────────────────────
import pandas as pd

# Market Growth Data
market_data = [
    (2007, 170, None, 45, 8.2, 1.8, 890),
    (2008, 155, -8.8, 42, 9.1, 3.2, 720),
    (2009, 148, -4.5, 38, 9.8, 4.1, 650),
    (2010, 178, 20.3, 44, 8.9, 2.8, 820),
    (2011, 220, 23.6, 50, 8.5, 2.1, 980),
    (2012, 275, 25.0, 55, 7.9, 1.9, 1150),
    (2013, 340, 23.6, 60, 7.5, 1.6, 1380),
    (2014, 420, 23.5, 63, 7.2, 1.4, 1620),
    (2015, 500, 19.0, 65, 7.0, 1.5, 1850),
    (2016, 580, 16.0, 66, 7.1, 1.7, 2050),
    (2017, 670, 15.5, 68, 7.3, 1.5, 2280),
    (2018, 770, 14.9, 70, 8.1, 1.6, 2510),
    (2019, 870, 13.0, 72, 7.8, 1.8, 2720),
    (2020, 900, 3.4, 68, 7.2, 2.9, 2450),
    (2021, 1050, 16.7, 75, 6.8, 1.4, 3100),
    (2022, 1300, 23.8, 82, 9.5, 1.8, 3580),
    (2023, 1500, 15.4, 85, 11.8, 2.1, 3820),
    (2024, 1700, 13.3, 88, 11.2, 2.3, 4100),
]

df_market = pd.DataFrame(market_data, columns=[
    "year", "aum_billions", "yoy_growth_pct",
    "avg_deal_size_m", "avg_interest_rate_pct",
    "default_rate_pct", "nr_of_deals"
])

# Create Delta Table
spark.createDataFrame(df_market).write.format("delta").mode("overwrite").saveAsTable("direct_lending_market")
print(" Table created: direct_lending_market")
print(df_market.head())

%sql

-- Transform: Add risk-adjusted return metric
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

SELECT * FROM direct_lending_transformed LIMIT 5;

%sql

-- Data Quality Checks
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN aum_billions IS NULL THEN 1 END) as null_aum,
    COUNT(CASE WHEN default_rate_pct IS NULL THEN 1 END) as null_default_rate,
    COUNT(CASE WHEN yoy_growth_pct IS NULL THEN 1 END) as null_growth,
    MIN(year) as min_year,
    MAX(year) as max_year,
    ROUND(AVG(avg_interest_rate_pct), 2) as avg_interest_rate,
    ROUND(AVG(default_rate_pct), 2) as avg_default_rate
FROM direct_lending_transformed;