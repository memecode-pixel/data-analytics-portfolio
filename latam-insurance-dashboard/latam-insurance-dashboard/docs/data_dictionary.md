# Data Dictionary

## Source
OECD Global Insurance Statistics (DF_INSIND)  
Downloaded from: stats.oecd.org  
License: OECD Terms and Conditions (free for non-commercial use with attribution)

---

## Clean Dataset: `oecd_latam_insurance_clean.csv`

| Column | Type | Description | Unit |
|--------|------|-------------|------|
| country_code | string | ISO 3-letter country code | — |
| country | string | Country name | — |
| year | integer | Reference year | — |
| penetration_total_pct | float | Total insurance premiums as % of GDP | % |
| penetration_life_pct | float | Life insurance premiums as % of GDP | % |
| penetration_nonlife_pct | float | Non-life insurance premiums as % of GDP | % |
| density_total_usd | float | Total premiums per capita | USD |
| density_life_usd | float | Life premiums per capita | USD |
| density_nonlife_usd | float | Non-life premiums per capita | USD |
| gross_premiums_total_usd | float | Total gross written premiums | USD millions |
| gross_premiums_life_usd | float | Life gross written premiums | USD millions |
| gross_premiums_nonlife_usd | float | Non-life gross written premiums | USD millions |
| life_share_pct | float | Life premiums as % of total premiums | % |
| retention_ratio_total_pct | float | % of premiums retained (not reinsured) | % |
| retention_ratio_life_pct | float | Life retention ratio | % |
| retention_ratio_nonlife_pct | float | Non-life retention ratio | % |
| life_nonlife_ratio | float | Ratio of life to non-life premiums | ratio |
| yoy_growth_life_pct | float | Year-over-year growth of life premiums | % |
| market_tier | string | Market development classification | category |

---

## Calculated Fields (added in ELT)

**life_nonlife_ratio**  
`gross_premiums_life_usd / gross_premiums_nonlife_usd`  
Measures relative weight of life vs non-life segment per country.

**yoy_growth_life_pct**  
`pct_change(gross_premiums_life_usd)` grouped by country  
Year-over-year growth rate of life insurance premiums.

**market_tier**  
Segmentation based on penetration_total_pct:  
- Underdeveloped: < 1.5%  
- Developing: 1.5% – 3.0%  
- Mature: > 3.0%
