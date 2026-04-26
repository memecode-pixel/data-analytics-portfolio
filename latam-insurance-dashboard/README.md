# LATAM Insurance Market Dashboard

## Overview
ELT pipeline and interactive Tableau dashboard analyzing life and non-life insurance market indicators across 17 Latin American countries (2018–2022).

**Data Source:** OECD Global Insurance Statistics (DF_INSIND)  
**Countries:** Argentina, Bolivia, Brazil, Chile, Colombia, Costa Rica, Dominican Republic, Ecuador, El Salvador, Guatemala, Honduras, Mexico, Nicaragua, Panama, Paraguay, Peru, Uruguay  
**Dashboard:** [View on Tableau Public](#)

---

## Project Structure
- `data/raw/` — original OECD download, unmodified
- `data/clean/` — transformed dataset ready for Tableau
- `elt/` — Python ELT pipeline with validation and logging
- `docs/` — data dictionary, validation rules, assumptions
- `tableau/` — dashboard screenshots and public link

## How to Run the ELT
```bash
pip install pandas numpy
python elt/elt_oecd_insurance.py
```

## Key Metrics
- Insurance penetration rate % of GDP
- Premium density USD per capita
- Life vs Non-Life premium split
- YoY growth of life premiums
- Market development tier classification

## Tools
- Python — pandas, numpy
- Tableau Public
- GitHub
