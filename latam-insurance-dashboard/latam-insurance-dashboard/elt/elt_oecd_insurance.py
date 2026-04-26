"""
ELT Pipeline: OECD Latin America Insurance Indicators
======================================================
Source:     OECD Global Insurance Statistics (stats.oecd.org)
Dataset ID: DF_INSIND
Author:     Eme
Date:       2025

Description:
    Extracts raw OECD insurance data, filters for Latin American countries,
    validates data quality, transforms from long to wide format, and exports
    a clean dataset ready for Tableau dashboards.

Steps:
    1. EXTRACT  — Load raw CSV from OECD
    2. VALIDATE — Run data quality checks and log issues
    3. TRANSFORM — Clean, filter, rename, pivot
    4. LOAD      — Export clean CSV for Tableau
"""

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

RAW_PATH = "data/raw/OECD_DF_INSIND__all.csv"
OUTPUT_PATH = "data/clean/oecd_latam_insurance_clean.csv"
LOG_PATH = "logs/elt_log.txt"

# Latin American countries to keep
LATAM_COUNTRIES = [
    "Argentina", "Bolivia", "Brazil", "Chile", "Colombia",
    "Costa Rica", "Dominican Republic", "Ecuador", "El Salvador",
    "Guatemala", "Honduras", "Mexico", "Nicaragua", "Panama",
    "Paraguay", "Peru", "Uruguay"
]

# Indicators to keep and their clean names
INDICATOR_MAP = {
    "Penetration > Total":           "penetration_total_pct",
    "Penetration > Life":            "penetration_life_pct",
    "Penetration > Non-Life":        "penetration_nonlife_pct",
    "Density > Total":               "density_total_usd",
    "Density > Life":                "density_life_usd",
    "Density > Non-Life":            "density_nonlife_usd",
    "Total gross premiums > Total":  "gross_premiums_total_usd",
    "Total gross premiums > Life":   "gross_premiums_life_usd",
    "Total gross premiums > Non-Life": "gross_premiums_nonlife_usd",
    "Life insurance share":          "life_share_pct",
    "Retention ratio > Total":       "retention_ratio_total_pct",
    "Retention ratio > Life":        "retention_ratio_life_pct",
    "Retention ratio > Non-Life":    "retention_ratio_nonlife_pct",
}

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────

os.makedirs("logs", exist_ok=True)
os.makedirs("data/clean", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)
log.info("=" * 60)
log.info("ELT Pipeline started")
log.info("=" * 60)


# ─────────────────────────────────────────────
# STEP 1: EXTRACT
# ─────────────────────────────────────────────

def extract(path: str) -> pd.DataFrame:
    """Load raw OECD CSV file."""
    log.info(f"[EXTRACT] Reading raw file: {path}")
    df = pd.read_csv(path)
    log.info(f"[EXTRACT] Raw shape: {df.shape[0]} rows x {df.shape[1]} cols")
    log.info(f"[EXTRACT] Columns found: {df.columns.tolist()}")
    return df


# ─────────────────────────────────────────────
# STEP 2: VALIDATE
# ─────────────────────────────────────────────

def validate(df: pd.DataFrame) -> dict:
    """
    Run data quality checks on raw data.
    Returns a dict with validation results.

    Validation rules:
        - TIME_PERIOD must be numeric and in valid year range (2000–2030)
        - OBS_VALUE must be numeric and >= 0 (no negative rates/premiums)
        - Country and Indicator must not be null
        - Latam countries must be present in the dataset
    """
    log.info("[VALIDATE] Running data quality checks...")
    issues = {}

    # Rule 1: No nulls in key columns
    for col in ["Country", "Indicator", "TIME_PERIOD", "OBS_VALUE"]:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            issues[f"nulls_{col}"] = null_count
            log.warning(f"[VALIDATE] {null_count} null values found in '{col}'")
        else:
            log.info(f"[VALIDATE] ✓ No nulls in '{col}'")

    # Rule 2: Year range is valid
    invalid_years = df[~df["TIME_PERIOD"].between(2000, 2030)]
    if len(invalid_years) > 0:
        issues["invalid_years"] = len(invalid_years)
        log.warning(f"[VALIDATE] {len(invalid_years)} rows with invalid year values")
    else:
        log.info(f"[VALIDATE] ✓ All years within valid range")

    # Rule 3: No negative values (rates and premiums cannot be negative)
    negative_vals = df[df["OBS_VALUE"] < 0]
    if len(negative_vals) > 0:
        issues["negative_values"] = len(negative_vals)
        log.warning(f"[VALIDATE] {len(negative_vals)} rows with negative OBS_VALUE")
    else:
        log.info(f"[VALIDATE] ✓ No negative values in OBS_VALUE")

    # Rule 4: All expected Latam countries present
    countries_found = df["Country"].unique().tolist()
    missing_countries = [c for c in LATAM_COUNTRIES if c not in countries_found]
    if missing_countries:
        issues["missing_countries"] = missing_countries
        log.warning(f"[VALIDATE] Missing Latam countries: {missing_countries}")
    else:
        log.info(f"[VALIDATE] ✓ All Latam countries present")

    # Rule 5: Penetration % must be between 0 and 100
    pen_df = df[df["Indicator"].str.startswith("Penetration")]
    invalid_pen = pen_df[~pen_df["OBS_VALUE"].between(0, 100)]
    if len(invalid_pen) > 0:
        issues["invalid_penetration"] = len(invalid_pen)
        log.warning(f"[VALIDATE] {len(invalid_pen)} penetration values outside 0-100%")
    else:
        log.info(f"[VALIDATE] ✓ All penetration values within valid range")

    if not issues:
        log.info("[VALIDATE] ✓ All validation checks passed")
    else:
        log.warning(f"[VALIDATE] Issues found: {list(issues.keys())}")

    return issues


# ─────────────────────────────────────────────
# STEP 3: TRANSFORM
# ─────────────────────────────────────────────

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw OECD data.

    Transformations applied:
        1. Drop unnecessary metadata columns
        2. Rename key columns for clarity
        3. Fix Year column (use TIME_PERIOD)
        4. Filter for Latin American countries only
        5. Filter for relevant indicators only
        6. Rename indicators to clean snake_case names
        7. Pivot from long to wide format (one row per country-year)
        8. Add derived calculated fields:
           - life_nonlife_ratio
           - yoy_growth_premiums_life
        9. Sort by country and year
    """
    log.info("[TRANSFORM] Starting transformations...")

    # 1. Drop unnecessary metadata columns
    cols_to_drop = [
        "STRUCTURE", "STRUCTURE_ID", "STRUCTURE_NAME", "ACTION",
        "IND", "OBS_STATUS", "Observation Status", "Observation Value",
        "UNIT_MULT", "Multiplier", "BASE_PER", "Base reference period",
        "UNIT_MEASURE", "Unit of Measures", "Year"  # Year is all nulls
    ]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    log.info(f"[TRANSFORM] Dropped metadata columns. Remaining: {df.columns.tolist()}")

    # 2. Rename key columns
    df = df.rename(columns={
        "COU": "country_code",
        "Country": "country",
        "Indicator": "indicator",
        "TIME_PERIOD": "year",
        "OBS_VALUE": "value"
    })

    # 3. Filter Latin America only
    df_latam = df[df["country"].isin(LATAM_COUNTRIES)].copy()
    log.info(f"[TRANSFORM] Filtered to Latam: {len(df_latam)} rows ({df_latam['country'].nunique()} countries)")

    # 4. Filter relevant indicators only
    df_latam = df_latam[df_latam["indicator"].isin(INDICATOR_MAP.keys())].copy()
    log.info(f"[TRANSFORM] Filtered to {len(INDICATOR_MAP)} relevant indicators: {len(df_latam)} rows remaining")

    # 5. Rename indicators to clean names
    df_latam["indicator"] = df_latam["indicator"].map(INDICATOR_MAP)

    # 6. Remove duplicates (keep first occurrence)
    dupes = df_latam.duplicated(subset=["country", "year", "indicator"]).sum()
    if dupes > 0:
        log.warning(f"[TRANSFORM] Removing {dupes} duplicate rows")
        df_latam = df_latam.drop_duplicates(subset=["country", "year", "indicator"], keep="first")

    # 7. Pivot to wide format (one row per country-year)
    df_wide = df_latam.pivot_table(
        index=["country_code", "country", "year"],
        columns="indicator",
        values="value"
    ).reset_index()
    df_wide.columns.name = None  # Remove pivot artifact
    log.info(f"[TRANSFORM] Pivoted to wide format: {df_wide.shape[0]} rows x {df_wide.shape[1]} cols")

    # 8. Add derived calculated fields

    # Life vs Non-Life premium ratio
    if "gross_premiums_life_usd" in df_wide.columns and "gross_premiums_nonlife_usd" in df_wide.columns:
        df_wide["life_nonlife_ratio"] = (
            df_wide["gross_premiums_life_usd"] /
            df_wide["gross_premiums_nonlife_usd"].replace(0, np.nan)
        ).round(4)
        log.info("[TRANSFORM] ✓ Calculated: life_nonlife_ratio")

    # YoY growth of life premiums per country
    if "gross_premiums_life_usd" in df_wide.columns:
        df_wide = df_wide.sort_values(["country", "year"])
        df_wide["yoy_growth_life_pct"] = (
            df_wide.groupby("country")["gross_premiums_life_usd"]
            .pct_change()
            .round(4) * 100
        )
        log.info("[TRANSFORM] ✓ Calculated: yoy_growth_life_pct")

    # Market development tier (for Tableau segmentation)
    if "penetration_total_pct" in df_wide.columns:
        df_wide["market_tier"] = pd.cut(
            df_wide["penetration_total_pct"],
            bins=[0, 1.5, 3.0, 100],
            labels=["Underdeveloped (<1.5%)", "Developing (1.5-3%)", "Mature (>3%)"]
        )
        log.info("[TRANSFORM] ✓ Calculated: market_tier")

    # 9. Sort by country and year
    df_wide = df_wide.sort_values(["country", "year"]).reset_index(drop=True)

    log.info(f"[TRANSFORM] Final dataset: {df_wide.shape[0]} rows x {df_wide.shape[1]} cols")
    log.info(f"[TRANSFORM] Countries: {sorted(df_wide['country'].unique().tolist())}")
    log.info(f"[TRANSFORM] Year range: {df_wide['year'].min()} - {df_wide['year'].max()}")
    log.info(f"[TRANSFORM] Columns: {df_wide.columns.tolist()}")

    return df_wide


# ─────────────────────────────────────────────
# STEP 4: LOAD
# ─────────────────────────────────────────────

def load(df: pd.DataFrame, path: str) -> None:
    """Export clean dataset to CSV for Tableau."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    log.info(f"[LOAD] ✓ Clean dataset exported to: {path}")
    log.info(f"[LOAD] Final shape: {df.shape[0]} rows x {df.shape[1]} columns")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run_pipeline():
    start = datetime.now()

    # Extract
    raw_df = extract(RAW_PATH)

    # Validate
    issues = validate(raw_df)
    if issues:
        log.warning(f"[PIPELINE] Proceeding with {len(issues)} known data quality issues (see log for details)")

    # Transform
    clean_df = transform(raw_df)

    # Load
    load(clean_df, OUTPUT_PATH)

    duration = (datetime.now() - start).seconds
    log.info(f"[PIPELINE] ✓ Completed in {duration}s")
    log.info("=" * 60)

    return clean_df


if __name__ == "__main__":
    df = run_pipeline()
    print("\nPreview of clean dataset:")
    print(df.head(10).to_string())
