# Data Validation Rules

## Rules Applied in ELT Pipeline

| Rule | Column | Condition | Action |
|------|--------|-----------|--------|
| No nulls | Country, Indicator, TIME_PERIOD, OBS_VALUE | Must not be null | Log warning |
| Valid year range | TIME_PERIOD | Must be between 2000–2030 | Log warning |
| No negative values | OBS_VALUE | Must be >= 0 | Log warning |
| Valid penetration | Penetration indicators | Must be between 0–100% | Log warning |
| All Latam countries present | Country | 17 expected countries | Log warning |

---

## Issues Found in Raw Data

| Issue | Detail | Resolution |
|-------|--------|------------|
| `Year` column all null | OECD exports year in `TIME_PERIOD`, not `Year` | Used `TIME_PERIOD` as year source |
| `Observation Value` all null | Duplicate of `OBS_VALUE`, always empty in this export | Dropped column |
| 1 negative OBS_VALUE | One row with negative value in raw data | Logged, kept in dataset for transparency |
| Many metadata columns | STRUCTURE, ACTION, IND, etc. not needed for analysis | Dropped in transform step |

---

## Assumptions

See `assumptions.md` for full list of analytical assumptions.
