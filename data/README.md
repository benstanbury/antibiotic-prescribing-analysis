## Data Layout

Raw inputs:

- `data/raw/` — source files (e.g., ODS prescriber `epraccur.csv`).

Processed outputs:

- `data/processed/monthly/` — per-month antibiotics extracts and per-practice totals.
- `data/processed/lookups/` — lookup tables (BNF, ICB, practice, demographics).
- `data/processed/full/` — full-sized outputs for multi-month analysis.
- `data/processed/teaching/` — latest-month extracts for the teaching notebook.

Teaching notebook (`intro_to_data_analysis.Rmd`) reads from
`data/processed/teaching/` using the latest-month files.
