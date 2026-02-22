# NHS Antibiotic Prescribing — Teaching Materials

R Markdown (default):  
[https://benstanbury.github.io/antibiotic-prescribing-analysis/](https://benstanbury.github.io/antibiotic-prescribing-analysis/)

Quarto version:  
[https://benstanbury.github.io/antibiotic-prescribing-analysis/intro_to_data_analysis_qmd.html](https://benstanbury.github.io/antibiotic-prescribing-analysis/intro_to_data_analysis_qmd.html)

Session for the University of Bristol School of Mathematics on 24th Feb 2026.

Presenter: Ben Stanbury, Head of Data Science, UKHSA.

## Contents

- `intro_to_data_analysis.Rmd`
- `intro_to_data_analysis.qmd`
- `data/` (required inputs)
- `scripts/` (download scripts used to build the data)

Data layout (brief):

- `data/raw/` — source inputs (not modified)
- `data/processed/` — derived outputs
  - `full/` — multi‑month datasets
  - `teaching/` — latest‑month extracts used in the teaching Rmd

## Requirements

- R (4.x recommended)
- RStudio (for R Markdown) and/or Quarto CLI (for Quarto)
- R packages: see `DESCRIPTION` (used for the minimal `renv` lockfile)

Install packages once with:

```r
install.packages(c("tidyverse", "broom", "lubridate", "knitr", "scales", "rmarkdown", "jsonlite"))
```

## Reproducibility (renv)

This project includes `renv.lock` for reproducible package versions.

To restore the exact environment:

```r
install.packages("renv")
renv::restore()
```

If `renv` is not activated automatically, open the project in RStudio or run:

```r
source("renv/activate.R")
```

## Quick Start

### R Markdown

Open `intro_to_data_analysis.Rmd` in RStudio and Knit, or run in R:

```r
rmarkdown::render("intro_to_data_analysis.Rmd")
```

### Quarto

Render the Quarto version in R:

```r
quarto::quarto_render("intro_to_data_analysis.qmd")
```


## One-month vs Full Mode

By default, the workbook runs in one-month mode to keep runtime and file size small. The one‑month teaching data is included in the repo, so it runs out of the box. Before running the full 12‑month mode, download the data locally by running `scripts/download_antibiotic_prescriptions.R` and `scripts/download_demographics.R`. To run the full 12-month version, change the YAML params at the top of the document:

```yaml
params:
  use_one_month: false
  sample_month: 202511
```

To change which month is used in one-month mode, set `sample_month` to another `YYYYMM`.

## Data Requirements

The main notebooks read from the following files.

Included in the repo (teaching, latest month only):
- `data/processed/teaching/antibiotics_practice_chemical_latest.csv` — practice × chemical for **one month** (latest available in the repo, e.g. 202511).
- `data/processed/teaching/antibiotics_practice_summary_latest.csv` — practice‑level summary for the **same month** (latest available).
- `data/processed/lookups/bnf_chemical_substance_lookup.csv` — BNF chemical code to name lookup.
- `data/processed/lookups/practice_demographics.csv` — list size and demographics (IMD, age profile).
- `data/raw/epraccur.csv` — ODS prescriber file used to classify provider type (GP vs walk‑in/urgent care).

Generated locally if you run full downloads (all months in the selected range):
- `data/processed/full/antibiotics_items_all_months.csv` — stacked antibiotics‑only rows across **all months**.
- `data/processed/full/antibiotics_practice_chemical_all_months.csv` — practice + chemical + **month** across all months.
- `data/processed/full/antibiotics_practice_summary_all_months.csv` — practice‑level monthly summary across all months.
- `data/processed/monthly/antibiotics_epd_YYYYMM.csv` — one file **per month** (YYYYMM, e.g. 202511 = Nov 2025).
- `data/processed/monthly/antibiotics_totals_YYYYMM.csv` — per‑practice totals for the **same month**.

The `scripts/` folder contains the download scripts used to create the full datasets.

Example monthly files created by the download script:

- `data/processed/monthly/antibiotics_epd_202412.csv` — antibiotic rows for Dec 2024 (BNF 5.1 only).
- `data/processed/monthly/antibiotics_totals_202412.csv` — total prescription items per practice for Dec 2024 (denominator for rates).

## Downloading Data

The main script is `scripts/download_antibiotic_prescriptions.R`.

Run these in a terminal (command line):

```bash
Rscript scripts/download_antibiotic_prescriptions.R
```

Demographics (command line):

```bash
Rscript scripts/download_demographics.R
```

Select a month range:

```bash
EPD_START_MONTH=202412 EPD_END_MONTH=202511 Rscript scripts/download_antibiotic_prescriptions.R
```

Select specific months (comma-separated):

```bash
EPD_MONTHS=202412,202501,202502 Rscript scripts/download_antibiotic_prescriptions.R
```

If you prefer RStudio, set the same variables in the Console and then run:

```r
Sys.setenv(EPD_START_MONTH = "202412", EPD_END_MONTH = "202511")
source("scripts/download_antibiotic_prescriptions.R")
```

Demographics can be downloaded in the same way:

```r
source("scripts/download_demographics.R")
```
