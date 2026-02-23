# ==============================================================================
# Download NHS Antibiotic Prescribing Data via NHSBSA API
# ==============================================================================
# Purpose: Download antibiotic prescribing data (BNF 0501) from the NHSBSA
#          Open Data Portal using server-side SQL filtering. This avoids
#          downloading the full ~7GB monthly files — antibiotics are only ~3%
#          of all prescriptions.
#
# Data source: English Prescribing Dataset (EPD) with SNOMED Code
#   https://opendata.nhsbsa.net/dataset/
#     english-prescribing-dataset-epd-with-snomed-code
#
# API: CKAN datastore_search_sql endpoint with SQL WHERE clause
#   Resource names: EPD_SNOMED_YYYYMM (predictable pattern)
#
# Strategy: Each month is saved to its own file in data/processed/monthly/.
#   If the download crashes, already-fetched months are safe on disk.
#   At the end, all monthly files are stacked into the combined output.
#
# Output: data/processed/monthly/antibiotics_epd_YYYYMM.csv  (one per month)
#         data/processed/full/antibiotics_items_all_months.csv
#         data/processed/full/antibiotics_practice_summary_all_months.csv
#         data/processed/teaching/antibiotics_practice_chemical_latest.csv
#         data/processed/teaching/antibiotics_practice_summary_latest.csv
# ==============================================================================

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(stringr)
  library(jsonlite)
})

# ---- Configuration ----

# Date range (YYYYMM integers). Override via env vars:
#   EPD_START_MONTH=202412 EPD_END_MONTH=202511 \
#     Rscript scripts/download_antibiotic_prescriptions.R
#
# Or specify explicit months (comma-separated):
#   EPD_MONTHS=202412,202501,202502 \
#     Rscript scripts/download_antibiotic_prescriptions.R
# In RStudio:
#   Sys.setenv(EPD_MONTHS = "202412,202501,202502")
#   source("scripts/download_antibiotic_prescriptions.R")
start_month <- as.integer(Sys.getenv("EPD_START_MONTH", "202412"))
end_month   <- as.integer(Sys.getenv("EPD_END_MONTH", "202511"))
explicit_months <- Sys.getenv("EPD_MONTHS", "")

# API settings
api_base <- paste0(
  "https://opendata.nhsbsa.net",
  "/api/3/action/datastore_search_sql"
)
page_size    <- 10000L
bnf_filter   <- "0501%"  # BNF section 5.1: Antibacterial Drugs

# Column schemas: NHSBSA changed column names at 202503
# Old (<=202502): BNF_CODE, BNF_CHEMICAL_SUBSTANCE, ADQUSAGE
# New (>=202503): BNF_PRESENTATION_CODE, BNF_CHEMICAL_SUBSTANCE_CODE, ADQ_USAGE
schema_cutoff <- 202503L

old_columns <- c(
  "YEAR_MONTH", "ICB_CODE", "PRACTICE_CODE", "PRACTICE_NAME",
  "BNF_CHEMICAL_SUBSTANCE", "BNF_CODE",
  "QUANTITY", "ITEMS", "ADQUSAGE", "NIC"
)
new_columns <- c(
  "YEAR_MONTH", "ICB_CODE", "PRACTICE_CODE", "PRACTICE_NAME",
  "BNF_CHEMICAL_SUBSTANCE_CODE", "BNF_PRESENTATION_CODE",
  "QUANTITY", "ITEMS", "ADQ_USAGE", "NIC"
)

# Column renaming to pipeline conventions (covers both schemas)
column_renames <- c(
  "BNF_PRESENTATION_CODE"       = "bnf_code",
  "BNF_CHEMICAL_SUBSTANCE_CODE" = "bnf_chemical_substance",
  "BNF_CODE"                    = "bnf_code",
  "BNF_CHEMICAL_SUBSTANCE"      = "bnf_chemical_substance",
  "ADQ_USAGE"                   = "adqusage"
)

# Canonical monthly column set (kept consistent across months)
monthly_columns <- c(
  "year_month",
  "icb_code",
  "practice_code",
  "practice_name",
  "bnf_chemical_substance",
  "bnf_code",
  "quantity",
  "items",
  "adqusage",
  "nic"
)

#' Get the correct column list and BNF filter column for a month
get_schema <- function(yyyymm) {
  if (yyyymm < schema_cutoff) {
    list(columns = old_columns, bnf_col = "BNF_CODE")
  } else {
    list(columns = new_columns, bnf_col = "BNF_PRESENTATION_CODE")
  }
}

# Output paths
out_dir               <- "data/processed"
full_dir              <- file.path(out_dir, "full")
teaching_dir          <- file.path(out_dir, "teaching")
monthly_dir           <- file.path(out_dir, "monthly")
antibiotics_file      <- file.path(full_dir, "antibiotics_items_all_months.csv")
practice_analysis_file <- file.path(full_dir, "antibiotics_practice_summary_all_months.csv")

force_reprocess <- Sys.getenv("FORCE_REPROCESS", "FALSE") == "TRUE"

dir.create(full_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(teaching_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(monthly_dir, showWarnings = FALSE, recursive = TRUE)

# ---- Helper functions ----

#' Generate sequence of YYYYMM integers between start and end
generate_month_sequence <- function(start_yyyymm, end_yyyymm) {
  start_date <- as.Date(paste0(
    substr(start_yyyymm, 1, 4), "-",
    substr(start_yyyymm, 5, 6), "-01"
  ))
  end_date <- as.Date(paste0(
    substr(end_yyyymm, 1, 4), "-",
    substr(end_yyyymm, 5, 6), "-01"
  ))
  dates <- seq.Date(start_date, end_date, by = "month")
  as.integer(format(dates, "%Y%m"))
}

#' Convert SNOMED YEAR_MONTH string ("2025-11") to integer (202511)
convert_year_month <- function(ym_string) {
  as.integer(gsub("-", "", ym_string))
}

#' Build the file path for a monthly cache file
monthly_file_path <- function(yyyymm) {
  file.path(monthly_dir, sprintf("antibiotics_epd_%d.csv", yyyymm))
}

#' Build the file path for a monthly totals cache file
totals_file_path <- function(yyyymm) {
  file.path(monthly_dir, sprintf("antibiotics_totals_%d.csv", yyyymm))
}

#' Rename SNOMED EPD columns to match existing pipeline conventions
rename_to_pipeline <- function(df) {
  for (old_name in names(column_renames)) {
    new_name <- column_renames[[old_name]]
    if (old_name %in% names(df)) {
      names(df)[names(df) == old_name] <- new_name
    }
  }
  names(df) <- tolower(names(df))
  df
}

#' Fetch one page of results from the NHSBSA API
fetch_page <- function(resource_name, columns, bnf_col,
                       bnf_filter, limit, offset) {
  cols_sql <- paste(columns, collapse = ", ")
  sql <- sprintf(
    paste0("SELECT %s FROM `%s`",
           " WHERE %s LIKE '%s'",
           " ORDER BY PRACTICE_CODE, %s",
           " LIMIT %d OFFSET %d"),
    cols_sql, resource_name, bnf_col, bnf_filter,
    bnf_col, limit, offset
  )
  url <- paste0(
    api_base, "?resource_id=", resource_name,
    "&sql=", utils::URLencode(sql, reserved = TRUE)
  )

  response <- tryCatch(
    fromJSON(url, flatten = TRUE),
    error = function(e) {
      warning(sprintf("API request failed: %s", e$message))
      NULL
    }
  )

  if (is.null(response) || !isTRUE(response$success)) {
    warning("API returned unsuccessful response")
    return(NULL)
  }

  inner <- response$result$result
  if (is.null(inner) || is.null(inner$records)) {
    return(NULL)
  }

  records <- inner$records
  if (is.data.frame(records) && nrow(records) > 0) {
    as_tibble(records)
  } else {
    NULL
  }
}

#' Build a lookup SQL query, using correct column names for the month's schema
#'
#' @param select_cols Named character vector: API column name -> output alias.
#'   Names prefixed with "old:" or "new:" are schema-specific; unprefixed are
#'   shared across both schemas.
#' @param resource_name API resource (e.g. "EPD_SNOMED_202511")
#' @param yyyymm Integer month, used to pick old vs new schema
#' @param where_clause Optional WHERE clause (without "WHERE" keyword)
#' @return SQL string ready for the API
build_lookup_query <- function(select_cols, resource_name, yyyymm,
                               where_clause = NULL) {
  schema <- get_schema(yyyymm)
  is_new <- yyyymm >= schema_cutoff

  # Resolve schema-specific column names
  resolved <- character(0)
  for (i in seq_along(select_cols)) {
    col <- names(select_cols)[i]
    alias <- select_cols[i]
    if (startsWith(col, "new:") && is_new) {
      resolved[sub("^new:", "", col)] <- alias
    } else if (startsWith(col, "old:") && !is_new) {
      resolved[sub("^old:", "", col)] <- alias
    } else if (!startsWith(col, "new:") && !startsWith(col, "old:")) {
      resolved[col] <- alias
    }
  }

  # Build SELECT clause with aliases
  parts <- vapply(names(resolved), function(col) {
    alias <- resolved[col]
    if (col == alias) col else sprintf("%s AS %s", col, alias)
  }, character(1))
  select_sql <- paste(parts, collapse = ", ")

  where_sql <- if (!is.null(where_clause)) {
    sprintf(" WHERE %s", where_clause)
  } else {
    ""
  }

  sprintf("SELECT DISTINCT %s FROM `%s`%s LIMIT 50000",
          select_sql, resource_name, where_sql)
}

#' Fetch a lookup table from the API (single query, no pagination)
#'
#' @param sql SQL query string
#' @param resource_name API resource name
#' @return tibble of results, or NULL on failure
fetch_lookup <- function(sql, resource_name) {
  url <- paste0(
    api_base, "?resource_id=", resource_name,
    "&sql=", utils::URLencode(sql, reserved = TRUE)
  )

  response <- tryCatch(
    fromJSON(url, flatten = TRUE),
    error = function(e) {
      warning(sprintf("Lookup API request failed: %s", e$message))
      NULL
    }
  )

  if (is.null(response) || !isTRUE(response$success)) {
    warning("Lookup API returned unsuccessful response")
    return(NULL)
  }

  inner <- response$result$result
  if (is.null(inner) || is.null(inner$records)) {
    return(NULL)
  }

  records <- inner$records
  if (is.data.frame(records) && nrow(records) > 0) {
    as_tibble(records)
  } else {
    NULL
  }
}

# ICB code -> NHS region mapping (42 ICBs as of 2025)
icb_region_map <- c(
  "QE1"  = "North West",
  "QF7"  = "North East and Yorkshire",
  "QGH"  = "Midlands",
  "QH8"  = "East of England",
  "QHG"  = "East of England",
  "QHL"  = "Midlands",
  "QHM"  = "North East and Yorkshire",
  "QJ2"  = "Midlands",
  "QJG"  = "East of England",
  "QJK"  = "South West",
  "QJM"  = "Midlands",
  "QK1"  = "Midlands",
  "QKK"  = "London",
  "QKS"  = "South East",
  "QM7"  = "East of England",
  "QMF"  = "London",
  "QMJ"  = "London",
  "QMM"  = "East of England",
  "QNC"  = "Midlands",
  "QNQ"  = "South East",
  "QNX"  = "South East",
  "QOC"  = "Midlands",
  "QOP"  = "North West",
  "QOQ"  = "North East and Yorkshire",
  "QOX"  = "South West",
  "QPM"  = "Midlands",
  "QR1"  = "South West",
  "QRL"  = "South East",
  "QRV"  = "London",
  "QSL"  = "South West",
  "QT1"  = "Midlands",
  "QT6"  = "South West",
  "QU9"  = "South East",
  "QUA"  = "Midlands",
  "QUE"  = "East of England",
  "QUY"  = "South West",
  "QVV"  = "South West",
  "QWE"  = "London",
  "QWO"  = "North East and Yorkshire",
  "QWU"  = "Midlands",
  "QXU"  = "South East",
  "QYG"  = "North West"
)

#' Download all antibiotic rows for one month, paginating through the API
download_month <- function(yyyymm) {
  resource_name <- sprintf("EPD_SNOMED_%d", yyyymm)
  schema <- get_schema(yyyymm)
  month_start <- Sys.time()
  cat(sprintf("  Downloading %d from %s [started %s]\n",
              yyyymm, resource_name, format(month_start, "%H:%M:%S")))

  all_pages <- list()
  offset <- 0L
  page_num <- 1L

  repeat {
    page <- fetch_page(
      resource_name, schema$columns, schema$bnf_col,
      bnf_filter, page_size, offset
    )

    if (is.null(page) || nrow(page) == 0) {
      break
    }

    all_pages[[page_num]] <- page
    n_rows <- nrow(page)
    total_so_far <- sum(vapply(all_pages, nrow, integer(1)))
    cat(sprintf("    Page %d: %s rows (total: %s)\n",
                page_num, format(n_rows, big.mark = ","),
                format(total_so_far, big.mark = ",")))

    if (n_rows < page_size) {
      break
    }

    offset <- offset + page_size
    page_num <- page_num + 1L
  }

  if (length(all_pages) == 0) {
    warning(sprintf("No data returned for %d", yyyymm))
    return(NULL)
  }

  month_df <- bind_rows(all_pages)
  elapsed <- as.numeric(difftime(Sys.time(), month_start, units = "secs"))
  cat(sprintf("    Done: %s rows in %.0f seconds\n",
              format(nrow(month_df), big.mark = ","), elapsed))
  month_df
}

#' Transform raw API data: rename columns, convert types
transform_month <- function(df) {
  df <- rename_to_pipeline(df)
  # YEAR_MONTH is INTEGER in old schema, STRING in new
  if (is.character(df$year_month)) {
    df$year_month <- convert_year_month(df$year_month)
  }
  df |>
    mutate(
      year_month = as.integer(year_month),
      across(c(quantity, items, adqusage, nic), as.numeric)
    )
}

#' Standardize to a canonical monthly column set
standardize_month <- function(df) {
  missing_cols <- setdiff(monthly_columns, names(df))
  if (length(missing_cols) > 0) {
    for (col in missing_cols) {
      df[[col]] <- NA
    }
  }
  df |>
    select(all_of(monthly_columns))
}

#' Download total items per practice for one month (server-side aggregation)
#' Returns ~8,000 rows (one per practice), used for abx_rate_per_1000
download_total_items <- function(yyyymm) {
  resource_name <- sprintf("EPD_SNOMED_%d", yyyymm)
  cat(sprintf("  Totals for %d...", yyyymm))

  sql <- sprintf(
    "SELECT PRACTICE_CODE, SUM(ITEMS) as TOTAL_ITEMS FROM `%s` GROUP BY PRACTICE_CODE LIMIT 50000",
    resource_name
  )
  url <- paste0(
    api_base, "?resource_id=", resource_name,
    "&sql=", utils::URLencode(sql, reserved = TRUE)
  )

  response <- tryCatch(
    fromJSON(url, flatten = TRUE),
    error = function(e) {
      warning(sprintf("Totals API request failed: %s", e$message))
      NULL
    }
  )

  if (is.null(response) || !isTRUE(response$success)) {
    cat(" FAILED\n")
    return(NULL)
  }

  inner <- response$result$result
  if (is.null(inner) || is.null(inner$records)) {
    cat(" no records\n")
    return(NULL)
  }

  records <- as_tibble(inner$records)
  names(records) <- tolower(names(records))
  records$total_items <- as.numeric(records$total_items)
  records$year_month <- as.integer(yyyymm)
  cat(sprintf(" %s practices\n",
              format(nrow(records), big.mark = ",")))
  records
}

# ---- Determine which months to download ----

if (nzchar(explicit_months)) {
  target_months <- as.integer(trimws(strsplit(explicit_months, ",")[[1]]))
} else {
  target_months <- generate_month_sequence(start_month, end_month)
}
cat(sprintf("Target months: %s\n", paste(target_months, collapse = ", ")))
cat(sprintf("Date range: %d to %d (%d months)\n",
            start_month, end_month, length(target_months)))

# ---- Migrate existing combined file into monthly files (one-time) ----

if (!force_reprocess && file.exists(antibiotics_file)) {
  cat("Checking for monthly files to migrate from existing data...\n")
  existing_data <- read_csv(antibiotics_file, show_col_types = FALSE)
  months_in_combined <- sort(unique(existing_data$year_month))

  for (m in months_in_combined) {
    mf <- monthly_file_path(m)
    if (!file.exists(mf)) {
      month_slice <- existing_data |> filter(year_month == m)
      write_csv(month_slice, mf)
      cat(sprintf("  Migrated %d: %s rows -> %s\n",
                  m, format(nrow(month_slice), big.mark = ","), mf))
    }
  }
}

# Check which monthly files already exist on disk
cached_months <- integer(0)
existing_files <- list.files(monthly_dir, pattern = "^antibiotics_epd_\\d{6}\\.csv$")
if (length(existing_files) > 0) {
  cached_months <- as.integer(gsub("antibiotics_epd_(\\d{6})\\.csv", "\\1", existing_files))
  cached_months <- sort(cached_months)
  cat(sprintf("Cached monthly files: %s\n",
              paste(cached_months, collapse = ", ")))
}

months_to_download <- if (force_reprocess) {
  target_months
} else {
  setdiff(target_months, cached_months)
}

if (length(months_to_download) == 0) {
  cat("\nAll target months already cached. Skipping download.\n")
  cat("Set FORCE_REPROCESS=TRUE to re-download.\n")
} else {
  cat(sprintf("\nMonths to download: %s\n",
              paste(months_to_download, collapse = ", ")))

  # ---- Download each month ----
  cat("\n=================================================\n")
  cat("DOWNLOADING ANTIBIOTIC PRESCRIBING DATA\n")
  cat(sprintf("Started: %s\n", format(Sys.time(), "%H:%M:%S")))
  cat("=================================================\n\n")

  download_start <- Sys.time()

  for (i in seq_along(months_to_download)) {
    m <- months_to_download[i]
    cat(sprintf("[%d/%d] ", i, length(months_to_download)))
    raw_data <- download_month(m)

    if (!is.null(raw_data)) {
      # Transform and save immediately
      month_df <- transform_month(raw_data) |> standardize_month()
      mf <- monthly_file_path(m)
      write_csv(month_df, mf)
      file_mb <- file.info(mf)$size / 1024^2
      cat(sprintf("    Saved: %s (%.1f MB)\n", mf, file_mb))
    }
  }

  download_elapsed <- as.numeric(difftime(Sys.time(), download_start, units = "mins"))
  cat(sprintf("\nAll downloads finished in %.1f minutes\n", download_elapsed))
}

# ---- Download total items per practice (for rate calculation) ----

cat("\n=================================================\n")
cat("DOWNLOADING TOTAL ITEMS PER PRACTICE\n")
cat("=================================================\n\n")

# Check which totals files already exist
totals_to_download <- integer(0)
for (m in target_months) {
  tf <- totals_file_path(m)
  if (force_reprocess || !file.exists(tf)) {
    totals_to_download <- c(totals_to_download, m)
  }
}

if (length(totals_to_download) == 0) {
  cat("All totals already cached.\n")
} else {
  cat(sprintf("Downloading totals for %d months...\n",
              length(totals_to_download)))
  for (m in totals_to_download) {
    result <- download_total_items(m)
    if (!is.null(result)) {
      write_csv(result, totals_file_path(m))
    }
  }
}

# ---- Stack all monthly files into combined output ----

cat("\n=================================================\n")
cat("COMBINING MONTHLY FILES\n")
cat("=================================================\n\n")

# Read all monthly files that fall within the target range
monthly_files <- vapply(target_months, monthly_file_path, character(1))
found <- file.exists(monthly_files)

if (sum(found) == 0) {
  stop("No monthly files found. Check download results.")
}

if (sum(!found) > 0) {
  missing <- target_months[!found]
  cat(sprintf("WARNING: Missing monthly files for: %s\n",
              paste(missing, collapse = ", ")))
}

cat(sprintf("Stacking %d monthly files...\n", sum(found)))
antibiotics_df <- lapply(monthly_files[found], function(f) {
  read_csv(f, show_col_types = FALSE) |> standardize_month()
}) |>
  bind_rows() |>
  distinct() |>
  arrange(year_month, practice_code, bnf_code)

cat(sprintf("Total: %s rows across %d months\n",
            format(nrow(antibiotics_df), big.mark = ","),
            n_distinct(antibiotics_df$year_month)))

write_csv(antibiotics_df, antibiotics_file)
file_size_mb <- file.info(antibiotics_file)$size / 1024^2
cat(sprintf("Saved: %s (%.1f MB)\n", antibiotics_file, file_size_mb))

# ---- Build practice + chemical + month aggregation (shareable) ----
cat("\nAggregating practice + chemical + month...\n")
practice_chemical_file <- file.path(full_dir, "antibiotics_practice_chemical_all_months.csv")
practice_chemical <- antibiotics_df |>
  group_by(year_month, practice_code, bnf_chemical_substance) |>
  summarise(
    items = sum(items, na.rm = TRUE),
    quantity = sum(quantity, na.rm = TRUE),
    adqusage = sum(adqusage, na.rm = TRUE),
    nic = sum(nic, na.rm = TRUE),
    .groups = "drop"
  )
write_csv(practice_chemical, practice_chemical_file)
pc_size_mb <- file.info(practice_chemical_file)$size / 1024^2
cat(sprintf("Saved: %s (%.1f MB)\n", practice_chemical_file, pc_size_mb))

# Latest-month extract (smaller, shareable)
latest_month <- max(practice_chemical$year_month, na.rm = TRUE)
practice_chemical_latest <- practice_chemical |>
  filter(year_month == latest_month)
practice_chemical_latest_file <- file.path(teaching_dir, "antibiotics_practice_chemical_latest.csv")
write_csv(practice_chemical_latest, practice_chemical_latest_file)
pc_latest_size_mb <- file.info(practice_chemical_latest_file)$size / 1024^2
cat(sprintf("Saved: %s (%.1f MB, month %d)\n",
            practice_chemical_latest_file, pc_latest_size_mb, latest_month))

# ---- Build practice-level aggregation ----
cat("\nAggregating practice-level statistics...\n")

# Load totals for all target months
totals_files <- vapply(target_months, totals_file_path, character(1))
totals_found <- file.exists(totals_files)
if (sum(totals_found) > 0) {
  totals_df <- lapply(totals_files[totals_found], function(f) {
    read_csv(f, show_col_types = FALSE)
  }) |> bind_rows()
  cat(sprintf("Loaded totals: %s practice x months\n",
              format(nrow(totals_df), big.mark = ",")))
} else {
  warning("No totals files found — rates will be unavailable")
  totals_df <- NULL
}

grouping_cols <- intersect(
  c("practice_code", "icb_code", "year_month", "practice_name"),
  names(antibiotics_df)
)

practice_analysis <- antibiotics_df |>
  group_by(across(all_of(grouping_cols))) |>
  summarise(
    antibiotic_items = sum(items, na.rm = TRUE),
    antibiotic_quantity = sum(quantity, na.rm = TRUE),
    antibiotic_nic = sum(nic, na.rm = TRUE),
    .groups = "drop"
  )

# Join total items and compute rates
if (!is.null(totals_df)) {
  practice_analysis <- practice_analysis |>
    left_join(
      totals_df |> select(practice_code, year_month, total_items),
      by = c("practice_code", "year_month")
    ) |>
    rename(total_all_items = total_items)
} else {
  practice_analysis$total_all_items <- practice_analysis$antibiotic_items
}

practice_analysis <- practice_analysis |>
  mutate(
    total_all_items = coalesce(total_all_items, antibiotic_items),
    abx_rate_per_1000 = (antibiotic_items / pmax(total_all_items, 1)) * 1000,
    cost_per_item = antibiotic_nic / pmax(antibiotic_items, 1),
    cost_per_quantity = antibiotic_nic / pmax(antibiotic_quantity, 1)
  )

write_csv(practice_analysis, practice_analysis_file)
cat(sprintf("Saved: %s (%s practice x months)\n",
            practice_analysis_file,
            format(nrow(practice_analysis), big.mark = ",")))

# Latest-month extract (smaller, shareable)
practice_analysis_latest <- practice_analysis |>
  filter(year_month == latest_month)
practice_analysis_latest_file <- file.path(teaching_dir, "antibiotics_practice_summary_latest.csv")
write_csv(practice_analysis_latest, practice_analysis_latest_file)
pa_latest_size_mb <- file.info(practice_analysis_latest_file)$size / 1024^2
cat(sprintf("Saved: %s (%.1f MB, month %d)\n",
            practice_analysis_latest_file, pa_latest_size_mb, latest_month))

cat("\n=================================================\n")
cat("COMPLETE\n")
cat("=================================================\n")
cat(sprintf("Months: %d\n", n_distinct(antibiotics_df$year_month)))
cat(sprintf("Total rows: %s\n", format(nrow(antibiotics_df), big.mark = ",")))
cat(sprintf("File size: %.1f MB\n", file_size_mb))
cat("=================================================\n")

# ---- Generate lookup tables ----

cat("\n=================================================\n")
cat("GENERATING LOOKUP TABLES\n")
cat("=================================================\n\n")

lookup_dir <- file.path(out_dir, "lookups")
dir.create(lookup_dir, showWarnings = FALSE, recursive = TRUE)

latest_month <- max(target_months)
resource <- sprintf("EPD_SNOMED_%d", latest_month)
schema <- get_schema(latest_month)
bnf_where <- sprintf("%s LIKE '0501%%'", schema$bnf_col)

# a) Practice lookup
cat("Practice lookup... ")
practice_sql <- build_lookup_query(
  c("PRACTICE_CODE" = "PRACTICE_CODE", "PRACTICE_NAME" = "PRACTICE_NAME"),
  resource, latest_month, bnf_where
)
practice_lkp <- fetch_lookup(practice_sql, resource)
if (!is.null(practice_lkp)) {
  names(practice_lkp) <- c("practice_code", "practice_name")
  practice_lkp <- practice_lkp |> arrange(practice_code)
  write_csv(practice_lkp, file.path(lookup_dir, "practice_lookup.csv"))
  cat(sprintf("%d practices\n", nrow(practice_lkp)))
} else {
  cat("FAILED\n")
}

# b) ICB lookup
cat("ICB lookup... ")
icb_sql <- build_lookup_query(
  c("ICB_CODE" = "ICB_CODE", "ICB_NAME" = "ICB_NAME"),
  resource, latest_month
)
icb_lkp <- fetch_lookup(icb_sql, resource)
if (!is.null(icb_lkp)) {
  names(icb_lkp) <- c("icb_code", "icb_name")
  icb_lkp <- icb_lkp |>
    mutate(icb_region = unname(icb_region_map[icb_code])) |>
    mutate(icb_region = coalesce(icb_region, "UNIDENTIFIED")) |>
    arrange(icb_code)
  write_csv(icb_lkp, file.path(lookup_dir, "icb_lookup.csv"))
  cat(sprintf("%d ICBs\n", nrow(icb_lkp)))
} else {
  cat("FAILED\n")
}

# c) BNF chemical substance lookup
cat("BNF chemical substance lookup... ")
chem_sql <- build_lookup_query(
  c("new:BNF_CHEMICAL_SUBSTANCE_CODE"  = "BNF_CHEMICAL_SUBSTANCE_CODE",
    "old:BNF_CHEMICAL_SUBSTANCE"       = "BNF_CHEMICAL_SUBSTANCE_CODE",
    "new:BNF_CHEMICAL_SUBSTANCE"       = "BNF_CHEMICAL_SUBSTANCE",
    "old:CHEMICAL_SUBSTANCE_BNF_DESCR" = "BNF_CHEMICAL_SUBSTANCE"),
  resource, latest_month, bnf_where
)
chem_lkp <- fetch_lookup(chem_sql, resource)
if (!is.null(chem_lkp)) {
  names(chem_lkp) <- c("bnf_chemical_substance", "bnf_chemical_substance_name")
  chem_lkp <- chem_lkp |> arrange(bnf_chemical_substance)
chem_file <- file.path(lookup_dir, "bnf_chemical_substance_lookup.csv")
  write_csv(chem_lkp, chem_file)
  cat(sprintf("%d substances\n", nrow(chem_lkp)))
} else {
  cat("FAILED\n")
}

# d) BNF code (presentation) lookup
cat("BNF presentation lookup... ")
pres_sql <- build_lookup_query(
  c("new:BNF_PRESENTATION_CODE" = "BNF_PRESENTATION_CODE",
    "old:BNF_CODE"              = "BNF_PRESENTATION_CODE",
    "new:BNF_PRESENTATION_NAME" = "BNF_PRESENTATION_NAME",
    "old:BNF_DESCRIPTION"       = "BNF_PRESENTATION_NAME"),
  resource, latest_month, bnf_where
)
pres_lkp <- fetch_lookup(pres_sql, resource)
if (!is.null(pres_lkp)) {
  names(pres_lkp) <- c("bnf_code", "bnf_description")
  pres_lkp <- pres_lkp |> arrange(bnf_code)
  write_csv(pres_lkp, file.path(lookup_dir, "bnf_code_lookup.csv"))
  cat(sprintf("%d presentations\n", nrow(pres_lkp)))
} else {
  cat("FAILED\n")
}

cat("\nLookup tables saved to: %s\n" |> sprintf(lookup_dir))
