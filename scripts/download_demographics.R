# ==============================================================================
# Download Practice-Level Demographics
# ==============================================================================
# Purpose: Download and prepare practice-level demographic data for the
#          prescribing analysis model. Combines two NHS data sources:
#
#          1. IMD (deprivation) scores from Fingertips API
#          2. Age profile from NHS Digital "Patients Registered" publication
#
# Output:  data/processed/lookups/practice_demographics.csv
# Run:     Only needs to run once (output is cached). Re-run to refresh.
# ==============================================================================

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
})

out_file <- "data/processed/lookups/practice_demographics.csv"

if (file.exists(out_file)) {
  cat("Demographics file already exists:", out_file, "\n")
  cat("Delete it and re-run this script to refresh.\n")
  # Still continue to regenerate if FORCE_REPROCESS is set
  if (Sys.getenv("FORCE_REPROCESS") != "TRUE") {
    cat("Skipping download. Set FORCE_REPROCESS=TRUE to override.\n")
    q(save = "no")
  }
}

dir.create("data/processed/lookups", showWarnings = FALSE, recursive = TRUE)

# ==============================================================================
# 1. IMD DEPRIVATION SCORES (Fingertips API)
# ==============================================================================
# Indicator 93553: "Deprivation score (IMD 2019)"
# Area type 7 = GP practice
# Population-weighted average of LSOA-level IMD for each practice's patients
# ==============================================================================

cat("Downloading IMD deprivation scores from Fingertips API...\n")

imd_url <- "https://fingertips.phe.org.uk/api/all_data/csv/by_indicator_id?indicator_ids=93553&area_type_id=7"
imd_tmp <- tempfile(fileext = ".csv")
download.file(imd_url, imd_tmp, mode = "wb", quiet = TRUE)

imd_raw <- read_csv(imd_tmp, show_col_types = FALSE)

# Keep latest time period only, select relevant columns
imd_data <- imd_raw %>%
  filter(`Time period` == max(`Time period`)) %>%
  select(
    practice_code = `Area Code`,
    imd_score = Value
  ) %>%
  filter(!is.na(imd_score)) %>%
  distinct(practice_code, .keep_all = TRUE) %>%
  mutate(
    imd_decile = ntile(imd_score, 10)
  )

cat(sprintf("IMD data: %d practices (IMD %d)\n",
            nrow(imd_data), max(imd_raw$`Time period`)))

# ==============================================================================
# 2. AGE PROFILE (NHS Digital "Patients Registered at a GP Practice")
# ==============================================================================
# Latest publication: dynamically discovered from digital.nhs.uk
# Source: digital.nhs.uk/data-and-information/publications/statistical/
#         patients-registered-at-a-gp-practice
# ==============================================================================

cat("Processing age profile data...\n")

age_file <- "data/raw/gp-reg-pat-prac-quin-age.csv"
age_zip <- "data/raw/gp-reg-pat-prac-quin-age.zip"

if (!file.exists(age_file)) {
  if (!file.exists(age_zip)) {
    cat("Downloading age data from NHS Digital...\n")
    # Discover the latest publication URL dynamically
    # Publication pages follow a predictable pattern; file download hashes do not
    pub_base <- "https://digital.nhs.uk/data-and-information/publications/statistical/patients-registered-at-a-gp-practice"
    latest_month <- tolower(format(Sys.Date() - 30, "%B-%Y"))  # previous month (latest likely published)
    pub_url <- paste0(pub_base, "/", latest_month)
    cat(sprintf("Fetching publication page: %s\n", pub_url))

    page_html <- tryCatch(
      readLines(pub_url, warn = FALSE),
      error = function(e) NULL
    )

    if (is.null(page_html)) {
      stop("Could not fetch NHS Digital publication page: ", pub_url,
           "\nCheck the URL or download manually.")
    }

    # Extract the download URL for the 5-year age group ZIP
    page_text <- paste(page_html, collapse = "\n")
    match <- regmatches(page_text,
      regexpr("https://files\\.digital\\.nhs\\.uk/[A-Z0-9]{2}/[A-Z0-9]{6}/gp-reg-pat-prac-quin-age\\.zip",
              page_text))

    if (length(match) == 0) {
      stop("Could not find download link for gp-reg-pat-prac-quin-age.zip on: ", pub_url)
    }

    age_url <- match[1]
    cat(sprintf("Found download URL: %s\n", age_url))
    download.file(age_url, age_zip, mode = "wb", quiet = TRUE)
  }
  unzip(age_zip, exdir = "data/raw")
}

age_raw <- read_csv(age_file, show_col_types = FALSE)

# Get total list size per practice (SEX = "ALL", AGE_GROUP_5 = "ALL")
list_sizes <- age_raw %>%
  filter(ORG_TYPE == "GP", SEX == "ALL", AGE_GROUP_5 == "ALL") %>%
  select(practice_code = ORG_CODE, list_size = NUMBER_OF_PATIENTS)

# Compute age band totals per practice (sum FEMALE + MALE)
age_bands <- age_raw %>%
  filter(ORG_TYPE == "GP", SEX %in% c("FEMALE", "MALE")) %>%
  group_by(practice_code = ORG_CODE, AGE_GROUP_5) %>%
  summarise(patients = sum(NUMBER_OF_PATIENTS, na.rm = TRUE), .groups = "drop")

# Define age band groups
elderly_bands <- c("65_69", "70_74", "75_79", "80_84", "85_89", "90_94", "95+")
young_bands   <- c("0_4")
school_bands  <- c("5_9", "10_14", "15_19")

age_summary <- age_bands %>%
  group_by(practice_code) %>%
  summarise(
    total_patients = sum(patients, na.rm = TRUE),
    over_65  = sum(patients[AGE_GROUP_5 %in% elderly_bands], na.rm = TRUE),
    under_5  = sum(patients[AGE_GROUP_5 %in% young_bands], na.rm = TRUE),
    age_5_19 = sum(patients[AGE_GROUP_5 %in% school_bands], na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    pct_over_65 = 100 * over_65 / pmax(total_patients, 1),
    pct_under_5 = 100 * under_5 / pmax(total_patients, 1),
    pct_5_19    = 100 * age_5_19 / pmax(total_patients, 1)
  ) %>%
  select(practice_code, pct_over_65, pct_under_5, pct_5_19)

cat(sprintf("Age data: %d practices\n", nrow(age_summary)))

# ==============================================================================
# 3. COMBINE AND SAVE
# ==============================================================================

demographics <- list_sizes %>%
  left_join(imd_data, by = "practice_code") %>%
  left_join(age_summary, by = "practice_code")

cat(sprintf("\nCombined demographics: %d practices\n", nrow(demographics)))
cat(sprintf("  With IMD score: %d\n", sum(!is.na(demographics$imd_score))))
cat(sprintf("  With age data:  %d\n", sum(!is.na(demographics$pct_over_65))))

write_csv(demographics, out_file)
cat(sprintf("\nSaved: %s\n", out_file))

# Quick summary
cat("\nDemographic ranges:\n")
cat(sprintf("  IMD score:    %.1f - %.1f (mean: %.1f)\n",
            min(demographics$imd_score, na.rm = TRUE),
            max(demographics$imd_score, na.rm = TRUE),
            mean(demographics$imd_score, na.rm = TRUE)))
cat(sprintf("  %% over 65:    %.1f%% - %.1f%% (mean: %.1f%%)\n",
            min(demographics$pct_over_65, na.rm = TRUE),
            max(demographics$pct_over_65, na.rm = TRUE),
            mean(demographics$pct_over_65, na.rm = TRUE)))
cat(sprintf("  %% under 5:    %.1f%% - %.1f%% (mean: %.1f%%)\n",
            min(demographics$pct_under_5, na.rm = TRUE),
            max(demographics$pct_under_5, na.rm = TRUE),
            mean(demographics$pct_under_5, na.rm = TRUE)))
cat(sprintf("  %% aged 5-19:  %.1f%% - %.1f%% (mean: %.1f%%)\n",
            min(demographics$pct_5_19, na.rm = TRUE),
            max(demographics$pct_5_19, na.rm = TRUE),
            mean(demographics$pct_5_19, na.rm = TRUE)))
cat(sprintf("  List size:     %s - %s (median: %s)\n",
            format(min(demographics$list_size, na.rm = TRUE), big.mark = ","),
            format(max(demographics$list_size, na.rm = TRUE), big.mark = ","),
            format(median(demographics$list_size, na.rm = TRUE), big.mark = ",")))
