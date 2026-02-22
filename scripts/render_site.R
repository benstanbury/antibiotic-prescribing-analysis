# Render both Rmd and Qmd outputs to docs/ for GitHub Pages.
# Use temporary output dirs to avoid Quarto cleaning the docs/ folder.

docs_dir <- "docs"
dir.create(docs_dir, showWarnings = FALSE)

# Quarto -> docs/intro_to_data_analysis_qmd.html (alternate)
# Requires Quarto CLI installed (brew install quarto)
tmp_qmd <- tempfile("quarto_qmd_")
dir.create(tmp_qmd)
system(sprintf(
  "quarto render intro_to_data_analysis.qmd --output-dir %s --output intro_to_data_analysis_qmd.html",
  shQuote(tmp_qmd)
))
file.copy(
  file.path(tmp_qmd, "intro_to_data_analysis_qmd.html"),
  file.path(docs_dir, "intro_to_data_analysis_qmd.html"),
  overwrite = TRUE
)

# R Markdown -> docs/index.html (default landing page)
rmarkdown::render(
  "intro_to_data_analysis.Rmd",
  output_file = "index.html",
  output_dir = docs_dir
)
