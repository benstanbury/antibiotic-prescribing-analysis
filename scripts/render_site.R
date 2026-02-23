# Render both Rmd and Qmd outputs to docs/ for GitHub Pages.

docs_dir <- "docs"
dir.create(docs_dir, showWarnings = FALSE)

# Quarto -> docs/intro_to_data_analysis_qmd.html
system("quarto render intro_to_data_analysis.qmd")

# R Markdown -> docs/intro_to_data_analysis.html
rmarkdown::render(
  "intro_to_data_analysis.Rmd",
  output_dir = docs_dir
)

# Copy Rmd output as the default landing page
file.copy(
  file.path(docs_dir, "intro_to_data_analysis.html"),
  file.path(docs_dir, "index.html"),
  overwrite = TRUE
)

cat("Done. All outputs in docs/\n")
