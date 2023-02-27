library(magrittr)

month <- lubridate::month(Sys.Date(), label = T)
year <- lubridate::year(Sys.Date())

filename <- paste("enumerators_summary", month, year, sep = "_")
filename <- paste0(filename, ".pdf")

rmarkdown::render(
  input = system.file("report/enumerators_summary/enumerators_summary.Rmd",
    package = "peskas.timor.data.pipeline"
  ),
  output_dir = system.file("report/enumerators_summary", package = "peskas.timor.data.pipeline"),
  clean = FALSE,
  output_file = filename
)
