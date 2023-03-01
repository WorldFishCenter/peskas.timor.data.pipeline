library(magrittr)

month <- seq(lubridate::floor_date(Sys.Date(), "month"), length = 2, by = "-1 months")[2]
month <- lubridate::month(month, label = T)
year <- lubridate::year(Sys.Date())

filename <- paste(month, year, "enumerators_summary_report", sep = "_")
filename <- paste0(filename, ".pdf")

rmarkdown::render(
  input = system.file("report/enumerators_summary.Rmd", package = "peskas.timor.data.pipeline"),
  output_dir = system.file("report", package = "peskas.timor.data.pipeline"),
  clean = FALSE,
  output_file = filename
)
