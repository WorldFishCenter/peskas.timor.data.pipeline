library(magrittr)

#month <- seq(lubridate::floor_date(Sys.Date(), "month"), length = 2, by = "-1 months")[2]
#month <- lubridate::month(month, label = T)
#year <- lubridate::year(Sys.Date())

#filename <- paste(month, year, "enumerators_summary_report", sep = "_")
filename <- paste0("enumerators_summary_report", ".html")

rmarkdown::render(
  input = system.file("report/enumerators_summary.Rmd", package = "peskas.timor.data.pipeline"),
  output_dir = system.file("report", package = "peskas.timor.data.pipeline"),
  clean = FALSE,
  output_file = filename
)

path <- paste("report", filename, sep = "/")

peskas.timor.data.pipeline::upload_cloud_file(
  file = system.file(path, package = "peskas.timor.data.pipeline"),
  name = filename,
  provider = pars$public_storage$google$key,
  options = pars$public_storage$google$options
)
