library(magrittr)

filename <- paste0("unanswered_summary", ".html")

rmarkdown::render(
  input = system.file("report/unanswered_summary.Rmd", package = "peskas.timor.data.pipeline"),
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
