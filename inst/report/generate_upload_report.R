library(magrittr)

conf <- peskas.timor.data.pipeline::read_config()
filename <- "data_report.html"

rmarkdown::render(
  input = system.file(
    "report/data_report.Rmd",
    package = "peskas.timor.data.pipeline"
  ),
  output_dir = system.file("report", package = "peskas.timor.data.pipeline"),
  clean = FALSE,
  output_file = filename
)

path <- paste("report", filename, sep = "/")

coasts::upload_cloud_file(
  file = system.file(path, package = "peskas.timor.data.pipeline"),
  name = filename,
  provider = conf$public_storage$google$key,
  options = conf$public_storage$google$options
)
