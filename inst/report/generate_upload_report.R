library(magrittr)

setwd("../..")
pars <- peskas.timor.data.pipeline::read_config()

# Skip versioning for the moment until prepare for appropriate func in peskas app
# Overwrite data report at each upload

#filename <- pars$report$file_prefix %>%
#  peskas.timor.data.pipeline::add_version(extension = "pdf")

filename <- "data_report.pdf"

rmarkdown::render(
  input = system.file("report/data_report.Rmd", package = "peskas.timor.data.pipeline"),
  output_dir = system.file("report", package = "peskas.timor.data.pipeline"),
  output_file = filename
)

path <- paste("report", filename, sep = "/")

peskas.timor.data.pipeline::upload_cloud_file(
  file = system.file(path, package = "peskas.timor.data.pipeline"),
  name = filename,
  provider = pars$public_storage$google$key,
  options = pars$public_storage$google$options
)
