library(peskas.timor.data.pipeline)

setwd("../..")
pars <- peskas.timor.data.pipeline::read_config()

dataverse <- pars$export_dataverse$dataverse_id
key <- pars$export_dataverse$token
server <- pars$export_dataverse$server

rmarkdown::render(
  input = system.file("export/DESCRIPTION.Rmd", package = "peskas.timor.data.pipeline"),
  output_dir = system.file("export", package = "peskas.timor.data.pipeline"))

logger::log_info("Generating metadata...")
metadat <- generate_metadata(pars)

dataverse::initiate_sword_dataset(
  dataverse = dataverse,
  server=server,
  key=key,
  body = metadat
)

release_files_names <- c(files_names,'DESCRIPTION.html')

logger::log_info("Uploading files...")
upload_files(
  file_list = release_files_names,
  key = key,
  dataverse = dataverse,
  server = server
)

file.remove(release_files_names)
