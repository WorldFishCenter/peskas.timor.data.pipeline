library(peskas.timor.data.pipeline)

logger::log_threshold(logger::DEBUG)
logger::log_errors()

logger::log_info("Loading configuration file...")
pars <- config::get(
  config = Sys.getenv("R_CONFIG_ACTIVE"),
  file = system.file("conf.yml", package = "peskas.timor.data.pipeline"))
logger::log_info("Using configutation: {attr(pars, 'config')}")
logger::log_debug("Running with parameters {pars}")

dir.create("tmp", showWarnings = FALSE)

logger::log_info("Downloading survey metadata...")
metadata_filename <- add_version("timor-landings-metadata", "json")
download_survey_metadata(id = pars$landings$survey_id,
                         token = pars$landings$token,
                         api = pars$landings$api) %>%
  jsonlite::write_json(metadata_filename)
logger::log_success("Metadata download succeeded")


logger::log_info("Downloading survey csv data...")
csv_filename <- add_version("timor-landings-raw", "csv")
download_survey_data(path = csv_filename,
                     id = pars$landings$survey_id,
                     token = pars$landings$token,
                     format = "csv")
logger::log_success("Survey csv data download succeeded")

logger::log_info("Downloading survey json data...")
json_filename <- add_version("timor-landings-raw", "json")
download_survey_data(path = json_filename,
                     id = pars$landings$survey_id,
                     token = pars$landings$token,
                     format = "json")
logger::log_success("Survey json data download succeeded")

logger::log_info("Uploading files to cloud...")
file_list <- c(metadata_filename, csv_filename, json_filename)
purrr::map(.x = file_list,
           .f = upload_cloud_file,
           provider = "gcs",
           options = list(
             service_account_key = pars$storage$options$service_account_key,
             bucket = pars$storage$options$bucket))
logger::log_success("File upload succeded")
