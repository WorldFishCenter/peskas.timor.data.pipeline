library(peskas.timor.data.pipeline)

pars <- config::get(
  file = system.file("conf.yml", package = "peskas.timor.data.pipeline"))

dir.create("tmp", showWarnings = FALSE)

metadata_filename <- add_version("timor-landings-metadata", "json")
download_survey_metadata(id = pars$landings$survey_id,
                         token = pars$landings$token,
                         api = pars$landings$api) %>%
  jsonlite::write_json(metadata_filename)

csv_filename <- add_version("timor-landings-raw", "csv")
download_survey_data(path = csv_filename,
                     id = pars$landings$survey_id,
                     token = pars$landings$token,
                     format = "csv")

json_filename <- add_version("timor-landings-raw", "json")
download_survey_data(path = json_filename,
                     id = pars$landings$survey_id,
                     token = pars$landings$token,
                     format = "json")

file_list <- c(metadata_filename, csv_filename, json_filename)

purrr::map(.x = file_list,
           .f = upload_cloud_file,
           provider = "gcs",
           options = list(
             service_account_key = pars$storage$options$service_account_key,
             bucket = pars$storage$options$bucket))
