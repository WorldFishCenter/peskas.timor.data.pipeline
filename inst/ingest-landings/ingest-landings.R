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

