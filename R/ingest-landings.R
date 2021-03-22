#' Ingest Timor Landings Survey data
#'
#' Downloads landings information that has been collected using Kobo Toolbox and
#' uploads it to cloud storage services.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#'
#' @return No output. This funcrion is used for it's side effects
#' @export
#'
#' @details
#'
#' This function downloads the survey metadata (survey information) as well as
#' the survey responses. Afterwards it uploads this information to cloud
#' services. To improve reproducibility, the file names used contain a
#' versioning string that includes the date-time and, if available, the first 7
#' digits of the git commit sha. This is acomplished using [add_version()]
#'
#' The information about the surveys and the cloud storage is obtained from the
#' configuration file `conf.yml`. This file is located in the *inst* directory
#' of the package and can be accessed using
#' `system.file("conf.yml",package="peskas.timor.data.pipeline")`. The specific
#' configuration is detemined using the environment variable `R_CONFIG_ACTIVE`.
#'
#' Progress through the function is tracked using the package *logger*.
#'
ingest_timor_landings <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)
  logger::log_errors()

  logger::log_info("Loading configuration file...")
  pars <- config::get(
    config = Sys.getenv("R_CONFIG_ACTIVE"),
    file = system.file("conf.yml", package = "peskas.timor.data.pipeline"))
  logger::log_info("Using configutation: {attr(pars, 'config')}")
  logger::log_debug("Running with parameters {pars}")

  dir.create("tmp", showWarnings = FALSE)

  metadata_filename <- add_version("timor-landings-metadata", "json")
  csv_filename <- add_version("timor-landings-raw", "csv")
  json_filename <- add_version("timor-landings-raw", "json")

  logger::log_info("Downloading survey metadata as {metadata_filename}...")
  download_survey_metadata(id = pars$landings$survey_id,
                           token = pars$landings$token,
                           api = pars$landings$api) %>%
    jsonlite::write_json(metadata_filename)
  logger::log_success("Metadata download succeeded")

  logger::log_info("Downloading survey csv data as {csv_filename}...")
  download_survey_data(path = csv_filename,
                       id = pars$landings$survey_id,
                       token = pars$landings$token,
                       format = "csv")
  logger::log_success("Survey csv data download succeeded")

  logger::log_info("Downloading survey json data as {json_filename}...")
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

}
