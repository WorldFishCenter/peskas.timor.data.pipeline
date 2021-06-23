#' Pre-process Pelagic Data System trips
#'
#' Downloads raw structured data from cloud storage services and pre-process
#' into a binary format that is easier to deal with in R.
#'
#' This function downloads the landings data from a given version (specified in
#' the config file `conf.yml`. The parameters needed are:
#'
#' ```
#' pds:
#'   trips:
#'     token:
#'     secret:
#'     file_prefix:
#'   version:
#'     preprocess:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold
#' @inheritParams ingest_pds_trips
#' @keywords workflow
#' @return no outputs. This function is used for it's side effects
#' @export
#'
preprocess_pds_trips <- function(log_threshold = logger::DEBUG){

  pars <- read_config()

  pds_trips_csv <- cloud_object_name(prefix = pars$pds$trips$file_prefix,
                                     provider = pars$storage$google$key,
                                     extension = "csv",
                                     version = pars$pds$trips$version$preprocess,
                                     options = pars$storage$google$options)

  logger::log_info("Retrieving {pds_trips_csv}")
  download_cloud_file(name = pds_trips_csv,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)
  pds_trips_raw <- readr::read_csv(
    file = pds_trips_csv,
    col_types = "iTTicccdddccc") %>%
    dplyr::mutate(
      Started = lubridate::with_tz(.data$Started, "Asia/Dili"),
      Ended = lubridate::with_tz(.data$Ended, "Asia/Dili"),
      `Last Seen` = lubridate::as_datetime(`Last Seen`,
                                           format = "%a %b %d %X UTC %Y",
                                           tz = "UTC"))

  preprocessed_filename <- paste(pars$pds$trips$file_prefix, "preprocessed", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(x = pds_trips_raw,
                   file = preprocessed_filename,
                   compress = "gz")

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  upload_cloud_file(file = preprocessed_filename,
                    provider = pars$storage$google$key,
                    options = pars$storage$google$options)
}
