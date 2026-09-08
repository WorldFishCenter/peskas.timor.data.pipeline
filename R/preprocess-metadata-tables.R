#' Preprocess metadata tables
#'
#' Takes the ingested Google Sheets metadata tables, parses them and writes one
#' versioned preprocessed list for the rest of the pipeline to read.
#'
#' One table remains: `devices`, the tracker IMEI roster read by
#' [validate_imeis()]. Everything the Airtable frame covers is read from there,
#' and the small code-to-label lookups are fixed in the package.
#'
#' This function requires no arguments because it retrieves the parameters from
#' `conf.yml`. The fields required are:
#'
#' ```
#' metadata:
#'   spreadsheet:
#'     name:
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
#' @param log_threshold The (standard Apache logj4) log level used as a threshold for the logging infrastructure. See [logger::log_levels] for more details
#'
#' @keywords workflow
#' @export
#'
preprocess_metadata_tables <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  metadata_filename <- coasts::cloud_object_name(
    prefix = conf$metadata$google_sheets$name,
    provider = conf$storage$google$key,
    extension = "rds",
    version = conf$metadata$version$preprocess,
    exact_match = TRUE,
    options = conf$storage$google$options
  )

  logger::log_info("Downloading metadata tables as {metadata_filename}...")
  coasts::download_cloud_file(
    name = metadata_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("Reading {metadata_filename}...")
  metadata_tables <- readr::read_rds(metadata_filename)

  logger::log_info("Preprocessing metadata tables...")
  preprocessed_metadata <- list(
    devices = pt_validate_devices(metadata_tables$devices)
  )

  preprocessed_filename <- paste(
    conf$metadata$google_sheets$name,
    "preprocessed",
    sep = "_"
  ) %>%
    add_version(extension = "rds")
  readr::write_rds(
    x = preprocessed_metadata,
    file = preprocessed_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  coasts::upload_cloud_file(
    file = preprocessed_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}


#' Parse and validate devices table
#'
#' Convert date and date-time columns and ensures that device_imei is stored as
#' a character. Currently this table performs no validations.
#'
#' @param devices_table a data frame with the devices
#'
#' @return a tibble
#' @keywords preprocessing
#' @export
#'
pt_validate_devices <- function(devices_table) {
  devices_table %>%
    dplyr::mutate(device_imei = as.character(.data$device_imei))
}


