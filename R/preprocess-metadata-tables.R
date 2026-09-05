#' Preprocess metadata tables
#'
#' Takes the ingested Google Sheets metadata tables, parses them and writes one
#' versioned preprocessed list for the rest of the pipeline to read.
#'
#' Six tables are left, all with live readers: `devices` (`validate_imeis()`),
#' `catch_types`, `stations`, `reporting_unit`, `habitat` and `conservation`.
#' `vms_installs`, `centro_pescas`, `boats`, `fishing_vessel_statistics` and
#' `registered_boats` went in migration Phase 11 — the frame is authoritative
#' for the last of those (`get_registered_boats()`) and the other four had no
#' reader. `morphometric_table` went on 2026-09-05, when its 559 curated rows
#' became a package snapshot (`inst/extdata/morphometric-coefficients.csv`) so
#' that `calculate_weights()` reads no Google Sheet.
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
    devices = pt_validate_devices(metadata_tables$devices),
    catch_types = pt_validate_catch_types(metadata_tables$catch_types),
    stations = pt_validate_stations(metadata_tables$stations),
    reporting_unit = pt_validate_reporting_unit(metadata_tables$reporting_unit),
    habitat = pt_validate_habitat(metadata_tables$habitat),
    conservation = pt_validate_conservation(metadata_tables$conservation)
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


#' Parse and validate catch types table
#'
#' Currently this function does not perform anything and is a placeholder
#'
#' @param catch_type_table a data frame with the catch type info
#'
#' @return a tibble
#'
#' @keywords preprocessing
pt_validate_catch_types <- function(catch_type_table) {
  catch_type_table
}

# NOTE: `pt_validate_gear_types()` and `pt_validate_vessel_types()` went with
# their tables in migration Phase 5 — `validate_gear_type()` and
# `validate_vessel_type()` read the labels `preprocess_landings()` resolves from
# the PESKAS | FRAME frame, which is authoritative for both (PLAN §2.5).

pt_validate_stations <- function(stations_table) {
  stations_table
}

pt_validate_reporting_unit <- function(x) {
  x
}

pt_validate_habitat <- function(x) {
  x
}

pt_validate_conservation <- function(conservation_table) {
  conservation_table %>%
    dplyr::select(.data$conservation_code, .data$conservation_place) %>%
    dplyr::mutate(conservation_code = as.character(.data$conservation_code))
}
