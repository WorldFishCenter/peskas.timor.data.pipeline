#' Preprocess metadata tables
#'
#' Takes ingested metadata tables, validates them, and processes them into
#' artefacts (devices information, flags) to be used further down in the
#' pipeline.
#'
#' Specifically this function:
#'
#' * Validates the flags table containing the columns: flag_id, flag_category, flag_message
#' * Constructs a devices table containing the columns: imei
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
#' @param log_threshold
#' @inheritParams ingest_landings
#'
#' @keywords workflow
#' @export
#'
preprocess_metadata_tables <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  metadata_filename <- cloud_object_name(
    prefix = pars$metadata$airtable$name,
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$metadata$version$preprocess,
    exact_match = TRUE,
    options = pars$storage$google$options
  )

  logger::log_info("Downloading metadata tables as {metadata_filename}...")
  download_cloud_file(
    name = metadata_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  logger::log_info("Reading {metadata_filename}...")
  metadata_tables <- readr::read_rds(metadata_filename)

  logger::log_info("Preprocessing metadata tables...")
  preprocessed_metadata <- list(
    devices = pt_validate_devices(metadata_tables$devices),
    device_installs = pt_validate_vms_installs(metadata_tables$vms_installs),
    boats = pt_validate_boats(metadata_tables$boats),
    catch_types = pt_validate_catch_types(metadata_tables$catch_types),
    fao_catch = pt_validate_fao_catch(metadata_tables$fao_catch),
    morphometric_table = pt_validate_morphometric_table(metadata_tables$morphometric_table),
    centro_pescas = pt_validate_centro_pescas(metadata_tables$centro_pescas),
    vessel_types = pt_validate_vessel_types(metadata_tables$vessel_types),
    gear_types = pt_validate_gear_types(metadata_tables$gear_types),
    stations = pt_validate_stations(metadata_tables$stations),
    reporting_unit = pt_validate_reporting_unit(metadata_tables$reporting_unit),
    habitat = pt_validate_habitat(metadata_tables$habitat),
    vessels_stats = pt_validate_vessels_stats(metadata_tables$fishing_vessel_statistics),
    registered_boats = pt_validate_reg_boats(metadata_tables$registered_boats)
  )

  preprocessed_filename <- paste(pars$metadata$airtable$name,
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
  upload_cloud_file(
    file = preprocessed_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}


#' Parse and validate vms_installs table
#'
#' Convert date and date-time columns and checks that
#' * The date of device installations is prior to the date of recorded damage
#' * The devices are installed in a single boat
#'
#' @param vms_installs_table a data frame with the vms movements
#'
#' @return a tibble with the vms_installs
#' @export
#'
pt_validate_vms_installs <- function(vms_installs_table) {
  v <- vms_installs_table %>%
    dplyr::mutate(
      device_event_date = lubridate::as_date(.data$device_event_date),
      createdTime = lubridate::ymd_hms(.data$createdTime),
      created_date = lubridate::as_date(.data$created_date)
    )

  # Check that installs are recorded prior to damage
  ok_date_damage <- v %>%
    dplyr::group_by(.data$device_imei) %>%
    dplyr::filter(any(.data$device_event_type == "damage recorded")) %>%
    dplyr::summarise(
      ok_date_damage =
        .data$device_event_date[.data$device_event_type == "damage recorded"] >
          .data$device_event_date[.data$device_event_type == "installation"],
      .groups = "drop"
    )
  if (any(isFALSE(ok_date_damage$ok_date_damage))) {
    stop("detected damage recorded in vms prior to vms installation")
  }

  # Check that devices are installed in a single boat
  ok_boat_installs <- v %>%
    dplyr::group_by(.data$device_imei) %>%
    dplyr::summarise(
      n_boats = dplyr::n_distinct(.data$boat_id),
      .groups = "drop"
    )
  # if (any(ok_boat_installs$n_boats > 1))
  # stop("detected a vms device in more than one boat")

  v
}

#' Parse and validate devices table
#'
#' Convert date and date-time columns and ensures that device_imei is stored as
#' a character. Currently this table performs no validations.
#'
#' @param devices_table a data frame with the devices
#'
#' @return a tibble
#' @export
#'
pt_validate_devices <- function(devices_table) {
  devices_table %>%
    dplyr::mutate(createdTime = lubridate::ymd_hms(.data$createdTime)) %>%
    dplyr::mutate(device_imei = as.character(.data$device_imei))
}


#' Validate flags from metadata tables
#'
#' Checks the flags from the metadata tables. Specifically check that flags are
#' unique and all flags have an id
#'
#' @param flags_table the table containing the flags. Must have columns flag_id,
#'   flag_category, and flag_message
#'
#' @return a data frame with columns flag_id, flag_category, and flag_message
#' @export
#'
pt_validate_flags <- function(flags_table) {
  f <- flags_table %>%
    dplyr::filter(!is.na(.data$flag_message)) %>%
    dplyr::mutate(flag_id = as.character(.data$flag_id))

  n_codes <- dplyr::n_distinct(f$flag_id)
  n_flags <- nrow(f)
  if (any(is.na(f$flag_id))) stop("not all flags have a flag_id")
  if (n_codes < n_flags) stop("flag_id are not unique")

  f
}

#' Parse and validate boats table
#'
#' Convert date and date-time columns and checks that
#' * The recorded length of the boats are valid
#'
#' @param boats_table a data frame with the boats info
#'
#' @return a tibble
#' @export
#'
pt_validate_boats <- function(boats_table) {
  b <- boats_table %>%
    dplyr::mutate(
      createdTime = lubridate::ymd_hms(.data$createdTime),
      created_date = lubridate::as_date(.data$created_date),
      last_modified_time = lubridate::ymd_hms(.data$last_modified_time)
    )

  # Check that boat length is valid
  boat_length_ok <- b %>%
    dplyr::filter(!is.na(.data$boat_length)) %>%
    dplyr::mutate(boat_length_ok = .data$boat_length > 0 &
      .data$boat_length < 30)

  if (any(isFALSE(boat_length_ok$boat_length_ok))) {
    stop("detected boats with unvalid lengths")
  }

  b
}

#' Parse and validate catch types table
#'
#' Currently this function does not perform anything and is a placeholder
#'
#' @param catch_type_table a data frame with the catch type info
#'
#' @return a tibble
#'
pt_validate_catch_types <- function(catch_type_table) {
  catch_type_table
}

#' Parse and validate fao catch table
#'
#' Currently this function does not perform anything and is a placeholder
#'
#' @param fao_catch_table a data frame with the catch type info
#'
#' @return a tibble
#'
pt_validate_fao_catch <- function(fao_catch_table) {
  fao_catch_table
}

#' Parse and validate morphometric table
#'
#' @param morphometric_table a data frame with morphometric info
#'
#' @return a tibble
pt_validate_morphometric_table <- function(morphometric_table) {
  morphometric_table
}

#' Parse and validate centro de pescas table
#'
#' Currently this function does not perform anything and is a placeholder
#'
#' @param centro_pescas_table a data frame with info on centro de pescas
#'
#' @return a tibble
#'
pt_validate_centro_pescas <- function(centro_pescas_table) {
  centro_pescas_table
}

pt_validate_gear_types <- function(gear_types_table) {
  gear_types_table
}

pt_validate_vessel_types <- function(vessel_types_table) {
  vessel_types_table
}

pt_validate_stations <- function(stations_table) {
  stations_table
}

pt_validate_reporting_unit <- function(x) {
  x
}

pt_validate_habitat <- function(x) {
  x
}

pt_validate_vessels_stats <- function(vessels_stats_table) {
  vessels_stats_table %>%
    tidyr::separate(.data$boat_numbers,
      into = c("reporting_region", "type", NA),
      sep = "([|])"
    ) %>%
    dplyr::select(.data$reporting_region, .data$type, .data$n_boats, .data$info_date) %>%
    dplyr::mutate(dplyr::across(where(is.character), stringr::str_trim))
}

pt_validate_reg_boats <- function(reg_boats_table) {
  reg_boats_table %>%
    dplyr::select(
      reporting_region = .data$Municipality,
      boats_2016 = .data$registered_boats_2016,
      boats_2022 = .data$registered_boats_2022
    ) %>%
    dplyr::mutate(n_boats = dplyr::case_when(
      .data$reporting_region == "Dili" ~ .data$boats_2022,
      is.na(.data$boats_2022) | .data$boats_2022 < .data$boats_2016 ~
        .data$boats_2016, TRUE ~ .data$boats_2022
    )) %>%
    dplyr::select(.data$reporting_region, .data$n_boats)
}
