#'Validate landings
#'
#'Downloads the preprocessed cersion of the data from cloud storage services and
#'validates a range of information so that it can be safely used for analysis.
#'
#'The parameters needed in the config file are those required for
#'`preprocess_landings()`, `preprocess_metadata_tables()`, and
#'`ingest_validation_tables()` combined.
#'
#'To avoid synchronisation problems always is recommended that this called
#'together with `ingest_validation_tables()`
#'
#'@param log_threshold
#'@inheritParams ingest_landings
#'@export
#'
validate_landings <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)
  pars <- read_config()
  validation <- get_validation_tables(pars)
  metadata <- get_preprocessed_metadata(pars)
  landings <- get_preprocessed_landings(pars)

  # deployed_imeis <- get_deployed_imeis(metadata)
  # for now using all the deployed imeis
  deployed_imeis <- metadata$devices$device_imei

  logger::log_info("Validating IMEIs...")
  imei_alerts <- landings$`trip_group/IMEI` %>%
    rlang::set_names(landings$submission_id) %>%
    purrr::imap(validate_this_imei,
             deployed_imeis) %>%
    purrr::map_dfr(tibble::as_tibble)

  # CREATE VALIDATED OUTPUT -----------------------------------------------

  validated_landings <- dplyr::select(imei_alerts,
                                      .data$submission_id, .data$imei)

  validated_landings_filename <- paste(pars$surveys$landings$file_prefix,
                                       "validated", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(x = validated_landings,
                   file = validated_landings_filename,
                   compress = "gz")
  logger::log_info("Uploading {validated_landings_filename} to cloud sorage")
  upload_cloud_file(file = validated_landings_filename,
                    provider = pars$storage$google$key,
                    options = pars$storage$google$options)

  # HANDLE FLAGS ------------------------------------------------------------

  alerts <- imei_alerts %>%
    dplyr::select(.data$alert_number, .data$submission_id)

  # Wrangle a bot landings, alerts and flags data frames to fit the workflow
  landings_info <- landings %>%
    dplyr::select(.data$submission_id, .data$landing_date)
  remote_alerts <- validation$alerts %>%
    dplyr::select(.data$id, .data$alert_number) %>%
    dplyr::rename(alert = .data$id)
  remote_flags <- validation$flags %>%
    dplyr::select(.data$id, .data$submission_id, .data$alert) %>%
    dplyr::left_join(remote_alerts, by = c("alert" = "alert")) %>%
    dplyr::rename(remote_flag_id = .data$id, remote_alert_id = .data$alert)

  submission_alerts <- alerts %>%
    dplyr::left_join(remote_flags, by = c("submission_id", "alert_number"))

  # Determine which flags need to be uploaded cause they haven't been fixed
  new_flags_to_upload <- submission_alerts %>%
    dplyr::filter(is.na(.data$remote_flag_id),
                  !is.na(.data$alert_number)) %>%
    dplyr::left_join(remote_alerts, by = "alert_number") %>%
    dplyr::left_join(landings_info, by = "submission_id") %>%
    dplyr::select(.data$submission_id, .data$alert, .data$landing_date) %>%
    dplyr::mutate(flag_date = lubridate::today("GMT"))
  # If there are new flags, upload them
  if (nrow(new_flags_to_upload) > 0) {
    logger::log_info("Uploading new flags to Airtable")
    air_tibble_to_records(new_flags_to_upload, link_fields = "alert") %>%
      air_upload_records(table = pars$validation$airtable$flags_table,
                         base_id = pars$validation$airtable$base_id,
                         api_key = pars$validation$airtable$api_key)
  }

  # Determine which flags have been fixed
  flags_fixed <- submission_alerts %>%
    dplyr::filter(!is.na(.data$remote_flag_id),
                  is.na(.data$alert_number)) %>%
    dplyr::select(.data$remote_flag_id) %>%
    dplyr::mutate(fixing_date = lubridate::today("GMT"))

  if (nrow(flags_fixed) > 0) {
    logger::log_info("Updating fixed flags in Airtable")
    air_tibble_to_records(flags_fixed, id_fields = "remote_flag_id") %>%
      air_upload_records(table = pars$validation$airtable$flags_table,
                         base_id = pars$validation$airtable$base_id,
                         api_key = pars$validation$airtable$api_key,
                         request_type = "update")
  }
}

get_validation_tables <- function(pars){
  validation_rds <- cloud_object_name(
    prefix = paste(pars$validation$airtable$name, sep = "_"),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$validation$version$preprocess,
    options = pars$storage$google$options)
  logger::log_info("Downloading {validation_rds}...")
  download_cloud_file(name = validation_rds,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)
  readr::read_rds(file = validation_rds)
}

get_preprocessed_landings <- function(pars){
  landings_rds <- cloud_object_name(
    prefix = paste(pars$surveys$landings$file_prefix, 'preprocessed', sep = "_"),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$surveys$landings$version$preprocess,
    options = pars$storage$google$options)
  logger::log_info("Downloading {landings_rds}...")
  download_cloud_file(name = landings_rds,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)
  readr::read_rds(file = landings_rds)
}

get_preprocessed_metadata <- function(pars){
  metadata_rds <- cloud_object_name(
    prefix = paste(pars$metadata$airtable$name, 'preprocessed', sep = "_"),
    provider = pars$storage$google$key,
    extension = "rds",
    options = pars$storage$google$options)
  logger::log_info("Downloading {metadata_rds}...")
  download_cloud_file(name = metadata_rds,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)
  readr::read_rds(file = metadata_rds)
}
