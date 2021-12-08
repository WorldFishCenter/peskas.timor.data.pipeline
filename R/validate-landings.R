#'Validate landings
#'
#'Downloads the preprocessed version of the data from cloud storage services and
#'validates a range of information so that it can be safely used for analysis.
#'By default the function uses the method of the median absolute deviation (MAD)
#'for outliers identification.
#'
#'The parameters needed in the config file are those required for
#'`preprocess_landings()`, `preprocess_metadata_tables()`, and
#'`ingest_validation_tables()` combined, as well as parameters needed to
#'outliers identification that are  `hrs`, `method` and `k`.
#'
#'To avoid synchronisation problems always is recommended that this called
#'together with `ingest_validation_tables()`
#'
#' @param log_threshold
#' @inheritParams ingest_landings
#' @inheritParams validate_surveys_time
#' @inheritParams validate_catch_price
#' @keywords workflow
#' @return no outputs. This function is used for it's side effects
#'
#' @importFrom rlang .data
#' @export
#'
validate_landings <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)

  pars <- read_config()
  validation <- get_validation_tables(pars)
  metadata <- get_preprocessed_metadata(pars)
  landings <- get_merged_landings(pars, "_weight")

  # read arguments for outliers identification
  default_max_limit <-  pars$validation$landings$default$max
  default_method <-  pars$validation$landings$default$method
  default_k <-  pars$validation$landings$default$k
  cook_dist <-  pars$validation$landings$cook_dist

  # deployed_imeis <- get_deployed_imeis(metadata)
  # for now using all the deployed imeis
  deployed_imeis <- metadata$devices$device_imei

  logger::log_info("Validating IMEIs...")
  imei_alerts <- landings$`trip_group/IMEI` %>%
    rlang::set_names(landings$`_id`) %>%
    purrr::imap(validate_this_imei,
             deployed_imeis) %>%
    purrr::map_dfr(tibble::as_tibble)

  logger::log_info("Validating surveys trips...")
  surveys_time_alerts <- validate_surveys_time(
    data = landings,
    hrs = pars$validation$landings$survey_time$max_duration %||% default_max_limit,
    submission_delay = pars$validation$landings$survey_time$submission_delay)
  logger::log_info("Validating catches values...")
  surveys_price_alerts <- validate_catch_price(
    data = landings,
    method = pars$validation$landings$prices$method %||% default_method,
    k = pars$validation$landings$prices$k %||% default_k)
  logger::log_info("Validating catches parameters...")
  surveys_catch_alerts <- validate_catch_params(
    landings,
    method = pars$validation$landings$catch$method %||% default_method,
    k_ind = pars$validation$catch$n_individuals$k %||% default_k,
    k_length = pars$validation$catch$length$k %||% default_k)
  price_weight_alerts <- validate_price_weight(
    surveys_catch_alerts,
    surveys_price_alerts,
    cook_dist = cook_dist)
  vessel_type_alerts <- validate_vessel_type(
    landings,
    metadata$vessel_types)
  gear_type_alerts <- validate_gear_type(
    landings,
    metadata$gear_types)
  site_alerts <- validate_sites(
    landings,
    metadata$stations, metadata$reporting_unit
  )
  n_fishers_alerts <- validate_n_fishers(
    landings,
    method = pars$validation$landings$n_fishers$method %||% default_method,
    k = pars$validation$landings$n_fishers$k %||% default_k
  )


  # CREATE VALIDATED OUTPUT -----------------------------------------------

  # take ready (?) columns

  ready_cols <- landings %>%
    dplyr::select(
      # site_number = .data$landing_site_name,
      submission_id = .data$`_id`,
      n_gleaners = .data$how_many_gleaners_today,
      n_child_fishers = .data$`trip_group/no_fishers/no_child_fishers`,
      n_women_fishers = .data$`trip_group/no_fishers/no_women_fishers`,
      submission_id = .data$`_id`
    ) %>%
    dplyr::mutate(submission_id = as.integer(.data$submission_id))

  logger::log_info("Renaming data fields")
  validated_landings <-
    list(imei_alerts,
         surveys_time_alerts$validated_dates,
         surveys_time_alerts$validated_duration,
         price_weight_alerts,
         vessel_type_alerts,
         gear_type_alerts,
         site_alerts,
         n_fishers_alerts) %>%
    purrr::map(~ dplyr::select(.x,-alert_number)) %>%
    purrr::reduce(dplyr::left_join, by = "submission_id") %>%
    dplyr::left_join(ready_cols, by = "submission_id") %>%
    dplyr::mutate(
      species_group = purrr::map(
        .x = .data$species_group, .f = purrr::modify_at,
        .at = "length_individuals",
        purrr::map, dplyr::select,
        length = .data$mean_length,
        individuals = .data$n_individuals,
        weight = .data$weight),
      species_group = purrr::map(
        .x = .data$species_group, .f = dplyr::select,
        catch_taxon=.data$species,
        catch_purpose = .data$food_or_sale,
        length_type = .data$length_type,
        length_frequency = .data$length_individuals)) %>%
    dplyr::select(
      landing_id = .data$submission_id,
      landing_date = .data$date,
      tracker_imei = .data$imei,
      trip_duration = .data$trip_duration,
      landing_catch = .data$species_group,
      landing_value = .data$total_catch_value,
      landing_station = .data$station_name,
      reporting_region = .data$reporting_region,
      tidyselect::starts_with("fisher_number"),
      ##municipality = .data$`municipality (from administrative_posts)`,
      .data$gear_type,
      .data$vessel_type,
      .data$n_gleaners,
      .data$n_child_fishers,
      .data$n_women_fishers)

  validated_landings_filename <- paste(pars$surveys$merged_landings$file_prefix,
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

  alerts <-
  list(imei_alerts,
       surveys_time_alerts$validated_dates,
       surveys_time_alerts$validated_duration,
       price_weight_alerts,
       vessel_type_alerts,
       gear_type_alerts,
       site_alerts,
       n_fishers_alerts
       ) %>%
    purrr::map(~ dplyr::select(.x,alert_number,submission_id)) %>%
    purrr::reduce(dplyr::bind_rows)

  # Wrangle a bot landings, alerts and flags data frames to fit the workflow
  landings_info <- landings %>%
    dplyr::rename(submission_id = .data$`_id`,
                  submission_date = .data$`_submission_time`) %>%
    dplyr::mutate(submission_id = as.integer(.data$submission_id),
                  submission_date = lubridate::as_date(.data$submission_date)) %>%
    dplyr::select(.data$submission_id, .data$submission_date)
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
    dplyr::select(.data$submission_id, .data$alert, .data$submission_date) %>%
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

get_merged_landings <- function(pars, suffix = ""){
  landings_rds <- cloud_object_name(
    prefix = paste0(pars$surveys$merged_landings$file_prefix, suffix),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$surveys$merged_landings$version,
    options = pars$storage$google$options,
    exact_match = TRUE)
  logger::log_info("Downloading {landings_rds}...")
  download_cloud_file(name = landings_rds,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)
  readr::read_rds(file = landings_rds)
}

