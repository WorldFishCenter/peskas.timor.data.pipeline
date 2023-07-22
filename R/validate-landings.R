#' Validate landings
#'
#' Downloads the preprocessed version of the data from cloud storage services and
#' validates a range of information so that it can be safely used for analysis.
#' By default the function uses the method of the median absolute deviation (MAD)
#' for outliers identification.
#'
#' The parameters needed in the config file are those required for
#' `preprocess_landings_step_1()` or `preprocess_landings_step_2()`,
#' `preprocess_metadata_tables()` ` combined,
#' as well as parameters needed to outliers identification
#'  that are  `hrs`, `method` and `k`.
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
validate_landings <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()
  metadata <- get_preprocessed_metadata(pars)
  landings <- get_merged_landings(pars, "_weight")

  # read arguments for outliers identification
  default_max_limit <- pars$validation$landings$default$max
  default_method <- pars$validation$landings$default$method
  default_k <- pars$validation$landings$default$k
  cook_dist <- pars$validation$landings$cook_dist

  # deployed_imeis <- get_deployed_imeis(metadata)
  # for now using all the deployed imeis
  deployed_imeis <- metadata$devices$device_imei

  logger::log_info("Validating IMEIs...")
  imei_alerts <- landings$`trip_group/IMEI` %>%
    rlang::set_names(landings$`_id`) %>%
    purrr::imap(
      validate_this_imei,
      deployed_imeis
    ) %>%
    purrr::map_dfr(tibble::as_tibble)

  logger::log_info("Validating surveys trips...")
  surveys_time_alerts <- validate_surveys_time(
    data = landings,
    hrs = pars$validation$landings$survey_time$max_duration %||% default_max_limit,
    submission_delay = pars$validation$landings$survey_time$submission_delay
  )
  logger::log_info("Validating catches values...")
  regular_landings <- validate_landing_regularity(landings)
  regular_landings_data <- regular_landings$regular_landings
  regularity_alerts <- regular_landings$regularity_alerts
  surveys_price_alerts <- validate_catch_price(
    data = regular_landings_data,
    method = pars$validation$landings$prices$method %||% default_method,
    k = pars$validation$landings$prices$k %||% default_k
  )
  logger::log_info("Validating catches parameters...")
  surveys_catch_alerts <- validate_catch_params(
    regular_landings_data,
    method = pars$validation$landings$catch$method %||% default_method,
    k_ind = pars$validation$catch$n_individuals$k %||% default_k,
    k_length = pars$validation$catch$length$k %||% default_k
  )
  price_weight_alerts <- validate_price_weight(
    catch_alerts = surveys_catch_alerts,
    price_alerts = surveys_price_alerts,
    non_regular_ids = regularity_alerts,
    cook_dist = cook_dist,
    price_weight_min = pars$validation$landings$price_per_weight$min_limit,
    price_weight_max = pars$validation$landings$price_per_weight$max_limit
  )
  vessel_type_alerts <- validate_vessel_type(
    landings,
    metadata$vessel_types
  )
  gear_type_alerts <- validate_gear_type(
    landings,
    metadata$gear_types
  )
  site_alerts <- validate_sites(
    landings,
    metadata$stations, metadata$reporting_unit
  )
  n_fishers_alerts <- validate_n_fishers(
    landings,
    method = pars$validation$landings$n_fishers$method %||% default_method,
    k = pars$validation$landings$n_fishers$k %||% default_k
  )
  habitat_alerts <- validate_habitat(
    landings,
    metadata$habitat
  )
  mesh_alerts <- validate_mesh(landings,
    mesh_limit = pars$validation$landings$mesh
  )
  gleaners_alerts <- validate_gleaners(
    landings,
    method = default_method,
    k_gleaners = pars$validation$landings$gleaners$k
  )
  fuel_alerts <- validate_fuel(
    landings,
    method = default_method,
    k_fuel = pars$validation$landings$fuel$k
  )


  # CREATE VALIDATED OUTPUT -----------------------------------------------

  landings_ids <-
    landings %>%
    dplyr::select(
      submission_id = .data$`_id`
    ) %>%
    dplyr::mutate(submission_id = as.integer(.data$submission_id))

  logger::log_info("Renaming data fields")
  validated_landings <-
    list(
      imei_alerts,
      surveys_time_alerts$validated_dates,
      surveys_time_alerts$validated_duration,
      price_weight_alerts,
      vessel_type_alerts,
      gear_type_alerts,
      site_alerts,
      n_fishers_alerts,
      habitat_alerts,
      mesh_alerts,
      gleaners_alerts,
      fuel_alerts
    ) %>%
    purrr::map(~ dplyr::select(.x, -alert_number)) %>%
    purrr::reduce(dplyr::left_join, by = "submission_id") %>%
    dplyr::left_join(landings_ids, by = "submission_id") %>%
    dplyr::mutate(
      species_group = purrr::map(
        .x = .data$species_group, .f = purrr::modify_at,
        .at = "length_individuals",
        purrr::map, dplyr::select,
        length = .data$mean_length,
        individuals = .data$n_individuals,
        weight = .data$weight,
        .data$Selenium_mu:.data$Vitamin_A_mu
      ),
      species_group = purrr::map(
        .x = .data$species_group, .f = dplyr::select,
        catch_taxon = .data$species,
        catch_purpose = .data$food_or_sale,
        length_type = .data$length_type,
        length_frequency = .data$length_individuals
      )
    ) %>%
    dplyr::select(
      landing_id = .data$submission_id,
      landing_date = .data$date,
      tracker_imei = .data$imei,
      trip_duration = .data$trip_duration,
      landing_catch = .data$species_group,
      landing_value = .data$total_catch_value,
      landing_station = .data$station_name,
      reporting_region = .data$reporting_region,
      habitat = .data$habitat_type,
      tidyselect::starts_with("fisher_number"),
      .data$gear_type,
      .data$mesh_size,
      .data$vessel_type,
      .data$n_gleaners,
      .data$fuel
    )

  validated_landings_filename <- paste(pars$surveys$merged_landings$file_prefix,
    "validated",
    sep = "_"
  ) %>%
    add_version(extension = "rds")
  readr::write_rds(
    x = validated_landings,
    file = validated_landings_filename,
    compress = "gz"
  )
  logger::log_info("Uploading {validated_landings_filename} to cloud sorage")
  upload_cloud_file(
    file = validated_landings_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
  # HANDLE FLAGS ------------------------------------------------------------

  alerts <-
    list(
      imei_alerts,
      surveys_time_alerts$validated_dates,
      surveys_time_alerts$validated_duration,
      price_weight_alerts,
      vessel_type_alerts,
      gear_type_alerts,
      site_alerts,
      n_fishers_alerts,
      habitat_alerts,
      mesh_alerts,
      gleaners_alerts
    ) %>%
    purrr::map(~ dplyr::arrange(., .data$submission_id)) %>%
    purrr::map(~ dplyr::select(.x, alert_number, submission_id)) %>%
    dplyr::bind_cols() %>%
    dplyr::select(tidyselect::contains("alert")) %>%
    tidyr::unite(col = "alert", sep = "-", na.rm = TRUE)

  # Wrangle a bot landings, alerts and flags data frames to fit the workflow
  landings_info <-
    landings %>%
    dplyr::rename(
      submission_id = .data$`_id`,
      submission_date = .data$`_submission_time`
    ) %>%
    dplyr::mutate(
      submission_id = as.integer(.data$submission_id),
      submission_date = lubridate::as_date(.data$submission_date)
    ) %>%
    dplyr::select(.data$submission_id, .data$submission_date) %>%
    dplyr::arrange(.data$submission_id)

  ## Google sheets validation pipeline ##
  alerts_df <-
    dplyr::bind_cols(landings_info, alerts) %>%
    dplyr::arrange(.data$submission_date, .data$submission_id) %>%
    dplyr::mutate(
      submission_id = as.integer(.data$submission_id),
      alert = ifelse(.data$alert == "", "0", .data$alert),
      flag_date = lubridate::today("GMT"),
      validated = rep(FALSE, nrow(.)),
      comments = NA_character_,
      validated_when_ymd = NA_real_,
      validated_when_ymd = as.Date(.data$validated_when_ymd)
    ) %>%
    dplyr::select(
      .data$submission_id, .data$submission_date,
      .data$flag_date, .data$alert, .data$validated,
      .data$validated_when_ymd, .data$comments
    )

  logger::log_info("Authenticating for google drive")
  googlesheets4::gs4_auth(
    path = pars$storage$google$options$service_account_key,
    use_oob = TRUE
  )

  logger::log_info("Retriving validation sheet and arrange by submission date")

  peskas_alerts <-
    googlesheets4::range_read(
      ss = pars$validation$google_sheets$sheet_id,
      sheet = pars$validation$google_sheets$flags_table,
      col_types = "iDDclDc"
    )

  logger::log_info("Upload backup validation sheet to GC")
  alerts_filename <-
    pars$validation$google_sheets$file_prefix %>%
    add_version(extension = "rds")
  readr::write_rds(
    x = peskas_alerts,
    file = alerts_filename,
    compress = "gz"
  )
  upload_cloud_file(
    file = alerts_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  new_flags_ids <- setdiff(alerts_df$submission_id, peskas_alerts$submission_id)
  new_flags_obs <- alerts_df %>% dplyr::filter(.data$submission_id %in% new_flags_ids)
  old_flags_df <- alerts_df %>% dplyr::filter(!.data$submission_id %in% new_flags_ids)

  if (nrow(old_flags_df) < nrow(peskas_alerts)) {
    stop("The table is shorter than remote table")
  }

  logger::log_info("Updating flags table")

  sync_table <-
    dplyr::left_join(old_flags_df, peskas_alerts, by = "submission_id") %>%
    dplyr::arrange(
      dplyr::desc(.data$submission_date.x),
      dplyr::desc(.data$submission_id)
    ) %>%
    dplyr::transmute(
      submission_id = .data$submission_id,
      submission_date = .data$submission_date.x,
      flag_date = dplyr::case_when(
        .data$alert.x == .data$alert.y ~
          .data$flag_date.y, TRUE ~ .data$flag_date.x
      ),
      alert = .data$alert.x,
      validated = .data$validated.y,
      validated_when_ymd = .data$validated_when_ymd.y,
      comments = .data$comments.y
    ) %>%
    dplyr::ungroup()

  logger::log_info("New {nrow(new_flags_obs)} submissions flags to upload")
  if (nrow(new_flags_obs) > 0) {
    logger::log_info("Appending new {nrow(new_flags_obs)} flags")

    sync_table <- dplyr::bind_rows(sync_table, new_flags_obs)

    googlesheets4::sheet_write(
      data = sync_table,
      ss = pars$validation$google_sheets$sheet_id,
      sheet = pars$validation$google_sheets$flags_table
    )
  } else {
    logger::log_info("No new flags to append")
  }
}
get_validation_tables <- function(pars) {
  validation_rds <- cloud_object_name(
    prefix = paste(pars$validation$airtable$name, sep = "_"),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$validation$version$preprocess,
    options = pars$storage$google$options
  )
  logger::log_info("Downloading {validation_rds}...")
  download_cloud_file(
    name = validation_rds,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
  readr::read_rds(file = validation_rds)
}

get_preprocessed_landings <- function(pars) {
  landings_rds <- cloud_object_name(
    prefix = paste(pars$surveys$landings$file_prefix, "preprocessed", sep = "_"),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$surveys$landings$version$preprocess,
    options = pars$storage$google$options
  )
  logger::log_info("Downloading {landings_rds}...")
  download_cloud_file(
    name = landings_rds,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
  readr::read_rds(file = landings_rds)
}

get_preprocessed_metadata <- function(pars) {
  metadata_rds <- cloud_object_name(
    prefix = paste(pars$metadata$airtable$name, "preprocessed", sep = "_"),
    provider = pars$storage$google$key,
    extension = "rds",
    options = pars$storage$google$options
  )
  logger::log_info("Downloading {metadata_rds}...")
  download_cloud_file(
    name = metadata_rds,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
  readr::read_rds(file = metadata_rds)
}

#' Download merged landings
#'
#' Download validated surveys landings and PDS trips.
#'
#' @param pars Configuration file.
#' @param suffix A character indicating dataframe version. Use "_weight" to download
#' version with calculated catch weight.
#'
#' @return A dataframe.
#' @export
get_merged_landings <- function(pars, suffix = "") {
  landings_rds <- cloud_object_name(
    prefix = paste0(pars$surveys$merged_landings$file_prefix, suffix),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$surveys$merged_landings$version,
    options = pars$storage$google$options,
    exact_match = TRUE
  )
  logger::log_info("Downloading {landings_rds}...")
  download_cloud_file(
    name = landings_rds,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
  readr::read_rds(file = landings_rds)
}
