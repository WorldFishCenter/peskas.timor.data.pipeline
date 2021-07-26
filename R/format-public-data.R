#'Format public data
#'
#'Format the merged trips into files that are fit for distribution.
#'Specifically, this function produces a trips and a catch table as well as
#'tables containing aggregated data. These files are stored both as tsv files
#'and rds in a storage bucket with public access.
#'
#'#'The parameters needed in the config file are those required for
#'`merge_trips()` in addition to:
#'
#'```
#'public_storage:
#'  google:
#'    key: gcs
#'    options:
#'      project:
#'      bucket:
#'      service_account_key:
#'export:
#'  file_prefix:
#'```
#'@param log_threshold
#' @inheritParams ingest_landings
#'@keywords workflow
#'@return no outputs. This function is used for it's side effects
#'@export
#'
format_public_data <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)
  pars <- read_config()

  logger::log_info("Retrieving merged trips...")
  merged_trips <- get_merged_trips(pars)

  logger::log_info("Calculating summary fields")
  merged_trips_with_addons <- add_calculated_fields(merged_trips)

  logger::log_info("Creating trips table")
  trips_table <- get_trips_table(merged_trips_with_addons)
  logger::log_info("Creating catch table")
  catch_table <- get_catch_table(merged_trips_with_addons)

  logger::log_info("Aggregating data")
  aggregated <- c("day", "week", "month", "year") %>%
    rlang::set_names() %>%
    purrr::map(summarise_trips, merged_trips_with_addons)

  logger::log_info("Saving and exporting public data as tsv")
  tsv_filenames <- c("day", "week", "month", "year") %>%
    paste0("aggregated-", .) %>%
    c('trips', "catch", .) %>%
    paste0(pars$export$file_prefix, "_", .) %>%
    purrr::map_chr(add_version, extension = "tsv")

  tsv_filenames %T>%
    purrr::walk2(c(list(trips_table, catch_table), aggregated),
                 ~ readr::write_tsv(.y, .x)) %>%
    purrr::walk(upload_cloud_file,
                provider = pars$public_storage$google$key,
                options = pars$public_storage$google$options)

  logger::log_info("Saving and exporting public data as rds")
  c('trips', "catch", "aggregated") %>%
    paste0(pars$export$file_prefix, "_", .) %>%
    purrr::map_chr(add_version, extension = "rds") %T>%
    purrr::walk2(list(trips_table, catch_table, aggregated),
                 ~ readr::write_rds(.y, .x, compress = "gz")) %>%
    purrr::walk(upload_cloud_file,
                provider = pars$public_storage$google$key,
                options = pars$public_storage$google$options)

}

#' @importFrom rlang .data
#' @importFrom digest digest
#' @importFrom stats na.omit
add_calculated_fields <- function(merged_trips){

  # Helper functions
  count_taxa <- function(x){
    if (is.null(x)) return(NA)
    x <- x$catch_taxon
    taxa_excluding_no_catch <- x[x != "0"]
    dplyr::n_distinct(taxa_excluding_no_catch, na.rm = TRUE)
  }
  collapse_taxa <- function(x){
    if (is.null(x)) return(NA)
    x$catch_taxon %>% na.omit() %>% sort() %>% paste(collapse = " ")
  }

  # A dataset with extra calculated fields
  merged_trips %>%
    dplyr::rowwise() %>%
    # get a random (but reproducible) ID for each trip based on a hash of the
    # landing and the pds trip id
    dplyr::mutate(trip_id = digest::digest(
      object = paste(.data$landing_id, .data$tracker_trip_id),
      serialize = FALSE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(n_taxa = purrr::map_int(.data$landing_catch, count_taxa)) %>%
    dplyr::mutate(taxa = purrr::map_chr(.data$landing_catch, collapse_taxa)) %>%
    dplyr::mutate(landing_date = lubridate::as_date(.data$landing_date))
}

#'@importFrom rlang .data
get_trips_table <- function(merged_trips_with_addons){

  merged_trips_with_addons %>%
    dplyr::select(
      .data$trip_id,
      .data$landing_date,
      landing_n_taxa = .data$n_taxa,
      landing_taxa = .data$taxa,
      .data$landing_value,
      landing_survey_trip_duration = .data$trip_duration,
      .data$tracker_trip_start,
      .data$tracker_trip_end,
    )
}

#'@importFrom rlang .data
get_catch_table <- function(merged_trips_with_addons){
  merged_trips_with_addons %>%
    dplyr::select(.data$trip_id, .data$landing_catch) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency)
}

#'@importFrom rlang .data
summarise_trips <- function(bin_unit = "month", merged_trips_with_addons){

  # We need to count landings and tracks separately because they have different
  # base dates
  landing_date_bin <- merged_trips_with_addons %>%
    dplyr::mutate(date_bin_start = lubridate::floor_date(.data$landing_date,
                                                         bin_unit,
                                                         week_start = 7)) %>%
    dplyr::group_by(.data$date_bin_start) %>%
    dplyr::summarise(n_landings = dplyr::n_distinct(.data$landing_id, na.rm = T))

   track_end_bin <- merged_trips_with_addons %>%
     dplyr::mutate(tracker_trip_end = lubridate::as_date(.data$tracker_trip_end),
                   date_bin_start = lubridate::floor_date(.data$tracker_trip_end,
                                                          bin_unit,
                                                          week_start = 7)) %>%
     dplyr::group_by(.data$date_bin_start) %>%
     dplyr::summarise(n_tracks = dplyr::n_distinct(.data$tracker_trip_id,
                                                   na.rm = T))

   matched_date_bin <- merged_trips_with_addons %>%
     dplyr::mutate(date_bin_start = lubridate::floor_date(.data$landing_date,
                                                          bin_unit,
                                                          week_start = 7)) %>%
     dplyr::group_by(.data$date_bin_start) %>%
     dplyr::summarise(n_matched = sum(!is.na(.data$landing_id) &
                                        !is.na(.data$tracker_trip_id)))

   landing_date_bin %>%
     dplyr::full_join(track_end_bin, by = "date_bin_start") %>%
     dplyr::full_join(matched_date_bin, by = "date_bin_start") %>%
     # Counts that are NA are really zero here
     dplyr::mutate(dplyr::across(dplyr::starts_with("n_"),
                                 tidyr::replace_na, replace = 0)) %>%
     dplyr::mutate(prop_matched = .data$n_matched /
                     (.data$n_landings + .data$n_tracks - .data$n_matched)) %>%
     # For the data export we need to optimise a bit to minimise space use
     dplyr::mutate(prop_matched = round(.data$prop_matched, 4),
                   dplyr::across(dplyr::starts_with("n_"), as.integer)) %>%
     dplyr::arrange(dplyr::desc(.data$date_bin_start))
}
