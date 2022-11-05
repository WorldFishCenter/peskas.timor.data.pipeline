#' Merge trips
#'
#' Merges validated landings and the validated pds trips into one single data
#' frame. Merging is done only when there is a single landing and tracked trip
#' per day. Days are determined using the end of the tracking trip.
#'
#' The parameters needed in the config file are those required for
#' `validate_landings()`, and `validate_pds_trips()`combined, as well as
#' merged_trips.
#'
#' @param log_threshold
#' @keywords workflow
#' @return no outputs. This function is used for it's side effects
#'
#' @importFrom rlang .data
#' @export
#'
merge_trips <- function() {
  pars <- read_config()
  logger::log_info("Retrieving validated landings...")
  validated_landings <- get_validated_landings(pars)
  logger::log_info("Retrieving validated pds trips...")
  validated_pds_trips <- get_validated_pds_trips(pars)

  logger::log_info("Preparing datasets...")
  landings <- validated_landings %>%
    dplyr::group_by(.data$landing_date, .data$tracker_imei) %>%
    dplyr::mutate(unique_trip_per_day = dplyr::n() == 1) %>%
    dplyr::ungroup() %>%
    split(.$unique_trip_per_day)

  pds <- validated_pds_trips %>%
    # We assume the landing date to be the same as the date when the trip ended
    dplyr::mutate(landing_date = lubridate::as_date(.data$tracker_trip_end)) %>%
    dplyr::group_by(.data$landing_date, .data$tracker_imei) %>%
    dplyr::mutate(unique_trip_per_day = dplyr::n() == 1) %>%
    dplyr::ungroup() %>%
    split(.$unique_trip_per_day)

  logger::log_info("Merging datasets datasets...")
  # Only join when we have one landing and one tracking per day, otherwise we
  # cannot do guarantee that the landing corresponds to a trip
  merged_trips <- dplyr::full_join(
    landings$`TRUE`,
    pds$`TRUE`,
    by = c("landing_date", "tracker_imei", "unique_trip_per_day")
  ) %>%
    dplyr::bind_rows(landings$`FALSE`) %>%
    dplyr::bind_rows(pds$`FALSE`) %>%
    dplyr::select(-.data$unique_trip_per_day)

  merged_trips_filename <- pars$merged_trips$file_prefix %>%
    add_version(extension = "rds")

  readr::write_rds(
    x = merged_trips,
    file = merged_trips_filename,
    compress = "gz"
  )
  logger::log_info("Uploading {merged_trips_filename} to cloud storage")
  upload_cloud_file(
    file = merged_trips_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}


#' Ingest PDS matched trip
#'
#' Ingest matched PDS tracks and landing surveys in a compressed zip folder.
#' GPS tracks file are uploaded in two files: one using the original temporal
#' resolution (1 sec recording frequency) and one using a lower temporal
#' resolution based on a 1 minute scale.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#'
#' @return no outputs. This function is used for it's side effects
#' @importFrom rlang .data
#'
#' @export
#'
ingest_pds_matched_trips <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  logger::log_info("Downloading all trips")
  pars <- read_config()
  trips <- get_merged_trips(pars)

  # prepare and clean data
  trips_unnested <-
    trips %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::filter(!is.na(.data$landing_id) & !is.na(.data$tracker_trip_id) & !is.na(.data$catch_taxon)) %>%
    dplyr::select(
      .data$landing_id, .data$tracker_trip_id, .data$habitat, .data$reporting_region,
      .data$landing_station, .data$gear_type:.data$vessel_type, .data$trip_duration:.data$catch_purpose,
      .data$length:.data$weight, .data$landing_value
    ) %>%
    dplyr::filter(!is.na(.data$weight) & !is.na(.data$landing_value))

  # get trips with successful chatches
  trips_catches <-
    trips_unnested %>%
    dplyr::filter(.data$individuals > 0)

  # get trips with unsuccessful catches
  trips_no_catches <-
    trips_unnested %>%
    dplyr::filter(.data$catch_taxon == "0") %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::sample_n(1)

  # bind successful and unsuccessful trips
  matched_pds_landings <- dplyr::bind_rows(trips_catches, trips_no_catches)

  tracks_list <-
    googleCloudStorageR::gcs_list_objects(pars$pds_storage$google$options$bucket) %>%
    dplyr::mutate(trip = stringr::str_extract_all(.data$name, "(?<=pds-track-).+(?=__20)", simplify = T)) %>%
    dplyr::filter(.data$trip %in% matched_pds_landings$tracker_trip_id) %>%
    magrittr::extract2("name")

  logger::log_info("Downloading matched pds tracks")
  matched_pds_tracks <-
    purrr::map(
      tracks_list,
      download_cloud_file,
      pars$pds_storage$google$key,
      pars$pds_storage$google$options
    ) %>%
    readr::read_csv() %>%
    dplyr::bind_rows()

  logger::log_info("Generating low resolution tracks")
  matched_pds_tracks_reduced <-
    matched_pds_tracks %>%
    dplyr::mutate(Time = lubridate::floor_date(.data$Time, unit = "minute")) %>%
    dplyr::group_by(.data$Trip, .data$Time) %>%
    dplyr::summarise(
      Boat = dplyr::first(.data$Boat),
      Lat = dplyr::first(.data$Lat),
      Lng = dplyr::first(.data$Lng),
      `Speed (M/S)` = mean(.data$`Speed (M/S)`, na.rm = T),
      `Range (Meters)` = dplyr::first(.data$`Range (Meters)`),
      Heading = dplyr::first(.data$Heading),
      `Boat Name` = dplyr::first(.data$`Boat Name`),
      Community = dplyr::first(.data$Community)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(.data$Time, .data$Boat, .data$Trip, tidyselect::everything())


  readr::write_csv(matched_pds_tracks, "matched_pds_tracks.csv")
  readr::write_csv(matched_pds_tracks_reduced, "matched_pds_tracks_reduced_1min.csv")
  readr::write_csv(matched_pds_landings, "matched_pds_landings.csv")

  logger::log_info("Compressing files")
  zip::zip(
    zipfile = "matched_tracks_landings.zip",
    compression_level = 9,
    include_directories = FALSE,
    files = c(
      "matched_pds_tracks.csv",
      "matched_pds_tracks_reduced_1min.csv",
      "matched_pds_landings.csv"
    )
  )

  # upload
  logger::log_info("Uploading zip folder to cloud storage")
  upload_cloud_file(
    file = "matched_tracks_landings.zip",
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}
