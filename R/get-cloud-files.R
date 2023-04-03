#' Download Peskas validated landings
#'
#' Download validated Peskas data from Google Cloud.
#'
#' @param log_threshold
#' @inheritParams ingest_landings
#'
#' @return A dataframe of validated survey landings.
#' @export
#'
get_validated_landings <- function(log_threshold = logger::DEBUG) {
  pars <- read_config()

  landings_rds <- cloud_object_name(
    prefix = paste(pars$surveys$validated_landings$file_prefix),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$surveys$validated_landings$version,
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

get_validated_pds_trips <- function(pars) {
  cloud_object_name(
    prefix = paste0(pars$pds$trips$file_prefix, "_", "validated"),
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  ) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    ) %>%
    readr::read_rds()
}


get_merged_trips <- function(pars, ...) {
  cloud_object_name(
    prefix = paste0(pars$merged_trips$file_prefix),
    provider = pars$storage$google$key,
    options = pars$storage$google$options,
    ...
  ) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    ) %>%
    readr::read_rds()
}

get_public_files <- function(pars) {
  c("trips", "catch", "aggregated") %>%
    rlang::set_names() %>%
    purrr::map(~ paste0(pars$export$file_prefix, "_", .)) %>%
    purrr::map(
      .f = cloud_object_name,
      extension = "rds",
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options
    ) %>%
    purrr::map(
      .f = download_cloud_file,
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options
    ) %>%
    purrr::map(readr::read_rds)
}

#' Download models estimates
#'
#' @param pars Configuration file
#'
#' @return A list of dataframes with national and municipal estimations
#' @export
#'
get_models <- function(pars) {
  cloud_object_name(
    prefix = paste0(pars$models$file_prefix),
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  ) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    ) %>%
    readr::read_rds()
}

#' Download Peskas metadata
#'
#' Download preprocessed Peskas metadata from Google Cloud
#'
#' @param pars The configuration file
#' @export
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

#' Download and synchronize tracks data in a single file
#'
#' This function downloads, synchronize and ingest tracks data from cloud
#' storage in a single file. Since the transferred data is relatively large,
#' the synchronization and uploading are refreshed when the complete track file
#' is outdated by at least 5000 trips.
#'
#' @param pars The configuration file.
#'
#' @return The tabulated tracks collected in Timor..
#' @export
#'
get_sync_tracks <- function(pars) {
  logger::log_info("Downloading all tracks...")
  full_tracks <- get_full_tracks(pars)
  # remove after loading to save memory
  file.remove(list.files(pattern = "pds-track-complete__"))

  logger::log_info("Checking sync status...")
  updated_trips <-
    get_preprocessed_trips(pars) %>%
    magrittr::extract2("Trip") %>%
    unique()
  # remove after loading to save memory
  file.remove(list.files(pattern = "pds-trips-preprocessed__"))


  check_trips <-
    get_full_trips(pars)

  new_trips <- setdiff(updated_trips, check_trips)

  if (isTRUE(length(new_trips) < pars$pds$tracks$complete$new_trips)) {
    full_tracks
  } else {
    logger::log_info("Syncing {length(new_trips)} tracks...")
    new_tracks <-
      get_tracks_ids(pars) %>%
      dplyr::filter(.data$Trip %in% new_trips)

    get_track <- function(x) {
      track <-
        download_cloud_file(
          name = x,
          provider = pars$pds_storage$google$key,
          options = pars$pds_storage$google$options
        )
      readr::read_csv(track, show_col_types = FALSE)[+c(3:5)]
    }

    logger::log_info("Donwloading and binding the new tracks...")
    new_tracks_batch <-
      purrr::map(new_tracks$name, get_track) %>%
      purrr::reduce(dplyr::bind_rows)

    complete_tracks <- dplyr::bind_rows(full_tracks, new_tracks_batch)
    complete_tracks_trips <- unique(full_tracks$Trip)

    logger::log_info("Uploading new file to cloud...")
    ingest_complete_tracks(pars,
      data = complete_tracks,
      trips = complete_tracks_trips
    )

    complete_tracks
  }
}

#' Download tracks data in a single file
#'
#' Download the latest version of PDS single-file tracks data.
#'
#' @param pars The configuration file.
#'
#' @return A dataframes with tracks coordinates for each trip
#' @export
#'
get_full_tracks <- function(pars) {
  cloud_object_name(
    prefix = paste(pars$pds$tracks$complete$file_prefix, sep = "_"),
    provider = pars$storage$google$key,
    options = pars$storage$google$options,
    exact_match = TRUE
  ) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    ) %>%
    readr::read_rds()
}

#' Get trips from single-file tracks data.
#'
#' Download the list of trips from the latest version of PDS single-file
#' tracks data.
#'
#' @param pars the configuration file.
#'
#' @return A vector of unique trips.
#' @export
#'
get_full_trips <- function(pars) {
  cloud_object_name(
    prefix = paste(pars$pds$tracks$complete$file_prefix, "trips", sep = "_"),
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  ) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    ) %>%
    readr::read_rds()
}

#' Get map of Timor pds tracks.
#'
#' Download the static map of Timor pds tracks.
#'
#' @param pars the configuration file.
#'
#' @export
#'
get_tracks_map <- function(pars) {
  download_cloud_file(
    name = paste(pars$pds$tracks$map$png$file_prefix, pars$pds$tracks$map$png$extension, sep = "."),
    provider = pars$public_storage$google$key,
    options = pars$public_storage$google$options
  )
}

#' Get pds IDs
#'
#' Get the list of pds-tracks IDs stored in the pds bucket
#'
#' @param pars the configuration file.
#'
#' @export
#'
get_tracks_ids <- function(pars) {
  cloud_object_name(
    prefix = pars$pds$tracks$bucket_content$file_prefix,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  ) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    ) %>%
    readr::read_rds()
}

#' Get peskas validation sheet
#'
#' Get the peskas validation backup sheet from google cloud
#'
#' @param pars the configuration file.
#'
#' @export
#'
get_validation_sheet <- function(pars) {
  cloud_object_name(
    prefix = pars$validation$google_sheets$file_prefix,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  ) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    ) %>%
    readr::read_rds()
}
