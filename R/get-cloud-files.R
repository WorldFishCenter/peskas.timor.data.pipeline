
get_validated_landings <- function(pars){
  cloud_object_name(
    prefix = paste0(pars$surveys$merged_landings$file_prefix, "_", "validated"),
    provider = pars$storage$google$key,
    options = pars$storage$google$options) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options) %>%
    readr::read_rds()
}

get_validated_pds_trips <- function(pars){
  cloud_object_name(
    prefix = paste0(pars$pds$trips$file_prefix, "_", "validated"),
    provider = pars$storage$google$key,
    options = pars$storage$google$options) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options) %>%
    readr::read_rds()
}


get_merged_trips <- function(pars, ...){
  cloud_object_name(
    prefix = paste0(pars$merged_trips$file_prefix),
    provider = pars$storage$google$key,
    options = pars$storage$google$options,
    ...) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options) %>%
    readr::read_rds()
}

get_public_files <- function(pars){
  c("trips", "catch", "aggregated") %>%
    rlang::set_names() %>%
    purrr::map(~ paste0(pars$export$file_prefix, "_", .)) %>%
    purrr::map(
      .f = cloud_object_name,
      extension = "rds",
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options) %>%
    purrr::map(
      .f = download_cloud_file,
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options) %>%
    purrr::map(readr::read_rds)
}

get_models <- function(pars){
  cloud_object_name(
    prefix = paste0(pars$models$file_prefix),
    provider = pars$storage$google$key,
    options = pars$storage$google$options) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options) %>%
    readr::read_rds()
}


get_sync_tracks <- function(pars) {

  full_tracks <- get_full_tracks(pars)

  # check sync status of pds tracks
  updated_trips <-
    get_preprocessed_trips(pars) %>%
    magrittr::extract2("Trip") %>%
    unique()

  check_trips <-
    get_sync_trips(pars) %>%
    magrittr::extract2("Trip") %>%
    unique()

  new_trips <- setdiff(updated_trips, check_trips)

  if (isFALSE(length(new_trips) > 20000)) {

    full_tracks

  } else {

    new_tracks <-
      googleCloudStorageR::gcs_list_objects(pars$pds_storage$google$options$bucket) %>%
      dplyr::mutate(Trip = stringr::str_match(.data$name, "pds-track-*(.*?)\\__")[, 2]) %>%
      dplyr::filter(.data$Trip %in% new_trips)

    get_track <- function(x) {
      track <-
        download_cloud_file(
          name = x,
          provider = pars$pds_storage$google$key,
          options = pars$pds_storage$google$options
        )
      readr::read_rds(track)[+c(1:6)]
    }

    future::plan(future::multisession,
                 workers = pars$pds$tracks$multisession$n_sessions
    )
    new_tracks_batch <-
      furrr::future_map(new_tracks$name, get_track) %>%
      purrr::reduce(dplyr::bind_rows)

    complete_tracks <- dplyr::bind_rows(full_tracks, new_tracks_batch)
    complete_tracks_trips <- unique(full_tracks$Trip)

    ingest_complete_tracks(pars,
                           data = complete_tracks,
                           trips = complete_tracks_trips)

    complete_tracks
  }
}

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

get_sync_trips <- function(pars) {
  cloud_object_name(
    prefix = paste(pars$pds$tracks$complete$file_prefix, "metadata", sep = "_"),
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  ) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    ) %>%
    readr::read_rds()
}
