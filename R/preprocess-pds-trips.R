#' Pre-process Pelagic Data System trips
#'
#' Downloads raw structured data from cloud storage services and pre-process
#' into a binary format that is easier to deal with in R.
#'
#' This function downloads the landings data from a given version (specified in
#' the config file `conf.yml`. The parameters needed are:
#'
#' ```
#' pds:
#'   trips:
#'     token:
#'     secret:
#'     file_prefix:
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
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold
#' @inheritParams ingest_pds_trips
#' @keywords workflow
#' @return no outputs. This function is used for it's side effects
#' @export
#'
preprocess_pds_trips <- function(log_threshold = logger::DEBUG) {

  pars <- read_config()

  pds_trips_csv <- cloud_object_name(prefix = pars$pds$trips$file_prefix,
                                     provider = pars$storage$google$key,
                                     extension = "csv",
                                     version = pars$pds$trips$version$preprocess,
                                     options = pars$storage$google$options)

  logger::log_info("Retrieving {pds_trips_csv}")
  download_cloud_file(name = pds_trips_csv,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)
  pds_trips_raw <- readr::read_csv(
    file = pds_trips_csv,
    col_types = "iTTicccdddccc") %>%
    dplyr::mutate(
      Started = lubridate::with_tz(.data$Started, "Asia/Dili"),
      Ended = lubridate::with_tz(.data$Ended, "Asia/Dili"),
      `Last Seen` = lubridate::as_datetime(.data$`Last Seen`,
                                           format = "%a %b %d %X UTC %Y",
                                           tz = "UTC"))

  preprocessed_filename <- paste(pars$pds$trips$file_prefix, "preprocessed", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(x = pds_trips_raw,
                   file = preprocessed_filename,
                   compress = "gz")

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  upload_cloud_file(file = preprocessed_filename,
                    provider = pars$storage$google$key,
                    options = pars$storage$google$options)
}


#' Extract summaries from pds tracks
#'
#' This function extracts summaries from pds tracks potentially useful to the
#' validation step. The function extracts the `start_end_distance`: the distance
#' between the initial and the final point of a trip, the `outliers_proportion`:
#' the proportion of track points exceeding 30 m/s, the `timetrace_dispersion`:
#' measures the irregularity in the track signal.
#'
#' @param Trip A vector of pds trips to process.
#' @param tracks_list The list of pds tracks files.
#' @param pars The configuration file.
#'
#' @return A dataframe with summaries for each pds trip ID.
#' @export
#'
get_tracks_descriptors <- function(Trip, pars, tracks_list) {
  tracks_descriptors <- data.frame()

  track_id <- paste(pars$pds$tracks$file_prefix, as.character(Trip), sep = "-")
  track_file <- dplyr::filter(tracks_list, grepl(track_id, .data$name)) %>%
    magrittr::extract2("name")

  track <-
    purrr::map(pars$pds_storage, ~ purrr::walk(
      .x = track_file,
      .f = ~ insistent_download_cloud_file(
        name = .,
        provider = pars$pds_storage$google$key,
        options = pars$pds_storage$google$options
      )
    )) %>%
    readr::read_csv(show_col_types = FALSE)

  file.remove(track_file)

  descriptors <-
    data.frame(
      Trip = Trip,
      Boat = unique(track$Boat),
      start_end_distance = geosphere::distm(c(track[1, ]$Lng, track[1, ]$Lat),
        c(track[nrow(track), ]$Lng, track[nrow(track), ]$Lat),
        fun = geosphere::distGeo
      )[1],
      outliers_proportion = dplyr::filter(track, .data$`Speed (M/S)` > 30) %>% nrow() / nrow(track) * 100,
      timetrace_dispersion = sd(diff(track$Time)),
      start_lat = dplyr::first(track$Lat),
      start_lng = dplyr::first(track$Lng),
      end_lat = dplyr::last(track$Lat),
      end_lng = dplyr::last(track$Lng)
    )
  tracks_descriptors <-
    rbind(tracks_descriptors, descriptors) %>%
    tibble::tibble()
}


#' Pre-process Pelagic Data System tracks
#'
#' Downloads raw structured data from cloud storage services and pre-process
#' into a binary format that is easier to deal with in R.
#'
#' The parameters needed are:
#'
#' ```
#' pds:
#'  tracks:
#'   token: !expr Sys.getenv('PDS_TOKEN')
#'   secret: !expr Sys.getenv('PDS_SECRET')
#'   file_prefix:
#' pds_storage:
#'   google:
#'    options:
#'     bucket: pds-timor

#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold
#' @inheritParams ingest_pds_tracks
#' @keywords workflow
#' @return no outputs. This function is used for it's side effects
#' @export
#'

preprocess_pds_tracks <- function(log_threshold = logger::DEBUG) {
  pars <- read_config()

  pds_trips <- get_preprocessed_trips(pars)
  tracks_list <- googleCloudStorageR::gcs_list_objects(pars$pds_storage$google$options$bucket)

  list_prep_tracks <-
    googleCloudStorageR::gcs_list_objects(pars$storage$google$options$bucket) %>%
    dplyr::filter(grepl(
      paste(pars$pds$tracks$file_prefix, "preprocessed", sep = "_"),
      .data$name
    ))

  future::plan(future::multisession,
               workers = pars$pds$tracks$multisession$n_sessions)

  if (nrow(list_prep_tracks) == 0) {
    tracks_to_download <- unique(pds_trips$Trip)
    tracks_descriptors <-
      furrr::future_map_dfr(tracks_to_download, get_tracks_descriptors, pars, tracks_list, .progress = TRUE)
  } else {
    # add function to save and read the already-processed tracks
    preprocessed_tracks <- get_preprocessed_tracks(pars)

    # filter the new trips
    pds_trips <-
      pds_trips %>%
      dplyr::filter(!.data$Trip %in% unique(preprocessed_tracks$Trip))

    tracks_to_download <- unique(pds_trips$Trip)
    tracks_descriptors <-
      furrr::future_map_dfr(tracks_to_download, get_tracks_descriptors, pars, tracks_list, .progress = TRUE)

    # bind new tracks_descriptors with latest ones
    tracks_descriptors <- dplyr::bind_rows(preprocessed_tracks, tracks_descriptors)
  }

  preprocessed_filename <- paste(pars$pds$tracks$file_prefix, "preprocessed", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(
    x = tracks_descriptors,
    file = preprocessed_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {preprocessed_filename} to cloud...")
  # Iterate over multiple storage providers if there are more than one
  purrr::map(pars$storage, ~ purrr::walk(
    .x = preprocessed_filename,
    .f = ~ insistent_upload_cloud_file(
      file = .,
      provider = pars$storage$google$key,
      options = pars$storage$google$options)))
  logger::log_success("File upload succeded")

}

# Download preprocessed tracks
get_preprocessed_tracks <- function(pars){
  pds_tracks_rds <- cloud_object_name(
    prefix = paste(pars$pds$tracks$file_prefix, 'preprocessed', sep = "_"),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$pds$tracks$version$preprocess,
    options = pars$storage$google$options)
  logger::log_info("Downloading {pds_tracks_rds}...")
  download_cloud_file(name = pds_tracks_rds,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)
  readr::read_rds(file = pds_tracks_rds)
}
