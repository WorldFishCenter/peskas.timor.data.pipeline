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
                                           tz = "UTC")) %>%
    associate_pds_trips()

  preprocessed_filename <- paste(pars$pds$trips$file_prefix, "preprocessed", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(x = pds_trips_raw,
                   file = preprocessed_filename,
                   compress = "gz")

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  upload_cloud_file(file = preprocessed_filename,
                    provider = pars$storage$google$key,
                    options = pars$storage$google$options)

  preprocess_pds_tracks()
}

#' Label short interval trips
#'
#' The pds data are quite patchy and sometimes a single trip is recorded as 2
#' different trips. This function calculates the time interval between the end
#' and the start of a trip for each boat and labels the trips that start less
#' than 35 minutes from the other.
#'
#' @param x A data frame containing raw pds trips data..
#'
#' @return A dataframe with pds trips including a new column `associated_to`
#' indicating the trips to be potentially merged.
#'
#' @importFrom rlang .data
#' @export
#'
associate_pds_trips <- function(x) {
  x %>%
    dplyr::arrange(.data$Boat, .data$Started) %>%
    dplyr::group_by(.data$Boat) %>%
    dplyr::mutate(start_end_diff = difftime(dplyr::lead(.data$Started),
                                            .data$Ended, units = "mins")) %>%
    dplyr::mutate(
      associated_to = dplyr::case_when(
        start_end_diff < 35 ~ .data$Trip,
        TRUE ~ NA_integer_
      ),
      associated_to = dplyr::lag(.data$associated_to),
      associated_to = dplyr::coalesce(.data$associated_to, .data$Trip)
    ) %>%
    dplyr::ungroup()
}


#' Extract summaries from pds tracks
#'
#' This function extracts summaries from pds tracks potentially useful to the
#' validation step. The function extracts the `start_end_distance`: the distance
#' between the initial and the final point of a trip, the `outliers_proportion`:
#' the proportion of track points exceeding 30 m/s, the `timetrace_dispersion`:
#' measures the irregularity in the track signal.
#'
#' @param pds_trips The table of pds trips.
#' @param pds_tracks The list of pds tracks files.
#' @param pars The configuration file.
#'
#' @return A dataframe with summaries for each pds trip ID.
#' @export
#'
get_tracks_descriptors <- function(pars, pds_trips = NULL, pds_tracks = NULL) {
  tracks_descriptors <- data.frame()

  for (i in c(pds_trips$Trip)) {
    Trip <- i
    track_id <- paste(pars$pds$tracks$file_prefix, as.character(Trip), sep = "-")
    track_file <- dplyr::filter(pds_tracks, grepl(track_id, .data$name)) %>%
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
        start_end_distance = geosphere::distm(c(track[1, ]$Lng, track[1, ]$Lat),
          c(track[nrow(track), ]$Lng, track[nrow(track), ]$Lat),
          fun = geosphere::distGeo
        )[1],
        outliers_proportion = dplyr::filter(track, .data$`Speed (M/S)` > 30) %>% nrow() / nrow(track) * 100,
        timetrace_dispersion = sd(diff(track$Time))
      )
    tracks_descriptors <- rbind(tracks_descriptors, descriptors)
  }
  tibble::as_tibble(tracks_descriptors)
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

  logger::log_info("Retrieving pds tracks list")
  tracks_list <- googleCloudStorageR::gcs_list_objects(pars$pds_storage$google$options$bucket)

  tracks_descriptors <-
    get_tracks_descriptors(pars, pds_trips = pds_trips, pds_tracks = tracks_list)

  preprocessed_filename <- paste(pars$pds$tracks$file_prefix, "preprocessed", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(
    x = tracks_descriptors,
    file = preprocessed_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  upload_cloud_file(
    file = preprocessed_filename,
    provider = pars$pds_storage$google$key,
    options = pars$pds_storage$google$options
  )
}
