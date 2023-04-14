#' Validate Pelagic Data System trips
#'
#' Downloads the preprocessed version of pds trips and pds tracks disgnostics
#' from cloud storage services and validates a range of information so that it
#' can be safely used for analysis.
#'
#' The parameters needed in the config file are those required for
#' `preprocess_pds_trips()`, as well as parameters needed to identify anomalous
#' trips.
#'
#' @param log_threshold
#' @inheritParams ingest_pds_trips
#'
#' @return no outputs. This function is used for it's side effects
#' @export
#' @importFrom rlang .data
#'
validate_pds_trips <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()
  pds_trips <- get_preprocessed_trips(pars)
  pds_tracks <- get_preprocessed_tracks(pars)


  # call validation coefficients
  max_hrs <- pars$validation$pds_trips$max_trip_hours
  min_hrs <- pars$validation$pds_trips$min_trip_hours
  km <- pars$validation$pds_trips$trip_km
  se_km <- pars$validation$pds_trips$start_end_km
  outl <- pars$validation$pds_trips$outliers
  timet <- pars$validation$pds_trips$timetrace
  consecutive_time <- pars$validation$pds_trips$consecutive_time
  consecutive_distance <- pars$validation$pds_trips$consecutive_distance


  # remove duplicated trips, join trips with tracks diagnostics and merge consecutive trips
  pds_full <-
    pds_trips %>%
    dplyr::arrange(dplyr::desc(.data$Trip)) %>%
    dplyr::distinct(pds_trips, dplyr::across(-.data$Trip), .keep_all = TRUE) %>%
    dplyr::filter(.data$`Last Seen` > .data$Ended & .data$`Last Seen` > .data$Started) %>%
    dplyr::arrange(.data$Boat, .data$Started) %>%
    dplyr::group_by(.data$Boat) %>%
    dplyr::mutate(
      end_start_time = difftime(dplyr::lead(.data$Started),
        .data$Ended,
        units = "mins"
      ),
      end_start_time = as.double(.data$end_start_time)
    ) %>%
    dplyr::left_join(pds_tracks, by = c("Trip", "Boat")) %>%
    dplyr::ungroup() %>%
    merge_consecutive_trips(
      consecutive_time = consecutive_time,
      consecutive_distance = consecutive_distance
    )

  logger::log_info("Validating pds trips...")
  pds_alerts <- validate_pds_data(pds_full,
    max_hrs = max_hrs,
    min_hrs = min_hrs,
    km = km,
    se_km = se_km,
    outl = outl,
    timet = timet
  )

  ready_cols <- pds_full %>%
    dplyr::select(
      .data$`Last Seen`,
      .data$IMEI,
      .data$Trip,
      .data$`Device Id`
    )

  validated_trips <-
    list(
      pds_alerts$validated_pds_duration,
      pds_alerts$validated_pds_distance,
      pds_alerts$validated_pds_quality
    ) %>%
    purrr::map(~ dplyr::select(.x, -alert_number)) %>%
    purrr::reduce(dplyr::left_join) %>%
    dplyr::left_join(ready_cols) %>%
    # rename columns that better align with the ontology terms
    dplyr::rename(
      tracker_trip_duration = .data$`Duration (Seconds)`,
      tracker_trip_start = .data$Started,
      tracker_trip_end = .data$Ended,
      tracker_trip_id = .data$Trip,
      tracker_imei = .data$IMEI,
      tracker_device_id = .data$`Device Id`,
      tracker_last_seen = .data$`Last Seen`,
      tracker_trip_distance = .data$`Distance (Meters)`
    )

  validated_trips_filename <- paste(pars$pds$trips$file_prefix,
    "validated",
    sep = "_"
  ) %>%
    add_version(extension = "rds")

  readr::write_rds(
    x = validated_trips,
    file = validated_trips_filename, compress = "gz"
  )

  logger::log_info("Uploading {validated_trips_filename} to cloud sorage")
  upload_cloud_file(
    file = validated_trips_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
  logger::log_success("File upload succeded")
}


#' Validate pds trips duration and distance
#'
#' This function takes pds trips data and returns validated
#' trip duration and distance.
#'
#' @param data A data frame containing pds trips
#' @param max_hrs Upper limit of trip duration (hours) to be considered a valid
#' catch session.
#' @param min_hrs Lower limit of trip duration (hours) to be considered a valid
#' catch session.
#' @param km Limit of trip distance traveled (Km) to be considered a valid
#' catch session.
#' @param se_km Distance between the start and end point of a trip to be
#' considered a valid catch session.
#' @param outl Limit of speed outlier points in each trip to be considered a
#' good quality pds track.
#' @param timet Limit of signal trace dispersion to be considered a
#' good quality pds track.
#'
#' @return A list containing data frames with validated catch duration and
#' catch distance traveled
#'
#' @export
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#' pars <- read_config()
#' pds_trips <- get_preprocessed_trips()
#' validate_pds(pds_trips)
#' }
#'
validate_pds_data <- function(data,
                              max_hrs = NULL,
                              min_hrs = NULL,
                              km = NULL,
                              se_km = NULL,
                              outl = NULL,
                              timet = NULL) {
  validated_pds_list <- list(
    validated_pds_duration = data %>%
      dplyr::transmute(
        alert_number =
        # test if trip duration is longer or shorter than n hours
          dplyr::case_when(.data$`Duration (Seconds)` > max_hrs * 60^2 |
            .data$`Duration (Seconds)` < min_hrs * 60^2 ~ 8, TRUE ~ NA_real_),
        `Duration (Seconds)` = dplyr::case_when(is.na(.data$alert_number) ~
          .data$`Duration (Seconds)`, TRUE ~ NA_real_),
        Started = ifelse(is.na(.data$alert_number), .data$Started, NA_real_),
        Ended = ifelse(is.na(.data$alert_number), .data$Ended, NA_real_),
        .data$Trip
      ) %>%
      dplyr::mutate(dplyr::across(
        .cols = c(.data$Started, .data$Ended),
        ~ lubridate::as_datetime(.x, tz = "Asia/Dili")
      )),
    validated_pds_distance = data %>%
      dplyr::transmute(
        alert_number =
          dplyr::case_when(
            # test if trip distance is too long and if it's a one-way trip
            .data$`Distance (Meters)` > km * 1000 ~ 9, TRUE ~ NA_real_,
            .data$start_end_distance > se_km * 100 ~ 12, TRUE ~ NA_real_
          ),
        `Distance (Meters)` = dplyr::case_when(
          .data$`Distance (Meters)` > km * 1000 ~ NA_real_,
          TRUE ~ .data$`Distance (Meters)`
        ),
        Trip = .data$Trip
      ),
    validated_pds_quality = data %>%
      dplyr::transmute(
        alert_number =
        # test quality of trips
          dplyr::case_when(.data$outliers_proportion > outl |
            .data$timetrace_dispersion > timet ~ 13, TRUE ~ NA_real_),
        `Distance (Meters)` = dplyr::case_when(is.na(.data$alert_number) ~ .data$`Distance (Meters)`, TRUE ~ NA_real_),
        Started = ifelse(is.na(.data$alert_number), .data$Started, NA_real_),
        Ended = ifelse(is.na(.data$alert_number), .data$Ended, NA_real_),
        .data$Trip
      ) %>%
      dplyr::mutate(dplyr::across(
        .cols = c(.data$Started, .data$Ended),
        ~ lubridate::as_datetime(.x, tz = "Asia/Dili")
      ))
  )
  validated_pds_list
}

get_preprocessed_trips <- function(pars) {
  pds_trips_rds <- cloud_object_name(
    prefix = paste(pars$pds$trips$file_prefix, "preprocessed", sep = "_"),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$pds$trips$version$preprocess,
    options = pars$storage$google$options
  )
  logger::log_info("Downloading {pds_trips_rds}...")
  download_cloud_file(
    name = pds_trips_rds,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
  readr::read_rds(file = pds_trips_rds)
}


#' Merge short intervalled trips
#'
#' PDS data are quite patchy, sometimes an actual single trip is recorded as 2
#' different trips. This function calculates the temporal and the geographical
#' distance between two consecutive trips for each boat, merging those consecutive
#' trips that start before 180 minutes from the following within a range of 2 km.
#'
#' @param x A data frame containing raw pds trips data.
#' @param consecutive_time Time between consecutive trips (hrs).
#' @param consecutive_distance Distance between consecutive trips (Km).
#'
#' @return A dataframe with pds trips including a new column `associated_to`
#' indicating the consecutive trips to be merged.
#'
#' @importFrom rlang .data
#' @export
#'

merge_consecutive_trips <- function(x,
                                    consecutive_time = NULL,
                                    consecutive_distance = NULL) {
  x %>%
    dplyr::mutate(
      end_start_distance = get_distance(.),
      end_start_distance = dplyr::case_when(
        is.na(.data$end_start_time) ~ NA_real_,
        TRUE ~ .data$end_start_distance
      )
    ) %>%
    dplyr::group_by(.data$Boat) %>%
    dplyr::mutate(start_end_diff = difftime(dplyr::lead(.data$Started),
      .data$Ended,
      units = "mins"
    )) %>%
    dplyr::mutate(
      associated_to = dplyr::case_when(
        .data$end_start_time < consecutive_time * 60 &
          .data$end_start_distance < consecutive_distance * 1000 ~ .data$Trip,
        TRUE ~ NA_integer_
      ),
      associated_to = dplyr::lag(.data$associated_to),
      associated_to = dplyr::coalesce(.data$associated_to, .data$Trip)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(.data$Trip:.data$end_lng, .data$associated_to) %>%
    dplyr::group_by(.data$associated_to) %>%
    dplyr::summarise(
      Trip = dplyr::first(.data$Trip),
      Started = dplyr::first(.data$Started),
      Ended = dplyr::last(.data$Ended),
      Boat = dplyr::first(.data$Boat),
      `Boat Name` = dplyr::first(.data$`Boat Name`),
      `Boat Gear` = dplyr::first(.data$`Boat Gear`),
      Community = dplyr::first(.data$Community),
      `Duration (Seconds)` = lubridate::int_length(
        lubridate::interval(
          dplyr::first(.data$Started), dplyr::last(.data$Ended)
        )
      ),
      `Range (Meters)` = sum(.data$`Range (Meters)`),
      `Distance (Meters)` = sum(.data$`Distance (Meters)`),
      IMEI = dplyr::first(.data$IMEI),
      `Device Id` = dplyr::first(.data$`Device Id`),
      `Last Seen` = dplyr::last(.data$`Last Seen`),
      outliers_proportion = max(.data$outliers_proportion),
      timetrace_dispersion = max(.data$timetrace_dispersion),
      start_lat = dplyr::first(.data$start_lat),
      start_lng = dplyr::first(.data$start_lng),
      end_lat = dplyr::last(.data$end_lat),
      end_lng = dplyr::last(.data$end_lng)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-.data$associated_to) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      start_end_distance =
        geosphere::distm(
          x = cbind(.data$start_lng, .data$start_lat),
          y = cbind(.data$end_lng, .data$end_lat),
          fun = geosphere::distGeo
        )[, 1]
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-c(.data$start_lat:.data$end_lng))
}

#' Estimate distance between consecutive trips
#'
#' This function calculates the geographical distance in meters between the end
#' and the start of two consecutive trips.
#'
#' @param x A data frame containing trips coordinates.
#'
#' @return A vector of distances in meters
#' @export
#'
get_distance <- function(x) {
  dist <- NULL
  for (i in 1:nrow(x)) {
    dat <- x[i:(i + 1), ]
    dist_row <-
      geosphere::distm(
        x = cbind(dat[1, ]$end_lng, dat[1, ]$end_lat),
        y = cbind(dat[2, ]$start_lng, dat[2, ]$start_lat),
        fun = geosphere::distGeo
      )
    dist <- c(dist, dist_row)
  }
  dist
}
