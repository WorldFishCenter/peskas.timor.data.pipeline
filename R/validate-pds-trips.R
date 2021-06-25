#' Validate Pelagic Data System trips
#'
#'Downloads the preprocessed version of pds trips from cloud storage services and
#'validates a range of information so that it can be safely used for analysis.
#'
#'The parameters needed in the config file are those required for
#'`preprocess_pds_trips()`, as well as parameters needed to identify anomalous
#'trips, that are  `hrs`and `km`.
#'
#' @param log_threshold
#' @inheritParams ingest_pds_trips
#'
#' @return no outputs. This function is used for it's side effects
#' @export
#' @importFrom rlang .data
#'
validate_pds_trips <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)
  pars <- read_config()
  pds_trips <- get_preprocessed_trips(pars)

  # remove duplicated trips
  pds_trips <- pds_trips %>%
    dplyr::arrange(dplyr::desc(.data$Trip)) %>%
    dplyr::distinct(pds_trips, dplyr::across(-.data$Trip), .keep_all = TRUE)

  hrs <- pars$validation$pds_trips$trip_hours
  km <- pars$validation$pds_trips$trip_km

  logger::log_info("Validating pds trips...")
  navigation_alerts <- validate_pds_navigation(pds_trips,hrs=hrs, km=km)

  # take ready (?) columns
  ready_cols <- pds_trips %>%
    dplyr::select(.data$`Last Seen`,
                  .data$IMEI,
                  .data$Trip,
                  .data$`Device Id`)

  validated_trips <-
    list(navigation_alerts$validated_pds_duration,
         navigation_alerts$validated_pds_distance) %>%
    purrr::map(~ dplyr::select(.x,-alert_number)) %>%
    purrr::reduce(dplyr::left_join) %>%
    dplyr::left_join(ready_cols)

  validated_trips_filename <- paste(pars$pds$trips$file_prefix,
                                    "validated", sep = "_") %>%
    add_version(extension = "rds")

  readr::write_rds(x = validated_trips,
                   file = validated_trips_filename,compress = "gz")

  logger::log_info("Uploading {validated_trips_filename} to cloud sorage")
  upload_cloud_file(file = validated_trips_filename,
                    provider = pars$storage$google$key,
                    options = pars$storage$google$options)
  logger::log_success("File upload succeded")
  }


#' Validate pds trips duration and distance
#'
#' This function takes pds trips data and returns validated
#' trip duration and distance.
#'
#' @param data A data frame containing pds trips
#' @param hrs Limit of trip duration in hours to be considered a valid
#' catch session.
#' @param km Limit of trip distance traveled in Km to be considered a valid
#' catch session.
#'
#' @return A list containing data frames with validated catch duration and
#' catch distance traveled
#'
#' @export
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#'   pars <- read_config()
#'   pds_trips <- get_preprocessed_trips()
#'   validate_pds_navigation(pds_trips)
#' }
#'
validate_pds_navigation <- function(data,hrs =NULL, km=NULL){

  validated_duration <- list(

    validated_pds_duration = data %>%
      dplyr::transmute(alert_number=
                         dplyr::case_when(.data$`Duration (Seconds)`> hrs*60^2 ~8 ,TRUE ~ NA_real_),#test if trip duration is longer than n hours
                       dplyr::across(.cols = c(.data$`Duration (Seconds)`,
                                               .data$Started,
                                               .data$Ended),
                              .fns = ~ dplyr::case_when(
                                is.na(.data$alert_number) ~ .,
                                TRUE ~ NA_real_)),
                       .data$Trip),

    validated_pds_distance = data %>%
      dplyr::transmute(alert_number=
                         dplyr::case_when(.data$`Distance (Meters)`> km*1000 ~ 9 ,TRUE ~ NA_real_),#test if trip distance is longer than n km
                       `Distance (Meters)`=
                         dplyr::case_when(.data$`Distance (Meters)` > km*1000 ~ NA_real_,TRUE ~ .data$`Distance (Meters)`),
                       Trip=.data$Trip))
  validated_duration
}


get_preprocessed_trips <- function(pars){
  pds_trips_rds <- cloud_object_name(
    prefix = paste(pars$pds$trips$file_prefix, 'preprocessed', sep = "_"),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$pds$trips$version$preprocess,
    options = pars$storage$google$options)
  logger::log_info("Downloading {pds_trips_rds}...")
  download_cloud_file(name = pds_trips_rds,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)
  readr::read_rds(file = pds_trips_rds)
}
