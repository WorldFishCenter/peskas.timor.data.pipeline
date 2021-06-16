#' Validate Pelagic Data System trips
#'
#' @param log_threshold
#' @param hrs Limit of trip duration in hours to be considered a valid
#' catch session.
#' @param km Limit of trip distance traveled in Km to be considered a valid
#' catch session.
#'
#' @return
#' @export
#'
#' @examples
validate_pds_trips <- function(log_threshold = logger::DEBUG, hrs =18, km=60){

  logger::log_threshold(log_threshold)
  pars <- read_config()
  pds_trips <- get_preprocessed_trips(pars)


  logger::log_info("Validating pds trips...")
  navigation_alerts <- validate_pds_navigation(pds_trips)

  validated_trips <-
    list(navigation_alerts$validated_pds_duration,
         navigation_alerts$validated_pds_distance) %>%
    purrr::map(~ dplyr::select(.x,-alert_number)) %>%
    purrr::reduce(dplyr::left_join)

  validated_trips_filename <- paste(pars$pds$file_prefix,
                                       "validated", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(x = validated_trips,
                   file = validated_trips_filename,
                   compress = "gz")
  logger::log_info("Uploading {validated_trips_filename} to cloud sorage")
  upload_cloud_file(file = validated_trips_filename,
                    provider = pars$storage$google$key,
                    options = pars$storage$google$options)
  }


#' Validate pds trips duration and distance
#'
#' This function takes pds trips information to return validated data frames of
#' trip duration and distance.
#'
#' @param data A data frame containing pds trips
#' @inheritParams validate_pds_trips
#'
#' @return A list containing data frames with validated catch duration and
#' catch distance traveled
#' @export
#'
#' @examples
#' \dontrun{
#'   pars <- read_config()
#'   pds_trips <- retrieve_pds_trips(prefix = pars$pds$trips$file_prefix,
#'                                   secret = pars$pds$trips$secret,
#'                                   token = pars$pds$trips$token)%>%
#'  readr::read_csv(col_types = readr::cols(.default = readr::col_character()))
#'
#'   validate_pds_navigation(pds_trips)
#' }
#'
validate_pds_navigation <- function(data){

  validated_duration <- list(

    validated_pds_duration = data %>%
      dplyr::mutate(`Duration (Seconds)` = as.numeric(`Duration (Seconds)`)) %>%
      dplyr::transmute(alert_number=
                         dplyr::case_when(.$`Duration (Seconds)`> hrs*60^2 ~8 ,TRUE ~ NA_real_),
                       `Duration (Seconds)`=
                         dplyr::case_when(.$`Duration (Seconds)` > hrs*60^2 ~ NA_real_,TRUE ~ .$`Duration (Seconds)`),
                       Trip=.$Trip),

    validated_pds_distance = data %>%
      dplyr::mutate(`Distance (Meters)` = as.numeric(`Distance (Meters)`)) %>%
      dplyr::transmute(alert_number=
                         dplyr::case_when(.$`Distance (Meters)`> km*1000 ~ 9 ,TRUE ~ NA_real_),
                       `Distance (Meters)`=
                         dplyr::case_when(.$`Distance (Meters)` > km*1000 ~ NA_real_,TRUE ~ .$`Distance (Meters)`),
                       Trip=.$Trip))
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
