
# Get a vector with imeis deployed in the field
get_deployed_imeis <- function(metadata){
  metadata$devices %>%
    dplyr::right_join(metadata$device_installs,
                      by = c("id" = "device_imei")) %>%
    dplyr::filter(!is.na(.data$device_imei)) %>%
    magrittr::extract2("device_imei")
}

# Perform tests for a single imei and return the corrected value and the flag
validate_this_imei <- function(this_imei, this_id = NULL, valid_imeis){

  this_id <- as.integer(this_id)

  # If imei is NA there is nothing to validate
  if (is.na(this_imei)) {
    out <- list(imei = NA_character_, alert_number = NA_integer_, submission_id = this_id)
    return(out)
  }

  # Zero seems to be used for no IMEI as well
  if (this_imei == "0") {
    out <- list(imei = NA_character_, alert_number = NA_integer_, submission_id = this_id)
    return(out)
  }

  # If the IMEI is negative it was probably a typo
  this_imei <- as.numeric(this_imei)
  if (this_imei < 0) this_imei <- this_imei * -1

  # Optimistically we need at least 5 digits to work with and that might be
  if (this_imei < 9999) {
    out <- list(imei = NA_character_, alert_number = 1, submission_id = this_id)
    return(out)
  }

  # If a valid IMEI is found replace it
  imei_regex <- paste0(as.character(this_imei), "$")
  imei_matches <- stringr::str_detect(valid_imeis, imei_regex)
  n_matches <- sum(imei_matches)
  if (n_matches == 1) {
    list(imei = valid_imeis[imei_matches], alert_number = NA_integer_, submission_id = this_id)
  } else if (n_matches > 1) {
    list(imei = NA_character_, alert_number = 2, submission_id = this_id)
  } else if (n_matches == 0) {
    list(imei = NA_character_, alert_number = 3, submission_id = this_id)
  }
}


#' Validate surveys' temporal parameters
#'
#' This function takes a preprocessed landings' matrix and validate temporal
#' info associated to each survey.
#'
#' @param data A preprocessed data frame
#' @param hrs Limit of trip duration in hours to be considered a valid catch
#'   session.
#' @param submission_delay Limit for maximum difference (in days) between the
#'   survey submission date and the recorded landing datw
#'
#' @return A list containing data frames with validated catch dates and catch
#'   duration.
#'
#' @importFrom rlang .data
#' @export
#'
#' @examples
#' \dontrun{
#'   pars <- read_config()
#'   landings <- get_merged_landings(pars)
#'   validate_surveys_time(landings,hrs =18)
#' }
validate_surveys_time <- function(data, hrs = NULL, submission_delay){

  validated_time <- list(

    validated_dates = data %>%
      dplyr::select(.data$`_id`,.data$date,.data$`_submission_time`) %>%
      dplyr::mutate(
        `_submission_time` = lubridate::ymd_hms(.data$`_submission_time`),
        submission_date = lubridate::with_tz(`_submission_time`, "Asia/Dili"),
        # submission_date = lubridate::as_date(submission_date),
        date = lubridate::ymd(.data$date, tz = "Asia/Dili"),
        # date = lubridate::as_date(date),
        d = date - submission_date) %>%
      dplyr::transmute(
        # Alert needs to be checked before editing the date column
        alert_number = dplyr::case_when(
          # test if submission date is prior catch date
          .data$date > .data$submission_date ~ 4,
          .data$date < .data$submission_date - lubridate::duration(submission_delay, units = "days") ~ 10,
          TRUE ~ NA_real_),
        date = dplyr::case_when(
          is.na(alert_number) ~ .data$date,
          TRUE ~ NA_real_),
        submission_id = as.integer(.data$`_id`)),

    validated_duration = data %>%
      dplyr::select(.data$`_id`,.data$`trip_group/duration`) %>%
      dplyr::mutate(`trip_group/duration` = as.numeric(.data$`trip_group/duration`)) %>%
      dplyr::transmute(trip_duration=dplyr::case_when(.data$`trip_group/duration` >  hrs ~ NA_real_ ,TRUE ~ .data$`trip_group/duration`),#test if catch duration is longer than n hours
                       alert_number=dplyr::case_when(.data$`trip_group/duration` >  hrs ~ 5 ,TRUE ~ NA_real_),
                       submission_id=as.integer(.data$`_id`))
  )
  validated_time
}

#' Validate surveys' total catch values
#'
#' This function takes a preprocessed landings' matrix and uses univariate
#' techniques (see [univOutl::LocScaleB]) for the identification of outliers in
#' the distribution of the total catch values associated to surveys.
#'
#' @param data A preprocessed data frame
#' @inheritParams univOutl::LocScaleB
#'
#' @return A data frame containing validated catch values.
#' @export
#'
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#'   pars <- read_config()
#'   landings <- get_merged_landings(pars)
#'   validate_catch_value(landings,method="MAD",k=13)
#' }
#'
validate_catch_price <- function(data,method=NULL,k=NULL){

  validated_price = data %>%
    dplyr::select(.data$`_id`,.data$total_catch_value) %>%
    dplyr::mutate(total_catch_value = as.numeric(.data$total_catch_value)) %>%
    dplyr::transmute(
      alert_number = alert_outlier(
        x = .data$total_catch_value, alert_if_smaller = 9, alert_if_larger = 6,
        logt = TRUE, k = k, method = method),
      alert_number = dplyr::case_when(.data$total_catch_value < 0 ~ 8,
                                      TRUE ~ .data$alert_number),
      total_catch_value = dplyr::case_when(
        is.na(.data$alert_number) ~ .data$total_catch_value,
        TRUE ~ NA_real_),
      submission_id = as.integer(.data$`_id`))

  validated_price
}

#' Generate an alert vector based on the `univOutl::LocScaleB()` function
#'
#' @param x numeric vector where outliers will be checked
#' @param no_alert_value value to put in the output when there is no alert (x is within bounds)
#' @param alert_if_larger alert for when x is above the bounds found by `univOutl::LocScaleB()`
#' @param alert_if_smaller alert for when x is below the bounds found by `univOutl::LocScaleB()`
#' @param ... arguments for `univOutl::LocScaleB()`
#'
#' @return a vector of the same lenght as x
#'
alert_outlier <- function(x,
                          no_alert_value = NA_real_,
                          alert_if_larger = no_alert_value,
                          alert_if_smaller = no_alert_value,
                          ...){

  algo_args <- list(...)

  # Helper function to check if everything is NA or zero
  all_na_or_zero <- function(x){
    isTRUE(all(is.na(x) | x == 0))
  }

  # If everything is NA or zero there is nothing to compute
  if (all_na_or_zero(x)) return(NA_real_)
  # If the median absolute deviation is zero we shouldn't be using this algo
  if (mad(x, na.rm = T) <= 0) return(NA_real_)
  # If weights are specified and they are all NA or zero
  if (!is.null(algo_args$weights))
    if (all_na_or_zero(algo_args$weights)) return(NA_real_)

  bounds <- univOutl::LocScaleB(x, ...) %>%
    magrittr::extract2("bounds")

  if (isTRUE(algo_args$logt)) bounds <- exp(bounds) - 1

  dplyr::case_when(
    x < bounds[1] ~ alert_if_smaller,
    x > bounds[2] ~ alert_if_larger,
    TRUE ~ no_alert_value
  )
}

#' Validate surveys' catch parameters
#'
#' This function takes a preprocessed landings' matrix and uses univariate
#' techniques (see [univOutl::LocScaleB]) for the identification of outliers in
#' the distribution of the number of individuals per catch and their size. The
#' function returns a data frame with the survey id, the alert number and
#' a nested column `species_group` containing validated catches parameters.
#'
#' @param data A preprocessed data frame
#' @param k_ind Extension of bounds for the number of individuals
#' @param k_length Extension of bounds for the catch length
#' @inheritParams univOutl::LocScaleB
#'
#' @return A data frame containing the validated catches parameters.
#' @export
#'
#' @examples
#' \dontrun{
#'   pars <- read_config()
#'   landings <- get_merged_landings(pars)
#'   validate_catch_params(landings,method="MAD",k=13)
#' }
#'
validate_catch_params <- function(data,method=NULL, k_ind =NULL, k_length = NULL){

  catches_dat_unnested <-  data %>%
    dplyr::select(.data$`_id`,.data$species_group) %>%
    tidyr::unnest(.data$species_group,keep_empty = TRUE) %>%
    tidyr::unnest(.data$length_individuals,keep_empty = TRUE) %>%
    dplyr::select(.data$`_id`,.data$n,.data$species,
                  .data$mean_length,.data$n_individuals) %>%
    dplyr::mutate(dplyr::across(c(.data$mean_length,.data$n_individuals),.fns = as.numeric))

  validated_length <- catches_dat_unnested %>%
    dplyr::group_by(species) %>%
    dplyr::mutate(
      n_individuals = dplyr::case_when(
        .data$n_individuals == 0 ~ NA_real_,
        .data$n_individuals < 0 ~ .data$n_individuals * -1,
        TRUE ~ n_individuals),
      alert_n_individuals = alert_outlier(
        x = .data$n_individuals,
        alert_if_larger = 11, logt = TRUE, k = k_ind),
      n_individuals = dplyr::case_when(
        is.na(.data$n_individuals) & !is.na(.data$species) ~ 0,
        is.na(.data$alert_n_individuals) ~ .data$n_individuals,
        TRUE ~ NA_real_),
      alert_length = alert_outlier(
        x = .data$mean_length,
        alert_if_larger = 7, logt = TRUE, k = k_length),
      mean_length = dplyr::case_when(
        is.na(.data$alert_length) ~ .data$mean_length,
        TRUE ~ NA_real_),
      alert_number = dplyr::coalesce(alert_n_individuals, alert_length),
      submission_id = .data$`_id`) %>%
    dplyr::ungroup() %>%
    dplyr::select(-.data$alert_n_individuals, -.data$alert_length, -.data$`_id`)

  # extract alert number
  alert_number <- validated_length %>%
    dplyr::select(.data$submission_id,.data$n,.data$alert_number) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::filter(dplyr::row_number() ==1)

  # nest validated data
  validated_length_nested <-
    validated_length %>%
    dplyr::select(-.data$alert_number) %>%
    dplyr::group_by(.data$submission_id,.data$n,.data$species) %>%
    tidyr::nest(length_individuals = c(.data$mean_length,.data$n_individuals))

  # replace validated catches params in original data
  validated_catch_params <- data %>%
    dplyr::rename(submission_id=.data$`_id`) %>%
    dplyr::select(.data$submission_id,.data$species_group) %>%
    tidyr::unnest(.data$species_group,keep_empty = TRUE) %>%
    dplyr::mutate(length_individuals=validated_length_nested$length_individuals)%>%
    dplyr::group_by(.data$submission_id) %>%
    tidyr::nest() %>%
    dplyr::rename("species_group" = "data") %>%
    dplyr::ungroup() %>%
    dplyr::mutate(alert_number=alert_number$alert_number,
                  submission_id=as.integer(.data$submission_id))

  validated_catch_params
}