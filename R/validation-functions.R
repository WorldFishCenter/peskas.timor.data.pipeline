
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
#' @param hrs Limit of trip duration in hours to be considered a valid
#' catch session.
#'
#' @return A list containing data frames with validated catch dates and
#' catch duration.
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
validate_surveys_time <- function(data, hrs =NULL){

  validated_time <- list(

    validated_dates = data %>%
      dplyr::select(.data$`_id`,.data$date,.data$`_submission_time`) %>%
      dplyr::mutate(
        `_submission_time` = lubridate::ymd_hms(.data$`_submission_time`),
        submission_date = lubridate::with_tz(`_submission_time`, "Asia/Dili"),
        submission_date = lubridate::as_date(submission_date),
        date = lubridate::as_date(.data$date)) %>%
      dplyr::transmute(
        # Alert needs to be checked before editing the date column
        alert_number = dplyr::case_when(
          # test if submission date is prior catch date
          .data$date > .data$submission_date ~ 4,
          TRUE ~ NA_real_),
        date = dplyr::case_when(
          .data$date > .data$submission_date ~ NA_character_,
          TRUE ~ .data$date),
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
  # extract lower and upper bounds for outliers identification
  bounds <-
    data %>% dplyr::select(.data$`_id`,.data$total_catch_value) %>%
    dplyr::transmute(total_catch_value=as.numeric(.data$total_catch_value)) %>%
    magrittr::extract2(1) %>%
    univOutl::LocScaleB(method=method,k=k) %>%
    magrittr::extract2(2)

  validated_price = data %>%
    dplyr::select(.data$`_id`,.data$total_catch_value) %>%
    dplyr::mutate(total_catch_value=as.numeric(.data$total_catch_value)) %>%
    dplyr::transmute(
      # Need to check alert before changing the total_catch_value column
      alert_number = dplyr::case_when(
        .data$total_catch_value < 0 ~ 8,
        .data$total_catch_value < bounds[1] ~ 9,
        .data$total_catch_value > bounds[2] ~ 6,
        TRUE ~ NA_real_),
      total_catch_value = dplyr::case_when(
        .data$total_catch_value < 0 ~ NA_real_,
        .data$total_catch_value < bounds[1] ~ NA_real_,
        .data$total_catch_value > bounds[2] ~ NA_real_,
        TRUE ~ .data$total_catch_value),
      submission_id = as.integer(.data$`_id`))

  validated_price
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
validate_catch_params <- function(data,method=NULL,k=NULL){

  catches_dat_unnested <-  data %>%
    dplyr::select(.data$`_id`,.data$species_group) %>%
    tidyr::unnest(.data$species_group,keep_empty = TRUE) %>%
    tidyr::unnest(.data$length_individuals,keep_empty = TRUE) %>%
    dplyr::select(.data$`_id`,.data$n,.data$species,
                  .data$mean_length,.data$n_individuals) %>%
    dplyr::mutate(dplyr::across(c(.data$mean_length,.data$n_individuals),.fns = as.numeric))

  length_dat <- NULL
  individuals_dat <- NULL
  for(i in unique(catches_dat_unnested$species)){

    # exclude unidentified fishes catches
    if(i %in% c("0","300",NA)){next}
    else{
      # retrieve n individuals and length bounds for each fish category
      spe_code <- i
      limits_length <-
        dplyr::filter(catches_dat_unnested,.data$species %in% i & .data$n_individuals>0)  %>%
        magrittr::extract2("mean_length") %>%
        univOutl::LocScaleB(method=method,k=k) %>%
        magrittr::extract2(2) %>%t() %>%
        dplyr::as_tibble() %>%
        dplyr::rename(length_low=.data$lower.low,length_up=.data$upper.up) %>%
        dplyr::mutate(species=i,length_low=dplyr::case_when(
          .data$length_low<=0~0,TRUE~.data$length_low))

      limits_individuals <-
        dplyr::filter(catches_dat_unnested,.data$species %in% i & .data$n_individuals>0)  %>%
        magrittr::extract2("n_individuals") %>%
        univOutl::LocScaleB(method=method,k=k) %>%
        magrittr::extract2(2) %>% t() %>%
        dplyr::as_tibble() %>%
        dplyr::rename(inds_low=.data$lower.low,inds_up=.data$upper.up) %>%
        dplyr:: mutate(species=i,inds_low=dplyr::case_when(
          .data$inds_low<=0~0,TRUE~.data$inds_low))

    }
    length_dat <- rbind(length_dat,limits_length)
    individuals_dat <- rbind(individuals_dat,limits_individuals)
  }

  limits <- dplyr::full_join(length_dat,individuals_dat) %>%
    dplyr::select(.data$species,tidyr::everything())

  validated_length <-
    dplyr::left_join(catches_dat_unnested,limits) %>%
    dplyr::mutate(alert_number=
                    dplyr::case_when(abs(.data$n_individuals) < .data$inds_low |
                                     .data$n_individuals > .data$inds_up |
                                     .data$n_individuals >0 & .data$mean_length < .data$length_low |
                                     .data$n_individuals >0 & .data$mean_length > .data$length_up ~ 7,
                                     TRUE ~ NA_real_),
                  n_individuals=
                    dplyr::case_when(.data$n_individuals < 0 ~ .data$n_individuals * -1,#convert negative values to positive
                                      abs(.data$n_individuals) < .data$inds_low |#test if n_ind. is lower than lower bound
                                     .data$n_individuals > .data$inds_up  |#test if n_ind. is higher than upper bound
                                     .data$n_individuals >0 & .data$mean_length < .data$length_low |#test if length is lower than lower bound
                                     .data$n_individuals >0 & .data$mean_length > .data$length_up ~ NA_real_,#test if length is higher than higher bound
                                     TRUE ~ .data$n_individuals),
                  submission_id=.data$`_id`) %>%
    dplyr::select(.data$submission_id,.data$n,.data$species,
                  .data$mean_length,.data$n_individuals,.data$alert_number)


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
