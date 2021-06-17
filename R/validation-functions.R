
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
      dplyr::select(`_id`,date,`_submission_time`,start,end,`trip_group/duration`) %>%
      dplyr::mutate(`_submission_time`=as.character(lubridate::ymd_hms(`_submission_time`,tz="Asia/Dili")),
                    date=as.character(lubridate::as_date(date))) %>%
      dplyr::transmute(date=dplyr::case_when(.$date > .$`_submission_time` ~ NA_character_,TRUE ~ .$date),#test if submission date is prior catch date
                       alert_number=dplyr::case_when(.$date > .$`_submission_time` ~ 4,TRUE ~ NA_real_),
                       submission_id=as.integer(.$`_id`)),

    validated_duration = data %>%
      dplyr::select(`_id`,date,`trip_group/duration`) %>%
      dplyr::mutate(`trip_group/duration` = as.numeric(`trip_group/duration`)) %>%
      dplyr::transmute(trip_duration=dplyr::case_when(.$`trip_group/duration` >  hrs ~ NA_real_ ,TRUE ~ .$`trip_group/duration`),#test if catch duration is longer than n hours
                       alert_number=dplyr::case_when(.$`trip_group/duration` >  hrs ~ 5 ,TRUE ~ NA_real_),
                       submission_id=as.integer(.$`_id`))
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
    data %>% dplyr::select(`_id`,total_catch_value) %>%
    dplyr::mutate(total_catch_value=as.numeric(total_catch_value)) %>%
    .$total_catch_value %>%
    univOutl::LocScaleB(method=method,k=k) %>%
    magrittr::extract2(2)

  validated_price = data %>%
    dplyr::select(`_id`,total_catch_value) %>%
    dplyr::mutate(total_catch_value=as.numeric(total_catch_value)) %>%
    dplyr::transmute(total_catch_value=dplyr::case_when(.$total_catch_value < 0 |#test if price is negative
                                                          .$total_catch_value > bounds[2] ~ NA_real_,#test if price is higher than upper bound
                                                        TRUE ~ .$total_catch_value),
                     alert_number=dplyr::case_when(.$total_catch_value > bounds[2] ~ 6,TRUE ~ NA_real_),
                     submission_id=as.integer(.$`_id`))

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
    dplyr::select(`_id`,species_group) %>%
    tidyr::unnest(.,species_group,keep_empty = TRUE) %>%
    tidyr::unnest(.,length_individuals,keep_empty = TRUE) %>%
    dplyr::select(`_id`,n,species,mean_length,n_individuals) %>%
    dplyr::mutate(dplyr::across(c(mean_length,n_individuals),.fns = as.numeric))

  length_dat <- NULL
  individuals_dat <- NULL
  for(i in unique(catches_dat_unnested$species)){

    # exclude unidentified fishes catches
    if(i %in% c("0","300",NA)){next}
    else{
      # retrieve n individuals and length bounds for each fish category
      spe_code <- i
      limits_length <-
        dplyr::filter(catches_dat_unnested,species %in% i & n_individuals>0)  %>%
        .$mean_length %>%
        univOutl::LocScaleB(method=method,k=k) %>%
        magrittr::extract2(2) %>%t() %>%
        dplyr::as_tibble() %>%
        dplyr::rename(length_low=lower.low,length_up=upper.up) %>%
        dplyr::mutate(species=i,length_low=dplyr::case_when(length_low<=0~0,TRUE~length_low))

      limits_individuals <-
        dplyr::filter(catches_dat_unnested,species %in% i & n_individuals>0)  %>%
        .$n_individuals %>%
        univOutl::LocScaleB(method=method,k=k) %>%
        magrittr::extract2(2) %>% t() %>%
        dplyr::as_tibble() %>%
        dplyr::rename(inds_low=lower.low,inds_up=upper.up) %>%
        dplyr:: mutate(species=i,inds_low=dplyr::case_when(inds_low<=0~0,TRUE~inds_low))

    }
    length_dat <- rbind(length_dat,limits_length)
    individuals_dat <- rbind(individuals_dat,limits_individuals)
  }

  limits <- dplyr::full_join(length_dat,individuals_dat) %>%
    dplyr::select(species,tidyr::everything())

  validated_length <-
    dplyr::left_join(catches_dat_unnested,limits) %>%
    dplyr::mutate(alert_number=
                    dplyr::case_when(abs(.$n_individuals) < .$inds_low |
                                     .$n_individuals > .$inds_up |
                                     .$n_individuals >0 & .$mean_length < .$length_low |
                                     .$n_individuals >0 & .$mean_length > .$length_up ~ 7,
                                     TRUE ~ NA_real_),
                  n_individuals=
                    dplyr::case_when(.$n_individuals < 0 ~ .$n_individuals * -1,#convert negative values to positive
                                      abs(.$n_individuals) < .$inds_low |#test if n_ind. is lower than lower bound
                                     .$n_individuals > .$inds_up  |#test if n_ind. is higher than upper bound
                                     .$n_individuals >0 & .$mean_length < .$length_low |#test if length is lower than lower bound
                                     .$n_individuals >0 & .$mean_length > .$length_up ~ NA_real_,#test if length is higher than higher bound
                                     TRUE ~ .$n_individuals),
                  submission_id=.$`_id`) %>%
    dplyr::select(submission_id,n,species,mean_length,n_individuals,alert_number)


  # extract alert number
  alert_number <- validated_length %>%
    dplyr::select(submission_id,n,alert_number) %>%
    dplyr::group_by(submission_id) %>%
    dplyr::filter(dplyr::row_number() ==1)

  # nest validated data
  validated_length_nested <-
    validated_length %>%
    dplyr::select(-alert_number) %>%
    dplyr::group_by(submission_id,n,species) %>%
    tidyr::nest(length_individuals = c(mean_length,n_individuals))

  # replace validated catches params in original data
  validated_catch_params <- data %>%
    dplyr::rename(submission_id=`_id`) %>%
    dplyr::select(submission_id,species_group) %>%
    tidyr::unnest(species_group,keep_empty = TRUE) %>%
    dplyr::mutate(length_individuals=validated_length_nested$length_individuals)%>%
    dplyr::group_by(submission_id) %>%
    tidyr::nest() %>%
    dplyr::rename("species_group" = "data") %>%
    dplyr::ungroup() %>%
    dplyr::mutate(alert_number=alert_number$alert_number,
                  submission_id=as.integer(submission_id))

  validated_catch_params
}
