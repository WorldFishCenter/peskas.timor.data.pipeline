
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
validate_surveys_time <- function(data, hrs =18){

  validated_time <- list(

    validated_dates = landings %>%
      dplyr::select(`_id`,date,`_submission_time`,start,end,`trip_group/duration`) %>%
      dplyr::mutate(`trip_group/duration` = as.numeric(`trip_group/duration`)) %>%
      dplyr::transmute(date=dplyr::case_when(.$date > .$`_submission_time` ~ NA_character_,TRUE ~ .$date),#test if submission date is prior catch date
                       alert_number=dplyr::case_when(.$date > .$`_submission_time` ~ 4,TRUE ~ NA_real_),
                       submission_id=as.integer(.$`_id`)),

    validated_duration = landings %>%
      dplyr::select(`_id`,date,`_submission_time`,start,end,`trip_group/duration`) %>%
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
#' the distribution of the total catch values associated to surveys. By default
#' the function uses the method of the median absolute deviation (MAD) for
#' outliers identification.
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
validate_catch_value <- function(data,method="MAD",k=13){

  # extract lower and upper bounds for outliers identification
  bounds <-
    landings %>% dplyr::select(`_id`,total_catch_value,species_group) %>%
    dplyr::mutate(total_catch_value=as.numeric(total_catch_value)) %>%
    .$total_catch_value %>%
    univOutl::LocScaleB(method=method,k=k) %>%
    magrittr::extract2(2)

  validated_price = landings %>%
    dplyr::select(`_id`,total_catch_value) %>%
    dplyr::mutate(total_catch_value=as.numeric(total_catch_value)) %>%
    dplyr::transmute(total_catch_value=dplyr::case_when(.$total_catch_value < 0 |
                                                          .$total_catch_value > bounds[2] ~ NA_real_,
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
#' a nested column `species_group` containing validated catches parameters. By
#' default the function uses the method of the median absolute deviation (MAD)
#' for outliers identification.
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
validate_catch_params <- function(data,method="MAD",k=13){

  catches_dat_unnested <-  landings %>%
    dplyr::select(`_id`,species_group) %>%
    tidyr::unnest(.,species_group,keep_empty = TRUE) %>%
    tidyr::unnest(.,length_individuals,keep_empty = TRUE) %>%
    dplyr::select(`_id`,n,species,mean_length,n_individuals) %>%
    dplyr::mutate(dplyr::across(c(mean_length,n_individuals),.fns = as.numeric))

  length_dat <- NULL
  individuals_dat <- NULL
  for(i in unique(catches_dat_unnested$species)){

    if(i %in% c("0","300",NA)){next}
    else{
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
                    dplyr::case_when(.$n_individuals < 0 ~ .$n_individuals * -1,
                                     .$n_individuals > .$inds_up  |
                                     .$n_individuals >0 & .$mean_length < .$length_low |
                                     .$n_individuals >0 & .$mean_length > .$length_up ~ NA_real_,
                                     TRUE ~ .$n_individuals),
                  submission_id=.$`_id`) %>%
    dplyr::select(submission_id,n,species,mean_length,n_individuals,alert_number)


  # extract alert number
  alert_number <- validated_length %>%
    dplyr::select(submission_id,n,alert_number) %>%
    dplyr::group_by(submission_id) %>%
    dplyr::filter(dplyr::row_number() ==1)

  # nest validated data
  validated_length_nested <-# sembra ok
    validated_length %>%
    dplyr::select(-alert_number) %>%
    dplyr::group_by(submission_id,n,species) %>%
    tidyr::nest(length_individuals = c(mean_length,n_individuals))

  # replace with validated catches params
  validated_catch_params <- landings %>%
    dplyr::rename(submission_id=`_id`) %>%
    #dplyr::mutate(submission_id=as.integer(submission_id)) %>%
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



#' Validate pds trips duration and distance
#'
#' This function takes pds trips information to return validated data frames of
#' trip duration and distance.
#'
#' @param data A  data frame
#' @param hrs Limit of trip duration in hours to be considered a valid
#' catch session.
#' @param km Limit of trip distance traveled in Km to be considered a valid
#' catch session.
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
validate_pds_navigation <- function(data, hrs =18, km=60){

  validated_duration <- list(

    validated_pds_duration = pds_trips %>%
      dplyr::mutate(`Duration (Seconds)` = as.numeric(`Duration (Seconds)`)) %>%
      dplyr::transmute(alert_number=
                         dplyr::case_when(.$`Duration (Seconds)`> hrs*60^2 ~8 ,TRUE ~ NA_real_),
                       `Duration (Seconds)`=
                         dplyr::case_when(.$`Duration (Seconds)` > hrs*60^2 ~ NA_real_,TRUE ~ .$`Duration (Seconds)`),
                       Trip=.$Trip),

    validated_pds_distance = pds_trips %>%
      dplyr::mutate(`Distance (Meters)` = as.numeric(`Distance (Meters)`)) %>%
      dplyr::transmute(alert_number=
                         dplyr::case_when(.$`Distance (Meters)`> km*1000 ~ 9 ,TRUE ~ NA_real_),
                       `Distance (Meters)`=
                         dplyr::case_when(.$`Distance (Meters)` > km*1000 ~ NA_real_,TRUE ~ .$`Distance (Meters)`),
                       Trip=.$Trip))
  validated_duration
}
