# Get a vector with imeis deployed in the field
get_deployed_imeis <- function(metadata) {
  metadata$devices %>%
    dplyr::right_join(metadata$device_installs,
      by = c("id" = "device_imei")
    ) %>%
    dplyr::filter(!is.na(.data$device_imei)) %>%
    magrittr::extract2("device_imei")
}

# Perform tests for a single imei and return the corrected value and the flag
validate_this_imei <- function(this_imei, this_id = NULL, valid_imeis) {
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
#' pars <- read_config()
#' landings <- get_merged_landings(pars)
#' validate_surveys_time(landings, hrs = 18)
#' }
validate_surveys_time <- function(data, hrs = NULL, submission_delay) {
  validated_time <- list(
    validated_dates = data %>%
      dplyr::select(.data$`_id`, .data$date, .data$`_submission_time`) %>%
      dplyr::mutate(
        `_submission_time` = lubridate::ymd_hms(.data$`_submission_time`),
        submission_date = lubridate::with_tz(.data$`_submission_time`, "Asia/Dili"),
        date = as.POSIXct(.data$date, tz = "Asia/Dili"),
        d = date - .data$submission_date
      ) %>%
      dplyr::transmute(
        # Alert needs to be checked before editing the date column
        alert_number = dplyr::case_when(
          # test if submission date is prior catch date
          .data$date > .data$submission_date ~ 4,
          .data$date < .data$submission_date - lubridate::duration(submission_delay, units = "days") ~ 10,
          TRUE ~ NA_real_
        ),
        date = as.Date(.data$date, tz = "Asia/Dili"),
        submission_id = as.integer(.data$`_id`)
      ),
    validated_duration = data %>%
      dplyr::select(.data$`_id`, .data$`trip_group/duration`) %>%
      dplyr::mutate(`trip_group/duration` = abs(as.numeric(.data$`trip_group/duration`))) %>%
      dplyr::transmute(
        trip_duration = dplyr::case_when(
          .data$`trip_group/duration` > hrs |
            .data$`trip_group/duration` < 1 ~ NA_real_,
          TRUE ~ .data$`trip_group/duration`
        ), # test if catch duration is longer than n hours or minor than 1 hour
        alert_number = dplyr::case_when(
          .data$`trip_group/duration` > hrs |
            .data$`trip_group/duration` < 1 ~ 5,
          TRUE ~ NA_real_
        ),
        submission_id = as.integer(.data$`_id`)
      )
  )
  validated_time
}

validate_landing_regularity <- function(landings) {
  regularity_alerts <-
    landings %>%
    dplyr::select(.data$`_id`, .data$total_catch_value, .data$species_group) %>%
    tidyr::unnest(.data$species_group, keep_empty = TRUE) %>%
    tidyr::unnest(.data$length_individuals, keep_empty = TRUE) %>%
    dplyr::select(.data$`_id`, .data$species, .data$total_catch_value, .data$n_individuals) %>%
    dplyr::mutate(
      total_catch_value = as.double(.data$total_catch_value),
      total_catch_value = abs(.data$total_catch_value),
      n_individuals = abs(.data$n_individuals)
    ) %>%
    dplyr::group_by(.data$`_id`) %>%
    dplyr::summarise(
      species = dplyr::first(.data$species),
      total_catch_value = dplyr::first(.data$total_catch_value),
      n_individuals = sum(.data$n_individuals, na.rm = T)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      alert_regularity = dplyr::case_when(
        .data$species == "0" & .data$n_individuals > 0 |
          .data$species == "0" & .data$total_catch_value > 0 |
          !.data$species == "0" & .data$n_individuals <= 0 |
          !.data$species == "0" & .data$total_catch_value <= 0 |
          .data$total_catch_value <= 0 & .data$n_individuals > 0 |
          .data$total_catch_value > 0 & .data$n_individuals <= 0
        # is.na(.data$total_catch_value) & .data$n_individuals >= 0 |
        # is.na(.data$n_individuals) & .data$total_catch_value >= 0
        ~ 22, TRUE ~ NA_real_
      )
    ) %>%
    dplyr::rename(submission_id = .data$`_id`)

  no_regular_ids <-
    regularity_alerts %>%
    dplyr::filter(!is.na(.data$alert_regularity)) %>%
    magrittr::extract2("submission_id")

  regular_landings <-
    landings %>%
    tidyr::unnest(.data$species_group, keep_empty = TRUE) %>%
    tidyr::unnest(.data$length_individuals, keep_empty = TRUE) %>%
    dplyr::mutate(
      total_catch_value = as.double(.data$total_catch_value),
      total_catch_value = abs(.data$total_catch_value),
      n_individuals = abs(.data$n_individuals),
      alert_number = dplyr::case_when(
        .data$`_id` %in% no_regular_ids ~ 22,
        TRUE ~ NA_real_
      ),
      total_catch_value = dplyr::case_when(is.na(.data$alert_number) ~ .data$total_catch_value, TRUE ~ NA_real_),
      n_individuals = dplyr::case_when(is.na(.data$alert_number) ~ .data$n_individuals, TRUE ~ NA_real_),
      dplyr::across(
        c(.data$weight:.data$Vitamin_A_mu), ~ dplyr::case_when(
          is.na(.data$alert_number) ~ .data$.x, TRUE ~ NA_real_
        )
      )
    ) %>%
    tidyr::nest(length_individuals = c(.data$mean_length:.data$Vitamin_A_mu)) %>%
    tidyr::nest(species_group = c(
      .data$n, .data$species, .data$food_or_sale, .data$other_species_name,
      .data$photo, .data$length_individuals, .data$length_type
    ))

  list(
    regularity_alerts = regularity_alerts,
    regular_landings = regular_landings
  )
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
#' pars <- read_config()
#' landings <- get_merged_landings(pars)
#' validate_catch_value(landings, method = "MAD", k = 13)
#' }
#'
validate_catch_price <- function(data, method = NULL, k = NULL) {
  validated_price <-
    data %>%
    dplyr::filter(is.na(.data$alert_number)) %>%
    dplyr::select(.data$`_id`, .data$total_catch_value) %>%
    dplyr::transmute(
      alert_number = ifelse(.data$total_catch_value > 1500, 6, NA_integer_),
      # alert_number = alert_outlier(
      #  x = .data$total_catch_value, alert_if_smaller = 9, alert_if_larger = 6,
      #  logt = TRUE, k = k, method = method
      # ),
      total_catch_value = dplyr::case_when(
        is.na(.data$alert_number) ~ .data$total_catch_value,
        TRUE ~ NA_real_
      ),
      submission_id = as.integer(.data$`_id`)
    )

  data %>%
    dplyr::filter(!is.na(.data$alert_number)) %>%
    dplyr::select(.data$`_id`, .data$total_catch_value, .data$alert_number) %>%
    dplyr::rename(submission_id = .data$`_id`) %>%
    dplyr::mutate(submission_id = as.integer(.data$submission_id)) %>%
    dplyr::bind_rows(validated_price)
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
#' @importFrom stats mad
alert_outlier <- function(x,
                          no_alert_value = NA_real_,
                          alert_if_larger = no_alert_value,
                          alert_if_smaller = no_alert_value,
                          ...) {
  algo_args <- list(...)

  # Helper function to check if everything is NA or zero
  all_na_or_zero <- function(x) {
    isTRUE(all(is.na(x) | x == 0))
  }

  # If everything is NA or zero there is nothing to compute
  if (all_na_or_zero(x)) {
    return(NA_real_)
  }
  # If the median absolute deviation is zero we shouldn't be using this algo
  if (mad(x, na.rm = T) <= 0) {
    return(NA_real_)
  }
  # If weights are specified and they are all NA or zero
  if (!is.null(algo_args$weights)) {
    if (all_na_or_zero(algo_args$weights)) {
      return(NA_real_)
    }
  }

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
#' pars <- read_config()
#' landings <- get_merged_landings(pars)
#' validate_catch_params(landings, method = "MAD", k = 13)
#' }
#'
validate_catch_params <- function(data, method = NULL, k_ind = NULL, k_length = NULL) {
  catches_dat_unnested <-
    data %>%
    dplyr::filter(is.na(alert_number)) %>%
    dplyr::select(.data$`_id`, .data$`trip_group/gear_type`, .data$species_group) %>%
    tidyr::unnest(.data$species_group, keep_empty = TRUE) %>%
    tidyr::unnest(.data$length_individuals, keep_empty = TRUE)

  validated_length <-
    catches_dat_unnested %>%
    dplyr::group_by(.data$`trip_group/gear_type`, .data$species) %>%
    dplyr::mutate(
      n_individuals = dplyr::case_when(
        .data$n_individuals == 0 ~ NA_real_,
        TRUE ~ n_individuals
      ),
      alert_n_individuals = alert_outlier(
        x = .data$n_individuals,
        alert_if_larger = 11, logt = TRUE, k = k_ind
      ),
      n_individuals = dplyr::case_when(
        is.na(.data$n_individuals) & !is.na(.data$species) ~ 0,
        is.na(.data$alert_n_individuals) ~ .data$n_individuals,
        TRUE ~ NA_real_
      ),
      mean_length_valid = dplyr::case_when(
        .data$n_individuals > 0 ~ .data$mean_length,
        TRUE ~ NA_real_
      )
    ) %>%
    dplyr::group_by(.data$species) %>%
    dplyr::mutate(
      alert_length = alert_outlier(
        x = .data$mean_length_valid,
        alert_if_larger = 7, logt = TRUE, k = k_length
      ),
      mean_length = dplyr::case_when(
        is.na(.data$alert_length) ~ .data$mean_length,
        TRUE ~ NA_real_
      ),
      alert_number = dplyr::coalesce(.data$alert_n_individuals, .data$alert_length),
      submission_id = .data$`_id`
    ) %>%
    dplyr::select(-.data$mean_length_valid) %>%
    dplyr::ungroup() %>%
    # Adjusting weight and nutrients accordingly
    dplyr::mutate(
      dplyr::across(
        c(.data$weight:.data$Vitamin_A_mu),
        ~ dplyr::case_when(
          !is.na(.data$alert_number) ~ NA_real_,
          .data$n_individuals == 0 ~ 0,
          TRUE ~ .data$.x
        )
      )
    ) %>%
    dplyr::select(-.data$alert_n_individuals, -.data$alert_length, -.data$`_id`)

  # extract alert number
  alert_number <-
    validated_length %>%
    dplyr::select(.data$submission_id, .data$n, .data$alert_number) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::arrange(dplyr::desc(alert_number), .by_group = TRUE) %>%
    dplyr::filter(dplyr::row_number() == 1) %>%
    dplyr::ungroup() %>%
    dplyr::select(-.data$n) %>%
    dplyr::mutate(submission_id = as.integer(.data$submission_id))

  # nest validated data
  validated_length_nested <-
    validated_length %>%
    dplyr::select(-.data$alert_number) %>%
    dplyr::group_by(.data$submission_id, .data$n, .data$species) %>%
    tidyr::nest(length_individuals = c(.data$mean_length:.data$Vitamin_A_mu))

  # replace validated catches params in original data
  validated_catch_params <-
    data %>%
    dplyr::filter(is.na(.data$alert_number)) %>%
    dplyr::rename(submission_id = .data$`_id`) %>%
    dplyr::select(.data$submission_id, .data$species_group) %>%
    tidyr::unnest(.data$species_group, keep_empty = TRUE) %>%
    dplyr::mutate(length_individuals = validated_length_nested$length_individuals) %>%
    dplyr::group_by(.data$submission_id) %>%
    tidyr::nest() %>%
    dplyr::rename("species_group" = "data") %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      alert_number = alert_number$alert_number,
      submission_id = as.integer(.data$submission_id)
    ) %>%
    dplyr::left_join(alert_number, by = "submission_id") %>%
    dplyr::mutate(alert_number = dplyr::coalesce(.data$alert_number.x, .data$alert_number.y)) %>%
    dplyr::select(-c(.data$alert_number.x, .data$alert_number.y))


  data %>%
    dplyr::filter(!is.na(alert_number)) %>%
    dplyr::select(.data$`_id`, .data$species_group, .data$alert_number) %>%
    dplyr::rename(submission_id = .data$`_id`) %>%
    dplyr::mutate(submission_id = as.integer(.data$submission_id)) %>%
    dplyr::bind_rows(validated_catch_params)
}

#' Outlier identification based on Cook's distance
#'
#' This function adds an additional alert to both price and catch alert dataframes
#' when the relation between the price and weight assume abnormal values relatively
#' to each species. The relationship between weight and price is mostly linear,
#' this function identifies the survey IDs where the Cook's distance is higher than
#' cook_dist * mean_cook, where cook_dist is a multiplicative coefficient and
#' cook_dist is the average Cook's distance relatively to each species.
#'
#' Currently, cook_dist is set to 21 as default value.
#'
#' @param catch_alerts The dataframe of catch alerts.
#' @param price_alerts The dataframe of price alerts.
#' @param non_regular_ids The dataframe of landings regularity alerts.
#' @param cook_dist A number that go in the formula cook_dist * (mean(cooksd)).
#' @param price_weight_min Min price per weight value threshold.
#' @param price_weight_max Max price per weight value threshold.
#' @return The price and catch alert' dataframes including outlier identification
#' based on Cook's distance.
#' @export
#'
validate_price_weight <- function(catch_alerts = NULL,
                                  price_alerts = NULL,
                                  non_regular_ids = NULL,
                                  cook_dist = NULL,
                                  price_weight_min = NULL,
                                  price_weight_max = NULL) {
  # Extract single catches IDs
  single_catches <-
    catch_alerts %>%
    dplyr::mutate(n = purrr::map_dbl(.data$species_group, nrow)) %>%
    dplyr::filter(.data$n == 1) %>%
    magrittr::extract2("submission_id")

  # Extract IDs with abnormal price weight relation based on Cook's distance
  price_per_weight_alerts <-
    dplyr::left_join(price_alerts, catch_alerts, by = "submission_id") %>%
    # dplyr::filter(.data$submission_id %in% single_catches) %>%
    tidyr::unnest(.data$species_group) %>%
    tidyr::unnest(.data$length_individuals) %>%
    dplyr::select(.data$submission_id, .data$species, .data$total_catch_value, .data$weight) %>%
    dplyr::filter(!is.na(.data$weight) & !is.na(.data$total_catch_value) & .data$weight != 0) %>%
    # dplyr::group_by(.data$submission_id, .data$species) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(
      total_catch_value = dplyr::first(.data$total_catch_value),
      weight = sum(.data$weight, na.rm = T),
    ) %>%
    # dplyr::group_by(.data$species) %>%
    dplyr::mutate(model = broom::augment(stats::lm(formula = log(.data$total_catch_value + 1)
    ~ log(.data$weight / 1000 + 1)))) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(cooksd = .data$model$`.cooksd`) %>%
    dplyr::select(-.data$model) %>%
    dplyr::mutate(
      weight_kg = .data$weight / 1000,
      pk = .data$total_catch_value / .data$weight_kg,
      alert_number = dplyr::case_when( # .data$cooksd > (cook_dist * mean(.data$cooksd)) |
        .data$pk < price_weight_min |
          .data$pk > price_weight_max
        ~ 17, TRUE ~ NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(!is.na(.data$alert_number)) %>%
    magrittr::extract2("submission_id")

  regularity_alerts <-
    non_regular_ids %>%
    dplyr::filter(!is.na(.data$alert_regularity)) %>%
    magrittr::extract2("submission_id")

  n_individuals_alert <-
    catch_alerts %>%
    dplyr::filter(!is.na(.data$alert_number)) %>%
    magrittr::extract2("submission_id")

  revenue_alert <-
    price_alerts %>%
    dplyr::filter(!is.na(.data$alert_number)) %>%
    magrittr::extract2("submission_id")

  # Integrate new alert to prices and weights
  price_alerts %<>%
    dplyr::mutate(
      alert_number = dplyr::case_when(
        .data$submission_id %in% price_per_weight_alerts ~ 17,
        .data$submission_id %in% regularity_alerts ~ 22,
        .data$submission_id %in% n_individuals_alert ~ 11,
        TRUE ~ .data$alert_number
      ),
      total_catch_value = dplyr::case_when(
        is.na(.data$alert_number) ~ .data$total_catch_value, TRUE ~ NA_real_
      )
    )

  catch_alerts %<>%
    tidyr::unnest(.data$species_group, keep_empty = TRUE) %>%
    tidyr::unnest(.data$length_individuals, keep_empty = TRUE) %>%
    dplyr::mutate(
      alert_number = dplyr::case_when(
        .data$submission_id %in% price_per_weight_alerts ~ 17,
        .data$submission_id %in% regularity_alerts ~ 22,
        .data$submission_id %in% revenue_alert ~ 6,
        TRUE ~ .data$alert_number
      ),
      n_individuals = dplyr::case_when(is.na(.data$alert_number) ~ .data$n_individuals, TRUE ~ NA_real_),
      dplyr::across(
        c(.data$weight:.data$Vitamin_A_mu), ~ dplyr::case_when(
          is.na(.data$alert_number) ~ .data$.x, TRUE ~ NA_real_
        )
      )
    ) %>%
    tidyr::nest(length_individuals = c(.data$mean_length:.data$Vitamin_A_mu)) %>%
    tidyr::nest(species_group = c(
      .data$n, .data$species, .data$food_or_sale, .data$other_species_name,
      .data$photo, .data$length_individuals, .data$length_type
    ))

  dplyr::full_join(catch_alerts, price_alerts, by = c("submission_id")) %>%
    dplyr::mutate(alert_number = dplyr::coalesce(.data$alert_number.x, .data$alert_number.y)) %>%
    dplyr::select(-.data$alert_number.x, -.data$alert_number.y)
}

# Ideally this function would in the future, check for the integrity of the boat type
validate_vessel_type <- function(landings, metadata_vessel_table) {
  landings %>%
    dplyr::rename(
      submission_id = .data$`_id`,
      boat_code = .data$`trip_group/boat_type`
    ) %>%
    dplyr::mutate(boat_code = as.integer(.data$boat_code)) %>%
    dplyr::left_join(metadata_vessel_table, by = "boat_code") %>%
    dplyr::rename(vessel_type = .data$boat_type) %>%
    # If no vessel type is not what we expected
    dplyr::mutate(
      not_valid_code = !is.na(.data$boat_code) & is.na(.data$vessel_type),
      alert_number = dplyr::if_else(isTRUE(.data$not_valid_code), 12, NA_real_)
    ) %>%
    # If no vessel type was recorded when it should have
    dplyr::mutate(
      no_vessel_type = .data$`trip_group/has_boat` == "TRUE" & is.na(.data$vessel_type),
      alert_number = dplyr::if_else(isTRUE(.data$no_vessel_type), 13, NA_real_)
    ) %>%
    # Fixing types
    dplyr::mutate(submission_id = as.integer(.data$submission_id)) %>%
    dplyr::select(.data$vessel_type, .data$alert_number, .data$submission_id)
}

validate_gear_type <- function(landings, metadata_gear_table) {
  landings %>%
    dplyr::rename(
      submission_id = .data$`_id`,
      gear_code = .data$`trip_group/gear_type`
    ) %>%
    dplyr::left_join(metadata_gear_table, by = "gear_code") %>%
    # If no vessel type is not what we expected
    dplyr::mutate(
      not_valid_code = !is.na(.data$gear_code) & is.na(.data$gear_id),
      alert_number = dplyr::if_else(isTRUE(.data$not_valid_code), 14, NA_real_)
    ) %>%
    # If no gear type was recorded when it should have
    dplyr::mutate(
      no_vessel_type = .data$`trip_group/has_boat` == "TRUE" & is.na(.data$gear_code),
      alert_number = dplyr::if_else(isTRUE(.data$no_vessel_type), 15, NA_real_)
    ) %>%
    # Fixing types
    dplyr::mutate(submission_id = as.integer(.data$submission_id)) %>%
    dplyr::select(.data$gear_id, .data$alert_number, .data$submission_id) %>%
    dplyr::rename(gear_type = .data$gear_id)
}

validate_sites <- function(landings, metadata_stations, metadata_reporting_units) {
  sites_df <-
    metadata_stations %>%
    dplyr::filter(!is.na(.data$station_code)) %>%
    dplyr::inner_join(metadata_reporting_units, by = c("reporting_unit" = "id")) %>%
    dplyr::select(.data$station_code, .data$station_name, .data$reporting_unit.y) %>%
    dplyr::mutate(station_code = as.character(.data$station_code)) %>%
    dplyr::mutate(station_name = trimws(.data$station_name)) %>%
    dplyr::rename(reporting_region = .data$reporting_unit.y)

  landings %>%
    dplyr::rename(submission_id = .data$`_id`) %>%
    dplyr::mutate(station_code = as.character(.data$landing_site_name)) %>%
    dplyr::select(.data$submission_id, .data$station_code) %>%
    dplyr::left_join(sites_df, by = "station_code") %>%
    # If the station is not known to us
    dplyr::mutate(alert_number = dplyr::if_else(is.na(.data$station_name) | is.na(.data$reporting_region), 16, NA_real_)) %>%
    # Fixing types
    dplyr::mutate(submission_id = as.integer(.data$submission_id))
}


validate_n_fishers <- function(landings, method, k) {
  landings %>%
    dplyr::select(
      submission_id = .data$`_id`,
      fisher_number_child = .data$`trip_group/no_fishers/no_child_fishers`,
      fisher_number_man = .data$`trip_group/no_fishers/no_men_fishers`,
      fisher_number_woman = .data$`trip_group/no_fishers/no_women_fishers`
    ) %>%
    dplyr::mutate(dplyr::across(tidyselect::starts_with("fisher"), as.numeric)) %>%
    dplyr::mutate(dplyr::across(tidyselect::starts_with("fisher"), list(alert = alert_outlier), alert_if_larger = 18, alert_if_smaller = 18, k = k, logt = T, method = method)) %>%
    dplyr::mutate(alert_number = dplyr::coalesce(.data$fisher_number_child_alert, .data$fisher_number_man_alert, .data$fisher_number_woman_alert)) %>%
    dplyr::mutate(dplyr::across(tidyselect::starts_with("fisher"), ~ dplyr::if_else(!is.na(alert_number), NA_real_, .))) %>%
    dplyr::select(-tidyselect::ends_with("alert")) %>%
    # Fixing types
    dplyr::mutate(submission_id = as.integer(.data$submission_id))
}


validate_habitat <- function(landings, metadata_habitat) {
  landings %>%
    dplyr::rename(
      submission_id = .data$`_id`,
      habitat_code = .data$`trip_group/habitat`
    ) %>%
    dplyr::mutate(
      habitat_code = as.numeric(.data$habitat_code),
      habitat_type = dplyr::case_when(
        habitat_code == 1 ~ "Reef",
        habitat_code == 2 ~ "FAD",
        habitat_code == 3 ~ "Deep",
        habitat_code == 4 ~ "Beach",
        habitat_code == 5 ~ "Traditional FAD",
        habitat_code == 6 ~ "Mangrove",
        habitat_code == 7 ~ "Seagrass",
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::select(.data$submission_id, .data$habitat_code, .data$habitat_type) %>%
    dplyr::mutate(
      alert_number = dplyr::case_when(
        !.data$habitat_code %in% c(metadata_habitat$habitat_code, NA_integer_) ~ 19,
        TRUE ~ NA_real_
      ),
      habitat_type = dplyr::case_when(
        is.na(alert_number) ~ .data$habitat_type,
        TRUE ~ NA_character_
      ),
      submission_id = as.integer(.data$submission_id)
    )
}

validate_mesh <- function(landings, mesh_limit) {
  landings %>%
    dplyr::select(
      submission_id = .data$`_id`,
      .data$`trip_group/mesh_size`,
      .data$`trip_group/mesh_size_other`
    ) %>%
    dplyr::mutate(
      `trip_group/mesh_size` = dplyr::case_when(
        `trip_group/mesh_size` == "seluk" ~ NA_character_, TRUE ~ .data$`trip_group/mesh_size`
      ),
      mesh_size = dplyr::coalesce(.data$`trip_group/mesh_size`, .data$`trip_group/mesh_size_other`),
      mesh_size = as.double(.data$mesh_size),
      mesh_size = .data$mesh_size * 25.4, # convert in mm
      alert_number = dplyr::case_when(
        .data$mesh_size < 0 | .data$mesh_size > mesh_limit ~ 20,
        TRUE ~ NA_real_
      ),
      mesh_size = dplyr::case_when(
        is.na(alert_number) ~ .data$mesh_size,
        TRUE ~ NA_real_
      ),
      submission_id = as.integer(.data$submission_id)
    ) %>%
    dplyr::select(-c(.data$`trip_group/mesh_size`, .data$`trip_group/mesh_size_other`))
}


validate_gleaners <- function(landings, method, k_gleaners) {
  landings %>%
    dplyr::select(
      submission_id = .data$`_id`,
      n_gleaners = .data$how_many_gleaners_today
    ) %>%
    dplyr::mutate(
      n_gleaners = as.double(.data$n_gleaners),
      n_gleaners = abs(.data$n_gleaners),
      alert_number = alert_outlier(
        x = .data$n_gleaners,
        alert_if_larger = 21, logt = TRUE, k = k_gleaners
      ),
      n_gleaners = dplyr::case_when(
        is.na(alert_number) ~ .data$n_gleaners,
        TRUE ~ NA_real_
      ),
      submission_id = as.integer(.data$submission_id)
    )
}

validate_fuel <- function(landings, method, k_fuel) {
  landings %>%
    dplyr::select(
      submission_id = .data$`_id`,
      has_boat = .data$`trip_group/has_boat`,
      fuel = .data$fuel_L
    ) %>%
    dplyr::mutate(
      submission_id = as.integer(.data$submission_id),
      fuel = as.double(.data$fuel),
      alert_number.1 = ifelse(.data$fuel < 0, 23, NA_real_),
      alert_number.2 = alert_outlier(
        x = .data$fuel,
        alert_if_larger = 23, logt = TRUE, k = k_fuel
      ),
      alert_number.3 = dplyr::case_when(.data$fuel > 0 & isFALSE(.data$has_boat) ~ 23, TRUE ~ NA_real_),
      alert_number = dplyr::coalesce(.data$alert_number.1, .data$alert_number.2, .data$alert_number.3),
      fuel = dplyr::case_when(
        is.na(alert_number) ~ .data$fuel,
        TRUE ~ NA_real_
      )
    ) %>%
    dplyr::select(.data$submission_id, .data$fuel, .data$alert_number)
}
