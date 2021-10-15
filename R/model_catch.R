#' Model fisheries indicators
#'
#' Uses the trip data to model various fisheries statistics.
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' models:
#'   file_prefix:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' @param log_threshold
#' @inheritParams ingest_landings
#'
#' @export
#' @keywords workflow
#'
model_indicators <- function(log_threshold = logger::DEBUG){
  logger::log_threshold(log_threshold)
  pars <- read_config()

  trips <- get_merged_trips(pars)

  landings_model <- model_landings(trips)
  value_model <- model_value(trips)
  catch_model <- model_catch(trips)
  results <- estimate_statistics(landings_model, value_model, catch_model)

  models_filename <- add_version(pars$models$file_prefix, "rds")
  readr::write_rds(results, models_filename, compress = "gz")
  upload_cloud_file(models_filename,
                    pars$storage$google$key,
                    pars$storage$google$options)
}

#' Model number of landings per unit
#'
#' @param trips data frame
#'
#' @return a glmmTMB model
#' @importFrom glmmTMB glmmTMB
model_landings <- function(trips){

  landings_df <- trips %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$tracker_trip_end,
                                                         unit = "month"),
                  last_seen_period = lubridate::floor_date(.data$tracker_last_seen,
                                                           unit = "month")) %>%
    dplyr::filter(!is.na(.data$landing_period)) %>%
    dplyr::group_by(.data$tracker_imei, .data$landing_period) %>%
    dplyr::summarise(n_landings = dplyr::n_distinct(.data$tracker_trip_id),
                     # need these two to know when the tracking started and ended
                     first_trip = min(.data$landing_period),
                     last_seen = max(.data$last_seen_period)) %>%
    dplyr::ungroup() %>%
    # Need to account for months that are no present in the data
    tidyr::complete(.data$tracker_imei, .data$landing_period,
                    fill = list(n_landings = 0)) %>%
    dplyr::group_by(.data$tracker_imei) %>%
    # Removing observations from the first and last month as they are not complete
    dplyr::filter(landing_period > dplyr::first(na.omit(.data$first_trip)),
                  landing_period < dplyr::first(na.omit(.data$last_seen))) %>%
    dplyr::select(-first_trip, -last_seen) %>%
    dplyr::mutate(year = as.character(lubridate::year(.data$landing_period)),
                  month = as.character(lubridate::month(.data$landing_period)),
                  period = paste(.data$year, .data$month, sep = "-")) %>%
    dplyr::ungroup()

  glmmTMB(n_landings ~ (1 | month) + (1 | period),
          ziformula = ~ (1 | month) + (1 | period),
          family = "poisson",
          data = landings_df)
}


#' Model landing value
#'
#' @param trips data frame
#'
#' @return a glmmTMB model
#' @importFrom glmmTMB glmmTMB
model_value <- function(trips){

  value_df <- trips %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
                                                         unit = "month")) %>%
    dplyr::filter(!is.na(.data$landing_period), !is.na(.data$landing_value)) %>%
    dplyr::mutate(year = as.character(lubridate::year(.data$landing_period)),
                  month = as.character(lubridate::month(.data$landing_period)),
                  period = paste(.data$year, .data$month, sep = "-"))

  # Using a zero inflated zero poisson model here. I also checked a gaussian,
  # poisson and a zero inflated poission with just an intercept and this seemed
  # to perform best. Not much difference though. The model predictions fit the
  # real data very poorly but that's expected given the limited number of
  # predictors here
  glmmTMB(landing_value ~ (1 | month) + (1 | period),
          ziformula = ~ (1 | month) + (1 | period),
          family = "poisson",
          data = value_df)

}

estimate_statistics <- function(value_model, landings_model, catch_model){

  models <- list(landing_revenue = value_model,
                 n_landings_per_boat = landings_model,
                 landing_weight = catch_model)

  estimations <- models %>%
    purrr::imap(predict_variable) %>%
    purrr::reduce(dplyr::full_join)

  estimations_total <- estimations %>%
    dplyr::mutate(revenue = .data$landing_revenue * .data$n_landings_per_boat * 2334,
                  catch = .data$landing_weight * .data$n_landings_per_boat * 2334) %>%
    dplyr::arrange(.data$landing_period)

  list(
    predictions = list(
      aggregated = estimations_total
    ),
    models = models
  )
}


#' @importFrom stats predict
predict_variable <- function(model, var){
  model$frame %>%
    dplyr::select(period, month) %>%
    dplyr::distinct() %>%
    dplyr::mutate(landing_period = lubridate::ym(period),
                  {{var}} := predict(model, type = "response", newdata = .))
}

model_catch <- function(trips){

  catch_df <- trips %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
                                                         unit = "month")) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::filter(!is.na(.data$landing_period), !is.na(.data$landing_value)) %>%
    dplyr::mutate(landing_id = as.character(.data$landing_id),
                  weight = dplyr::if_else(.data$weight < 0, NA_real_, .data$weight)) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period) %>%
    summarise(landing_weight = sum(weight, na.rm = FALSE)/1000) %>%
    dplyr::mutate(year = as.character(lubridate::year(.data$landing_period)),
                  month = as.character(lubridate::month(.data$landing_period)),
                  period = paste(.data$year, .data$month, sep = "-"))

  glmmTMB(landing_weight ~ (1 | month) + (1 | period),
          ziformula = ~ (1 | month) + (1 | period),
          family = "poisson",
          data = catch_df)

}
}
