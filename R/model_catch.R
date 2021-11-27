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

  catch_taxa_models <- model_catch_per_taxa(trips, modelled_taxa = pars$models$modelled_taxa)
  results_per_taxa <- estimates_per_taxa(catch_taxa_models, results)

  results <- c(results, results_per_taxa)
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
    dplyr::filter(.data$landing_period > dplyr::first(na.omit(.data$first_trip)),
                  .data$landing_period < dplyr::first(na.omit(.data$last_seen))) %>%
    dplyr::select(-.data$first_trip, -.data$last_seen) %>%
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
    )#,
    # models = models
  )
}

estimates_per_taxa <- function(catch_models, general_results){

  national_estimates <- general_results$predictions$aggregated %>%
    dplyr::select(-.data$landing_weight) %>%
    dplyr::mutate(landing_revenue = NA,
                  revenue = NA,
                  catch = NA)

  estimations <- catch_models %>%
    purrr::map(predict_variable, var = "landing_weight") %>%
    purrr::imap(~ dplyr::mutate(.x, grouped_taxa = .y)) %>%
    purrr::reduce(dplyr::bind_rows)

  estimations_per_taxa <- estimations %>%
    dplyr::left_join(national_estimates) %>%
    dplyr::mutate(#revenue = .data$landing_revenue * .data$n_landings_per_boat * 2334,
      catch = .data$landing_weight * .data$n_landings_per_boat * 2334) %>%
    dplyr::arrange(.data$landing_period)

  list(
    predictions_taxa = list(aggregated = estimations_per_taxa)#,
    # models = models
    )
#
#   to_check <- estimations_per_taxa %>%
#     dplyr::group_by(landing_period) %>%
#     dplyr::summarise(catch = sum(catch)) %>%
#     dplyr::full_join(national_estimates, by = "landing_period")
#
#   to_check %>%
#     ggplot(aes(x = catch.x, y = catch.y)) +
#     geom_point() +
#     scale_x_log10() +
#     scale_y_log10()
#
#   to_check %>%
#     dplyr::mutate(diff = (catch.x - catch.y) / catch.x) %>%
#     ggplot(aes(x = landing_period, y = diff)) +
#     geom_point()
#
#   to_check %>%
#     ggplot(aes(x = landing_period)) +
#     geom_line(aes(y = catch.x), colour = "red") +
#     geom_line(aes(y = catch.y), colour = "blue")
#
#   estimations_per_taxa %>%
#     dplyr::group_by(grouped_taxa) %>%
#     dplyr::slice_tail(n = 24) %>%
#     dplyr::ungroup() %>%
#     dplyr::mutate(grouped_taxa = fct_reorder(grouped_taxa, catch, sum, na.rm = T)) %>%
#     ggplot(aes(x = landing_period, y = catch/1000)) +
#     geom_area(aes(fill = grouped_taxa), colour = "white", size =0.25) +
#     theme_minimal() +
#     facet_wrap("grouped_taxa", scales = "free")
#
#
#   estimations_per_taxa %>%
#     dplyr::ungroup() %>%
#     dplyr::mutate(grouped_taxa = fct_reorder(grouped_taxa, catch, sum, na.rm = T)) %>%
#     ggplot(aes(x = grouped_taxa, y = catch/1000)) +
#     geom_col(aes(fill = grouped_taxa))

}


#' @importFrom stats predict
predict_variable <- function(model, var){
  model$frame %>%
    dplyr::select(.data$period, .data$month) %>%
    dplyr::distinct() %>%
    dplyr::mutate(landing_period = lubridate::ym(.data$period),
                  {{var}} := predict(model, type = "response", newdata = .))
}

model_catch <- function(trips){

  catch_df <- trips %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
                                                         unit = "month")) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::filter(all(!is.na(.data$landing_period)), all(!is.na(.data$weight)), all(!is.na(.data$catch_taxon)), all(!is.na(.data$reporting_region))) %>%
    dplyr::filter(!is.na(.data$landing_period), !is.na(.data$landing_value)) %>%
    dplyr::mutate(landing_id = as.character(.data$landing_id),
                  weight = dplyr::if_else(.data$weight < 0, NA_real_, .data$weight)) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period) %>%
    dplyr::summarise(landing_weight = sum(.data$weight, na.rm = FALSE)/1000) %>%
    dplyr::mutate(year = as.character(lubridate::year(.data$landing_period)),
                  month = as.character(lubridate::month(.data$landing_period)),
                  period = paste(.data$year, .data$month, sep = "-"))

  glmmTMB(landing_weight ~ (1 | month) + (1 | period),
          ziformula = ~ (1 | month) + (1 | period),
          family = "poisson",
          data = catch_df)

}


model_catch_per_taxa <- function(trips, modelled_taxa){

  catch_df <- trips %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
                                                         unit = "month")) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::filter(all(!is.na(.data$landing_period)), all(!is.na(.data$weight)), all(!is.na(.data$catch_taxon)), all(!is.na(.data$reporting_region))) %>%
    dplyr::mutate(landing_id = as.character(.data$landing_id),
                  weight = dplyr::if_else(.data$weight < 0, NA_real_, .data$weight)) %>%
    dplyr::mutate(grouped_taxa = dplyr::if_else(.data$catch_taxon %in% c(modelled_taxa, "0"), .data$catch_taxon, "MZZ")) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period, .data$grouped_taxa) %>%
    dplyr::summarise(landing_weight = sum(.data$weight, na.rm = FALSE)/1000, .groups = "drop") %>%
    tidyr::complete(
      .data$grouped_taxa,
      tidyr::nesting(!!!dplyr::select(., tidyselect::all_of(c("landing_id", "landing_period")))),
      fill = list(landing_weight = 0)) %>%
    dplyr::mutate(year = as.character(lubridate::year(.data$landing_period)),
                  month = as.character(lubridate::month(.data$landing_period)),
                  period = paste(.data$year, .data$month, sep = "-")) %>%
    dplyr::filter(.data$grouped_taxa != "0")

  modelled_taxa <- c(modelled_taxa, "MZZ")
  models <- vector(mode = "list", length(modelled_taxa))
  names(models) <- modelled_taxa

  for (taxon in modelled_taxa) {
    message("  Evaluating model for ", taxon)
    models[[taxon]] <- glmmTMB(
      landing_weight ~ (1 | month) + (1 | period),
      # ziformula = ~ (1 | month) + (1 | period),
      family = "poisson",
      data = dplyr::filter(catch_df, grouped_taxa == taxon))
  }

  models

}

