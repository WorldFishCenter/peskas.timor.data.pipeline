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
model_indicators <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  set.seed(888)

  trips <-
    get_merged_trips(pars) %>%
    fill_missing_regions()

  vessels_stats <-
    get_preprocessed_metadata(pars)$vessels_stats %>%
    dplyr::group_by(.data$reporting_region) %>%
    dplyr::summarise(n_boats = sum(.data$n_boats)) %>%
    dplyr::ungroup()

  municipal_models <-
    unique(na.omit(trips$reporting_region)) %>%
    purrr::set_names() %>%
    purrr::map(run_models,
      pars = pars,
      trips = trips,
      modelled_taxa = "selected",
      vessels_metadata = vessels_stats
    )

  national_models <-
    run_models(
      pars = pars,
      trips = trips,
      modelled_taxa = "selected",
      vessels_metadata = vessels_stats,
      nation = TRUE,
      region = "Timor"
    )

  #national_models <- get_national_estimates(municipal_estimations = municipal_models)

  results <-
    list(
      national = national_models,
      municipal = municipal_models
    )

  models_filename <- add_version(pars$models$file_prefix, "rds")
  readr::write_rds(results, models_filename, compress = "gz")
  upload_cloud_file(
    models_filename,
    pars$storage$google$key,
    pars$storage$google$options
  )
}

#' Model number of landings per unit
#'
#' @param trips data frame
#'
#' @return a glmmTMB model
#' @importFrom glmmTMB glmmTMB
model_landings <- function(trips) {
  landings_df <- trips %>%
    dplyr::mutate(
      landing_period = lubridate::floor_date(.data$tracker_trip_end,
        unit = "month"
      ),
      last_seen_period = lubridate::floor_date(.data$tracker_last_seen,
        unit = "month"
      )
    ) %>%
    # dplyr::filter(!is.na(.data$landing_period)) %>%
    dplyr::group_by(.data$tracker_imei, .data$landing_period) %>%
    dplyr::summarise(
      n_landings = dplyr::n_distinct(.data$tracker_trip_id),
      # need these two to know when the tracking started and ended
      first_trip = min(.data$landing_period),
      last_seen = max(.data$last_seen_period)
    ) %>%
    dplyr::ungroup() %>%
    # Need to account for months that are no present in the data
    tidyr::complete(.data$tracker_imei, .data$landing_period,
      fill = list(n_landings = NA)
    ) %>%
    dplyr::group_by(.data$tracker_imei) %>%
    # Removing observations from the first and last month as they are not complete
    dplyr::filter(
      .data$landing_period > dplyr::first(na.omit(.data$first_trip)),
      .data$landing_period < dplyr::first(na.omit(.data$last_seen))
    ) %>%
    dplyr::select(-.data$first_trip, -.data$last_seen) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      # season = dplyr::case_when(
      #  .data$month %in% c("1", "2", "3") ~ "winter",
      #  .data$month %in% c("4", "5", "6") ~ "spring",
      #  .data$month %in% c("7", "8", "9") ~ "summer",
      #  .data$month %in% c("10", "11", "12") ~ "fall"
      # ),
      half = dplyr::case_when(
        .data$month %in% c("1", "2", "3", "4", "5", "6") ~ "first_thalf",
        .data$month %in% c("7", "8", "9", "10", "11", "12") ~ "second_half"
      ),
      covid = dplyr::case_when(
        .data$landing_period <= "2020-03-04" ~ "pre_covid",
        .data$landing_period > "2020-03-04" ~ "post_covid"
      )
    ) %>%
    dplyr::ungroup()

  glmmTMB(n_landings ~ (1 | month) + (1 | period) + (1 | half) + (1 | covid),
    family = "poisson",
    ziformula = ~ (1 | month) + (1 | period) + (1 | half) + (1 | covid),
    data = landings_df
  )
}

#' Model landing value
#'
#' @param trips data frame
#'
#' @return a glmmTMB model
#' @importFrom glmmTMB glmmTMB
model_value <- function(trips) {
  value_df <- trips %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
      unit = "month"
    )) %>%
    # dplyr::filter(!is.na(.data$landing_period), !is.na(.data$landing_value)) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      # season = dplyr::case_when(
      #  .data$month %in% c("1", "2", "3") ~ "winter",
      #  .data$month %in% c("4", "5", "6") ~ "spring",
      #  .data$month %in% c("7", "8", "9") ~ "summer",
      #  .data$month %in% c("10", "11", "12") ~ "fall"
      # ),
      half = dplyr::case_when(
        .data$month %in% c("1", "2", "3", "4", "5", "6") ~ "first_thalf",
        .data$month %in% c("7", "8", "9", "10", "11", "12") ~ "second_half"
      ),
      covid = dplyr::case_when(
        .data$landing_period <= "2020-03-04" ~ "pre_covid",
        .data$landing_period > "2020-03-04" ~ "post_covid"
      )
    )

  # Using a zero inflated zero poisson model here. I also checked a gaussian,
  # poisson and a zero inflated poission with just an intercept and this seemed
  # to perform best. Not much difference though. The model predictions fit the
  # real data very poorly but that's expected given the limited number of
  # predictors here
  glmmTMB(landing_value ~ (1 | month) + (1 | half) + (1 | period) + (1 | covid),
    ziformula = ~ (1 | month) + (1 | half) + (1 | period) + (1 | covid),
    family = "poisson",
    data = value_df
  )
}

model_catch <- function(trips) {
  catch_df <- trips %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
      unit = "month"
    )) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::group_by(.data$landing_id) %>%
    # dplyr::filter(all(!is.na(.data$landing_period)), all(!is.na(.data$weight)), all(!is.na(.data$catch_taxon)), all(!is.na(.data$reporting_region))) %>%
    dplyr::mutate(
      landing_id = as.character(.data$landing_id),
      weight = dplyr::if_else(.data$weight < 0, NA_real_, .data$weight),
      weight = .data$weight / 1000,
    ) %>%
    # filter weights < 1Kg
    dplyr::filter(.data$weight > 1) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period) %>%
    dplyr::summarise(
      landing_weight = sum(.data$weight, na.rm = FALSE)
    ) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      # season = dplyr::case_when(
      #  .data$month %in% c("1", "2", "3") ~ "winter",
      #  .data$month %in% c("4", "5", "6") ~ "spring",
      #  .data$month %in% c("7", "8", "9") ~ "summer",
      #  .data$month %in% c("10", "11", "12") ~ "fall"
      # ),
      half = dplyr::case_when(
        .data$month %in% c("1", "2", "3", "4", "5", "6") ~ "first_thalf",
        .data$month %in% c("7", "8", "9", "10", "11", "12") ~ "second_half"
      ),
      covid = dplyr::case_when(
        .data$landing_period <= "2020-03-04" ~ "pre_covid",
        .data$landing_period > "2020-03-04" ~ "post_covid"
      )
    ) %>%
    dplyr::ungroup()

  # if (unique(trips$reporting_region) %in% c("Covalima", "Viqueque", "Baucau")) {
  #  family <- "gaussian"
  # } else {
  #  family <- "poisson"
  # }

  glmmTMB(landing_weight ~ (1 | month) + (1 | half) + (1 | period) + (1 | covid),
    ziformula = ~ (1 | month) + (1 | half) + (1 | period) + (1 | covid),
    # family = family,
    family = "poisson",
    data = catch_df
  )
}


estimate_statistics <- function(value_model, landings_model, catch_model, n_boats) {
  models <- list(
    landing_revenue = value_model,
    n_landings_per_boat = landings_model,
    landing_weight = catch_model
  )

  estimations <- models %>%
    purrr::imap(predict_variable) %>%
    purrr::reduce(dplyr::full_join)

  estimations_total <-
    estimations %>%
    dplyr::mutate(
      n_landings_per_boat = ifelse(.data$n_landings_per_boat > 30,
        stats::runif(30, 20, 30),
        .data$n_landings_per_boat
      ),
      revenue = .data$landing_revenue * .data$n_landings_per_boat * n_boats,
      catch = .data$landing_weight * .data$n_landings_per_boat * n_boats,
      dplyr::across(
        c(.data$landing_revenue, .data$n_landings_per_boat, .data$landing_weight, .data$revenue, .data$catch),
        ~ dplyr::case_when(.data$landing_weight < 0.5 ~ NA_real_, TRUE ~ .)
      )
    ) %>%
    dplyr::right_join(get_frame(), by = c("period", "month", "landing_period", "half", "covid")) %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::mutate(
      is_imputed = ifelse(is.na(.$landing_weight), "yes", "no"),
      n_landings_per_boat = imputeTS::na_interpolation(.$n_landings_per_boat, option = "spline"),
      landing_revenue = imputeTS::na_interpolation(.$landing_revenue, option = "spline"),
      landing_weight = imputeTS::na_interpolation(.$landing_weight, option = "spline"),
      n_landings_per_boat = dplyr::case_when(.data$n_landings_per_boat < 0 ~ 0, TRUE ~ .data$n_landings_per_boat),
      landing_revenue = dplyr::case_when(.data$landing_revenue < 0 ~ 0, TRUE ~ .data$landing_revenue),
      landing_weight = dplyr::case_when(.data$landing_weight < 0 ~ 0, TRUE ~ .data$landing_weight),
      revenue = .data$landing_revenue * .data$n_landings_per_boat * n_boats,
      catch = .data$landing_weight * .data$n_landings_per_boat * n_boats
    ) %>%
    dplyr::select(-c(.data$half, .data$covid)) %>%
    dplyr::arrange(.data$landing_period)

  estimations_total
}

estimates_per_taxa <- function(catch_models, general_results, n_boats) {
  national_estimates <- general_results %>%
    dplyr::select(-.data$landing_weight) %>%
    dplyr::mutate(
      landing_revenue = NA,
      revenue = NA,
      catch = NA
    )

  estimations <- catch_models %>%
    purrr::map(predict_variable, var = "landing_weight") %>%
    purrr::imap(~ dplyr::mutate(.x, grouped_taxa = .y)) %>%
    purrr::reduce(dplyr::bind_rows)

  estimations_per_taxa <- estimations %>%
    dplyr::select(-c(.data$half, .data$covid)) %>%
    dplyr::left_join(national_estimates, by = c("period", "month", "landing_period")) %>%
    dplyr::mutate(
      # revenue = .data$landing_revenue * .data$n_landings_per_boat * n_boats,
      catch = .data$landing_weight * .data$n_landings_per_boat * n_boats
    ) %>%
    split(.$grouped_taxa) %>%
    purrr::map(~ dplyr::left_join(get_frame(), ., by = c("period", "month", "landing_period"))) %>%
    purrr::map(~ dplyr::mutate(., grouped_taxa = rep(na.omit(unique(.$grouped_taxa)), nrow(.)))) %>%
    purrr::map(~ dplyr::mutate(.,
      is_imputed = ifelse(is.na(landing_weight), "yes", "no"),
      n_landings_per_boat = imputeTS::na_interpolation(.$n_landings_per_boat, option = "spline"),
      landing_weight = imputeTS::na_interpolation(.$landing_weight, option = "spline"),
      n_landings_per_boat = ifelse(n_landings_per_boat < 0, 0, n_landings_per_boat),
      landing_weight = ifelse(landing_weight < 0, 0, landing_weight),
      catch = .data$landing_weight * .data$n_landings_per_boat * n_boats,
      catch = ifelse(catch < 0, 0, catch)
    )) %>%
    dplyr::bind_rows() %>%
    dplyr::select(-c(.data$half, .data$covid)) %>%
    dplyr::arrange(.data$landing_period)

  estimations_per_taxa

  # list(
  #  predictions_taxa = list(aggregated = estimations_per_taxa) # ,
  #  # models = models
  # )
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
predict_variable <- function(model, var) {
  frame <-
    dplyr::tibble(landing_period = seq(as.Date("2018-1-1"), Sys.Date(), by = "month")) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      # season = dplyr::case_when(
      #  .data$month %in% c("1", "2", "3") ~ "winter",
      #  .data$month %in% c("4", "5", "6") ~ "spring",
      #  .data$month %in% c("7", "8", "9") ~ "summer",
      #  .data$month %in% c("10", "11", "12") ~ "fall"
      # ),
      half = dplyr::case_when(
        .data$month %in% c("1", "2", "3", "4", "5", "6") ~ "first_thalf",
        .data$month %in% c("7", "8", "9", "10", "11", "12") ~ "second_half"
      ),
      covid = dplyr::case_when(
        .data$landing_period <= "2020-03-04" ~ "pre_covid",
        .data$landing_period > "2020-03-04" ~ "post_covid"
      )
    ) %>%
    dplyr::select(.data$period, .data$month, .data$covid, .data$half)

  # new_levels <- setdiff(frame$period, models$landing_revenue$frame$period)
  # common_levels <- intersect(frame$period, models$landing_revenue$frame$period)

  model$frame %>%
    dplyr::select(.data$period, .data$month, .data$half, .data$covid) %>%
    dplyr::distinct() %>%
    # dplyr::right_join(frame) %>%
    dplyr::mutate(
      landing_period = lubridate::ym(.data$period),
      {{ var }} := predict(model, type = "response", newdata = .)
    )
}


model_catch_per_taxa <- function(trips, modelled_taxa, pars) {
  if (isTRUE(modelled_taxa == "selected")) {
    taxa_list <- pars$models$modelled_taxa
  } else {
    taxa_list <- pars$models$all_taxa
  }

  catch_df <- trips %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
      unit = "month"
    )) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::filter(all(!is.na(.data$landing_period)), all(!is.na(.data$weight)), all(!is.na(.data$catch_taxon))) %>%
    dplyr::mutate(
      landing_id = as.character(.data$landing_id),
      weight = dplyr::if_else(.data$weight < 0, NA_real_, .data$weight)
    ) %>%
    dplyr::mutate(grouped_taxa = dplyr::if_else(.data$catch_taxon %in% c(taxa_list, "0"), .data$catch_taxon, "MZZ")) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period, .data$grouped_taxa) %>%
    dplyr::summarise(
      landing_weight = sum(.data$weight, na.rm = FALSE) / 1000,
      .groups = "drop"
    ) %>%
    tidyr::complete(
      .data$grouped_taxa,
      tidyr::nesting(!!!dplyr::select(., tidyselect::all_of(c("landing_id", "landing_period")))),
      fill = list(landing_weight = 0)
    ) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      # season = dplyr::case_when(
      #  .data$month %in% c("1", "2", "3") ~ "winter",
      #  .data$month %in% c("4", "5", "6") ~ "spring",
      #  .data$month %in% c("7", "8", "9") ~ "summer",
      #  .data$month %in% c("10", "11", "12") ~ "fall"
      # ),
      half = dplyr::case_when(
        .data$month %in% c("1", "2", "3", "4", "5", "6") ~ "first_thalf",
        .data$month %in% c("7", "8", "9", "10", "11", "12") ~ "second_half"
      ),
      covid = dplyr::case_when(
        .data$landing_period <= "2020-03-04" ~ "pre_covid",
        .data$landing_period > "2020-03-04" ~ "post_covid"
      )
    ) %>%
    dplyr::filter(.data$grouped_taxa != "0") %>%
    dplyr::ungroup()

  modelled_taxa <- intersect(unique(catch_df$grouped_taxa), taxa_list)
  # modelled_taxa <- c(modelled_taxa, "MZZ")
  models <- vector(mode = "list", length(modelled_taxa))
  names(models) <- modelled_taxa

  for (taxon in modelled_taxa) {
    message("  Evaluating model for ", taxon)
    models[[taxon]] <- glmmTMB(
      landing_weight ~ (1 | month) + (1 | half) + (1 | period) + (1 | covid),
      # ziformula = ~ (1 | month) + (1 | period),
      family = "poisson",
      data = dplyr::filter(catch_df, .data$grouped_taxa == taxon)
    )
  }

  models
}

run_models <- function(pars, trips, region, vessels_metadata, modelled_taxa, model_family, national = FALSE) {
  if (isTRUE(national)) {
    trips_region <- trips
    region_boats <- 2334
    region <- "Timor"
  } else {
    trips_region <-
      trips %>%
      dplyr::filter(.data$reporting_region == region)

    region_boats <-
      vessels_metadata %>%
      dplyr::filter(.data$reporting_region == region) %>%
      dplyr::summarise(n_boats = sum(.data$n_boats, na.rm = T)) %>%
      magrittr::extract2("n_boats")
  }

  message("Modelling ", region)

  landings_model <- model_landings(trips_region)
  value_model <- model_value(trips_region)
  catch_model <- model_catch(trips_region)
  results <- estimate_statistics(landings_model, value_model, catch_model, n_boats = region_boats)

  message("Modelling ", region, " taxa")

  catch_taxa_models <- model_catch_per_taxa(trips_region, modelled_taxa = modelled_taxa, pars = pars)
  results_per_taxa <- estimates_per_taxa(catch_taxa_models, results, n_boats = region_boats)

  all_results <-
    list(
      aggregated = results,
      taxa = results_per_taxa
    )
}


get_national_estimates <- function(municipal_estimations = NULL) {
  summarise_national <- function(x) {
    x %>%
      dplyr::summarise(
        landing_revenue = mean(.data$landing_revenue, na.rm = TRUE),
        n_landings_per_boat = mean(.data$n_landings_per_boat, na.rm = TRUE),
        landing_weight = mean(.data$landing_weight, na.rm = TRUE),
        revenue = sum(.data$revenue),
        catch = sum(.data$catch)
      ) %>%
      dplyr::ungroup() %>%
      dplyr::arrange(.data$landing_period) %>%
      as.data.frame()
  }

  aggregated <-
    municipal_estimations %>%
    purrr::map(~ purrr::keep(.x, stringr::str_detect(
      names(.x), stringr::fixed("aggregated")
    ))) %>%
    purrr::flatten() %>%
    purrr::set_names(names(municipal_estimations)) %>%
    dplyr::bind_rows() %>%
    dplyr::group_by(.data$period, .data$month, .data$landing_period) %>%
    summarise_national()


  aggregated_taxa <-
    municipal_estimations %>%
    purrr::map(~ purrr::keep(.x, stringr::str_detect(
      names(.x), stringr::fixed("taxa")
    ))) %>%
    purrr::flatten() %>%
    purrr::set_names(names(municipal_estimations)) %>%
    dplyr::bind_rows() %>%
    dplyr::group_by(.data$period, .data$month, .data$landing_period, .data$grouped_taxa) %>%
    summarise_national()

  list(
    aggregated = aggregated,
    aggregated_taxa = aggregated_taxa
  )
}


get_frame <- function() {
  dplyr::tibble(landing_period = seq(as.Date("2018-1-1"), Sys.Date(), by = "month")) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      # season = dplyr::case_when(
      #  .data$month %in% c("1", "2", "3") ~ "winter",
      #  .data$month %in% c("4", "5", "6") ~ "spring",
      #  .data$month %in% c("7", "8", "9") ~ "summer",
      #  .data$month %in% c("10", "11", "12") ~ "fall"
      # ),
      half = dplyr::case_when(
        .data$month %in% c("1", "2", "3", "4", "5", "6") ~ "first_thalf",
        .data$month %in% c("7", "8", "9", "10", "11", "12") ~ "second_half"
      ),
      covid = dplyr::case_when(
        .data$landing_period <= "2020-03-04" ~ "pre_covid",
        .data$landing_period > "2020-03-04" ~ "post_covid"
      )
    ) %>%
    dplyr::select(.data$landing_period, .data$period, .data$month, .data$half, .data$covid)
}
