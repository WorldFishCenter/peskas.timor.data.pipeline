#' Estimate fisheries indicators
#'
#' Uses the trip data to estimate various fisheries statistics.
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
estimate_fishery_indicators <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  trips <-
    get_merged_trips(pars) %>%
    fill_missing_regions()

  vessels_stats <- get_preprocessed_metadata(pars)$registered_boats

  municipal_estimations <-
    unique(na.omit(trips$reporting_region)) %>%
    purrr::set_names() %>%
    purrr::map(run_estimations,
      pars = pars,
      trips = trips,
      modelled_taxa = "selected",
      vessels_metadata = vessels_stats
    )

  national_estimations <- get_national_estimates(municipal_estimations = municipal_estimations)

  results <-
    list(
      national = national_estimations,
      municipal = municipal_estimations
    )

  models_filename <- add_version(pars$models$file_prefix, "rds")
  readr::write_rds(results, models_filename, compress = "gz")
  upload_cloud_file(
    models_filename,
    pars$storage$google$key,
    pars$storage$google$options
  )
}

estimate_catch <- function(trips) {
  catch_df <-
    trips %>%
    dplyr::mutate(
      landing_period = lubridate::floor_date(.data$landing_date, unit = "month"),
      landing_id = as.character(.data$landing_id)
    ) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::filter(!is.na(.data$weight)) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period) %>%
    dplyr::summarise(
      landing_weight = sum(.data$weight)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      landing_weight = .data$landing_weight / 1000
    ) %>%
    dplyr::group_by(.data$landing_period) %>%
    dplyr::summarise(landing_weight = mean(.data$landing_weight))
}

estimate_catch_taxa <- function(trips, modelled_taxa, pars) {
  if (isTRUE(modelled_taxa == "selected")) {
    taxa_list <- pars$models$modelled_taxa
  } else {
    taxa_list <- pars$models$all_taxa
  }

  catch_df <-
    trips %>%
    dplyr::mutate(
      landing_period = lubridate::floor_date(.data$landing_date, unit = "month"),
      landing_id = as.character(.data$landing_id)
    ) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::filter(!is.na(.data$weight)) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::mutate(grouped_taxa = dplyr::if_else(.data$catch_taxon %in% c(taxa_list, "0"), .data$catch_taxon, "MZZ")) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period, .data$grouped_taxa) %>%
    dplyr::summarise(
      landing_weight = sum(.data$weight),
    ) %>%
    dplyr::ungroup() %>%
    tidyr::complete(
      .data$grouped_taxa,
      tidyr::nesting(!!!dplyr::select(., tidyselect::all_of(c("landing_id", "landing_period")))),
      fill = list(landing_weight = 0)
    ) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      landing_weight = .data$landing_weight / 1000
    ) %>%
    dplyr::filter(.data$grouped_taxa != "0") %>%
    dplyr::group_by(.data$landing_period, .data$grouped_taxa) %>%
    dplyr::summarise(landing_weight = mean(.data$landing_weight, na.rm = T)) %>%
    dplyr::ungroup()
}

estimates_taxa <- function(catch_estimates, general_results, n_boats) {
  national_estimates <-
    general_results %>%
    dplyr::select(-.data$landing_weight) %>%
    dplyr::mutate(
      landing_revenue = NA,
      revenue = NA,
      catch = NA
    ) %>%
    dplyr::select(-.data$price_kg)

  estimations <-
    catch_estimates %>%
    split(.$grouped_taxa) %>%
    purrr::map(dplyr::right_join, get_frame()) %>%
    purrr::map(~ .x %>% dplyr::mutate(grouped_taxa = rep_len(unique(.data$grouped_taxa)[1], nrow(.x)))) %>%
    dplyr::bind_rows() %>%
    dplyr::group_by(.data$grouped_taxa) %>%
    dplyr::arrange(.data$landing_period, .by_group = T) %>%
    dplyr::ungroup()

  miss_df <-
    estimations %>%
    dplyr::group_by(.data$grouped_taxa) %>%
    dplyr::count(is.na(.data$landing_weight)) %>%
    dplyr::filter(.data$`is.na(.data$landing_weight)` == TRUE)

  if (nrow(miss_df) == 0) {
    imputed_df <- estimations
  } else {
    set.seed(666)
    imputed_df <-
      estimations %>%
      split(.$grouped_taxa) %>%
      purrr::map(as.data.frame) %>%
      purrr::map(Amelia::amelia,
        m = 40,
        ts = "landing_period",
        idvars = c("period", "version", "grouped_taxa"),
        sqrts = c("landing_weight", "month"),
        boot.type = "ordinary"
      ) %>%
      purrr::map(~ purrr::keep(.x, stringr::str_detect(
        names(.x), stringr::fixed("imputations")
      ))) %>%
      purrr::map(purrr::flatten) %>%
      purrr::map(dplyr::bind_rows) %>%
      dplyr::bind_rows() %>%
      dplyr::select(!dplyr::starts_with("imp")) %>%
      dplyr::group_by(.data$period, .data$month, .data$version, .data$landing_period, .data$grouped_taxa) %>%
      dplyr::summarise(dplyr::across(.cols = dplyr::everything(), ~ mean(.x))) %>%
      dplyr::ungroup()
  }

  estimations_per_taxa <-
    imputed_df %>%
    dplyr::select(-c(.data$version)) %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::left_join(national_estimates, by = c("period", "month", "landing_period")) %>%
    dplyr::mutate(
      catch = .data$landing_weight * .data$n_landings_per_boat * n_boats
    ) %>%
    split(.$grouped_taxa) %>%
    purrr::map(~ dplyr::mutate(., grouped_taxa = rep(na.omit(unique(.$grouped_taxa)), nrow(.)))) %>%
    dplyr::bind_rows() %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::ungroup()

  estimations_per_taxa
}

estimate_value <- function(trips) {
  value_df <-
    trips %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
      unit = "month"
    )) %>%
    dplyr::filter(!is.na(.data$landing_period), !is.na(.data$landing_value)) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-")
    ) %>%
    dplyr::group_by(.data$landing_period) %>%
    dplyr::summarise(landing_value = mean(.data$landing_value, na.rm = T)) %>%
    dplyr::right_join(get_frame()) %>%
    dplyr::rename(landing_revenue = .data$landing_value)

  value_df
}

estimate_indicators <- function(value_estimate, landings_model, catch_estimate, n_boats) {
  n_landings <-
    landings_model %>%
    predict_variable(var = "n_landings_per_boat") %>%
    dplyr::mutate(month = as.integer(.data$month))

  estimations <-
    list(
      n_landings,
      value_estimate,
      catch_estimate
    ) %>%
    purrr::reduce(dplyr::full_join) %>%
    dplyr::right_join(get_frame(),
      by = c("period", "month", "landing_period", "version")
    ) %>%
    dplyr::mutate(
      landing_weight = ifelse(.data$landing_weight < 0.5, NA_real_, .data$landing_weight),
      landing_revenue = ifelse(.data$landing_revenue < 0.5, NA_real_, .data$landing_revenue)
    ) %>%
    dplyr::arrange(.data$landing_period)

  set.seed(666)
  imputed_df <-
    Amelia::amelia(estimations,
      m = 40,
      ts = "landing_period",
      idvars = c("period", "version"),
      sqrts = c(
        "landing_weight", "landing_revenue",
        "n_landings_per_boat", "month"
      ),
      boot.type = "ordinary"
    )

  imputed_id <- dplyr::tibble(is_imputed = dplyr::as_tibble(imputed_df$missMatrix)$landing_weight)

  estimations_total <-
    imputed_df$imputations %>%
    purrr::keep(., stringr::str_detect(
      names(.), stringr::fixed("imp")
    )) %>%
    purrr::compact() %>%
    dplyr::bind_rows() %>%
    dplyr::group_by(.data$period, .data$month, .data$version, .data$landing_period) %>%
    dplyr::summarise(dplyr::across(.cols = dplyr::everything(), ~ mean(.x))) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::bind_cols(imputed_id) %>%
    dplyr::mutate(
      price_kg = .data$landing_revenue / .data$landing_weight,
      price_kg = ifelse(.data$price_kg > 15, NA_real_, .data$price_kg),
      landing_weight = ifelse(.data$price_kg > 15, NA_real_, .data$landing_weight),
      landing_revenue = ifelse(.data$price_kg > 15, NA_real_, .data$landing_revenue)
    ) %>%
    mice::mice(m = 5, maxit = 50, method = "pmm", seed = 666, printFlag = F) %>%
    mice::complete(action = "all") %>%
    purrr::map(dplyr::bind_rows) %>%
    dplyr::bind_rows() %>%
    dplyr::as_tibble() %>%
    dplyr::mutate(
      revenue = .data$landing_revenue * .data$n_landings_per_boat * n_boats,
      catch = .data$landing_weight * .data$n_landings_per_boat * n_boats
    ) %>%
    dplyr::select(-c(.data$version)) %>%
    dplyr::group_by(.data$period, .data$month, .data$landing_period, .data$is_imputed) %>%
    dplyr::summarise(dplyr::across(.cols = dplyr::everything(), ~ mean(.x))) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::mutate(n_boats = rep(n_boats))

  estimations_total
}

run_estimations <- function(pars, trips, region, vessels_metadata, modelled_taxa, national_level = FALSE) {
  #region <- "Lautem"
  #vessels_metadata <- vessels_stats
  if (isTRUE(national_level)) {
    trips_region <- trips
    region_boats <- sum(vessels_metadata$n_boats)
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
  value_estimate <- estimate_value(trips_region)
  catch_estimate <- estimate_catch(trips_region)
  results <- estimate_indicators(value_estimate, landings_model, catch_estimate, n_boats = region_boats)

  message("Modelling ", region, " taxa")
  catch_taxa_estimates <- estimate_catch_taxa(trips_region, modelled_taxa = modelled_taxa, pars = pars)
  taxa_estimates <- estimates_taxa(catch_taxa_estimates, results, n_boats = region_boats)
  message("Estimate taxa catch by relative composition")
  results_per_taxa <- model_taxa_porportion(results, taxa_estimates) %>% dplyr::mutate(catch = ifelse(is.na(.data$catch), 0, .data$catch))

  all_results <-
    list(
      aggregated = results,
      taxa = results_per_taxa
    )
}
