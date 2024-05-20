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
    unique(na.omit(trips$municipality)) %>%
    purrr::set_names() %>%
    purrr::map(run_estimations,
      pars = pars,
      trips = trips,
      modelled_taxa = "selected",
      vessels_metadata = vessels_stats,
      national_level = FALSE
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
    dplyr::filter(!is.na(.data$catch)) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period) %>%
    dplyr::summarise(
      landing_catch = sum(.data$catch)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      landing_catch = .data$landing_catch / 1000
    )

  overall_mean <- mean(catch_df$landing_catch, na.rm = TRUE)

  small_groups <-
    catch_df %>%
    dplyr::group_by(.data$landing_period) %>%
    dplyr::mutate(n = dplyr::n()) %>%
    dplyr::filter(.data$n < 3) %>%
    dplyr::ungroup()

  # Create new rows for these groups
  new_rows <- small_groups %>%
    dplyr::slice(rep(1:dplyr::n(), each = 5)) %>%
    dplyr::mutate(landing_catch = overall_mean)

  catch_df <-
    dplyr::bind_rows(catch_df, new_rows) %>%
    dplyr::select(-.data$n) %>%
    dplyr::group_by(.data$landing_period) %>%
    dplyr::summarise(landing_catch = mean(.data$landing_catch)) %>%
    dplyr::ungroup()

  catch_df
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
    dplyr::filter(!is.na(.data$catch)) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::mutate(grouped_taxa = dplyr::if_else(.data$catch_taxon %in% c(taxa_list, "0"), .data$catch_taxon, "MZZ")) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period, .data$grouped_taxa) %>%
    dplyr::summarise(
      landing_catch = sum(.data$catch),
    ) %>%
    dplyr::ungroup() %>%
    tidyr::complete(
      .data$grouped_taxa,
      tidyr::nesting(!!!dplyr::select(., tidyselect::all_of(c("landing_id", "landing_period")))),
      fill = list(landing_catch = 0)
    ) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      landing_catch = .data$landing_catch / 1000
    ) %>%
    dplyr::filter(.data$grouped_taxa != "0") %>%
    dplyr::group_by(.data$landing_period, .data$grouped_taxa) %>%
    dplyr::summarise(landing_catch = mean(.data$landing_catch, na.rm = T)) %>%
    dplyr::ungroup()
}

estimates_taxa <- function(catch_estimates, general_results, n_boats) {
  national_estimates <-
    general_results %>%
    dplyr::select(-.data$landing_catch) %>%
    dplyr::mutate(
      landing_catch_price = NA,
      catch_price = NA,
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
    dplyr::count(is.na(.data$landing_catch)) %>%
    dplyr::filter(.data$`is.na(.data$landing_catch)` == TRUE)

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
        sqrts = c("landing_catch", "month"),
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
      catch = .data$landing_catch * .data$n_landings_per_boat * n_boats
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
    dplyr::select(.data$landing_date, .data$catch_price) %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
      unit = "month"
    )) %>%
    dplyr::filter(!is.na(.data$landing_period), !is.na(.data$catch_price)) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-")
    )

  overall_mean <- mean(value_df$catch_price, na.rm = TRUE)

  small_groups <-
    value_df %>%
    dplyr::group_by(.data$landing_period) %>%
    dplyr::mutate(n = dplyr::n()) %>%
    dplyr::filter(.data$n < 3) %>%
    dplyr::ungroup()

  # Create new rows for these groups
  new_rows <- small_groups %>%
    dplyr::slice(rep(1:dplyr::n(), each = 5)) %>%
    dplyr::mutate(catch_price = overall_mean)

  value_df <-
    dplyr::bind_rows(value_df, new_rows) %>%
    dplyr::select(-.data$n) %>%
    dplyr::group_by(.data$landing_period) %>%
    dplyr::summarise(catch_price = mean(.data$catch_price, na.rm = T)) %>%
    dplyr::right_join(get_frame()) %>%
    dplyr::rename(landing_catch_price = .data$catch_price) %>%
    dplyr::ungroup()

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
      landing_catch = ifelse(.data$landing_catch < 0.5, NA_real_, .data$landing_catch),
      landing_catch_price = ifelse(.data$landing_catch_price < 0.5, NA_real_, .data$landing_catch_price)
    ) %>%
    dplyr::arrange(.data$landing_period)

  set.seed(666)
  imputed_df <-
    Amelia::amelia(estimations,
      m = 40,
      ts = "landing_period",
      idvars = c("period", "version"),
      sqrts = c(
        "landing_catch", "landing_catch_price",
        "n_landings_per_boat", "month"
      ),
      boot.type = "ordinary"
    )

  imputed_id <- dplyr::tibble(is_imputed = dplyr::as_tibble(imputed_df$missMatrix)$landing_catch)

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
      price_kg = .data$landing_catch_price / .data$landing_catch,
      price_kg = ifelse(.data$price_kg > 15, NA_real_, .data$price_kg),
      landing_catch = ifelse(.data$price_kg > 15, NA_real_, .data$landing_catch),
      landing_catch_price = ifelse(.data$price_kg > 15, NA_real_, .data$landing_catch_price)
    ) %>%
    mice::mice(m = 5, maxit = 50, method = "pmm", seed = 666, printFlag = F) %>%
    mice::complete(action = "all") %>%
    purrr::map(dplyr::bind_rows) %>%
    dplyr::bind_rows() %>%
    dplyr::as_tibble() %>%
    dplyr::mutate(
      catch_price = .data$landing_catch_price * .data$n_landings_per_boat * n_boats,
      catch = .data$landing_catch * .data$n_landings_per_boat * n_boats
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
  # region <- "Lautem"
  # vessels_metadata <- vessels_stats
  if (isTRUE(national_level)) {
    trips_region <- trips
    region_boats <- sum(vessels_metadata$n_boats)
    municipality <- "Timor"
  } else {
    trips_region <-
      trips %>%
      dplyr::filter(.data$municipality == region)

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


#' @importFrom stats predict
predict_variable <- function(model, var) {
  model$frame %>%
    dplyr::select(.data$period, .data$month, .data$version) %>%
    dplyr::distinct() %>%
    # dplyr::right_join(frame) %>%
    dplyr::mutate(
      landing_period = lubridate::ym(.data$period),
      {{ var }} := predict(model, type = "response", newdata = .)
    )
}

get_national_estimates <- function(municipal_estimations = NULL) {
  aggregated <-
    municipal_estimations %>%
    purrr::map(~ purrr::keep(.x, stringr::str_detect(
      names(.x), stringr::fixed("aggregated")
    ))) %>%
    purrr::flatten() %>%
    purrr::set_names(names(municipal_estimations)) %>%
    dplyr::bind_rows() %>%
    dplyr::group_by(.data$period, .data$month, .data$landing_period) %>%
    dplyr::summarise(
      landing_catch_price = mean(.data$landing_catch_price, na.rm = T),
      n_landings_per_boat = mean(.data$n_landings_per_boat, na.rm = T),
      landing_catch = mean(.data$landing_catch, na.rm = T),
      catch_price = sum(.data$catch_price, na.rm = T),
      catch = sum(.data$catch, na.rm = T),
      price_kg = stats::median(.data$price_kg, na.rm = T)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(.data$landing_period)


  taxa <-
    municipal_estimations %>%
    purrr::map(~ purrr::keep(.x, stringr::str_detect(
      names(.x), stringr::fixed("taxa")
    ))) %>%
    purrr::flatten() %>%
    purrr::set_names(names(municipal_estimations)) %>%
    dplyr::bind_rows() %>%
    dplyr::group_by(.data$period, .data$month, .data$landing_period, .data$grouped_taxa) %>%
    dplyr::summarise(
      landing_catch_price = mean(.data$landing_catch_price, na.rm = TRUE),
      n_landings_per_boat = mean(.data$n_landings_per_boat, na.rm = TRUE),
      landing_catch = mean(.data$landing_catch, na.rm = TRUE),
      catch_price = sum(.data$catch_price),
      catch = sum(.data$catch)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(.data$landing_period)

  list(
    aggregated = aggregated,
    taxa = taxa
  )
}

get_frame <- function() {
  dplyr::tibble(landing_period = seq(as.Date("2018-1-1"), Sys.Date(), by = "month")) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.integer(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      version = dplyr::case_when(
        .data$landing_period <= "2019-05-01" ~ "v1",
        .data$landing_period > "2019-05-01" ~ "v2"
      )
    ) %>%
    dplyr::select(.data$landing_period, .data$period, .data$month, .data$version)
}

#' Fill missing regions
#'
#' Replace empty region data based on boats geographic activity (trough tracker imeis)
#'
#' @param trips Dataframe  with Timor validated trips.
#'
#' @return A dataframe with filled regions (where possible).
#' @export
#'
#' @examples
fill_missing_regions <- function(trips = NULL) {
  imei_regions <-
    trips %>%
    dplyr::filter(!is.na(.data$tracker_imei)) %>%
    dplyr::select(.data$tracker_imei, .data$municipality) %>%
    dplyr::group_by(.data$tracker_imei) %>%
    dplyr::count(.data$municipality) %>%
    dplyr::filter(!is.na(.data$municipality)) %>%
    dplyr::group_by(.data$tracker_imei) %>%
    dplyr::arrange(dplyr::desc(.data$n), .by_group = TRUE) %>%
    dplyr::summarise(municipality = dplyr::first(.data$municipality)) %>%
    dplyr::rename(municipality_fill = .data$municipality) %>%
    dplyr::ungroup()

  dplyr::full_join(trips, imei_regions, by = "tracker_imei") %>%
    dplyr::mutate(municipality = dplyr::case_when(
      is.na(.data$municipality) ~ .data$municipality_fill,
      TRUE ~ municipality
    )) %>%
    dplyr::select(-.data$municipality_fill)
}

model_taxa_porportion <- function(aggregated_results, taxa_results) {
  t_results <-
    aggregated_results %>%
    dplyr::select(-c(
      .data$month, .data$is_imputed, .data$n_boats,
      .data$landing_catch_price, .data$n_landings_per_boat,
      .data$catch_price, .data$landing_catch, .data$price_kg
    )) %>%
    dplyr::rename(tot_catch = .data$catch)

  results_taxa <-
    taxa_results %>%
    dplyr::select(-c(.data$is_imputed, .data$n_boats)) %>%
    dplyr::rename(catch_taxa = .data$catch) %>%
    dplyr::group_by(.data$landing_period) %>%
    dplyr::mutate(
      tot_catch_taxa = sum(.data$catch_taxa),
      taxa_prop = (.data$catch_taxa / .data$tot_catch_taxa) * 100
    ) %>%
    dplyr::select(-c(.data$tot_catch_taxa, .data$catch_taxa)) %>%
    dplyr::ungroup()

  estimations_per_taxa <-
    dplyr::left_join(results_taxa, t_results, by = c("landing_period", "period")) %>%
    dplyr::mutate(
      catch = (.data$taxa_prop / 100) * .data$tot_catch
    ) %>%
    dplyr::select(-c(.data$taxa_prop, .data$tot_catch))

  estimations_per_taxa
}
