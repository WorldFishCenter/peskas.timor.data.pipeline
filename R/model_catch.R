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

  trips <-
    get_merged_trips(pars) %>%
    fill_missing_regions()

  vessels_stats <- get_preprocessed_metadata(pars)$registered_boats

  municipal_models <-
    unique(na.omit(trips$reporting_region)) %>%
    purrr::set_names() %>%
    purrr::map(run_models,
      pars = pars,
      trips = trips,
      modelled_taxa = "selected",
      vessels_metadata = vessels_stats
    )

  national_models <- get_national_estimates(municipal_estimations = municipal_models)


  # national_models <-
  #  run_models(
  #    pars = pars,
  #    trips = trips,
  #    modelled_taxa = "selected",
  #    vessels_metadata = vessels_stats,
  #    national_level = TRUE,
  #    region = "Timor"
  #  )


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
  landings_df <-
    trips %>%
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
    dplyr::ungroup() %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      version = dplyr::case_when(
        .data$landing_period <= "2019-05-01" ~ "v1",
        .data$landing_period > "2019-05-01" ~ "v2"
      )
    ) %>%
    dplyr::ungroup()

  if (unique(trips$reporting_region) == "Lautem") {
    family <- "poisson"
  } else {
    family <- "Gamma"
  }

  glmmTMB(n_landings ~ (1 | month) + (1 | period) + (1 | version),
    family = family,
    data = landings_df
  )
}

model_catch <- function(trips) {
  catch_df <-
    trips %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
      unit = "month"
    )) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::mutate(
      landing_id = as.character(.data$landing_id),
      weight = .data$weight / 1000,
    ) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period) %>%
    dplyr::summarise(
      landing_weight = sum(.data$weight),
      landing_value = dplyr::first(.data$landing_value),
    ) %>%
    dplyr::mutate(landing_weight = ifelse(is.na(.data$landing_value),
      NA_real_, .data$landing_weight
    )) %>%
    dplyr::select(-.data$landing_value) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      version = dplyr::case_when(
        .data$landing_period <= "2019-05-01" ~ "v1",
        .data$landing_period > "2019-05-01" ~ "v2"
      )
    ) %>%
    dplyr::ungroup()

  glmmTMB(landing_weight ~ (1 | month) + (1 | period) + (1 | version),
    ziformula = ~ (1 | month) + (1 | period) + (1 | version),
    family = "poisson",
    data = catch_df,
    control = glmmTMB::glmmTMBControl(
      conv_check = "skip",
      optimizer = stats::optim,
      optArgs = list(method = "BFGS")
    )
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
    # dplyr::filter(all(!is.na(.data$landing_period)), all(!is.na(.data$weight)), all(!is.na(.data$catch_taxon))) %>%
    dplyr::mutate(
      landing_id = as.character(.data$landing_id),
      weight = .data$weight / 1000,
    ) %>%
    dplyr::mutate(grouped_taxa = dplyr::if_else(.data$catch_taxon %in% c(taxa_list, "0"), .data$catch_taxon, "MZZ")) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period, .data$grouped_taxa) %>%
    dplyr::summarise(
      landing_weight = sum(.data$weight),
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
      version = dplyr::case_when(
        .data$landing_period <= "2019-05-01" ~ "v1",
        .data$landing_period > "2019-05-01" ~ "v2"
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
      landing_weight ~ (1 | month) + (1 | version) + (1 | period),
      ziformula = ~ (1 | month) + (1 | version) + (1 | period),
      family = "poisson",
      data = dplyr::filter(catch_df, .data$grouped_taxa == taxon),
      control = glmmTMB::glmmTMBControl(
        conv_check = "skip",
        optimizer = stats::optim,
        optArgs = list(method = "BFGS")
      )
    )
  }

  models
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
    dplyr::filter(!is.na(.data$landing_period), !is.na(.data$landing_value)) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-"),
      version = dplyr::case_when(
        .data$landing_period <= "2019-05-01" ~ "v1",
        .data$landing_period > "2019-05-01" ~ "v2"
      )
    )

  # Using a zero inflated zero poisson model here. I also checked a gaussian,
  # poisson and a zero inflated poission with just an intercept and this seemed
  # to perform best. Not much difference though. The model predictions fit the
  # real data very poorly but that's expected given the limited number of
  # predictors here
  glmmTMB(landing_value ~ (1 | month) + (1 | period) + (1 | version),
    ziformula = ~ (1 | month) + (1 | period) + (1 | version),
    family = "poisson",
    data = value_df
  )
}

run_models <- function(pars, trips, region, vessels_metadata, modelled_taxa, national_level = FALSE) {
  # region <- "Dili"
  # vessels_metadata <- vessels_stats
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

  pk_ids <-
    trips_region %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
      unit = "month"
    )) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::filter(!.data$catch_taxon %in% c("MZZ", "IAX", "SWX")) %>%
    magrittr::extract2("landing_id") %>%
    unique()

  trips_region_pk <-
    trips_region %>%
    dplyr::filter(.data$landing_id %in% pk_ids)

  message("Modelling ", region)

  landings_model <- model_landings(trips_region)
  value_model <- model_value(trips_region)
  value_model_pk <- model_value(trips_region_pk)
  catch_model <- model_catch(trips_region)
  results <- estimate_statistics(value_model, value_model_pk, landings_model, catch_model, n_boats = region_boats)

  message("Modelling ", region, " taxa")
  catch_taxa_models <- model_catch_per_taxa(trips_region, modelled_taxa = "selected", pars = pars)
  taxa_estimates <- estimates_per_taxa(catch_taxa_models, results, n_boats = region_boats)
  message("Estimate taxa catch by relative composition")
  results_per_taxa <- model_taxa_porportion(results, taxa_estimates)


  all_results <-
    list(
      aggregated = results,
      taxa = results_per_taxa
    )
}

estimate_statistics <- function(value_model, value_model_pk, landings_model, catch_model, n_boats) {
  models <- list(
    landing_revenue = value_model,
    landing_revenue_pk = value_model_pk,
    n_landings_per_boat = landings_model,
    landing_weight = catch_model
  )

  estimations <-
    models %>%
    purrr::imap(predict_variable) %>%
    purrr::reduce(dplyr::full_join) %>%
    dplyr::mutate(month = as.integer(.data$month)) %>%
    dplyr::right_join(get_frame(),
      by = c("period", "month", "landing_period", "version")
    ) %>%
    dplyr::arrange(.data$landing_period)

  set.seed(666)
  imputed_df <-
    Amelia::amelia(estimations,
      m = 20,
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
    dplyr::bind_rows() %>%
    dplyr::group_by(.data$period, .data$month, .data$version, .data$landing_period) %>%
    dplyr::summarise(dplyr::across(.cols = dplyr::everything(), ~ mean(.x))) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::bind_cols(imputed_id) %>%
    dplyr::mutate(
      landing_weight = ifelse(.data$landing_weight < 0.25, NA_real_, .data$landing_weight),
      landing_revenue = ifelse(.data$landing_revenue < 1, NA_real_, .data$landing_revenue),
      landing_revenue_pk = ifelse(.data$landing_revenue_pk < 1, NA_real_, .data$landing_revenue_pk)
    ) %>%
    mice::mice(m = 5, maxit = 500, method = "pmm", seed = 666, printFlag = F) %>%
    mice::complete(action = "all") %>%
    purrr::map(dplyr::bind_rows) %>%
    dplyr::bind_rows() %>%
    dplyr::as_tibble() %>%
    dplyr::group_by(.data$period, .data$month, .data$version, .data$landing_period, .data$is_imputed) %>%
    dplyr::summarise(dplyr::across(.cols = dplyr::everything(), ~ mean(.x))) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::mutate(
      revenue = .data$landing_revenue * .data$n_landings_per_boat * n_boats,
      revenue_pk = .data$landing_revenue_pk * .data$n_landings_per_boat * n_boats,
      catch = .data$landing_weight * .data$n_landings_per_boat * n_boats,
      price_kg = .data$revenue_pk / .data$catch
    ) %>%
    dplyr::select(-c(.data$version)) %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::mutate(n_boats = rep(n_boats)) %>%
    dplyr::select(-c(.data$landing_revenue_pk, .data$revenue_pk)) %>%
    mice::mice(m = 5, maxit = 500, method = "pmm", seed = 666, printFlag = F) %>%
    mice::complete(action = "all") %>%
    purrr::map(dplyr::bind_rows) %>%
    dplyr::bind_rows() %>%
    dplyr::as_tibble() %>%
    dplyr::group_by(.data$period, .data$month, .data$landing_period, .data$is_imputed) %>%
    dplyr::summarise(dplyr::across(.cols = dplyr::everything(), ~ mean(.x))) %>%
    dplyr::ungroup()

  estimations_total
}

estimates_per_taxa <- function(catch_models, general_results, n_boats) {
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
    catch_models %>%
    purrr::map(predict_variable, var = "landing_weight") %>%
    purrr::imap(~ dplyr::mutate(.x, grouped_taxa = .y)) %>%
    purrr::reduce(dplyr::bind_rows) %>%
    dplyr::mutate(month = as.integer(.data$month)) %>%
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
        m = 10,
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
      landing_revenue = mean(.data$landing_revenue, na.rm = T),
      n_landings_per_boat = mean(.data$n_landings_per_boat, na.rm = T),
      landing_weight = mean(.data$landing_weight, na.rm = T),
      revenue = sum(.data$revenue, na.rm = T),
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
      landing_revenue = mean(.data$landing_revenue, na.rm = TRUE),
      n_landings_per_boat = mean(.data$n_landings_per_boat, na.rm = TRUE),
      landing_weight = mean(.data$landing_weight, na.rm = TRUE),
      revenue = sum(.data$revenue),
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
    dplyr::select(.data$tracker_imei, .data$reporting_region) %>%
    dplyr::group_by(.data$tracker_imei) %>%
    dplyr::count(.data$reporting_region) %>%
    dplyr::filter(!is.na(.data$reporting_region)) %>%
    dplyr::group_by(.data$tracker_imei) %>%
    dplyr::arrange(dplyr::desc(.data$n), .by_group = TRUE) %>%
    dplyr::summarise(reporting_region = dplyr::first(.data$reporting_region)) %>%
    dplyr::rename(reporting_region_fill = .data$reporting_region) %>%
    dplyr::ungroup()

  dplyr::full_join(trips, imei_regions, by = "tracker_imei") %>%
    dplyr::mutate(reporting_region = dplyr::case_when(
      is.na(.data$reporting_region) ~ .data$reporting_region_fill,
      TRUE ~ reporting_region
    )) %>%
    dplyr::select(-.data$reporting_region_fill)
}

model_taxa_porportion <- function(aggregated_results, taxa_results) {
  t_results <-
    aggregated_results %>%
    dplyr::select(-c(
      .data$month, .data$is_imputed, .data$n_boats,
      .data$landing_revenue, .data$n_landings_per_boat,
      .data$revenue, .data$landing_weight, .data$price_kg
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
