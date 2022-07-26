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
generate_estimates <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  trips <-
    get_merged_trips(pars) %>%
    fill_missing_regions()
  vessels_stats <- get_preprocessed_metadata(pars)$vessels_stats

  municipal_estimates <-
    unique(na.omit(trips$reporting_region)) %>%
    purrr::set_names() %>%
    purrr::map(estimate_indicators,
      trips = trips,
      vessels_metadata = vessels_stats
    )

  national_estimates <-
    municipal_estimates %>%
    purrr::reduce(dplyr::bind_rows) %>%
    dplyr::select(-.data$region) %>%
    dplyr::group_by(.data$period, .data$month, .data$landing_period) %>%
    dplyr::summarise(
      landing_revenue = mean(.data$landing_revenue, na.rm = TRUE),
      n_landings_per_boat = mean(.data$n_landings_per_boat, na.rm = TRUE),
      landing_weight = mean(.data$landing_weight, na.rm = TRUE),
      revenue = sum(.data$revenue, na.rm = TRUE),
      catch = sum(.data$catch, na.rm = TRUE)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(.data$landing_period)

  results <-
    list(
      predictions = list(
        aggregated = national_estimates,
        municipal = municipal_estimates
      )
    )

  municipal_estimates_taxa <-
    unique(na.omit(trips$reporting_region)) %>%
    purrr::set_names() %>%
    purrr::map(estimate_indicators_taxa,
      trips = trips,
      municipal_estimates = municipal_estimates,
      vessels_metadata = vessels_stats,
      modelled_taxa = pars$models$modelled_taxa
    )

  national_estimates_taxa <-
    municipal_estimates_taxa %>%
    purrr::reduce(dplyr::bind_rows) %>%
    dplyr::group_by(.data$period, .data$month, .data$landing_period, .data$grouped_taxa) %>%
    dplyr::summarise(
      landing_revenue = mean(.data$landing_revenue, na.rm = TRUE),
      n_landings_per_boat = mean(.data$n_landings_per_boat, na.rm = TRUE),
      landing_weight = mean(.data$landing_weight, na.rm = TRUE),
      revenue = sum(.data$revenue, na.rm = TRUE),
      catch = sum(.data$catch, na.rm = TRUE)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(.data$landing_period)

  results_per_taxa <-
    list(
      predictions_taxa = list(
        aggregated = national_estimates_taxa,
        municipal = municipal_estimates_taxa
      )
    )

  results <- c(results, results_per_taxa)

  estimates_filename <- add_version(pars$models$file_prefix, "rds")
  readr::write_rds(results, estimates_filename, compress = "gz")
  upload_cloud_file(
    estimates_filename,
    pars$storage$google$key,
    pars$storage$google$options
  )
}

#' Estimate fishery indicators
#'
#' Estimate municipal fishery indicators based on vessels registration records.
#'
#' @param trips Validated dataframe containing Timor trips data.
#' @param vessels_metadata Vessels metadata.
#' @param region Timor municipality.
#'
#' @return A list of fishery indicators divided by municipality.
#' @export
#'
estimate_indicators <- function(trips = NULL, vessels_metadata = NULL, region = NULL) {
  trips_df <-
    trips %>%
    dplyr::filter(.data$reporting_region == region)

  region_boats <-
    vessels_metadata %>%
    dplyr::filter(.data$reporting_region == region) %>%
    dplyr::summarise(n_boats = sum(.data$n_boats, na.rm = T)) %>%
    magrittr::extract2("n_boats")

  message("Estimating indicators for ", region, " based on ", region_boats, " recorded boats.")

  landings_df <-
    trips_df %>%
    dplyr::mutate(
      landing_period = lubridate::floor_date(.data$tracker_trip_end,
        unit = "month"
      ),
      last_seen_period = lubridate::floor_date(.data$tracker_last_seen,
        unit = "month"
      )
    ) %>%
    dplyr::filter(!is.na(.data$landing_period)) %>%
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
      fill = list(n_landings = 0)
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
      month = as.character(lubridate::month(.data$landing_period))
    ) %>%
    dplyr::group_by(.data$year, .data$month, .data$landing_period) %>%
    dplyr::summarise(n_landings_per_boat = mean(.data$n_landings)) %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::ungroup()


  value_df <-
    trips_df %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
      unit = "month"
    )) %>%
    dplyr::filter(!is.na(.data$landing_period), !is.na(.data$landing_value)) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
    ) %>%
    dplyr::group_by(.data$year, .data$month, .data$landing_period) %>%
    dplyr::summarise(landing_revenue = mean(.data$landing_value)) %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::ungroup()


  catch_df <-
    trips_df %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
      unit = "month"
    )) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::filter(
      all(!is.na(.data$landing_period)), all(!is.na(.data$weight)),
      all(!is.na(.data$catch_taxon)), all(!is.na(.data$reporting_region))
    ) %>%
    dplyr::mutate(
      landing_id = as.character(.data$landing_id),
      weight = dplyr::if_else(.data$weight < 0, NA_real_, .data$weight)
    ) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period) %>%
    dplyr::summarise(landing_weight = sum(.data$weight) / 1000) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
    ) %>%
    dplyr::group_by(.data$year, .data$ month, .data$landing_period) %>%
    dplyr::summarise(landing_weight = mean(.data$landing_weight)) %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::ungroup()

  dplyr::full_join(landings_df, value_df) %>%
    dplyr::full_join(catch_df) %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::mutate(
      period = paste(.data$year, .data$month, sep = "-"),
      landing_period = lubridate::ym(.data$period),
      revenue = .data$landing_revenue * .data$n_landings_per_boat * region_boats,
      catch = .data$landing_weight * .data$n_landings_per_boat * region_boats,
      region = region
    ) %>%
    dplyr::select(
      .data$period, .data$month, .data$landing_period,
      .data$landing_revenue, .data$n_landings_per_boat,
      .data$landing_weight, .data$revenue, .data$catch,
      .data$region
    ) %>%
    dplyr::ungroup() %>%
    as.data.frame()
}


estimate_indicators_taxa <- function(trips, municipal_estimates, vessels_metadata, region, modelled_taxa) {
  trips_df <-
    trips %>%
    dplyr::filter(.data$reporting_region == region)

  region_boats <-
    vessels_metadata %>%
    dplyr::filter(.data$reporting_region == region) %>%
    dplyr::summarise(n_boats = sum(.data$n_boats, na.rm = T)) %>%
    magrittr::extract2("n_boats")

  catch_df <- trips_df %>%
    dplyr::mutate(landing_period = lubridate::floor_date(.data$landing_date,
      unit = "month"
    )) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::filter(
      all(!is.na(.data$landing_period)),
      all(!is.na(.data$weight)),
      all(!is.na(.data$catch_taxon)),
      all(!is.na(.data$reporting_region))
    ) %>%
    dplyr::mutate(
      landing_id = as.character(.data$landing_id),
      weight = dplyr::if_else(.data$weight < 0, NA_real_, .data$weight)
    ) %>%
    dplyr::mutate(grouped_taxa = dplyr::if_else(.data$catch_taxon %in% c(modelled_taxa, "0"), .data$catch_taxon, "MZZ")) %>%
    dplyr::group_by(.data$landing_id, .data$landing_period, .data$grouped_taxa) %>%
    dplyr::summarise(
      landing_weight = sum(.data$weight, na.rm = FALSE) / 1000, .groups = "drop",
      landing_revenue = dplyr::first(.data$landing_value)
    ) %>%
    tidyr::complete(
      .data$grouped_taxa,
      tidyr::nesting(!!!dplyr::select(., tidyselect::all_of(c("landing_id", "landing_period")))),
      fill = list(landing_weight = 0)
    ) %>%
    dplyr::mutate(
      year = as.character(lubridate::year(.data$landing_period)),
      month = as.character(lubridate::month(.data$landing_period)),
      period = paste(.data$year, .data$month, sep = "-")
    ) %>%
    dplyr::filter(.data$grouped_taxa != "0") %>%
    dplyr::ungroup()

  regional_estimates <-
    municipal_estimates[[region]] %>%
    dplyr::select(-c(.data$landing_weight, .data$landing_revenue)) %>%
    dplyr::mutate(
      revenue = NA,
      catch = NA
    )

  estimations <-
    catch_df %>%
    dplyr::group_by(.data$grouped_taxa, .data$landing_period) %>%
    dplyr::summarise(
      period = dplyr::first(.data$period),
      month = dplyr::first(.data$month),
      landing_weight = mean(.data$landing_weight),
      landing_revenue = mean(.data$landing_revenue, na.rm = TRUE)
    ) %>%
    dplyr::ungroup()

  estimations %>%
    dplyr::left_join(regional_estimates) %>%
    dplyr::mutate(
      landing_period = lubridate::ym(.data$period),
      catch = .data$landing_weight * .data$n_landings_per_boat * region_boats,
      revenue = .data$landing_revenue * .data$n_landings_per_boat * region_boats
    ) %>%
    dplyr::arrange(.data$landing_period) %>%
    dplyr::select(
      .data$period, .data$month, .data$landing_period,
      .data$landing_weight, .data$grouped_taxa, .data$landing_revenue,
      .data$n_landings_per_boat, .data$revenue, .data$catch
    )
}

#' Get updated vessels records
#'
#' Get 2016 and ongoing info on the number of vessels recorded from Timor MAF.
#'
#' @inheritParams air_get_records
#'
#' @return A dataframe.
#' @export
#'
get_vessels_records <- function(table = NULL, base_id = NULL, api_key = NULL) {
  table %>%
    rlang::set_names() %>%
    purrr::map(air_get_records,
      base_id = base_id,
      api_key = api_key
    ) %>%
    purrr::map(air_records_to_tibble) %>%
    magrittr::extract2(table) %>%
    dplyr::select(
      region = .data$municipality,
      "n_boats_recorded_2016" = .data$`total boat_ 2016 registration`,
      "n_boats_recorded_ong" = .data$`total boat _2021_2022_ongoing reg`,
      "percetange_rate" = .data$`percentage rate`
    )
  # Fix region names
  # dplyr::mutate(region = dplyr::case_when(
  #  .data$region == "RAEOA" ~ "Oecusse",
  #  .data$region == "Liquica" ~ "Liquiça",
  #  .data$region == "Lautem" ~ "Lautém",
  #  TRUE ~ .data$region
  # ))
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
