#' Format public data
#'
#' Format the merged trips into files that are fit for distribution.
#' Specifically, this function produces a trips and a catch table as well as
#' tables containing aggregated data. These files are stored both as tsv files
#' and rds in a storage bucket with public access.
#'
#' #'The parameters needed in the config file are those required for
#' `merge_trips()` in addition to:
#'
#' ```
#' public_storage:
#'  google:
#'    key: gcs
#'    options:
#'      project:
#'      bucket:
#'      service_account_key:
#' export:
#'  file_prefix:
#' ```
#' @param log_threshold
#' @inheritParams ingest_landings
#' @keywords workflow
#' @return no outputs. This function is used for it's side effects
#' @export
#'
format_public_data <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()
  RDI <- pars$metadata$nutrients$RDI$convert

  logger::log_info("Retrieving merged trips...")
  merged_trips <- get_merged_trips(pars)
  logger::log_info("Retrieving modelled data...")
  models <- get_models(pars)
  logger::log_info("Retrieving nutrient properties info...")
  nutrients_table <- get_nutrients_table(pars) %>%
    dplyr::rename(grouped_taxa = .data$interagency_code)

  logger::log_info("Calculating summary fields")
  merged_trips_with_addons <- add_calculated_fields(merged_trips)

  logger::log_info("Creating trips table")
  trips_table <- get_trips_table(merged_trips_with_addons)
  logger::log_info("Creating catch table")
  catch_table <- get_catch_table(merged_trips_with_addons)

  logger::log_info("Aggregating data")
  periods <- c("day", "week", "month", "year")
  aggregated_trips <-
    periods %>%
    rlang::set_names() %>%
    purrr::map(summarise_trips, merged_trips_with_addons)
  aggregated_estimations <-
    periods %>%
    rlang::set_names() %>%
    purrr::map(summarise_estimations, models$national$aggregated)
  # aggregated_estimations_municipal <-
  #  periods %>%
  #  rlang::set_names() %>%
  #  purrr::map(summarise_estimations, models$municipal$Covalima$aggregated)
  taxa_estimations <-
    periods %>%
    rlang::set_names() %>%
    purrr::map(summarise_estimations, models$national$aggregated_taxa, c("date_bin_start", "grouped_taxa"))
  # taxa_estimations_municipal <-
  #  periods %>%
  #  rlang::set_names() %>%
  #  purrr::map(summarise_estimations, models$predictions_taxa$municipal$Covalima, c("date_bin_start", "grouped_taxa"))

  nutrients_estimates <- purrr::map(taxa_estimations, summarise_nutrients, nutrients_table)
  nutrients_proportions <- get_nutrients_proportions(nutrients_estimates)
  # fill MZZ (miscellaneous unrecognized fishes) with average nutrients proportion of other groups
  aggregated_nutrients <- purrr::map(nutrients_estimates,
    fill_missing_group,
    nutrients_proportions,
    taxa = "MZZ"
  ) %>%
    purrr::map(aggregate_nutrients, RDI, pars)


  aggregated <- purrr::map2(aggregated_trips, aggregated_estimations, dplyr::full_join) %>%
    purrr::map(
      dplyr::select,
      .data$date_bin_start, .data$n_landings,
      .data$prop_landings_woman:.data$catch,
      .data$recorded_revenue, .data$recorded_catch
    )


  logger::log_info("Saving and exporting public data as tsv")
  tsv_filenames <- periods %>%
    paste0("aggregated-", .) %>%
    c("trips", "catch", .) %>%
    paste0(pars$export$file_prefix, "_", .) %>%
    purrr::map_chr(add_version, extension = "tsv")

  tsv_filenames %T>%
    purrr::walk2(
      c(list(trips_table, catch_table), aggregated),
      ~ readr::write_tsv(.y, .x)
    ) %>%
    purrr::walk(upload_cloud_file,
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options
    )

  logger::log_info("Saving and exporting public data as rds")
  c("trips", "catch", "aggregated", "taxa_aggregated", "nutrients_aggregated") %>%
    paste0(pars$export$file_prefix, "_", .) %>%
    purrr::map_chr(add_version, extension = "rds") %T>%
    purrr::walk2(
      list(trips_table, catch_table, aggregated, taxa_estimations, aggregated_nutrients),
      ~ readr::write_rds(.y, .x, compress = "gz")
    ) %>%
    purrr::walk(upload_cloud_file,
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options
    )
}

#' @importFrom rlang .data
#' @importFrom digest digest
#' @importFrom stats na.omit
add_calculated_fields <- function(merged_trips) {

  # Helper functions
  count_taxa <- function(x) {
    if (is.null(x)) {
      return(NA)
    }
    x <- x$catch_taxon
    taxa_excluding_no_catch <- x[x != "0"]
    dplyr::n_distinct(taxa_excluding_no_catch, na.rm = TRUE)
  }
  collapse_taxa <- function(x) {
    if (is.null(x)) {
      return(NA)
    }
    x$catch_taxon %>%
      na.omit() %>%
      sort() %>%
      paste(collapse = " ")
  }

  # A dataset with extra calculated fields
  merged_trips %>%
    dplyr::rowwise() %>%
    # get a random (but reproducible) ID for each trip based on a hash of the
    # landing and the pds trip id
    dplyr::mutate(trip_id = digest::digest(
      object = paste(.data$landing_id, .data$tracker_trip_id),
      serialize = FALSE
    )) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(n_taxa = purrr::map_int(.data$landing_catch, count_taxa)) %>%
    dplyr::mutate(taxa = purrr::map_chr(.data$landing_catch, collapse_taxa)) %>%
    dplyr::mutate(landing_date = lubridate::as_date(.data$landing_date)) # %>%
  # dplyr::mutate(fisher_proportion_woman = fisher_number_woman / (fisher_number_child + fisher_number_man + fisher_number_woman))
}

#' @importFrom rlang .data
get_trips_table <- function(merged_trips_with_addons) {
  merged_trips_with_addons %>%
    dplyr::select(
      .data$trip_id,
      .data$landing_date,
      landing_n_taxa = .data$n_taxa,
      landing_taxa = .data$taxa,
      .data$landing_station,
      .data$reporting_region,
      .data$habitat,
      .data$landing_value,
      .data$landing_catch,
      .data$vessel_type,
      .data$gear_type,
      .data$mesh_size,
      tidyselect::starts_with("fisher_"),
      .data$n_gleaners,
      landing_survey_trip_duration = .data$trip_duration,
      .data$tracker_trip_start,
      .data$tracker_trip_end,
    )
}

#' @importFrom rlang .data
get_catch_table <- function(merged_trips_with_addons) {
  merged_trips_with_addons %>%
    dplyr::select(.data$trip_id, .data$landing_catch) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency)
}

#' @importFrom rlang .data
summarise_trips <- function(bin_unit = "month", merged_trips_with_addons) {

  # We need to count landings and tracks separately because they have different
  # base dates
  landing_date_bin <- merged_trips_with_addons %>%
    dplyr::mutate(date_bin_start = lubridate::floor_date(.data$landing_date,
      bin_unit,
      week_start = 7
    )) %>%
    get_weight(.) %>%
    dplyr::group_by(.data$date_bin_start) %>%
    dplyr::summarise(
      n_landings = dplyr::n_distinct(.data$landing_id, na.rm = T),
      recorded_revenue = sum(.data$landing_value, na.rm = T),
      recorded_catch = sum(.data$recorded_weight, na.rm = T),
      prop_landings_woman = sum(.data$fisher_number_woman > 0, na.rm = T) / sum(!is.na(.data$fisher_number_woman), na.rm = T)
    )

  track_end_bin <- merged_trips_with_addons %>%
    dplyr::mutate(
      tracker_trip_end = lubridate::as_date(.data$tracker_trip_end),
      date_bin_start = lubridate::floor_date(.data$tracker_trip_end,
        bin_unit,
        week_start = 7
      )
    ) %>%
    dplyr::group_by(.data$date_bin_start) %>%
    dplyr::summarise(n_tracks = dplyr::n_distinct(.data$tracker_trip_id,
      na.rm = T
    ))

  matched_date_bin <- merged_trips_with_addons %>%
    dplyr::mutate(date_bin_start = lubridate::floor_date(.data$landing_date,
      bin_unit,
      week_start = 7
    )) %>%
    dplyr::group_by(.data$date_bin_start) %>%
    dplyr::summarise(n_matched = sum(!is.na(.data$landing_id) &
      !is.na(.data$tracker_trip_id)))

  landing_date_bin %>%
    dplyr::full_join(track_end_bin, by = "date_bin_start") %>%
    dplyr::full_join(matched_date_bin, by = "date_bin_start") %>%
    # Counts that are NA are really zero here
    dplyr::mutate(dplyr::across(dplyr::starts_with("n_"),
      tidyr::replace_na,
      replace = 0
    )) %>%
    dplyr::mutate(prop_matched = .data$n_matched /
      (.data$n_landings + .data$n_tracks - .data$n_matched)) %>%
    # For the data export we need to optimise a bit to minimise space use
    dplyr::mutate(
      prop_matched = round(.data$prop_matched, 4),
      dplyr::across(dplyr::starts_with("n_"), as.integer)
    ) %>%
    dplyr::arrange(dplyr::desc(.data$date_bin_start))
}

summarise_estimations <- function(bin_unit = "month", aggregated_predictions, groupings = "date_bin_start") {
  all_months <- seq(
    lubridate::floor_date(min(aggregated_predictions$landing_period), "year"),
    lubridate::ceiling_date(max(aggregated_predictions$landing_period), "year"),
    by = "month"
  )

  today <- Sys.Date()

  standardised_predictions <- aggregated_predictions %>%
    dplyr::rename(date_bin_start = .data$landing_period) %>%
    tidyr::complete(date_bin_start = all_months) %>%
    # Correct last month as predictions are for the full month but we should present only the estimates to date
    dplyr::mutate(
      current_period =
        today >= .data$date_bin_start &
          today < dplyr::lead(.data$date_bin_start),
      elapsed = as.numeric(today - .data$date_bin_start + 1),
      period_length = as.numeric(dplyr::lead(.data$date_bin_start) - .data$date_bin_start),
      n_landings_per_boat = dplyr::if_else(.data$current_period, .data$n_landings_per_boat * .data$elapsed / .data$period_length, .data$n_landings_per_boat),
      revenue = dplyr::if_else(.data$current_period, .data$revenue * .data$elapsed / .data$period_length, as.numeric(.data$revenue)),
      catch = dplyr::if_else(.data$current_period, .data$catch * .data$elapsed / .data$period_length, .data$catch)
    ) %>%
    dplyr::select(-.data$current_period, -.data$elapsed, -.data$period_length) %>%
    dplyr::mutate(date_bin_start = lubridate::floor_date(.data$date_bin_start,
      bin_unit,
      week_start = 7
    )) %>%
    dplyr::filter(dplyr::across(dplyr::all_of(groupings), ~ !is.na(.))) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(groupings)))

  binned_frame <- standardised_predictions %>%
    dplyr::summarise(
      landing_revenue = mean(.data$landing_revenue),
      landing_weight = mean(.data$landing_weight),
      n_landings_per_boat = sum(.data$n_landings_per_boat),
      revenue = sum(.data$revenue),
      catch = sum(.data$catch)
    ) %>%
    dplyr::ungroup()

  # remove rows where all the variables are NA
  binned_frame <- binned_frame %>%
    dplyr::filter(dplyr::if_any(where(is.numeric), ~ !is.na(.)))

  if (lubridate::duration(1, units = bin_unit) <
    lubridate::duration(1, units = "month")) {
    binned_frame <- binned_frame %>%
      dplyr::sample_n(0)
  }

  binned_frame
}



summarise_nutrients <- function(taxa_estimations, nutrients_table) {
  dplyr::left_join(taxa_estimations, nutrients_table) %>%
    dplyr::transmute(
      date_bin_start = .data$date_bin_start,
      grouped_taxa = .data$grouped_taxa,
      catch = .data$catch,
      selenium = (.data$Selenium_mu * (.data$catch * 1000)) / 1000,
      zinc = (.data$Zinc_mu * (.data$catch * 1000)) / 1000,
      protein = (.data$Protein_mu * (.data$catch * 1000)) / 1000,
      omega3 = (.data$Omega_3_mu * (.data$catch * 1000)) / 1000,
      calcium = (.data$Calcium_mu * (.data$catch * 1000) / 1000),
      iron = (.data$Iron_mu * (.data$catch * 1000)) / 1000,
      vitaminA = (.data$Vitamin_A_mu * (.data$catch * 1000)) / 1000
    )
}

get_nutrients_proportions <- function(nutrients_estimates) {
  nutrients_estimates$year %>%
    dplyr::rowwise() %>%
    dplyr::mutate(dplyr::across(c(.data$selenium:.data$vitaminA), ~ (.x / .data$catch) * 100)) %>%
    dplyr::ungroup() %>%
    dplyr::summarise(dplyr::across(c(.data$selenium:.data$vitaminA), ~ median(.x, na.rm = TRUE)))
}


fill_missing_group <- function(nutrients_estimates, nutrients_proportions, taxa = "MZZ") {
  nutrients_estimates %>%
    dplyr::mutate(
      selenium = dplyr::case_when(.data$grouped_taxa == taxa & .data$date_bin_start >= "2018-04-01"
      ~ (.data$catch * nutrients_proportions$selenium) / 100, TRUE ~ .data$selenium),
      zinc = dplyr::case_when(.data$grouped_taxa == taxa & .data$date_bin_start >= "2018-04-01"
      ~ (.data$catch * nutrients_proportions$zinc) / 100, TRUE ~ .data$zinc),
      protein = dplyr::case_when(.data$grouped_taxa == taxa & .data$date_bin_start >= "2018-04-01"
      ~ (.data$catch * nutrients_proportions$protein) / 100, TRUE ~ .data$protein),
      omega3 = dplyr::case_when(.data$grouped_taxa == taxa & .data$date_bin_start >= "2018-04-01"
      ~ (.data$catch * nutrients_proportions$omega3) / 100, TRUE ~ .data$omega3),
      calcium = dplyr::case_when(.data$grouped_taxa == taxa & .data$date_bin_start >= "2018-04-01"
      ~ (.data$catch * nutrients_proportions$calcium) / 100, TRUE ~ .data$calcium),
      iron = dplyr::case_when(.data$grouped_taxa == taxa & .data$date_bin_start >= "2018-04-01"
      ~ (.data$catch * nutrients_proportions$iron) / 100, TRUE ~ .data$iron),
      vitaminA = dplyr::case_when(.data$grouped_taxa == taxa & .data$date_bin_start >= "2018-04-01"
      ~ (.data$catch * nutrients_proportions$vitaminA) / 100, TRUE ~ .data$vitaminA)
    )
}

aggregate_nutrients <- function(x, RDI, pars) {
  x %>%
    dplyr::select(-c(.data$grouped_taxa, .data$catch)) %>%
    dplyr::group_by(.data$date_bin_start) %>%
    dplyr::summarise_all(sum, na.rm = TRUE) %>%
    tidyr::pivot_longer(-.data$date_bin_start,
      names_to = "nutrient",
      values_to = "nut_supply"
    ) %>%
    dplyr::mutate(nut_rdi = dplyr::case_when(
      nutrient == "selenium" ~ (.data$nut_supply * 1000) / pars$metadata$nutrients$RDI$name$selenium,
      nutrient == "zinc" ~ (.data$nut_supply * 1000) / pars$metadata$nutrients$RDI$name$zinc,
      nutrient == "protein" ~ (.data$nut_supply * 1000) / pars$metadata$nutrients$RDI$name$protein,
      nutrient == "omega3" ~ (.data$nut_supply * 1000) / pars$metadata$nutrients$RDI$name$omega3,
      nutrient == "calcium" ~ (.data$nut_supply * 1000) / pars$metadata$nutrients$RDI$name$calcium,
      nutrient == "iron" ~ (.data$nut_supply * 1000) / pars$metadata$nutrients$RDI$name$iron,
      nutrient == "vitaminA" ~ (.data$nut_supply * 1000) / pars$metadata$nutrients$RDI$name$vitaminA,
      TRUE ~ NA_real_
    )) %>%
    dplyr::ungroup()
}



where <- function(fn) {
  predicate <- rlang::as_function(fn)
  function(x, ...) {
    out <- predicate(x, ...)
    if (!rlang::is_bool(out)) {
      rlang::abort("`where()` must be used with functions that return `TRUE` or `FALSE`.")
    }
    out
  }
}

get_weight <- function(x) {
  x %>%
    tidyr::unnest(.data$landing_catch, keep_empty = T) %>%
    tidyr::unnest(.data$length_frequency, keep_empty = T) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::mutate(
      recorded_weight = sum(.data$weight, na.rm = T),
      recorded_weight = .data$recorded_weight / 1000
    ) %>% # convert to Kg
    dplyr::ungroup() %>%
    tidyr::nest(length_frequency = c(.data$length:.data$Vitamin_A_mu)) %>%
    tidyr::nest(landing_catch = c(.data$catch_taxon, .data$catch_purpose, .data$length_type, .data$length_frequency))
}
