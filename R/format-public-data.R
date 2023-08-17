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

  logger::log_info("Retrieving merged trips...")
  merged_trips <- get_merged_trips(pars) %>%
    dplyr::filter(.data$landing_date >= "2018-01-01")
  # dplyr::filter(.data$landing_date < lubridate::floor_date(Sys.Date(), unit = "month"))

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
    purrr::map(summarise_trips, merged_trips_with_addons) %>%
    purrr::imap(~ .x %>% dplyr::filter(!is.na(.data$date_bin_start)))
  aggregated_estimations <-
    periods %>%
    rlang::set_names() %>%
    purrr::map(summarise_estimations, models$national$aggregated)
  # purrr::imap(~ .x %>% dplyr::filter(.data$date_bin_start < lubridate::floor_date(Sys.Date(), unit = "month")))


  municipal_aggregated <-
    models$municipal %>%
    purrr::map(~ purrr::keep(.x, stringr::str_detect(
      names(.x), stringr::fixed("aggregated")
    ))) %>%
    purrr::flatten() %>%
    purrr::set_names(names(models$municipal)) %>%
    dplyr::bind_rows(.id = "region") %>%
    dplyr::rename(date_bin_start = .data$landing_period) %>%
    dplyr::select(-c(.data$period, .data$month))

  municipal_aggregated_recorded <-
    merged_trips %>%
    dplyr::mutate(date_bin_start = lubridate::floor_date(.data$landing_date,
      "month",
      week_start = 7
    )) %>%
    dplyr::group_by(.data$reporting_region) %>%
    get_weight() %>%
    dplyr::group_by(.data$reporting_region, .data$date_bin_start) %>%
    dplyr::summarise(
      n_landings = dplyr::n_distinct(.data$landing_id, na.rm = T),
      recorded_revenue = sum(.data$landing_value, na.rm = T),
      recorded_catch = sum(.data$recorded_weight, na.rm = T),
      prop_landings_woman = sum(.data$fisher_number_woman > 0, na.rm = T) / sum(!is.na(.data$fisher_number_woman), na.rm = T)
    ) %>%
    dplyr::mutate(
      recorded_revenue = ifelse(.data$recorded_revenue == 0, NA_real_, .data$recorded_revenue),
      recorded_catch = ifelse(.data$recorded_catch == 0, NA_real_, .data$recorded_catch)
    ) %>%
    dplyr::select(
      region = .data$reporting_region,
      .data$date_bin_start,
      .data$recorded_revenue,
      .data$recorded_catch
    ) %>%
    dplyr::ungroup()

  municipal_aggregated <-
    dplyr::left_join(municipal_aggregated, municipal_aggregated_recorded,
      by = c("region", "date_bin_start")
    )

  taxa_estimations <-
    periods %>%
    rlang::set_names() %>%
    purrr::map(summarise_estimations, models$national$taxa, groupings = c("date_bin_start", "grouped_taxa"))

  municipal_taxa <-
    models$municipal %>%
    purrr::map(~ purrr::keep(.x, stringr::str_detect(
      names(.x), stringr::fixed("taxa")
    ))) %>%
    purrr::flatten() %>%
    purrr::set_names(names(models$municipal)) %>%
    dplyr::bind_rows(.id = "region") %>%
    dplyr::rename(date_bin_start = .data$landing_period) %>%
    dplyr::select(-c(.data$period, .data$month))

  nutrients_estimates <- purrr::map(taxa_estimations, summarise_nutrients, nutrients_table)
  nutrients_proportions <- get_nutrients_proportions(nutrients_estimates)
  # fill MZZ (miscellaneous unrecognized fishes) with average nutrients proportion of other groups

  aggregated_nutrients <-
    purrr::map(nutrients_estimates,
      fill_missing_group,
      nutrients_proportions,
      taxa = "MZZ"
    ) %>%
    purrr::map(aggregate_nutrients, pars)

  aggregated <-
    purrr::map2(aggregated_trips, aggregated_estimations, dplyr::full_join) %>%
    purrr::map(
      dplyr::select,
      .data$date_bin_start,
      .data$n_landings,
      .data$prop_landings_woman:.data$catch,
      .data$price_kg,
      .data$recorded_revenue,
      .data$recorded_catch
    )

  logger::log_info("Saving and exporting public data as tsv")
  tsv_filenames <- periods %>%
    paste0("aggregated-", .) %>%
    c("trips", "catch", .) %>%
    paste0(pars$export$file_prefix, "_", .) %>%
    purrr::map_chr(add_version, extension = "tsv")

  tsv_filenames_municipal <-
    c("aggregated", "taxa") %>%
    paste0("municipal-", .) %>%
    paste0(pars$export$file_prefix, "_", .) %>%
    purrr::map_chr(add_version, extension = "tsv")

  tsv_filenames <- c(tsv_filenames, tsv_filenames_municipal)

  tsv_filenames %T>%
    purrr::walk2(
      c(list(trips_table, catch_table), aggregated, list(municipal_aggregated, municipal_taxa)),
      ~ readr::write_tsv(.y, .x)
    ) %>%
    purrr::walk(upload_cloud_file,
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options
    )

  logger::log_info("Saving and exporting public data as rds")
  c(
    "trips", "catch", "aggregated", "taxa_aggregated", "nutrients_aggregated",
    "municipal_aggregated", "municipal_taxa"
  ) %>%
    paste0(pars$export$file_prefix, "_", .) %>%
    purrr::map_chr(add_version, extension = "rds") %T>%
    purrr::walk2(
      list(
        trips_table, catch_table, aggregated, taxa_estimations, aggregated_nutrients,
        municipal_aggregated, municipal_taxa
      ),
      ~ readr::write_rds(.y, .x, compress = "gz")
    ) %>%
    purrr::walk(upload_cloud_file,
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options
    )

  summary_dat <- get_summary_data(data = merged_trips, pars)
  normalized_params <- get_normalized_params(merged_trips)
  normalized_nutrients <- get_normalized_nutrients(merged_trips, pars)
  summary_dat$catch_norm <- jsonify_indicators(normalized_params, .data$weight_stand_kg)
  summary_dat$revenue_norm <- jsonify_indicators(normalized_params, .data$revenue_stand)
  summary_dat$nutrients_norm <- jsonify_nutrients(normalized_nutrients)

  summary_data_filename <-
    pars$report$summary_data %>%
    add_version(extension = "rds")

  readr::write_rds(
    x = summary_dat,
    file = summary_data_filename, compress = "gz"
  )

  logger::log_info("Uploading {summary_data_filename} to cloud sorage")
  upload_cloud_file(
    file = summary_data_filename,
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
    ) %>%
    dplyr::mutate(
      recorded_revenue = ifelse(.data$recorded_revenue == 0, NA_real_, .data$recorded_revenue),
      recorded_catch = ifelse(.data$recorded_catch == 0, NA_real_, .data$recorded_catch)
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

  if (length(groupings) > 1) {
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
        catch = dplyr::if_else(.data$current_period, .data$catch * .data$elapsed / .data$period_length, .data$catch),
      ) %>%
      dplyr::filter(.data$elapsed > 0) %>%
      dplyr::select(-.data$current_period, -.data$elapsed, -.data$period_length) %>%
      dplyr::mutate(date_bin_start = lubridate::floor_date(.data$date_bin_start,
        bin_unit,
        week_start = 7
      )) %>%
      dplyr::filter(dplyr::across(dplyr::all_of(groupings), ~ !is.na(.))) %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(groupings)))

    binned_frame <-
      standardised_predictions %>%
      dplyr::summarise(
        landing_revenue = mean(.data$landing_revenue, na.rm = T),
        landing_weight = mean(.data$landing_weight, na.rm = T),
        n_landings_per_boat = sum(.data$n_landings_per_boat, na.rm = T),
        revenue = sum(.data$revenue, na.rm = T),
        catch = sum(.data$catch, na.rm = T)
      ) %>%
      dplyr::ungroup()
  } else {
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
      dplyr::filter(.data$elapsed > 0) %>%
      dplyr::select(-.data$current_period, -.data$elapsed, -.data$period_length) %>%
      dplyr::mutate(date_bin_start = lubridate::floor_date(.data$date_bin_start,
        bin_unit,
        week_start = 7
      )) %>%
      dplyr::filter(dplyr::across(dplyr::all_of(groupings), ~ !is.na(.))) %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(groupings)))

    binned_frame <-
      standardised_predictions %>%
      dplyr::summarise(
        landing_revenue = mean(.data$landing_revenue, na.rm = T),
        landing_weight = mean(.data$landing_weight, na.rm = T),
        n_landings_per_boat = sum(.data$n_landings_per_boat, na.rm = T),
        revenue = sum(.data$revenue, na.rm = T),
        catch = sum(.data$catch, na.rm = T),
        price_kg = mean(.data$price_kg, na.rm = T)
      ) %>%
      dplyr::ungroup()
  }
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

aggregate_nutrients <- function(x, pars) {
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
      recorded_weight = sum(.data$weight),
      recorded_weight = .data$recorded_weight / 1000
    ) %>% # convert to Kg
    dplyr::ungroup() %>%
    tidyr::nest(length_frequency = c(.data$length:.data$Vitamin_A_mu)) %>%
    tidyr::nest(landing_catch = c(.data$catch_taxon, .data$catch_purpose, .data$length_type, .data$length_frequency))
}


get_municipal_nutrients <- function(nutrients_table = NULL,
                                    municipal_estimates = NULL,
                                    region = NULL,
                                    pars) {
  municipal_estimates[[region]]$taxa %>%
    dplyr::left_join(nutrients_table, by = "grouped_taxa") %>%
    # convert nutrients in Kg
    dplyr::mutate(
      selenium = (.data$Selenium_mu * (.data$catch * 1000)) / 1000,
      zinc = (.data$Zinc_mu * (.data$catch * 1000)) / 1000,
      protein = (.data$Protein_mu * (.data$catch * 1000)) / 1000,
      omega3 = (.data$Omega_3_mu * (.data$catch * 1000)) / 1000,
      calcium = (.data$Calcium_mu * (.data$catch * 1000) / 1000),
      iron = (.data$Iron_mu * (.data$catch * 1000)) / 1000,
      vitaminA = (.data$Vitamin_A_mu * (.data$catch * 1000)) / 1000
    ) %>%
    dplyr::group_by(.data$landing_period) %>%
    dplyr::summarise(
      catch = dplyr::first(.data$catch),
      dplyr::across(c(.data$selenium:.data$vitaminA), sum, na.rm = TRUE)
    ) %>%
    tidyr::pivot_longer(-c(.data$landing_period, .data$catch),
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
    ))
}

get_summary_data <- function(data = NULL, pars) {
  data_area <-
    data %>%
    fill_missing_regions() %>%
    dplyr::mutate(Area = dplyr::case_when(
      .data$reporting_region %in% c("Bobonaro", "Liquica", "Dili", "Baucau", "Oecusse") |
        .data$landing_station %in% c(
          "Com", "Tutuala", "Ililai",
          "Sentru/Liarafa/Sika/Rau Moko", "Comando"
        ) ~ "North Coast",
      .data$reporting_region == "Atauro" ~ "Atauro island",
      is.na(.data$reporting_region) | is.na(.data$reporting_region) |
        is.na(.data$reporting_region) & is.na(.data$reporting_region) ~ NA_character_,
      TRUE ~ "South Coast"
    ))

  list(
    n_surveys = data_area %>%
      dplyr::filter(!is.na(.data$landing_id) & !is.na(.data$Area)) %>%
      dplyr::group_by(.data$Area) %>%
      dplyr::count() %>%
      dplyr::ungroup(),
    n_tracks = data_area %>%
      dplyr::filter(!is.na(.data$tracker_trip_id)) %>%
      dplyr::mutate(year = lubridate::year(.data$landing_date)) %>%
      dplyr::group_by(.data$year) %>%
      dplyr::summarise(n_tracks = dplyr::n()) %>%
      dplyr::ungroup(),
    # miles_tracked = data_area %>%
    #  dplyr::filter(!is.na(.data$tracker_trip_id)) %>%
    #  dplyr::summarise(
    #    tracker_trip_distance =
    #      as.integer(sum(.data$tracker_trip_distance, na.rm = T) / 1852)
    #  ) %>%
    #  t() %>%
    #  dplyr::as_tibble() %>%
    #  dplyr::rename(miles = .data$V1) %>%
    #  dplyr::mutate(var = "Miles tracked"),
    groups_comp = data_area %>%
      dplyr::select(.data$landing_catch) %>%
      tidyr::unnest(.data$landing_catch) %>%
      tidyr::unnest(.data$length_frequency) %>%
      dplyr::filter(.data$individuals > 0) %>%
      dplyr::select(.data$catch_taxon, .data$weight) %>%
      convert_taxa_names(pars) %>%
      dplyr::filter(!is.na(.data$fish_group)) %>%
      dplyr::mutate(tot_weight = sum(.data$weight, na.rm = T)) %>%
      dplyr::group_by(.data$fish_group) %>%
      dplyr::summarise(
        weight_contr = sum(.data$weight, na.rm = T),
        tot_weight = dplyr::first(.data$tot_weight),
        weight = sum(.data$weight, na.rm = T),
        weight_contr = .data$weight_contr / .data$tot_weight * 100
      ) %>%
      dplyr::filter(!.data$weight_contr == 0) %>%
      dplyr::mutate(fish_group = ifelse(.data$weight_contr < 1, "Other", .data$fish_group)) %>%
      dplyr::group_by(.data$fish_group) %>%
      dplyr::summarise(
        weight = sum(.data$weight, na.rm = T) / 1000000
      ) %>%
      dplyr::arrange(dplyr::desc(.data$weight)) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(weight = as.integer(.data$weight))
  )
}

get_normalized_params <- function(x) {
  x %>%
    dplyr::filter(!is.na(.data$landing_id)) %>%
    dplyr::mutate(n_fishers = .data$fisher_number_child + .data$fisher_number_woman + .data$fisher_number_man) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::select(
      .data$landing_id, .data$trip_duration, .data$n_fishers,
      .data$landing_value, .data$gear_type, .data$habitat,
      .data$weight, .data$Selenium_mu:.data$Vitamin_A_mu
    ) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::summarise(
      dplyr::across(c(.data$trip_duration:.data$habitat), ~ dplyr::first(.x)),
      dplyr::across(c(.data$weight:.data$Vitamin_A_mu), ~ sum(.x, na.rm = T))
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(!is.na(.data$habitat)) %>%
    dplyr::select(
      .data$trip_duration, .data$n_fishers, .data$habitat,
      .data$gear_type, .data$landing_value, .data$weight
    ) %>%
    dplyr::mutate(dplyr::across(
      c(.data$gear_type, .data$habitat),
      ~ as.factor(.x)
    )) %>%
    na.omit() %>%
    dplyr::mutate(
      weight_stand = (.data$weight / .data$n_fishers) / .data$trip_duration,
      weight_stand_kg = .data$weight_stand / 1000,
      revenue_stand = (.data$landing_value / .data$n_fishers) / .data$trip_duration
    ) %>%
    dplyr::select(
      .data$habitat, .data$gear_type,
      .data$revenue_stand, .data$weight_stand_kg
    ) %>%
    dplyr::filter_all(dplyr::all_vars(!is.infinite(.))) %>%
    dplyr::mutate(gear_type = stringr::str_to_title(.data$gear_type))
}

get_normalized_nutrients <- function(x, pars) {
  nut_rdi <-
    as.data.frame(pars$metadata$nutrients$RDI$name) %>%
    tidyr::pivot_longer(dplyr::everything(), names_to = "nutrient", values_to = "RDI_coeff")

  x %>%
    dplyr::filter(!is.na(.data$landing_id)) %>%
    dplyr::mutate(n_fishers = .data$fisher_number_child + .data$fisher_number_woman + .data$fisher_number_man) %>%
    tidyr::unnest(.data$landing_catch) %>%
    tidyr::unnest(.data$length_frequency) %>%
    dplyr::select(
      .data$landing_id, .data$trip_duration, .data$n_fishers,
      .data$landing_value, .data$gear_type, .data$habitat,
      .data$weight, .data$Selenium_mu:.data$Vitamin_A_mu
    ) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::summarise(
      dplyr::across(c(.data$trip_duration:.data$habitat), ~ dplyr::first(.x)),
      dplyr::across(c(.data$weight:.data$Vitamin_A_mu), ~ sum(.x, na.rm = T))
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(!is.na(.data$habitat)) %>%
    dplyr::select(
      .data$habitat, .data$weight,
      .data$Selenium_mu:.data$Vitamin_A_mu
    ) %>%
    na.omit() %>%
    dplyr::mutate(dplyr::across(c(.data$Selenium_mu:.data$Vitamin_A_mu), ~ (.x / .data$weight) * 1000)) %>%
    dplyr::filter_all(dplyr::all_vars(!is.infinite(.))) %>%
    dplyr::select(c(.data$habitat, .data$Selenium_mu:.data$Vitamin_A_mu)) %>%
    tidyr::pivot_longer(-.data$habitat, names_to = "nutrient", values_to = "grams_rate") %>%
    dplyr::mutate(
      nutrient = stringr::str_replace(.data$nutrient, "_mu", ""),
      nutrient = stringr::str_replace(.data$nutrient, "_", ""),
      nutrient = tolower(.data$nutrient),
      nutrient = ifelse(.data$nutrient == "vitamina", "vitaminA", .data$nutrient)
    ) %>%
    dplyr::left_join(nut_rdi, by = "nutrient") %>%
    dplyr::filter(!.data$nutrient == "selenium") %>%
    dplyr::mutate(level_kg = .data$grams_rate / .data$RDI_coeff) %>%
    dplyr::mutate(nutrient = stringr::str_to_title(.data$nutrient),
                  nutrient = ifelse(.data$nutrient == "Omega3", "Omega-3", .data$nutrient),
                  nutrient = ifelse(.data$nutrient == "Vitamina", "Vitamin A", .data$nutrient))
}


jsonify_indicators <- function(data, parameter) {
  var <- enquo(arg = parameter)

  to_take <-
    data %>%
    dplyr::mutate(habitat_gear = paste(.data$habitat, .data$gear_type, sep = "_")) %>%
    dplyr::group_by(.data$habitat_gear) %>%
    dplyr::count() %>%
    dplyr::filter(.data$n > 50) %>%
    magrittr::extract2("habitat_gear")


  df_ord <-
    data %>%
    dplyr::mutate(habitat_gear = paste(.data$habitat, .data$gear_type, sep = "_")) %>%
    dplyr::filter(.data$habitat_gear %in% to_take) %>%
    dplyr::select(-.data$habitat_gear) %>%
    dplyr::group_by(.data$habitat, .data$gear_type) %>%
    dplyr::summarise(
      col_selected = round(mean(!!var, na.rm = T), 3),
      n = dplyr::n()
    ) %>%
    dplyr::arrange(-.data$col_selected) %>%
    dplyr::group_by(.data$habitat) %>%
    dplyr::mutate(col_selected_mean = sum(.data$col_selected)) %>%
    dplyr::arrange(-.data$col_selected_mean) %>%
    dplyr::ungroup()


  df_split <- df_ord %>% split(.$habitat)
  df_split_ord <- df_split[unique(df_ord$habitat)]

  dat <- lapply(names(df_split_ord), function(habitat) {
    org_data <- df_split_ord[[habitat]]
    list(
      name = habitat,
      data = lapply(1:nrow(org_data), function(i) {
        list(x = org_data$gear_type[i], y = org_data$col_selected[i])
      })
    )
  })
  dat
}

jsonify_nutrients <- function(data) {
  df_ord <-
    normalized_nutrients %>%
    dplyr::group_by(.data$nutrient, .data$habitat) %>%
    dplyr::summarise(
      col_selected = round(mean(.data$level_kg, na.rm = T), 3),
      n = dplyr::n()
    ) %>%
    dplyr::arrange(-.data$col_selected) %>%
    dplyr::group_by(.data$nutrient) %>%
    dplyr::mutate(col_selected_mean = sum(.data$col_selected)) %>%
    dplyr::arrange(-.data$col_selected_mean) %>%
    dplyr::ungroup()

  df_split <- df_ord %>% split(.$nutrient)
  df_split_ord <- df_split[c("Protein", "Omega-3", "Zinc", "Calcium", "Vitamin A", "Iron")]

  dat <- lapply(names(df_split_ord), function(nutrient) {
    org_data <- df_split_ord[[nutrient]]
    list(
      name = nutrient,
      data = lapply(1:nrow(org_data), function(i) {
        list(x = org_data$habitat[i], y = org_data$col_selected[i])
      })
    )
  })

  dat
}
