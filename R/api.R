#' Export the raw trips table to the cross-country API bucket
#'
#' Projects the **weighted** (pre-validation) long catch table onto the
#' 22-column schema `peskas-api-{dev,prod}` publishes for every country, and
#' uploads it to `conf$api$trips$raw$cloud_path`.
#'
#' @details
#' The schema was read off the live `peskas-api-prod` objects (2026-08-10) and is
#' the same 22 columns, in the same order, for Kenya, Mozambique and Zanzibar,
#' raw and validated alike:
#'
#' ```
#' survey_id, trip_id, landing_date, gaul_1_code, gaul_1_name, gaul_2_code,
#' gaul_2_name, landing_site, n_fishers, trip_duration_hrs, gear, vessel_type,
#' catch_habitat, catch_outcome, n_catch, catch_taxon, scientific_name,
#' length_cm, catch_kg, catch_price, tot_catch_kg, tot_catch_price
#' ```
#'
#' Three Timor-specific decisions, all recorded in the migration Phase 6 entry
#' of `.claude/migration/STATE.md`:
#'
#' * **Grain.** The API is one row per (trip, catch); Timor's long tables are one
#'   row per (submission, catch, **length bin**). The bins are collapsed:
#'   `catch_kg` is their sum and `length_cm` their mean weighted by the number of
#'   individuals counted in each bin. Empty bins are kept in the input and
#'   contribute nothing to either, so no catch record is lost.
#' * **`trip_id`** is `TRIP_<submission_id>`, as in all three other countries — a
#'   trip identifier, not the PDS `tracker_trip_id`. `coasts::summarize_data()`
#'   does `group_by(trip_id) |> slice(1)`, so it must be unique per landing, and
#'   only half of Timor's landings match a tracked trip.
#' * **`catch_price`** is `NA`: Timor records revenue per landing, not per catch.
#'   The landing's revenue is `tot_catch_price`, which is what Mozambique — also
#'   without per-catch prices — publishes.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details.
#' @return no outputs. This function is used for its side effects.
#' @keywords workflow export
#' @importFrom rlang .data
#' @export
export_api_raw <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Downloading the weighted long catch table...")
  landings <- get_weighted_landings(conf) %>%
    dplyr::mutate(
      submission_id = as.integer(.data$submission_id),
      catch_kg = .data$weight / 1000
    ) %>%
    dplyr::rename(catch_habitat = "habitat")

  upload_api_trips(api_trips(landings, conf), conf$api$trips$raw, conf)
}

#' Export the validated trips table to the cross-country API bucket
#'
#' Projects the long validated catch table written by [validate_landings()] onto
#' the 22-column schema `peskas-api-{dev,prod}` publishes for every country, and
#' uploads it to `conf$api$trips$validated$cloud_path`. Same schema as
#' [export_api_raw()]; the two differ only in their input.
#'
#' @inherit export_api_raw details
#'
#' @inheritParams export_api_raw
#' @return no outputs. This function is used for its side effects.
#' @keywords workflow export
#' @importFrom rlang .data
#' @export
export_api_validated <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Downloading the long validated catch table...")
  landings <- get_validated_landings_long(conf)

  upload_api_trips(api_trips(landings, conf), conf$api$trips$validated, conf)
}

# Project a long catch table (standard names, `catch_kg` in kilos) onto the
# 22 API columns. See [export_api_raw()] for the schema and the three decisions
# behind it.
api_trips <- function(landings, conf) {
  asset_ids <- purrr::map_chr(conf$ingestion$landings, "asset_id")

  catch <- landings %>%
    dplyr::mutate(
      # "0" is the no-catch sentinel; the other countries write NA.
      catch_taxon = dplyr::na_if(.data$catch_taxon, "0"),
      bin_n = dplyr::if_else(
        is.na(.data$n_individuals) | is.na(.data$length), 0, .data$n_individuals
      ),
      bin_length = .data$bin_n * .data$length
    ) %>%
    dplyr::group_by(.data$submission_id, .data$n_catch, .data$catch_taxon) %>%
    dplyr::summarise(
      dplyr::across(
        dplyr::all_of(c(
          "survey_version", "landing_date", "gaul_1_code", "gaul_1_name",
          "gaul_2_code", "gaul_2_name", "landing_site", "n_fishers",
          "trip_duration", "gear", "vessel_type", "catch_habitat",
          "catch_outcome", "scientific_name", "catch_price"
        )),
        dplyr::first
      ),
      individuals = sum(.data$bin_n, na.rm = TRUE),
      length_sum = sum(.data$bin_length, na.rm = TRUE),
      weighed_bins = sum(!is.na(.data$catch_kg)),
      catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      length_cm = dplyr::if_else(
        .data$individuals > 0, .data$length_sum / .data$individuals, NA_real_
      ),
      # A catch nothing could be weighed for is unknown, not zero.
      catch_kg = dplyr::if_else(
        .data$weighed_bins > 0, .data$catch_kg, NA_real_
      )
    )

  catch %>%
    # Timor prices the landing, not the catch item.
    dplyr::rename(tot_catch_price = "catch_price") %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::mutate(
      tot_catch_kg = dplyr::if_else(
        any(!is.na(.data$catch_kg)), sum(.data$catch_kg, na.rm = TRUE), NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::transmute(
      survey_id = unname(asset_ids[.data$survey_version]),
      trip_id = paste0("TRIP_", .data$submission_id),
      .data$landing_date,
      .data$gaul_1_code,
      .data$gaul_1_name,
      .data$gaul_2_code,
      .data$gaul_2_name,
      .data$landing_site,
      .data$n_fishers,
      trip_duration_hrs = .data$trip_duration,
      .data$gear,
      .data$vessel_type,
      .data$catch_habitat,
      .data$catch_outcome,
      n_catch = as.integer(.data$n_catch),
      .data$catch_taxon,
      .data$scientific_name,
      .data$length_cm,
      .data$catch_kg,
      catch_price = NA_real_,
      .data$tot_catch_kg,
      .data$tot_catch_price
    )
}

# Write the API table as versioned parquet under `spec$cloud_path`.
upload_api_trips <- function(trips, spec, conf) {
  logger::log_info(
    "{nrow(trips)} catch records from {dplyr::n_distinct(trips$trip_id)} trips"
  )

  filename <- add_version(spec$file_prefix, extension = "parquet")
  arrow::write_parquet(
    trips,
    sink = filename,
    compression = "lz4",
    compression_level = 12
  )

  cloud_path <- file.path(spec$cloud_path, filename)
  logger::log_info("Uploading {cloud_path} to cloud storage")
  coasts::upload_cloud_file(
    file = filename,
    provider = conf$storage$google$key,
    # `resolve_storage_opts()` knows "coasts", "country", "pds" and "public"
    # but not "api" — see COASTS-TODO C16.
    options = conf$storage$google$options_api,
    name = cloud_path
  )

  file.remove(filename)
  invisible(NULL)
}
