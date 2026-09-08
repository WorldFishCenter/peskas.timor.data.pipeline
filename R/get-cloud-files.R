#' Download a versioned cloud object and read it as `.rds`
#'
#' Internal helper behind every `get_*()` accessor in this file. Resolves the
#' requested version of `prefix` with [coasts::cloud_object_name()], downloads it
#' with [coasts::download_cloud_file()] and deserialises it with
#' [readr::read_rds()].
#'
#' @param prefix Object-name prefix, without the version string.
#' @param provider Cloud storage provider key, e.g. `conf$storage$google$key`.
#' @param options Provider options, e.g. from [coasts::resolve_storage_opts()].
#' @param version Either `"latest"` or an explicit version string.
#' @param extension File extension to match. `""` matches any.
#' @param exact_match Whether `prefix` must match the object's base name
#'   exactly rather than being a prefix of it.
#'
#' @return The deserialised object.
#'
#' @keywords storage
#' @noRd
download_versioned_rds <- function(prefix,
                                   provider,
                                   options,
                                   version = "latest",
                                   extension = "rds",
                                   exact_match = FALSE) {
  object_name <- coasts::cloud_object_name(
    prefix = prefix,
    version = version,
    extension = extension,
    provider = provider,
    exact_match = exact_match,
    options = options
  )

  logger::log_info("Downloading {object_name}...")
  coasts::download_cloud_file(
    name = object_name,
    provider = provider,
    options = options
  )
  readr::read_rds(file = object_name)
}

#' Download the merged landings
#'
#' The merged table is the flat long catch table produced by
#' [merge_landings()]: one row per (submission, catch, length bin).
#'
#' @param conf The configuration file.
#' @return A tibble of merged landings.
#' @keywords storage
#' @export
get_merged_landings <- function(conf) {
  coasts::download_parquet_from_cloud(
    prefix = conf$surveys$landings$merged$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    version = conf$surveys$landings$merged$version
  )
}

#' Download the merged landings with catch weights
#'
#' The flat long catch table of [merge_landings()] with `weight` (grams) and the
#' seven per-catch nutrient columns added by [calculate_weights()].
#'
#' @param conf The configuration file.
#' @return A tibble, one row per (submission, catch, length bin).
#' @keywords storage
#' @export
get_weighted_landings <- function(conf) {
  coasts::download_parquet_from_cloud(
    prefix = conf$surveys$landings$weight$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    version = conf$surveys$landings$weight$version
  )
}

#' Download the validated landings in the flat long shape
#'
#' The same content as [get_validated_landings()], one row per (submission,
#' catch, length bin) under the standard column names and with `catch_kg` in
#' kilograms. Read by [export_api_validated()].
#'
#' @param conf The configuration file.
#' @return A tibble, one row per (submission, catch, length bin).
#' @keywords storage
#' @export
get_validated_landings_long <- function(conf) {
  coasts::download_parquet_from_cloud(
    prefix = conf$surveys$landings$validated_long$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    version = conf$surveys$landings$validated_long$version
  )
}

#' Download Peskas validated landings, in the nested portal shape
#'
#' One row per submission, with the catch as the `landing_catch` /
#' `length_frequency` list-columns that [merge_trips()], `format_public_data()`
#' and `estimate_fishery_indicators()` read.
#'
#' This is a view over [get_validated_landings_long()], re-nested on read; there
#' is no separate stored artefact in this shape.
#'
#' @param conf The configuration file.
#'
#' @return A tibble of validated survey landings, one row per submission.
#' @keywords storage
#' @export
#'
get_validated_landings <- function(conf) {
  long <- get_validated_landings_long(conf)

  catch <- long %>%
    dplyr::transmute(
      .data$submission_id, .data$n_catch, .data$catch_taxon,
      .data$catch_use, .data$length,
      number_of_fish = .data$n_individuals,
      catch = .data$catch_kg * 1000,
      dplyr::across(tidyselect::ends_with("_mu"))
    )

  submissions <- long %>%
    dplyr::select(-dplyr::all_of(long_catch_cols())) %>%
    dplyr::distinct()

  # The guard for `long_catch_cols()`: a catch-level column added to
  # `long_validated_landings()` and not listed there survives the distinct() and
  # silently turns one submission into several. Without this the damage first
  # shows up two jobs later, in test_merged_trips.R.
  if (anyDuplicated(submissions$submission_id) > 0) {
    stop(
      "The long validated table did not collapse to one row per submission: ",
      sum(duplicated(submissions$submission_id)), " duplicates. A catch-level ",
      "column is missing from long_catch_cols()."
    )
  }

  submissions %>%
    dplyr::left_join(nest_landing_catch(catch), by = "submission_id") %>%
    dplyr::select(
      landing_id = "submission_id",
      "landing_date",
      "tracker_imei",
      trip_length = "trip_duration",
      "landing_catch",
      "catch_price",
      "landing_site",
      "municipality",
      habitat = "catch_habitat",
      tidyselect::starts_with("fisher_number"),
      "gear",
      "mesh_size",
      propulsion_gear = "vessel_type",
      "n_gleaners",
      "fuel",
      "catch_preservation",
      "happiness"
    )
}

# The catch-level columns of the long validated table — everything that is not
# a property of the submission. Everything else is `distinct()`-ed down to one
# row per submission by `get_validated_landings()`, so a new catch-level column
# added to `long_validated_landings()` and not listed here would silently
# multiply the submission rows.
long_catch_cols <- function() {
  c(
    "n_catch", "catch_taxon", "scientific_name", "catch_use", "catch_outcome",
    "length", "n_individuals", "catch_kg",
    "Selenium_mu", "Zinc_mu", "Protein_mu", "Omega_3_mu", "Calcium_mu",
    "Iron_mu", "Vitamin_A_mu"
  )
}

# Nest the validated catch into the `landing_catch` / `length_frequency`
# list-columns the portal path consumes. Grouping is on the submission alone, so
# two catches of one submission that share a taxon and use stay two rows.
#
# The leading select() is load-bearing: `nest()` groups on every column it is
# not nesting, so an extra column in `validated_catch` would silently change the
# grouping and with it what the portal reads. `n_catch` is part of that grouping
# and is dropped again afterwards.
nest_landing_catch <- function(validated_catch) {
  validated_catch %>%
    dplyr::select(
      "submission_id", "n_catch", "catch_taxon", "catch_use",
      "length", "number_of_fish", "catch", tidyselect::ends_with("_mu")
    ) %>%
    tidyr::nest(
      length_frequency = c(
        "length", "number_of_fish", "catch",
        tidyselect::ends_with("_mu")
      )
    ) %>%
    dplyr::select(
      "submission_id", "catch_taxon", "catch_use", "length_frequency"
    ) %>%
    tidyr::nest(landing_catch = -"submission_id")
}

# Download validated PDS trips.
get_validated_pds_trips <- function(conf) {
  download_versioned_rds(
    prefix = paste(conf$pds$pds_trips$file_prefix, "validated", sep = "_"),
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    extension = ""
  )
}

#' Download Peskas surveys and PDS data
#'
#' Download validated Peskas surveys and PDS data merged by temporal matching
#'
#' @param conf The configuration file
#' @param ... Any additional parameters, passed on to
#'   [coasts::cloud_object_name()] — for example `version` or `exact_match`.
#' @return A dataframe of validated survey landings and PDS trips.
#' @keywords storage
#' @export
#'
get_merged_trips <- function(conf, ...) {
  download_versioned_rds(
    prefix = conf$merged_trips$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    extension = "",
    ...
  )
}

# Download the public trips / catch / aggregated tables.
get_public_files <- function(conf) {
  c("trips", "catch", "aggregated") %>%
    rlang::set_names() %>%
    purrr::map(~ paste(conf$export$file_prefix, .x, sep = "_")) %>%
    purrr::map(
      .f = download_versioned_rds,
      provider = conf$public_storage$google$key,
      options = coasts::resolve_storage_opts(conf, "public")
    )
}

#' Download models estimates
#'
#' @param conf Configuration file
#'
#' @return A list of dataframes with national and municipal estimations
#' @keywords storage
#' @export
#'
get_models <- function(conf) {
  download_versioned_rds(
    prefix = conf$models$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    extension = ""
  )
}

#' Download the Airtable frame mapping tables for Timor
#'
#' Wraps [coasts::get_assets()], which downloads the shared snapshot and returns
#' only the rows belonging to Timor's survey forms.
#'
#' @param conf The configuration file.
#' @return A named list of five tibbles: `taxa`, `gear`, `vessels`, `sites`,
#'   `geo`.
#' @keywords storage
#' @export
get_assets <- function(conf) {
  coasts::get_assets(
    form_ids = timor_form_ids(conf),
    prefix = conf$metadata$airtable$name,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "coasts")
  )
}

#' Resolve a KoBo asset id to its Airtable form record id
#'
#' Reads the frame's `forms` table, the only place the two ids are linked.
#' Errors unless the asset id matches exactly one record, since an unresolved id
#' would silently filter every mapping table down to nothing.
#'
#' @param kobo_asset_id A single KoBoToolbox asset id.
#' @param conf The configuration file.
#' @return The Airtable record id, length 1.
#' @keywords helper
#' @export
get_airtable_form_id <- function(kobo_asset_id = NULL, conf = NULL) {
  if (
    length(kobo_asset_id) != 1 || is.na(kobo_asset_id) || !nzchar(kobo_asset_id)
  ) {
    stop(
      "`kobo_asset_id` must be a single non-empty string (got ",
      class(kobo_asset_id)[1], " of length ", length(kobo_asset_id),
      "). Check the matching `ingestion.landings.*.asset_id` entry in ",
      "config.yml and that its environment variable is set.",
      call. = FALSE
    )
  }

  airtable_id <-
    coasts::airtable_to_df(
      base_id = conf$airtable$frame$base_id,
      table_name = "forms",
      token = conf$airtable$token
    ) %>%
    janitor::clean_names() %>%
    dplyr::filter(.data$form_id == kobo_asset_id) %>%
    dplyr::pull(.data$airtable_id) %>%
    unique()

  if (length(airtable_id) != 1) {
    stop(
      "Expected exactly 1 Airtable `forms` record for kobo asset id \'",
      kobo_asset_id, "\', found ", length(airtable_id), ".",
      call. = FALSE
    )
  }

  airtable_id
}

# The Airtable record ids of Timor's two survey forms, resolved from the KoBo
# asset ids in `ingestion.landings`.
timor_form_ids <- function(conf) {
  vapply(
    conf$ingestion$landings[c("v2", "v3")],
    function(x) get_airtable_form_id(x$asset_id, conf),
    character(1),
    USE.NAMES = FALSE
  )
}

#' Download Peskas metadata
#'
#' Download preprocessed Peskas metadata from Google Sheets
#'
#' @param conf The configuration file
#' @keywords storage
#' @export
get_preprocessed_sheets <- function(conf) {
  download_versioned_rds(
    prefix = paste(conf$metadata$google_sheets$name, "preprocessed", sep = "_"),
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country")
  )
}
