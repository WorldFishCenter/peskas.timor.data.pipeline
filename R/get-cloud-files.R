#' Download a versioned cloud object and read it as `.rds`
#'
#' Internal helper behind every `get_*()` accessor in this file. Resolves the
#' requested version of `prefix` with [coasts::cloud_object_name()], downloads it
#' with [coasts::download_cloud_file()] and deserialises it with
#' [readr::read_rds()].
#'
#' The interchange format is still `.rds` with nested list-columns. It flips to
#' flat long parquet in migration Phase 4, at which point these helpers become
#' [coasts::download_parquet_from_cloud()] calls and this helper goes away.
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
#' [merge_landings()]: one row per (submission, catch, length bin), parquet
#' since migration Phase 4.
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
#' seven per-catch nutrient columns added by [calculate_weights()]. Parquet
#' since migration Phase 5, when the re-nesting that validation used to need
#' was deleted.
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
#' kilograms. Written beside the nested artefact since migration Phase 5 and
#' read by [export_api_validated()].
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
#' Until migration Phase 8 this read a second stored artefact,
#' `timor-landings-merged_validated__*.rds`, written by [validate_landings()]
#' beside the long parquet. That artefact is gone: this is now a **view** over
#' [get_validated_landings_long()], rebuilt by `nest_landing_catch()`. The two
#' were proven interchangeable before the switch — 97,360 submissions and
#' 1,648,016 catch rows compared column by column, every column equal, with
#' 1,599 catch weights differing by at most 2.9e-11 g (one ULP of the
#' grams → kg → grams round trip) and the national total unchanged to 20
#' significant digits.
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
      .data$catch_use, .data$length_type, .data$length,
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
    "length_type", "length", "n_individuals", "catch_kg",
    "Selenium_mu", "Zinc_mu", "Protein_mu", "Omega_3_mu", "Calcium_mu",
    "Iron_mu", "Vitamin_A_mu"
  )
}

# Nest the validated catch into the `landing_catch` / `length_frequency`
# list-columns the portal path consumes. Written by `validate_landings()` until
# migration Phase 8; it is a read-side reshape now that the nested artefact is
# gone.
#
# `nest(landing_catch = -submission_id)` groups on the submission alone, so two
# catches of one submission that happen to share a taxon, use and length type
# stay two rows — 4,873 submissions do.
#
# The leading select() is load-bearing: `nest()` groups on every column it is
# not nesting, so an extra column in `validated_catch` would silently change the
# grouping and with it what the portal reads. `n_catch` is part of that grouping
# and is dropped again afterwards.
nest_landing_catch <- function(validated_catch) {
  validated_catch %>%
    dplyr::select(
      "submission_id", "n_catch", "catch_taxon", "catch_use", "length_type",
      "length", "number_of_fish", "catch", tidyselect::ends_with("_mu")
    ) %>%
    tidyr::nest(
      length_frequency = c(
        "length", "number_of_fish", "catch",
        tidyselect::ends_with("_mu")
      )
    ) %>%
    dplyr::select(
      "submission_id", "catch_taxon", "catch_use", "length_type",
      "length_frequency"
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

#' Download the Airtable frame assets snapshot
#'
#' Reads the versioned `assets__*.rds` written by [ingest_assets()] from the
#' shared coasts hub bucket. The snapshot is **cross-country**: use
#' [timor_assets()] to narrow a table to Timor's rows.
#'
#' @param conf The configuration file
#' @return A named list of asset tables (`taxa`, `gear`, `vessels`, `sites`,
#'   `geo`, `forms`, `devices`, `frame`).
#' @keywords storage
#' @export
get_assets <- function(conf) {
  download_versioned_rds(
    prefix = conf$metadata$airtable$name,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "coasts")
  )
}

#' Keep only Timor's rows of an assets table
#'
#' The snapshot carries no `country` column, so rows are selected by the
#' record ids of Timor's two KoBo forms (`metadata.airtable.form_ids`). Tables
#' whose `form_id` lists several forms are matched if any of them is Timor's.
#'
#' @param x An asset table from [get_assets()], carrying a `form_id` column.
#' @param conf The configuration file.
#' @return `x`, filtered to Timor.
#' @keywords helper
#' @export
timor_assets <- function(x, conf) {
  dplyr::filter(
    x,
    grepl(
      paste(conf$metadata$airtable$form_ids, collapse = "|"),
      .data$form_id
    )
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
