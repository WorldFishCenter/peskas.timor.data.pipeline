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

  long %>%
    dplyr::select(-dplyr::all_of(long_catch_cols())) %>%
    dplyr::distinct() %>%
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

#' Download Peskas metadata
#'
#' Download preprocessed Peskas metadata from Google Cloud
#'
#' @param conf The configuration file
#' @keywords storage
#' @export
get_preprocessed_metadata <- function(conf) {
  download_versioned_rds(
    prefix = paste(conf$metadata$google_sheets$name, "preprocessed", sep = "_"),
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country")
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

#' Download and synchronize tracks data in a single file
#'
#' This function downloads, synchronize and ingest tracks data from cloud
#' storage in a single file. Since the transferred data is relatively large,
#' the synchronization and uploading are refreshed when the complete track file
#' is outdated by at least 5000 trips.
#'
#' @param conf The configuration file.
#'
#' @return The tabulated tracks collected in Timor..
#' @keywords storage
#' @export
#'
get_sync_tracks <- function(conf) {
  logger::log_info("Downloading all tracks...")
  full_tracks <- get_full_tracks(conf)
  # remove after loading to save memory
  file.remove(list.files(pattern = "pds-track-complete__"))

  logger::log_info("Checking sync status...")
  updated_trips <-
    get_pds_trips(conf) %>%
    magrittr::extract2("Trip") %>%
    unique()
  # remove after loading to save memory
  file.remove(list.files(pattern = "pds-trips__"))


  check_trips <-
    get_full_trips(conf)

  new_trips <- setdiff(updated_trips, check_trips)

  if (isTRUE(length(new_trips) < conf$pds$tracks$complete$new_trips)) {
    full_tracks
  } else {
    logger::log_info("Syncing {length(new_trips)} tracks...")
    new_tracks <-
      get_tracks_ids(conf) %>%
      dplyr::filter(.data$Trip %in% new_trips)

    get_track <- function(x) {
      coasts::download_cloud_file(
        name = x,
        provider = conf$pds_storage$google$key,
        options = coasts::resolve_storage_opts(conf, "pds")
      ) %>%
        arrow::read_parquet(col_select = c("Trip", "Lat", "Lng"))
    }

    logger::log_info("Donwloading and binding the new tracks...")
    new_tracks_batch <-
      purrr::map(new_tracks$name, get_track) %>%
      purrr::reduce(dplyr::bind_rows)

    complete_tracks <- dplyr::bind_rows(full_tracks, new_tracks_batch)
    complete_tracks_trips <- unique(full_tracks$Trip)

    logger::log_info("Uploading new file to cloud...")
    ingest_complete_tracks(conf,
      data = complete_tracks,
      trips = complete_tracks_trips
    )

    complete_tracks
  }
}

#' Download tracks data in a single file
#'
#' Download the latest version of PDS single-file tracks data.
#'
#' @param conf The configuration file.
#'
#' @return A dataframes with tracks coordinates for each trip
#' @keywords storage
#' @export
#'
get_full_tracks <- function(conf) {
  download_versioned_rds(
    prefix = conf$pds$tracks$complete$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    extension = "",
    exact_match = TRUE
  )
}

#' Get trips from single-file tracks data.
#'
#' Download the list of trips from the latest version of PDS single-file
#' tracks data.
#'
#' @param conf the configuration file.
#'
#' @return A vector of unique trips.
#' @keywords storage
#' @export
#'
get_full_trips <- function(conf) {
  download_versioned_rds(
    prefix = paste(conf$pds$tracks$complete$file_prefix, "trips", sep = "_"),
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    extension = ""
  )
}

#' Get map of Timor pds tracks.
#'
#' Download the static map of Timor pds tracks.
#'
#' @param conf the configuration file.
#'
#' @keywords storage
#' @export
#'
get_tracks_map <- function(conf) {
  # Not a versioned object: written under a fixed name.
  coasts::download_cloud_file(
    name = paste(
      conf$pds$tracks$map$png$file_prefix,
      conf$pds$tracks$map$png$extension,
      sep = "."
    ),
    provider = conf$public_storage$google$key,
    options = coasts::resolve_storage_opts(conf, "public")
  )
}

#' Get pds IDs
#'
#' Enumerates the PDS bucket and returns each track object name with the trip id
#' it belongs to.
#'
#' Until migration Phase 7 this read a cached `pds-track-list__*.rds` written by
#' Timor's own `ingest_pds_tracks()`. `coasts::ingest_pds_tracks()` writes no
#' such cache, and a cache of a bucket listing is a staleness trap for no gain —
#' this is the listing the cache was a copy of.
#'
#' @param conf the configuration file.
#'
#' @return A tibble: `name`, `Trip`.
#' @keywords storage
#' @export
#'
get_tracks_ids <- function(conf) {
  tibble::tibble(
    name = coasts::cloud_object_names(
      prefix = conf$pds$pds_tracks$file_prefix,
      provider = conf$pds_storage$google$key,
      options = coasts::resolve_storage_opts(conf, "pds"),
      extension = "parquet"
    )
  ) %>%
    dplyr::mutate(Trip = as.integer(stringr::str_extract(
      .data$name,
      "[[:digit:]]+"
    )))
}

#' Get the validation flags snapshot
#'
#' The versioned snapshot [validate_landings()] writes alongside the MongoDB
#' push. Since migration Phase 5 that push replaces the collection wholesale, so
#' this is the only history of what was flagged when.
#'
#' @param conf the configuration file.
#' @return A tibble: `submission_id`, `submission_date`, `flag_date`, `alert`,
#'   `validated`, `validated_when_ymd`, `comments`.
#'
#' @keywords storage
#' @export
#'
get_validation_flags <- function(conf) {
  coasts::download_parquet_from_cloud(
    prefix = conf$surveys$landings$validation$flags$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    version = conf$surveys$landings$validation$flags$version
  )
}
