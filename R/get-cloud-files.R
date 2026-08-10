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

#' Download Peskas validated landings
#'
#' Download validated Peskas data from Google Cloud.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a threshold for the logging infrastructure. See [logger::log_levels] for more details
#'
#' @return A dataframe of validated survey landings.
#' @keywords storage
#' @export
#'
get_validated_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  download_versioned_rds(
    prefix = conf$surveys$validated_landings$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    version = conf$surveys$validated_landings$version,
    exact_match = TRUE
  )
}

# Download validated PDS trips.
get_validated_pds_trips <- function(conf) {
  download_versioned_rds(
    prefix = paste(conf$pds$trips$file_prefix, "validated", sep = "_"),
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
    get_preprocessed_trips(conf) %>%
    magrittr::extract2("Trip") %>%
    unique()
  # remove after loading to save memory
  file.remove(list.files(pattern = "pds-trips-preprocessed__"))


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
      track <-
        coasts::download_cloud_file(
          name = x,
          provider = conf$pds_storage$google$key,
          options = coasts::resolve_storage_opts(conf, "pds")
        )
      readr::read_csv(track, show_col_types = FALSE)[+c(3:5)]
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
#' Get the list of pds-tracks IDs stored in the pds bucket
#'
#' @param conf the configuration file.
#'
#' @keywords storage
#' @export
#'
get_tracks_ids <- function(conf) {
  download_versioned_rds(
    prefix = conf$pds$tracks$bucket_content$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    extension = ""
  )
}

#' Get peskas validation sheet
#'
#' Get the peskas validation backup sheet from google cloud
#'
#' @param conf the configuration file.
#'
#' @keywords storage
#' @export
#'
get_validation_sheet <- function(conf) {
  download_versioned_rds(
    prefix = conf$validation$google_sheets$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    extension = ""
  )
}
