#' Ingest landings surveys from KoboToolbox
#'
#' Downloads the live landing-survey forms from KoboToolbox, flattens each
#' submission into one row, and uploads the result as a versioned parquet object
#' to the country bucket.
#'
#' Only the **live** forms are ingested. `v1` (SSF Landings) received its last
#' submission on 2020-08-28 and is frozen: its preprocessed output was snapshot
#' once by `data-raw/freeze-landings-v1.R` and is read from
#' `surveys.landings.v1.frozen` thereafter.
#'
#' The parameters needed in `config.yml` are:
#'
#' ```
#' ingestion:
#'   landings:
#'     <version>:
#'       asset_id:
#'       username:
#'       password:
#' surveys:
#'   landings:
#'     <version>:
#'       raw:
#'         file_prefix:
#' storage:
#'   google:
#'     key:
#'     options:
#' ```
#'
#' @param versions Character vector of form versions to ingest. Defaults to the
#'   live forms, `c("v2", "v3")`.
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels].
#'
#' @return No output. This function is used for its side effects.
#' @keywords workflow ingestion
#' @export
#'
ingest_landings <- function(versions = c("v2", "v3"),
                            log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  purrr::walk(versions, function(v) {
    logger::log_info("Downloading landings {v} from KoboToolbox...")
    data_raw <- coasts::get_kobo_data(
      assetid = conf$ingestion$landings[[v]]$asset_id,
      uname = conf$ingestion$landings[[v]]$username,
      pwd = conf$ingestion$landings[[v]]$password
    )

    ids <- unlist(purrr::map(data_raw, "_id"))
    if (anyDuplicated(ids) > 0) {
      stop("Duplicated submission ids returned for landings ", v)
    }

    logger::log_info("Flattening {length(data_raw)} submissions...")
    raw_survey <- purrr::map(data_raw, flatten_row) %>%
      dplyr::bind_rows()

    coasts::upload_parquet_to_cloud(
      data = raw_survey,
      prefix = conf$surveys$landings[[v]]$raw$file_prefix,
      provider = conf$storage$google$key,
      options = coasts::resolve_storage_opts(conf, "country")
    )
  })
}

#' Ingest the Airtable frame assets snapshot
#'
#' Thin wrapper over [coasts::ingest_assets()], which pulls taxa, gears,
#' vessels, landing sites, districts, forms, PDS devices and the fleet frame
#' from the PESKAS | FRAME base and writes one versioned `assets__*.rds`.
#'
#' Airtable is authoritative wherever it overlaps the Google Sheets metadata
#' tables (taxa, gears, vessels, landing sites, districts, PDS devices); the
#' Sheets keep only the five tables the frame does not cover.
#'
#' @section The extra upload:
#' `coasts::ingest_assets()` uploads the snapshot to `storage.google.options`
#' (the country bucket) while every reader in coasts — `ingestion-pds.R` and,
#' since 4.6.0, `enrich_taxa()` — resolves it through
#' `resolve_storage_opts(conf, "coasts")` (the hub). Inside coasts the two are
#' the same bucket so the disagreement is invisible; from here they are not.
#' Until the upstream fix lands (COASTS-TODO C11) the snapshot is mirrored to
#' the hub after the delegated call.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels].
#'
#' @return No output. This function is used for its side effects.
#' @keywords workflow ingestion metadata
#' @export
#'
ingest_assets <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  coasts::ingest_assets(
    log_threshold = log_threshold,
    package = "peskas.timor.data.pipeline"
  )

  snapshot <- sort(list.files(
    pattern = paste0("^", conf$metadata$airtable$name, "__.*__\\.rds$")
  ))
  snapshot <- snapshot[length(snapshot)]

  logger::log_info("Mirroring {snapshot} to the coasts hub bucket...")
  coasts::upload_cloud_file(
    file = snapshot,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "coasts")
  )
}

# Flatten one KoBo submission into a single-row tibble.
flatten_row <- function(x) {
  x %>%
    # Each row is composed of several fields
    purrr::imap(flatten_field) %>%
    rlang::squash() %>%
    tibble::as_tibble()
}

flatten_field <- function(x, p) {
  # If the field is a simple vector do nothing but if the field is a list we
  # need more logic
  if (inherits(x, "list")) {
    if (length(x) > 0) {
      if (purrr::vec_depth(x) == 2) {
        # If the field-list has named elements is we just need to rename the list
        x <- list(x) %>%
          rlang::set_names(p) %>%
          unlist() %>%
          as.list()
      } else {
        # If the field-list is an "array" we need to iterate over its children
        x <- purrr::imap(x, rename_child, p = p)
      }
    }
  } else {
    if (is.null(x)) x <- NA
  }
  x
}

# Appends parent name or number to element
rename_child <- function(x, i, p) {
  if (length(x) == 0) {
    if (is.null(x)) {
      x <- NA
    }
    x <- list(x)
    x <- rlang::set_names(x, paste(p, i - 1, sep = "."))
  } else {
    if (inherits(i, "character")) {
      x <- rlang::set_names(x, paste(p, i, sep = "."))
    } else if (inherits(i, "integer")) {
      x <- rlang::set_names(x, paste(p, i - 1, names(x), sep = "."))
    }
  }
  x
}

#' Read a raw landings parquet the way the old CSV path read it
#'
#' Preprocessing was written against `readr::read_csv(col_types = cols(.default
#' = col_character()))`, so every downstream comparison and recode assumes
#' character columns. Parquet keeps the JSON types, so they are coerced back.
#'
#' The trim and the empty-string blanking are not cosmetic: writing a CSV and
#' reading it back with readr's defaults silently trimmed surrounding spaces
#' and tabs and mapped `""` and `"NA"` to `NA`. Measured over all 64,997 v2
#' submissions, 13 of 311 columns differ without them — free-text fields such
#' as `reason_for_zero_boats` and `trip_group/boat_owner_name`. Reproducing the
#' old semantics keeps migration Phase 3 a change of serialization format and
#' nothing else, so that any Phase 4 golden-diff cannot be blamed on it.
#'
#' `whitespace` is readr's set deliberately: `trimws()`'s default would also
#' strip the stray leading and trailing newlines that ~60 free-text answers
#' carry, which is an improvement but not this phase's to make. Phase 4
#' rewrites preprocessing against a typed long table and this goes away.
#'
#' @param conf The configuration file.
#' @param version Form version, e.g. `"v2"`.
#'
#' @return A tibble of raw submissions, all columns character.
#' @keywords helper
#' @noRd
get_raw_landings <- function(conf, version) {
  coasts::download_parquet_from_cloud(
    prefix = conf$surveys$landings[[version]]$raw$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    version = conf$surveys$landings[[version]]$raw$version
  ) %>%
    dplyr::mutate(dplyr::across(
      dplyr::everything(),
      ~ dplyr::na_if(
        dplyr::na_if(trimws(as.character(.x), whitespace = "[ \t]"), ""),
        "NA"
      )
    ))
}
