#' Ingest landings surveys from KoboToolbox
#'
#' Downloads the live landing-survey forms from KoboToolbox, flattens each
#' submission into one row, and uploads the result as a versioned parquet object
#' to the country bucket.
#'
#' Only the **live** forms are ingested. `v1` (SSF Landings) received its last
#' submission on 2020-08-28 and is frozen: its preprocessed output was snapshot
#' once, out of band, and is read from
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
