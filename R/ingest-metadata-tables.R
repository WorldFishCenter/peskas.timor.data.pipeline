#' Ingest metadata tables
#'
#' Metadata tables are manually updated in a Airtable. This function downloads
#' the table and uploads it to a drive location. These tables include
#' information about boats, devices, municipalities, etc.
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' metadata:
#'   airtable:
#'     base_id:
#'     name:
#'     api_key:
#'     tables:
#'       -
#'       -
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
#' @examples
ingest_metadata_tables <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  metadata_filename <- add_version(pars$metadata$airtable$name, "rds")

  logger::log_info("Downloading metadata tables as {metadata_filename}...")

  pars$metadata$airtable$tables %>%
    rlang::set_names() %>%
    purrr::map(air_get_records,
      base_id = pars$metadata$airtable$base_id,
      api_key = pars$metadata$airtable$api_key
    ) %>%
    purrr::map(air_records_to_tibble) %>%
    readr::write_rds(metadata_filename,
      compress = "gz"
    )

  logger::log_success("Uploading to the cloud...")
  upload_cloud_file(metadata_filename, pars$storage$google$key, pars$storage$google$options)
  logger::log_success("File upload succeded")
}


#' Ingest flags tables
#'
#' Flags tables are manually updated in a Google Drive spreadsheet. This
#' function downloads the table and uploads it to a drive location. These tables
#' include information about flags, boats, devices, municipalities, etc.
#'
#' The parameters needed in `conf.yml` are:
#' ```
#' flags:
#'   airtable:
#'     base_id:
#'     name:
#'     api_key:
#'     tables:
#'       -
#'       -
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
#' @examples
ingest_validation_tables <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  flags_filename <- add_version(pars$validation$airtable$name, "rds")

  logger::log_info("Downloading flag tables as {flags_filename}...")

  pars$validation$airtable$tables %>%
    rlang::set_names() %>%
    purrr::map(air_get_records,
      base_id = pars$validation$airtable$base_id,
      api_key = pars$validation$airtable$api_key
    ) %>%
    purrr::map(air_records_to_tibble) %>%
    readr::write_rds(flags_filename,
      compress = "gz"
    )

  logger::log_success("Uploading to the cloud...")
  upload_cloud_file(flags_filename, pars$storage$google$key, pars$storage$google$options)
  logger::log_success("File upload succeded")
}
