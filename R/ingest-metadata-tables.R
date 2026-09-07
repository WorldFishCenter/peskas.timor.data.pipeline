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
#' @param log_threshold The (standard Apache logj4) log level used as a threshold for the logging infrastructure. See [logger::log_levels] for more details
#'
#' @export
#' @keywords workflow
#'
#' @examples
#' \dontrun{
#' ingest_metadata_tables()
#' }
ingest_metadata_tables <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  metadata_filename <- add_version(conf$metadata$google_sheets$name, "rds")

  logger::log_info("Authenticating for google drive")
  googlesheets4::gs4_auth(
    path = conf$storage$google$options$service_account_key,
    use_oob = TRUE
  )
  logger::log_info("Downloading metadata tables as {metadata_filename}...")

  conf$metadata$google_sheets$tables %>%
    rlang::set_names() %>%
    purrr::map(~ googlesheets4::range_read(
      ss = conf$metadata$google_sheets$sheet_id,
      sheet = .x,
      col_types = "c"
    )) %>%
    readr::write_rds(metadata_filename,
      compress = "gz"
    )

  logger::log_success("Uploading to the cloud...")
  coasts::upload_cloud_file(metadata_filename, conf$storage$google$key, conf$storage$google$options)
  logger::log_success("File upload succeded")
}
