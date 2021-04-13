#' Ingest metadata tables
#'
#' Metadata tables are manually updated in a Google Drive spreadsheet. This
#' function downloads the table and uploads it to a drive location. These tables
#' include information about flags, boats, devices, municipalities, etc.
#'
#' The parameters needed in `conf.yml` are:
#' ```
#' metadata:
#'   spreadsheet:
#'     id:
#'   auth:
#'     method:
#'     service_account_key:
#'
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
ingest_metadata_tables <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)
  pars <- read_config()

  authenticate_google_drive(pars$metadata$auth)
  metadata_filename <- add_version("metadata-tables", "xlsx")

  logger::log_info("Downloading metadata tables as {metadata_filename}...")
  googledrive::drive_download(
    file = googledrive::as_id(pars$metadata$spreadsheet$id),
    # path = metadata_filename,
    overwrite = TRUE)
  logger::log_success("Metadata tables download succeded")

  logger::log_info("Uploading files to cloud...")
  upload_cloud_file(metadata_filename, pars$storage$google$key, pars$storage$google$options)
  logger::log_success("File upload succeded")

}
