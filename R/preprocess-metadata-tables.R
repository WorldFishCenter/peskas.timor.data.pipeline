#' Preprocess metadata tables
#'
#' Takes ingested metadata tables, validates them, and processes them into
#' artefacts (devices information, flags) to be used further down in the
#' pipeline.
#'
#' Specifically this function:
#'
#' * Validates the flags table containing the columns: flag_id, flag_category, flag_message
#' * Constructs a devices table containing the columns: imei
#'
#' This function requires no arguments because it retrieves the parameters from
#' `conf.yml`. The fields required are:
#'
#' ```
#' metadata:
#'   spreadsheet:
#'     name:
#'   version:
#'     preprocess:
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
#' @keywords workflow
#' @export
#'
preprocess_metadata_tables <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)
  pars <- read_config()

  metadata_filename <- cloud_object_name(
    prefix = pars$metadata$spreadsheet$name,
    provider = pars$storage$google$key,
    extension = "xlsx",
    version = pars$metadata$version$preprocess,
    options = pars$storage$google$options)

  logger::log_info("Downloading metadata tables as {metadata_filename}...")
  download_cloud_file(name = metadata_filename,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)

  logger::log_info("Reading {metadata_filename}...")
  metadata_tables <- readxl::excel_sheets(metadata_filename) %>%
    rlang::set_names(.data$.) %>%
    purrr::map(~ readxl::read_excel(path = metadata_filename,
                                    sheet = .))

  logger::log_info("Preprocessing metadata tables...")
  preprocessed_metadata <- list(
    devices = pt_get_devices_table(metadata_tables$boats),
    flags = pt_validate_flags(metadata_tables$flags))

  preprocessed_filename <- paste(pars$metadata$spreadsheet$name,
                                 "preprocessed", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(x = preprocessed_metadata,
                   file = preprocessed_filename,
                   compress = "gz")

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  upload_cloud_file(file = preprocessed_filename,
                    provider = pars$storage$google$key,
                    options = pars$storage$google$options)

}

#' Get table containing information about the PDS devices
#'
#' @param boats_table the boat metadata table obtained from Google
#'   Spreadsheets
#'
#' @return a data frame with columns: imei
#' @export
#'
pt_get_devices_table <- function(boats_table){
  imeis <- boats_table$imei %>%
    stats::na.omit() %>%
    unique() %>%
    as.character()

  tibble::tibble(
    imei = imeis
  )
}


#' Validate flags from metadata tables
#'
#' Checks the flags from the metadata tables. Specifically check that flags are
#' unique and all flags have an id
#'
#' @param flags_table the table containing the flags. Must have columns flag_id,
#'   flag_category, and flag_message
#'
#' @return a data frame with columns flag_id, flag_category, and flag_message
#' @export
#'
#' @examples
pt_validate_flags <- function(flags_table){

  f <- flags_table %>%
    dplyr::filter(!is.na(.data$flag_message)) %>%
    dplyr::mutate(flag_id = as.character(.data$flag_id))

  n_codes <- dplyr::n_distinct(f$flag_id)
  n_flags <- nrow(f)
  if (any(is.na(f$flag_id))) stop("not all flags have a flag_id")
  if (n_codes < n_flags) stop("flag_id are not unique")

  f
}
