#' Ingest Landings Survey data
#'
#' Downloads landings information for multiple surveys collected using Kobo Toolbox and
#' uploads it to cloud storage services.
#'
#' This function downloads the survey metadata and responses for multiple landing surveys
#' (landings_1, landings_3). It then uploads this information to cloud
#' services. File names contain a versioning string that includes the date-time and,
#' if available, the first 7 digits of the git commit sha, accomplished using [add_version()].
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' surveys:
#'   kobo_username:
#'   kobo_password:
#'   landings_1:
#'     file_prefix:
#'     asset_id:
#'   landings_3:
#'     file_prefix:
#'     asset_id:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more details
#'
#' @keywords workflow
#'
#' @return No output. This function is used for its side effects
#' @export
#'
ingest_landings_v1v3 <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  # Create a vector of landing numbers
  landing_numbers <- c(1, 3)

  # Use purrr::map to apply get_kobo_data() to each landing number
  file_lists <- purrr::map(landing_numbers, function(i) {
    survey_name <- paste0("landings_", i)
    logger::log_info("Retrieving data for {survey_name}...")

    get_kobo_data(
      prefix = pars$surveys[[survey_name]]$file_prefix,
      uname = pars$surveys$kobo_username,
      pwd = pars$surveys$kobo_password,
      assetid = pars$surveys[[survey_name]]$asset_id
    )
  })

  # Combine all file lists
  all_files <- unlist(file_lists, recursive = FALSE)

  logger::log_info("Uploading files to cloud...")
  # Iterate over multiple storage providers if there are more than one
  purrr::walk(pars$storage, ~ upload_cloud_file(all_files, .$key, .$options))
  logger::log_success("File upload succeeded")
}

#' Ingest V2 Landings Survey data
#'
#' Downloads landings information for multiple surveys collected using Kobo Toolbox and
#' uploads it to cloud storage services.
#'
#' This function downloads the survey metadata and responses for landing surveys v2.
#'  It then uploads this information to cloud services. File names contain a
#'  versioning string that includes the date-time and,
#' if available, the first 7 digits of the git commit sha, accomplished using [add_version()].
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' surveys:
#'   kobo_username:
#'   kobo_password:
#'   landings_2:
#'     file_prefix:
#'     asset_id:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more details
#'
#' @keywords workflow
#'
#' @return No output. This function is used for its side effects
#' @export
#'
ingest_landings_v2 <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  landing_numbers <- 2

  # Use purrr::map to apply get_kobo_data() to each landing number
  file_lists <- purrr::map(landing_numbers, function(i) {
    survey_name <- paste0("landings_", i)
    logger::log_info("Retrieving data for {survey_name}...")

    get_kobo_data(
      prefix = pars$surveys[[survey_name]]$file_prefix,
      uname = pars$surveys$kobo_username,
      pwd = pars$surveys$kobo_password,
      assetid = pars$surveys[[survey_name]]$asset_id
    )
  })

  # Combine all file lists
  all_files <- unlist(file_lists, recursive = FALSE)

  logger::log_info("Uploading files to cloud...")
  # Iterate over multiple storage providers if there are more than one
  purrr::walk(pars$storage, ~ upload_cloud_file(all_files, .$key, .$options))
  logger::log_success("File upload succeeded")
}
