#' Ingest Legacy Landings Survey data
#'
#' Downloads legacy landings information that has been collected using Kobo Toolbox and
#' uploads it to cloud storage services.
#'
#' This function downloads the survey metadata (survey information) as well as
#' the survey responses. Afterwards it uploads this information to cloud
#' services. File names used contain a
#' versioning string that includes the date-time and, if available, the first 7
#' digits of the git commit sha. This is acomplished using [add_version()]
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' surveys:
#'   landings:
#'     api:
#'     survey_id:
#'     token:
#'     file_prefix:
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
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#'
#' @keywords workflow
#'
#' @return No output. This funcrion is used for it's side effects
#' @export
#'
ingest_legacy_landings <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)

  pars <- read_config()

  file_list <- retrieve_survey(pars$surveys$landings_legacy$file_prefix,
                               api = pars$surveys$landings_legacy$api,
                               id = pars$surveys$landings_legacy$survey_id,
                               token = pars$surveys$landings_legacy$token)

  logger::log_info("Uploading files to cloud...")
  # Iterate over multiple storage providers if there are more than one
  purrr::map(pars$storage, ~ upload_cloud_file(file_list, .$key, .$options))
  logger::log_success("File upload succeded")

}
