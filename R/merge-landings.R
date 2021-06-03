#' Merge recent and legacy pre-processed landings
#'
#' Downloads pre-processed versions of recent and legacy landings from cloud and
#' merge in a rds-format file using the function [dplyr::full_join].
#'
#' The merged file is then uploaded to the cloud. The parameters needed are:
#'
#' ```
#' surveys:
#'   merged_landings:
#'     file_prefix:
#'   version:
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
#' @param log_threshold
#' @inheritParams preprocess_landings
#' @inheritParams preprocess_legacy_landings
#' @return No outputs. This function is used for it's side effects
#' @keywords workflow
#' @export
merge_landings <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)

  pars <- read_config()

  preprocessed_landings <-
    cloud_object_name(prefix = paste(pars$surveys$landings$file_prefix,"preprocessed",sep="_"),
                      provider = pars$storage$google$key,
                      extension = "rds",
                      version = pars$surveys$landings$version$preprocess,
                      options = pars$storage$google$options)

  preprocessed_legacy_landings <-
    cloud_object_name(prefix = paste(pars$surveys$landings_legacy$file_prefix,"preprocessed",sep="_"),
                      provider = pars$storage$google$key,
                      extension = "rds",
                      version = pars$surveys$landings$version$preprocess,
                      options = pars$storage$google$options)

  logger::log_info("Retrieving {preprocessed_data}")
  purrr::map(c(preprocessed_landings,preprocessed_legacy_landings),
             download_cloud_file,
             provider = pars$storage$google$key,
             options = pars$storage$google$options)

  # adding a column "survey_version"
  prep_landings <- readRDS(preprocessed_landings)
  prep_landings <- dplyr::mutate(prep_landings,survey_version=rep("v1",nrow(prep_landings)))
  prep_legacy_landings <- readRDS(preprocessed_legacy_landings)
  prep_legacy_landings <- dplyr::mutate(prep_legacy_landings,survey_version=rep("v2",nrow(prep_legacy_landings)))


  merged_landings <-
    dplyr::full_join(prep_landings,prep_legacy_landings)

  merged_filename <- pars$surveys$merged_landings$file_prefix %>%
    add_version(extension = "rds")

  readr::write_rds(x = merged_landings,
                   file = merged_filename,
                   compress = "gz")

  logger::log_info("Uploading {merged_filename} to cloud sorage")
  upload_cloud_file(file = merged_filename,
                    provider = pars$storage$google$key,
                    options = pars$storage$google$options)
}
