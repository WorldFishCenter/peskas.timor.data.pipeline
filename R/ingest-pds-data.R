#' Ingest Pelagic Data System trips data
#'
#' Downloads Pelagic Data System (pds) trips information and uploads it to cloud
#' storage services.
#'
#' This function downloads trips information from Pelagic Data System devices.
#' Afterwards it uploads this information to cloud services. File names used
#' contain a versioning string that includes the date-time and, if available,
#' the first 7 digits of the git commit sha. This is acomplished
#' using [add_version()]
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' pds:
#'   trips:
#'     token:
#'     secret:
#'     file_prefix:
#' pds_storage:
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
ingest_pds_trips <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)

  pars <- read_config()

  file_list <- retrieve_pds_trips(prefix = pars$pds$trips$file_prefix,
                              secret = pars$pds$trips$secret,
                              token = pars$pds$trips$token)

  logger::log_info("Uploading files to cloud...")
  # Iterate over multiple storage providers if there are more than one
  purrr::map(pars$pds_storage, ~ upload_cloud_file(file_list, .$key, .$options))

  logger::log_success("File upload succeded")

}

#' Ingest Pelagic Data System tracks data
#'
#' This function read Pelagic Data System (pds) tracks data, merge them with
#' trips information by their id, then uploads it to cloud storage services as
#' a csv data frame.
#'
#' The function  downloads and uploads only the tracks data that are not yet
#' stored in the bucket.
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' pds:
#'   tracks:
#'     token:
#'     secret:
#'     file_prefix:
#' pds_storage:
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
#' @importFrom rlang .data
#'
ingest_pds_tracks <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)

  pars <- read_config()

  # get trips data frame
  pds_trips_mat <- get_pds_resp(data="trips",
                            secret = pars$pds$trips$secret,
                            token = pars$pds$trips$token)

  # extract unique trip identifiers
  trips_ID <- unique(pds_trips_mat$Trip)

  # autenticate
  googleCloudStorageR::gcs_auth(pars$pds_storage$google$options$service_account_key)

  # list id tracks already in bucket
  file_list_id <-
    googleCloudStorageR::gcs_list_objects(
      bucket = pars$pds_storage$google$options$bucket,
      detail = "summary",
      prefix = NULL,
      delimiter = NULL)$name %>%
    stringr::str_extract("[[:digit:]]+") %>%
    as.character()

    file_list <- NULL
  for(i in trips_ID){

    # check if id is alredy in the bucket
    file_exists <- i %in% file_list_id

    if (isTRUE(file_exists)){
      message("Object already exists in the bucket. It has not been rewritten.")
    } else {

      pds_tracks_mat <- get_pds_resp(data="tracks",
                                 secret = pars$pds$trips$secret,
                                 token = pars$pds$trips$token,
                                 id=i)

      # merge pds tracks and trips
      merge_pds <- dplyr::full_join(dplyr::filter(pds_trips_mat, .data$Trip==i),pds_tracks_mat)

      merged_filename <- paste(pars$pds$tracks$file_prefix, i,".csv",sep = "_")
      readr::write_csv(x = merge_pds,
                       file = merged_filename)

      file_list <- c(file_list, merged_filename)
      file_list
    }
  }

  logger::log_info("Uploading files to cloud...")
  # Iterate over multiple storage providers if there are more than one
  purrr::map(pars$pds_storage, ~ upload_cloud_file(file_list, .$key, .$options))
  logger::log_success("File upload succeded")
}