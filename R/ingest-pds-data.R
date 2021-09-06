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
  purrr::map(pars$storage, ~ upload_cloud_file(file_list, .$key, .$options))

  logger::log_success("File upload succeded")

}

#' Ingest Pelagic Data System tracks data
#'
#' Downloads Pelagic Data System (pds) trips information and uploads it to cloud
#' storage services.
#'
#' The function  downloads and uploads only tracks data that are not yet
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

  pds_trips_csv <-
    cloud_object_name(prefix = pars$pds$trips$file_prefix,
                      provider = pars$storage$google$key,
                      extension = "csv",
                      options = pars$storage$google$options)
  logger::log_info("Retrieving {pds_trips_csv}")
  # get trips data frame
  pds_trips_mat <- download_cloud_file(name = pds_trips_csv,
                                       provider = pars$storage$google$key,
                                       options = pars$storage$google$options)

  # extract unique trip identifiers
  trips_ID <- readr::read_csv(
    pds_trips_mat,
    col_types = readr::cols_only(Trip = readr::col_character())) %>%
    magrittr::extract2("Trip") %>%
    unique()

  if (isTRUE(pars$pds$tracks$compress)) ext <- "csv.gz" else ext <- "csv"

  # list id tracks already in bucket
  file_list_id <- cloud_object_name(prefix = pars$pds$tracks$file_prefix,
                    provider = pars$pds_storage$google$key,
                    extension = ext,
                    options = pars$pds_storage$google$options) %>%
    stringr::str_extract("[[:digit:]]+") %>%
    as.character()

  process_track <- function(id, pars){

    path <- paste0(pars$pds$tracks$file_prefix, "-", id) %>%
      add_version(extension = "csv")
    on.exit(file.remove(path))

    retrieve_pds_tracks_data(path,
                             secret = pars$pds$trips$secret,
                             token = pars$pds$trips$token,
                             id = id)

    if (isTRUE(pars$pds$tracks$compress)) {
      logger::log_info("Compressing file...")
      csv_path <- path
      path <- paste0(path, ".gz")
      readr::read_csv(csv_path,
                      col_types = readr::cols(.default = readr::col_character())) %>%
        readr::write_csv(path)
      on.exit(file.remove(csv_path, path))
    }

    logger::log_info("Uploading {path} to cloud...")
    # Iterate over multiple storage providers if there are more than one
    purrr::map(pars$pds_storage, ~ purrr::walk(
      .x = path,
      .f = ~ insistent_upload_cloud_file(
        file = .,
        provider = pars$pds_storage$google$key,
        options = pars$pds_storage$google$options)))
    logger::log_success("File upload succeded")

  }

  tracks_to_download <- trips_ID[!(trips_ID %in% file_list_id)]
  if (isTRUE(pars$pds$tracks$multisession$parallel)) {
    future::plan(future::multisession,
                 workers = pars$pds$tracks$multisession$n_sessions)
  }
  furrr::future_walk(tracks_to_download, process_track, pars, .progress = TRUE)
}

#' Insistent version of `upload_cloud_file()`
#'
#' Just like `upload_cloud_file()`, this function takes a vector of tracks files
#' as argument and upload them to the cloud. The function uses
#' [purrr::insistently] in order to continue to upload files despite stale OAuth
#' token.
#'
#' @param delay he time interval to suspend execution for, in seconds.
#' @param ... Inputs to `upload_cloud_file()`
#'
#' @return No output. This function is used for it's side effects
#' @export
#'
insistent_upload_cloud_file <- function(..., delay = 3){
  purrr::insistently(upload_cloud_file,
                     rate = purrr::rate_backoff(
                       pause_cap = 60*5,
                       max_times = 10),
                     quiet = F)(...)
  Sys.sleep(delay)
}

#' Insistent version of `download_cloud_file()`
#'
#' Just like `download_cloud_file()`, this function takes a vector of tracks files
#' as argument and download them to the cloud. The function uses
#' [purrr::insistently] in order to continue to download files despite stale OAuth
#' token.
#'
#' @param delay he time interval to suspend execution for, in seconds.
#' @param ... Inputs to `download_cloud_file()`
#'
#' @return No output. This function is used for it's side effects
#' @export
#'
insistent_download_cloud_file <- function(..., delay = 3){
  purrr::insistently(download_cloud_file,
                     rate = purrr::rate_backoff(
                       pause_cap = 60*5,
                       max_times = 10),
                     quiet = F)(...)
  Sys.sleep(delay)
}
