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
#' This function read Pelagic Data System (pds) tracks data, merge tracks with
#' trips information by each id, then uploads the merged file to cloud storage
#' services as a csv data frame.
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

  for(i in trips_ID){

    # check if id is alredy in the bucket
    file_exists <- i %in% file_list_id

    if (isFALSE(file_exists)){

      path <- paste0(pars$pds$tracks$file_prefix, "-", i) %>%
        add_version(extension = "csv")

      logger::log_info("Downloading and merging id {i} with trips info...")
      retrieve_pds_tracks_data(path,
                               secret = pars$pds$trips$secret,
                               token = pars$pds$trips$token,
                               id = i)
      logger::log_success("id {i} correctly downloaded and merged.")

      logger::log_info("Uploading {path} to cloud...")
      # Iterate over multiple storage providers if there are more than one
      purrr::map(pars$pds_storage, ~ purrr::walk(
        .x = path,
        .f = ~ upload_tracks(
          file = .,
          provider = pars$pds_storage$google$key,
          options = pars$pds_storage$google$options)))
      logger::log_success("File upload succeded")

      file.remove(path)
    }
  }
}

#' Upload tracks files
#'
#' This function takes a vector of tracks files as argument and upload them to
#' the cloud. The function uses [purrr::insistently] in order to continue to
#' upload files despite stale OAuth token.
#'
#' @param delay he time interval to suspend execution for, in seconds.
#'
#' @return No output. This function is used for it's side effects
#' @export
#'
upload_tracks <- function(..., delay = 3){
  purrr::insistently(upload_cloud_file,
                     rate = purrr::rate_backoff(
                       pause_cap = 60*5,
                       max_times = 10),
                     quiet = F)(...)
  Sys.sleep(delay)
}
