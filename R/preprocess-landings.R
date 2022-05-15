#' Pre-process Timor Landings Survey data (batch 1)
#'
#' Downloads raw structured data from cloud storage services and pre-process
#' into a binary format that is easier to deal with in R.
#'
#' This function downloads the landings data from a given version (specified in
#' the config file `conf.yml`. The parameters needed are:
#'
#' ```
#' surveys:
#'   landings:
#'     api:
#'     survey_id:
#'     token:
#'     file_prefix:
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
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold
#' @inheritParams ingest_landings
#' @keywords workflow
#' @return no outputs. This funcrion is used for it's side effects
#' @export
#'
preprocess_landings_batch1 <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)

  pars <- read_config()

  landings_csv <- cloud_object_name(prefix = pars$surveys$landings$file_prefix,
                                    provider = pars$storage$google$key,
                                    extension = "csv",
                                    version = pars$surveys$landings$version$preprocess,
                                    options = pars$storage$google$options)

  logger::log_info("Retrieving {landings_csv}")
  download_cloud_file(name = landings_csv,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)
  landings_raw <- readr::read_csv(
    file = landings_csv,
    col_types = readr::cols(.default = readr::col_character()))


  # split data
  half_data <- round(nrow(landings_raw) / 2, 0)
  landings_raw <- landings_raw[1:half_data, ]

  logger::log_info("Nesting landings attachment fields")
  landings_nested_attachments <- pt_nest_attachments(landings_raw)

  logger::log_info("Nesting landings species fields")
  landngs_nested_species <- pt_nest_species(landings_nested_attachments)

  preprocessed_filename <- paste(pars$surveys$landings$file_prefix, "preprocessed", "batch1", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(x = landngs_nested_species,
                   file = preprocessed_filename,
                   compress = "gz")

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  upload_cloud_file(file = preprocessed_filename,
                    provider = pars$storage$google$key,
                    options = pars$storage$google$options)
}



#' Pre-process Timor Landings Survey data (batch 2)
#'
#' Downloads raw structured data from cloud storage services and pre-process
#' into a binary format that is easier to deal with in R.
#'
#' This function downloads the landings data from a given version (specified in
#' the config file `conf.yml`. The parameters needed are:
#'
#' ```
#' surveys:
#'   landings:
#'     api:
#'     survey_id:
#'     token:
#'     file_prefix:
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
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold
#' @inheritParams ingest_landings
#' @keywords workflow
#' @return no outputs. This funcrion is used for it's side effects
#' @export
#'
preprocess_landings_batch2 <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)

  pars <- read_config()

  landings_csv <- cloud_object_name(
    prefix = pars$surveys$landings$file_prefix,
    provider = pars$storage$google$key,
    extension = "csv",
    version = pars$surveys$landings$version$preprocess,
    options = pars$storage$google$options
  )

  preprocessed_batch_1 <- cloud_object_name(
    prefix = paste(pars$surveys$landings$file_prefix, "preprocessed", "batch1", sep = "_"),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$surveys$landings$version$preprocess,
    options = pars$storage$google$options
  )

  logger::log_info("Retrieving {landings_csv} and {preprocessed_batch_1}")

  c(landings_csv, preprocessed_batch_1) %>%
  purrr::map(download_cloud_file,
             provider = pars$storage$google$key,
             options = pars$storage$google$options)

  landings_raw <- readr::read_csv(
    file = landings_csv,
    col_types = readr::cols(.default = readr::col_character()))

  preprocessed_batch_1 <- readr::read_rds(preprocessed_batch_1)

  # get ids of batch 2 to download and process
  batch_2_ids <- setdiff(landings_raw$`_id`, preprocessed_batch_1$`_id`)

  landings_raw <-
    landings_raw %>%
    dplyr::filter(.data$`_id` %in% batch_2_ids)

  logger::log_info("Nesting landings attachment fields")
  landings_nested_attachments <- pt_nest_attachments(landings_raw)

  logger::log_info("Nesting landings species fields")
  landings_nested_species <- pt_nest_species(landings_nested_attachments)

  # bind processed batch 1 and 2
  landings_nested_species <-
    dplyr::bind_rows(preprocessed_batch_1,
                     landings_nested_species)

preprocessed_filename <- paste(pars$surveys$landings$file_prefix, "preprocessed", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(x = landings_nested_species,
                   file = preprocessed_filename,
                   compress = "gz")

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  upload_cloud_file(file = preprocessed_filename,
                    provider = pars$storage$google$key,
                    options = pars$storage$google$options)
}
