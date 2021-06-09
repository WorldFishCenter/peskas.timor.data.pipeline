#' Select server from kobo
#'
#' Specifies the Host URL of the API to use
#'
#' @param api Either "kobo", "kobohr", "ona", or a custom (full) URL. API URLs
#'   are made available for KoBo Toolbox ("kobo",
#'   \url{https://kc.kobotoolbox.org/api/v1/}), KoBo Humanitarian Response
#'   ("kobohr", \url{https://kc.humanitarianresponse.info/api/v1/}), Ona ("ona",
#'   \url{https://ona.io/api/v1/}) and Unhcr ("unhcr",
#'   \url{https://kobocat.unhcr.org/api/v1/}) . For your own installation, or
#'   other installations using the same API but accessed at a different URL,
#'   enter the full URL.
#' @param version Wether using the API "v1" or "v2"
#'
#' @author Ananda Mahto
#'
#' @return An URL
#' @export
#'
#' @examples
#'
#' get_host_url("unhcr")
#' get_host_url("https://kobocat.unhcr.org/api/v1/")
#'
get_host_url <- function(api, version = "v1") {
  if (api %in% c("kobo", "kobohr", "ona","unhcr")) {
    if (version == "v1") {
      switch(api,
             kobo = "https://kc.kobotoolbox.org/api/v1",
             kobohr = "https://kc.humanitarianresponse.info/api/v1",
             ona = "https://ona.io/api/v1",
             unhcr = "https://kobocat.unhcr.org/api/v1")
    } else if (version == "v2") {
      switch(api,
             kobo = "https://kc.kobotoolbox.org/api/v2",
             kobohr = "https://kobo.humanitarianresponse.info/api/v2")
    }
  } else {
    api
  }
}

#' Download kobo data in as csv or json
#'
#' @param path string with path to file where API request should be saved
#' @param id survey id. Usually a 6 digit number. See [this support
#'   page](https://support.kobotoolbox.org/pulling_data_into_powerbi.html) for
#'   an example on how this can be obtained
#' @param token access token for the account e.g. "Token XXXXXXX"
#' @param api
#' @param format Either "csv" or "json"
#' @param overwrite Will only overwrite existing path if TRUE.
#'
#' @inheritParams get_host_url
#'
#' @return The file path
#' @export
#'
#' @examples
#'
#' retrieve_survey_data("test.csv", id = 753491)
#' retrieve_survey_data("test.json", id = 753491, format = "json")
#' file.remove("test.csv", "test.json")
#'
retrieve_survey_data <- function(path, id = NULL, token = NULL,
                               api = "kobohr", format = "csv",
                               overwrite = TRUE){

  request_url <- paste(get_host_url(api),
                       "data", as.character(id), sep = "/")

  resp <-
  httr::GET(url = request_url,
            config = httr::add_headers(Authorization = token),
            query = list(format = format),
            httr::write_disk(path, overwrite = overwrite))

  if(resp$status_code %in% 200:299){path}
  else{stop("Unsuccessful response from server")}
}

#' Download survey metadata
#'
#' @param id
#' @param token
#' @param api
#' @inheritParams retrieve_survey_data
#'
#' @return A list with survey metadata
#' @export
#'
#' @examples
#'
#' \dontrun{
#'   # It's only possible to donwload survey metadata with the account Token
#'   retrieve_survey_metadata(753491, token = "123XXXXXX")
#' }
#'
retrieve_survey_metadata <- function(id = NULL, token = NULL, api = "kobohr"){

  # If the token is not provided is not possible to get the
  no_token <- length(token) == 0 | is.null(token)
  if (no_token) {
    stop("Token is required")
  }

  assets_v1_raw <- httr::GET(
    url = paste(get_host_url(api, version = "v1"), "data", sep = "/"),
    config = httr::add_headers(Authorization = token))

  survey_basic_metadata <- purrr::keep(httr::content(assets_v1_raw),
                                       ~.$id == id)[[1]]

  # the version 2 of the api returns much richer information about the surveys
  httr::GET(
    url = paste(get_host_url(api, version = "v2"), "assets",
                survey_basic_metadata$id_string, sep = "/"),
    config = httr::add_headers(Authorization = token)) %>%
    httr::content() %>%
    c(survey_basic_metadata)
}


#' Download survey
#'
#' Download multiple survey artefacts (data and metadata) at once. Progress
#' through the function is tracked using the package *logger*.
#'
#' @param prefix name to be used as the prefix of the file names to be
#'   downloaded. Can be a path.
#' @param api
#' @param id
#' @param token
#' @param format
#' @param metadata whether to download metadata as well as data
#' @param append_version whether to append versioning information to the
#'   filename using [add_version].
#' @inheritParams retrieve_survey_data
#'
#' @return a character vector with paths of the downloaded files
#' @export
#'
#' @examples
#' \dontrun{
#'   # It's only possible to donwload survey metadata with the account Token
#'   retrieve_survey(prefix = "my-survey", api = "kobohr", id = 753491,
#'                   token = "123XXXXXX")
#'   # To download in a different path
#'   dir.create("my-data-dir")
#'   retrieve_survey(prefix = "my-data-dir/my-survey", api = "kobohr", id = 753491,
#'                   token = "123XXXXXX")
#' }
retrieve_survey <- function(prefix, api, id, token, format = c("csv", "json"),
                            metadata = TRUE, append_version = TRUE){

  metadata_filename <- paste(prefix, "metadata", sep = "_")
  data_filename <- paste(prefix, "raw", sep = "_")

  if (isTRUE(append_version)) {
    metadata_filename <- add_version(metadata_filename, "json")
    csv_filename <- add_version(data_filename, "csv")
    json_filename <- add_version(data_filename, "json")
  } else {
    metadata_filename <- paste0(metadata_filename, ".json")
    csv_filename <- paste0(data_filename, ".csv")
    json_filename <- paste0(data_filename, ".json")
  }

  filenames <- character()

  if (isTRUE(metadata)) {
    logger::log_info("Downloading survey metadata as {metadata_filename}...")
    retrieve_survey_metadata(id, token = token, api = api) %>%
      jsonlite::write_json(metadata_filename)
    logger::log_success("Metadata download succeeded")
    filenames <- c(filenames, metadata_filename)
  }

  if ("csv" %in% format) {
    logger::log_info("Downloading survey csv data as {csv_filename}...")
    retrieve_survey_data(path = csv_filename, id, token, format = "csv")
    logger::log_success("Survey csv data download succeeded")
    filenames <- c(filenames, csv_filename)
  }

  if ("json" %in% format) {
    logger::log_info("Downloading survey json data as {json_filename}...")
    retrieve_survey_data(json_filename, id, token, format = "json")
    logger::log_success("Survey json data download succeeded")
    filenames <- c(filenames, json_filename)
  }

  filenames

}
