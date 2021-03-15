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
#' kobo_host("unhcr")
#' kobo_host("https://kobocat.unhcr.org/api/v1/")
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
             kobo = "https://kobo.kobotoolbox.org/api/v2",
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
#' download_survey_data("test.csv", id = 753491)
#' download_survey_data("test.json", id = 753491, format = "json")
#'
download_survey_data <- function(path, id = NULL, token = NULL,
                               api = "kobohr", format = "csv",
                               overwrite = TRUE){

  request_url <- paste(get_host_url(api),
                       "data", as.character(id), sep = "/")

  httr::GET(url = request_url,
            config = httr::add_headers(Authorization = token),
            query = list(format = format),
            httr::write_disk(path, overwrite = overwrite))

  path
}

#' Download survey metadata
#'
#' @param id
#' @param token
#' @param api
#' @inheritParams download_survey_data
#'
#' @return A list with survey metadata
#' @export
#'
#' @examples
#'
#' /dontrun{
#'   # It's only possible to donwload survey metadata with the account Token
#'   download_survey_metadata(753491, token = "123XXXXXX")
#' }
#'
download_survey_metadata <- function(id = NULL, token = NULL, api = "kobohr"){

  # If the token is not provided is not possible to get the
  no_token <- length(token) > 0 | is.null(token)
  if (no_token & is.null(id)){
    stop("Argument `id_string` is required if `token` is not provided")
  }

  assets_v1_raw <- httr::GET(
    url = paste(get_host_url(api, version = "v1"), "data", sep = "/"),
    config = httr::add_headers(Authorization = token))

  survey_basic_metadata <- purrr::keep(httr::content(assets_v1_raw),
                                       ~.$id == id)[[1]]

  # the version 2 of the api returns much richer information about the surveys
  httr::GET(
    url = paste(get_host_url(api, version = "v2"), "assets",
                "aaztUDtRzb9SpSV7i9iptb", sep = "/"),
    config = httr::add_headers(Authorization = token)) %>%
    httr::content() %>%
    c(survey_basic_metadata)
}
