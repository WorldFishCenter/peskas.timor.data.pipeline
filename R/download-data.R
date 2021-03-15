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
kobo_host <- function(api, version = "v1") {
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
#' @inheritParams kobo_host
#'
#' @return The file path
#' @export
#'
#' @examples
#'
#' download_kobo_data("test.csv", id = 753491)
#' download_kobo_data("test.json", id = 753491, format = "json")
#'
download_kobo_data <- function(path, id = NULL, token = NULL,
                               api = "kobohr", format = "csv",
                               overwrite = TRUE){

  request_url <- paste(kobo_host(api),
                       "data", as.character(id), sep = "/")

  httr::GET(url = request_url,
            config = httr::add_headers(Authorization = token),
            query = list(format = format),
            httr::write_disk(path, overwrite = overwrite))

  path
}
