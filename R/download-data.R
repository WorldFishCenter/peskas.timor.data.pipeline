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
