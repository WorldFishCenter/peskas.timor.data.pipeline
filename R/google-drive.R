#' Authenticate to Google Drive
#'
#' @param options a list with information about the authentication method.
#'   Either "service_account_key" for authenticating an app or "OAuth" for
#'   authenticating a real person (for example when running the code locally).
#'
#' @export
#'
authenticate_google_drive <- function(options = list(method = "service_account_key")) {
  if (options$method == "service_account_key") {
    service_account_key <- options$service_account_key
    temp_auth_file <- tempfile(fileext = "json")
    writeLines(service_account_key, temp_auth_file)
    googledrive::drive_auth(path = temp_auth_file)
  } else if (options$method == "OAuth") {
    googledrive::drive_auth(use_oob = TRUE)
  }
}
