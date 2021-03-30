#' Authenticate to a storage cloud provider
#'
#' Usually used internally by other functions
#'
#' @param provider cloud provider to use, either "gcs" or "aws"
#' @param options named list with cloud provider options, see details
#'
#' @details
#'
#' ### Google Cloud Services
#'
#' For Google Cloud Services ("gcs") options must be a list with the field
#' `service_account_key` with the contents of the authentication json file you
#' have downloaded from your Google Project.
#'
#' This function uses [googleCloudStorageR::gcs_auth] under the hood to
#' authenticate.
#'
#' @export
#'
#' @examples
#'
#'  # Google Cloud Services
#' \dontrun{
#'   authentication_details <- readLines("location_of_json_file.json")
#'   cloud_storage_authenticate(
#'     provider = "gcs",
#'     options = list(service_account_key = authentication_details,
#'                    bucket = "my-bucket"))
#' }
cloud_storage_authenticate <- function(provider, options) {

  if ("gcs" %in% provider) {
    # Only need to authenticate if there is no token for downstream requests
    if (isFALSE(googleAuthR::gar_has_token())) {
      service_account_key <- options$service_account_key
      temp_auth_file <- tempfile(fileext = 'json')
      writeLines(service_account_key,temp_auth_file)
      googleCloudStorageR::gcs_auth(json_file = temp_auth_file)
    }
  }
}

#' Upload a local file to a cloud storage bucket
#'
#' @param file a file-path (character) to upload. A vector with multiple files
#'   is also supported.
#' @param provider
#' @param options
#' @param name What to call the file once uploaded. Default is the filepath
#' @inheritParams cloud_storage_authenticate
#' @details
#'
#' ### Google Cloud Services
#'
#' For Google Cloud Services ("gcs") options must be a list with two fields:
#' `bucket` with the bucketname (character) you are uploading to, and
#' `service_account_key` with the contents of the authentication json file you
#' have downloaded from your Google Project (if [cloud_storage_authenticate] has
#' not been called before).
#'
#' This function uses [googleCloudStorageR::gcs_upload] under the hood to upload
#' the file.
#'
#' @return If `provider` is "gcs" and if successful a list of medatada objects
#' @export
#'
#' @examples
#'
#' # Google Cloud Services
#' \dontrun{
#'   authentication_details <- readLines("location_of_json_file.json")
#'   upload_cloud_file(
#'     file = "table_to_upload.csv",
#'     provider = "gcs",
#'     options = list(service_account_key = authentication_details,
#'                    bucket = "my-bucket"))
#' }
#'
upload_cloud_file <- function(file, provider, options, name = file){

  cloud_storage_authenticate(provider, options)

  out <- list()
  if ("gcs" %in% provider) {

    # Iterate over multiple files (and names)
    google_output <- purrr::map2(
      file, name,
      ~ googleCloudStorageR::gcs_upload(
        file = .x,
        bucket = options$bucket,
        name = .y,
        predefinedAcl = "bucketLevel"))

    out <- c(out, google_output)
  }

  out
}
