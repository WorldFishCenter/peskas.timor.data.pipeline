#' Upload a local file to a cloud storage bucket
#'
#' @param file a filepath (character) to upload
#' @param provider cloud provider to use, either "gcs" or "aws"
#' @param options named list with cloud provider options, see details
#' @param name What to call the file once uploaded. Default is the filepath
#'
#' @details
#'
#' ### Google Cloud Services
#'
#' For Google Cloud Services ("gcs") options must be a list with two fields:
#' `bucket` with the bucketname (character) you are uploading to, and
#' `service_account_key` with the contents of the authentication json file you
#' have downloaded from your Google Project.
#'
#' This function uses [googleCloudStorageR::gcs_auth] and
#' [googleCloudStorageR::gcs_upload] under the hood to upload the file.
#'
#' @return If `provider` is "gcs" and if successful a medatada object
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
  if (provider == "gcs") {

    service_account_key <- options$service_account_key
    temp_auth_file <- tempfile(fileext = 'json')
    writeLines(service_account_key,temp_auth_file)
    googleCloudStorageR::gcs_auth(json_file = temp_auth_file)

    googleCloudStorageR::gcs_upload(file = file,
                                    name = name,
                                    bucket = options$bucket,
                                    predefinedAcl = "bucketLevel")
  }
}
