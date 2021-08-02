#' Publish a Dataverse repository
#'
#' This function publish a specific Dataverse repository.
#'
#' @param token API token associated to the Dataverse account.
#' @param dataverse A character string specifying the Dataverse ID.
#' @param server A character string specifying a Dataverse server.
#'
#' @export
#'
#' @examples
publish_dataverse <- function(token, dataverse, server) {
  url <- paste0("https://", server, "/api/dataverses/", dataverse, "/actions/:publish")
  res <- httr::POST(
    url = url,
    httr::add_headers(`X-Dataverse-key` = token)
  )
  res
}

#' Retrieve metadata information from survey landings
#'
#' This function downloads the latest version of landings and extract metadata
#' information.
#'
#' @param pars Configuration file
#'
#' @return A list with metadata information:
#' extent: time period covered
#' ....
#' @export
#'
#' @examples
get_metadata_info <- function(pars) {
  landings <- get_merged_landings(pars)
  extent <- paste(min(landings$date), max(landings$date), sep = "-")

  landings_metadata <- list(extent = extent)
  landings_metadata
}

#' Generate a list of metadata
#'
#' The function generate a list of metadata information to append to the
#' files to upload to a Dataverse repository.
#'
#' @return A list with metadata information
#' @export
#'
#' @examples
generate_metadata <- function() {
  pars <- read_config()
  landings_metadata <- get_metadata_info(pars)

  metadat <- list(
    title = paste("Project title"),
    creator = "Creator of the project",
    created = as.character(Sys.Date()),
    description = "Description of the data",
    extent = as.character(landings_metadata$extent),
    language = "English"
  )

  metadat
}

#' Upload files to Dataverse
#'
#' This function upload a list files in a specific Dataverse repository.
#'
#' @param file_list Paths indicating the files to be uploaded to Dataverse.
#' @param token API token associated to the Dataverse account.
#' @param dataverse A character string specifying the Dataverse ID.
#' @param server A character string specifying a Dataverse server.
#'
#' @export
#'
#' @examples
upload_files <- function(file_list = NULL, token = NULL, dataverse = NULL, server = NULL) {
  dataverse_content <-
    dataverse::dataverse_contents(
      dataverse = dataverse,
      key = token,
      server = server
    )

  last_dataset <- dataverse_content[length(dataverse_content)][[1]]
  PID <- paste0(
    last_dataset$protocol, ":",
    last_dataset$authority, "/",
    last_dataset$identifier
  )

  for (i in 1:length(file_list)) {
    dataverse::add_dataset_file(
      file = file_list[i],
      dataset = PID,
      key = token,
      description = "",
      server = server
    )
  }
}


#' Publish latest dataset created
#'
#' The function converts from "draft" to "public" the latest dataset uploaded
#' in a specific Dataverse repository.
#'
#' @inheritParams upload_files
#'
#' @export
#'
#' @examples
publish_last_dataset <- function(token = NULL, dataverse = NULL, server = NULL) {
  dataverse_content <-
    dataverse::dataverse_contents(
      dataverse = dataverse,
      key = token,
      server = server
    )

  last_data <- dataverse_content[length(dataverse_content)][[1]]

  dataverse::publish_dataset(
    dataset = last_data,
    minor = FALSE,
    key = token,
    server = server
  )
}

#' Upload and publish a dataset on Dataverse
#'
#' This function upload and publish data on a specific Dataverse repository
#' including associated metadata information.
#'
#' @inheritParams upload_files
#'
#' @export
#'
#' @examples
export_files <- function(token, dataverse, server) {
  metadat <- generate_metadata()

  dataverse::initiate_sword_dataset(
    dataverse = dataverse,
    body = metadat,
    key = token,
    server = server
  )

  file_list <- # complete with files to export

    upload_files(
      file_list = file_list,
      token = token,
      dataverse = dataverse,
      server = server
    )

  publish_last_dataset(
    token = token,
    dataverse = dataverse,
    server = server
  )
}
