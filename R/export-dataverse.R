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
#' @param pars The configuration file
#'
#' @return A list with metadata information
#' @export
#'
#' @examples
generate_metadata <- function(pars) {
  landings_metadata <- get_metadata_info(pars)

  metadat <- list(
    title = as.character(pars$export_dataverse$metadata$title),
    subject = as.character(pars$export_dataverse$metadata$subject),
    description = as.character(pars$export_dataverse$metadata$description),
    created = as.character(Sys.Date())
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
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#'
#' @export
#'
#' @examples
export_files <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  dataverse <- pars$export_dataverse$dataverse_id
  key <- pars$export_dataverse$token
  server <- pars$export_dataverse$server

  logger::log_info("Retrieving public data to release...")
  rmarkdown::render('./data_description.rmd',params=list(output_file = data_description.html,
                                                         pars))

  logger::log_info("Generating metadata...")
  metadat <- generate_metadata(pars)

  dataverse::initiate_sword_dataset(
    dataverse = dataverse,
    key = key,
    server = server,
    body = metadat
  )

  release_files_names <- c(files_names,'data_description.html')

  logger::log_info("Exporting files...")
  upload_files(
    file_list = release_files_names,
    token = key,
    dataverse = dataverse,
    server = server
  )

  #logger::log_info("Publishing data...")
  #publish_last_dataset(
  #  token = key,
  #  dataverse = dataverse,
  #  server = server
  #)
}
