#' Publish a Dataverse repository
#'
#' This function publish a specific Dataverse repository.
#'
#' @param key API token associated to the Dataverse account.
#' @param dataverse A character string specifying the Dataverse ID.
#' @param server A character string specifying a Dataverse server.
#'
#' @export
#'
#' @examples
publish_dataverse <- function(key, dataverse, server) {
  url <- paste0("https://", server, "/api/dataverses/", dataverse, "/actions/:publish")
  res <- httr::POST(
    url = url,
    httr::add_headers(`X-Dataverse-key` = key)
  )
  res
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

  metadat <- list(
    title = as.character(pars$export_dataverse$metadata$title),
    subject = as.character(pars$export_dataverse$metadata$subject),
    description = as.character(pars$export_dataverse$metadata$description),
    creator = as.character(pars$export_dataverse$metadata$creator),
    created = as.character(Sys.Date())
  )

  metadat
}

#' Upload files to Dataverse
#'
#' This function upload a list files in a specific Dataverse repository.
#'
#' @param file_list Paths indicating the files to be uploaded to Dataverse.
#' @param key API token associated to the Dataverse account.
#' @param dataverse A character string specifying the Dataverse ID.
#' @param server A character string specifying a Dataverse server.
#'
#' @export
#'
#' @examples
upload_files <- function(file_list = NULL, key = NULL, dataverse = NULL, server = NULL) {
  dataverse_content <-
    dataverse::dataverse_contents(
      dataverse = dataverse,
      key = key,
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
      key = key,
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
publish_last_dataset <- function(key = NULL, dataverse = NULL, server = NULL) {
  dataverse_content <-
    dataverse::dataverse_contents(
      dataverse = dataverse,
      key = key,
      server = server
    )

  last_data <- dataverse_content[length(dataverse_content)][[1]]

  dataverse::publish_dataset(
    dataset = last_data,
    minor = FALSE,
    key = key,
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
upload_dataverse <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  dataverse <- pars$export_dataverse$dataverse_id
  key <- pars$export_dataverse$token
  server <- pars$export_dataverse$server

  prefixes <- c("aggregated__", "trips", "catch")
  files_names <-
    purrr::map(prefixes, ~ cloud_object_name(
      prefix = paste(pars$export$file_prefix, .x, sep = "_"),
      version = "latest",
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options
    )) %>%
    do.call('rbind',.) %>%
    as.character() %>%
    unique()

  purrr::map(files_names,
             download_cloud_file,
             provider = pars$public_storage$google$key,
             options = pars$public_storage$google$options)

  aggregated_day <- readr::read_rds(grep('aggregated', list.files(),value=TRUE))$day

  # test passing stuff to rmarkdown document
  time_range <- paste(min(aggregated_day$date_bin_start,na.rm=TRUE),
                      max(aggregated_day$date_bin_start,na.rm=TRUE),sep="-")


  logger::log_info("Retrieving public data to release...")
  rmarkdown::render(input = system.file("export/DESCRIPTION.Rmd", package = "peskas.timor.data.pipeline"),
                    params = list(
                      time_range = time_range))

  logger::log_info("Generating metadata...")
  metadat <- generate_metadata(pars)

  rds_files <- grep(".rds",files_names, value=TRUE)
  release_files_names <- c(rds_files, system.file("export/DESCRIPTION.html",
                                                  package = "peskas.timor.data.pipeline"))

  dataverse::initiate_sword_dataset(
    dataverse = dataverse,
    server=server,
    key=key,
    body = metadat
  )

  logger::log_info("Exporting files...")
  upload_files(
    file_list = release_files_names,
    key = key,
    dataverse = dataverse,
    server = server
  )

  file.remove(release_files_names)

  #logger::log_info("Publishing data...")
  #publish_last_dataset(
  #  key = key,
  #  dataverse = dataverse,
  #  server = server
  #)
}
