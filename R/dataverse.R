#' Export file to Dataverse
#'
#' This function exports a file to a specified Dataverse repository.
#'
#' @param token API token associated to Dataverse account.
#' @param dataverse A character string specifying a Dataverse name.
#' @param upfile File to be exported.
#' @param metadat A list of metadata in Dublin Core format, see
#' \url{https://www.dublincore.org/specifications/dublin-core/dcmi-terms/}.
#' @param server A character string specifying a Dataverse server.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' metadat <- list(
#'   title = paste("My dataset", format(Sys.time(), "%Y%m%d_%H%M")),
#'   creator = "A big dinosaur",
#'   description = "This is an amazing dataset...",
#'   language = "English"
#' )
#'
#' upl_files <- "/Users/..../my_data.csv"
#'
#' export_dataverse(
#'   token = "a3k2XXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
#'   dataverse = "my_dataverse",
#'   upfile = file,
#'   metadat = metadat
#' )
#' }
#'
export_dataverse <- function(token, dataverse, upfile, metadat, server) {
  ds <- dataverse::initiate_sword_dataset(
    dataverse = dataverse,
    body = metadat,
    key = token,
    server = server
  )

  contents <- dataverse::dataverse_contents(dataverse = dataverse, server = server, key = token)
  last_content <- contents[[length(contents)]]
  mydatadoi <- paste0(
    last_content$protocol, ":",
    last_content$authority, "/",
    last_content$identifier
  )

  params <- list(
    `persistentId` = mydatadoi,
    `release` = "yes"
  )

  files <- list(
    `file` = httr::upload_file(upfile),
    `jsonData` = jsonlite::toJSON(metadat, auto_unbox = TRUE)
  )

  httr::POST(
    url = paste0("https://", server, "/api/datasets/:persistentId/add"),
    httr::add_headers(`X-Dataverse-key` = token),
    query = params,
    body = files
  )
}


# publish a dataverse
publish_dataverse <- function(token, dataverse, server) {
  url <- paste0("https://", server, "/api/dataverses/", dataverse, "/actions/:publish")
  res <- httr::POST(
    url = url,
    httr::add_headers(`X-Dataverse-key` = token)
  )
  res
}

# get metadata information from landings
get_metadata_info <- function(pars) {
  landings <- get_merged_landings(pars)
  extent <- paste(min(landings$date), max(landings$date), sep = "-")

  landings_metadata <- list(extent = extent)
  landings_metadata
}

# generate a list of metadata information to append to data upload
generate_metadata <- function() {
  pars <- read_config()
  landings_metadata <- get_metadata_info(pars)

  metadat <- list(
    title = paste("Project test"),
    creator = "The cat",
    created = as.character(Sys.Date()),
    description = "This is an incredible dataset, full of incredible stuff and numbers",
    extent = as.character(landings_metadata$extent),
    language = "English"
  )

  metadat
}

# add files to the latest dataset created
upload_files <- function(file_list, token, dataverse, server) {
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


# publish the last dataset of a specific dataverse repository
release_last_dataset <- function(token, dataverse, server) {
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

# upload and publish data in dataverse
export_files <- function(token, dataverse, file_list, metadat, server) {
  pars <- read_config()

  metadat <- generate_metadata()

  dataverse::initiate_sword_dataset(
    dataverse = dataverse,
    body = metadat,
    key = token,
    server = server
  )

  file_list <- # get files from cloud

    upload_files(
      file_list,
      token,
      dataverse,
      server
    )


  release_last_dataset(token, dataverse, server)
}
