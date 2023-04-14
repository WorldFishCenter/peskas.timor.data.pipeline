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

  prefixes <- c("trips", "catch", "aggregated-month")
  files_names <-
    purrr::map(prefixes, ~ cloud_object_name(
      prefix = paste(pars$export$file_prefix, .x, sep = "_"),
      version = "latest",
      extension = "tsv",
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options
    )) %>%
    do.call("rbind", .) %>%
    as.character() %>%
    unique()

  logger::log_info("Retrieving public data to release...")
  purrr::map(files_names,
    download_cloud_file,
    provider = pars$public_storage$google$key,
    options = pars$public_storage$google$options
  )

  data_description <- generate_description()


  logger::log_info("Generating README...")
  rmarkdown::render(
    input = system.file("export/README.Rmd", package = "peskas.timor.data.pipeline")
  )

  logger::log_info("Generating metadata...")
  # metadat <- generate_metadata(pars, temp_coverage = data_description$time_range)

  new_names <- gsub("__[^>]+__", "", files_names)
  file.rename(from = files_names, to = new_names)

  release_files_names <- c(new_names, system.file("export/README.html",
    package = "peskas.timor.data.pipeline"
  ))
  release_files_names <- release_files_names[c(4, 3, 1, 2)]

  metadat <- httr::upload_file(system.file("export/dataset-fields.json", package = "peskas.timor.data.pipeline"))

  logger::log_info("Initializing dataset in Peskas dataverse...")
  dataverse::create_dataset(
    dataverse = dataverse,
    server = server,
    key = key,
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

  # Restrict files "on request"
  # dataverse_info <-  get_dataverses(dataverse = dataverse, key = key, server = server)

  # purrr::walk(dataverse_info$dataset_$files$id, restrict_files, key = key, server = server)
  # allow_requests(key = key, server = server,id = dataverse_info$dataset_$datasetId)

  Sys.sleep(60 * 20)
  logger::log_info("Publishing data...")
  publish_last_dataset(
    key = key,
    dataverse = dataverse,
    server = server
  )
}


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
#' @param pars The configuration file.
#' @param temp_coverage Temporal coverage of the data to upload.
#'
#' @return A list with metadata information
#' @export
#'
#' @examples
generate_metadata <- function(pars, temp_coverage = NULL) {
  metadat <- list(
    title = as.character(pars$export_dataverse$metadata$title),
    subject = as.character(pars$export_dataverse$metadata$subject),
    language = as.character(pars$export_dataverse$metadata$language),
    description = paste(
      as.character(pars$export_dataverse$metadata$description),
      "Period covered:", temp_coverage
    ),
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

  purrr::walk(file_list, purrr::slowly(dataverse::add_dataset_file,
    rate = purrr::rate_delay(60 * 2),
    quiet = FALSE
  ),
  dataset = PID,
  key = key,
  description = "",
  server = server
  )
}



#' Delete a dataverse collection
#'
#' This function delete a specific Dataverse repository.
#'
#' @param key API token associated to the Dataverse account.
#' @param dataverse A character string specifying the Dataverse ID.
#' @param server A character string specifying a Dataverse server.
#'
#' @export
#'
delete_dataverse <- function(key, dataverse, server) {
  dataverse_content <-
    dataverse::dataverse_contents(
      dataverse = dataverse,
      key = key,
      server = server
    )

  # delete datasets inside dataverse collection
  clean_dataverse <- function(x) {
    id <-
      x %>%
      magrittr::extract2("id")
    delete_dataset(key = key, id = id, server = server)
  }

  purrr::walk(dataverse_content, clean_dataverse)

  url <- paste0("https://", server, "/api/dataverses/", dataverse)
  res <- httr::DELETE(
    url = url,
    httr::add_headers(`X-Dataverse-key` = key)
  )
  res
}


#' Delete a dataset of a dataverse collection
#'
#' This function delete a specific draft dataset.
#'
#' @param key API token associated to the Dataverse account.
#' @param id The dataset ID.
#' @param server A character string specifying a Dataverse server.
#'
#' @export
#'
delete_dataset <- function(key, id, server) {
  url <- paste0("https://", server, "/api/datasets/", id, "/versions/:draft")
  res <- httr::DELETE(
    url = url,
    httr::add_headers(`X-Dataverse-key` = key)
  )
  res
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


#' Generate data description
#'
#' This function generate the description of the data useful for the README file.
#'
#' @param ... unused; for backwards compatibility only
#'
#' @return A list with the description of each variable of each uploaded dataset.
#' @export
#'
generate_description <- function(...) {
  trips_dat <- readr::read_tsv(grep("timor_trips", list.files(), value = TRUE))
  catch_dat <- readr::read_tsv(grep("catch", list.files(), value = TRUE))
  aggr_dat <- readr::read_tsv(grep("aggregated", list.files(), value = TRUE))

  time_range <-
    paste(zoo::as.yearmon(min(trips_dat$landing_date, na.rm = TRUE)),
      zoo::as.yearmon(max(trips_dat$landing_date, na.rm = TRUE)),
      sep = " - "
    )

  trips_tab <-
    tibble::tibble(
      Variable = names(trips_dat),
      Description = c(
        "Unique identifier of a fishing trip",
        "Date of the fishing trip",
        "Number of taxa associated to the catch",
        "ISSCAAP codes identifying the taxa group",
        "Name of the landing station",
        "Timor municipality",
        "Ecological habitat where the capture was made",
        "Estimated value in USD of the catch",
        "Uninformative column - for backwards compatibility only",
        "The vessel type, can be motorised or unmotorised",
        "Gear type of the boat",
        "Size of the mesh (mm)",
        "Number of children involved in the fishing trip",
        "Number of adult males involved in the fishing trip",
        "Number of adult females involved in the fishing trip",
        "Number of gleaners",
        "Duration of the fishing trip in hours",
        "Start of the fishing trip",
        "End of the fishing trip"
      )
    )

  catch_tab <-
    tibble::tibble(
      Variable = names(catch_dat),
      Description = c(
        "Unique identifier of a fishing trip",
        "ISSCAAP codes identifying the taxa group (https://www.fao.org/fishery/en/collection/asfis/en)",
        "Destiny of the catch, whether for sale, self-sustaining or both",
        "Type of length used for catch measurement. TL: total length, FL: fork length, ShL: shell length, CW: carapace width",
        "Average length of the individuals in the catch (cm)",
        "Number of individuals in the catch",
        "Weight of the catch in grams",
        rep("Weight in grams", 7)
      )
    )

  aggr_tab <-
    tibble::tibble(
      Variable = names(aggr_dat),
      Description = c(
        "Period aggregated data refer to",
        "Number of landings",
        "Proportion of landings involving woman",
        "Number of PDS tracks recorded",
        "Number of trips matched with PDS tracks",
        "Proportion between total trips and matched PDS tracks",
        "Estimated revenue for each trip in USD",
        "Estimated weight for each trip in Kg",
        "Estimated number of landings per boat",
        "Estimated total revenue in USD",
        "Estimated total catch in Kg",
        "Recorded total revenue in USD",
        "Recorded total catch in Kg",
        "Price per Kg"
      )
    )

  list(
    trips_tab = trips_tab,
    catch_tab = catch_tab,
    aggr_tab = aggr_tab,
    time_range = time_range
  )
}


get_dataverses <- function(dataverse = dataverse, key = key, server = server) {
  dataverse_content <-
    dataverse::dataverse_contents(
      dataverse = dataverse,
      key = key,
      server = server
    )

  last_data <- list(
    dataverse_ = last_dataset <- dataverse_content[length(dataverse_content)][[1]],
    dataset_ = dataset_list <-
      dataverse::get_dataset(
        dataset = last_dataset,
        version = ":latest",
        key = key,
        server = server
      )
  )
  last_data
}


# restrict_files <- function(key = key, server = server, dat_id = NULL) {
#  url <- paste0("https://", server, "/api/files/", dat_id, "/restrict")
#  res <- httr::PUT(
#    url = url,
#    httr::add_headers(`X-Dataverse-key` = key),
#    body = "true"
#  )
#  res
# }

# allow_requests <- function(key = key, server = server, id) {
#  url <- paste0("https://", server, "/api/access/", id, "/allowAccessRequest")
#  res <- httr::PUT(
#    url = url,
#    httr::add_headers(`X-Dataverse-key` = key),
#    body = "true"
#  )
#  res
# }
