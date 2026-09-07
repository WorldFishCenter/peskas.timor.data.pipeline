#' Upload and publish a dataset on Dataverse
#'
#' This function upload and publish data on a specific Dataverse repository
#' including associated metadata information.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#'
#' @keywords workflow export
#' @export
#'
upload_dataverse <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  dataverse <- conf$export_dataverse$dataverse_id
  key <- conf$export_dataverse$token
  server <- conf$export_dataverse$server

  prefixes <- c("trips", "catch", "aggregated-month")
  files_names <-
    purrr::map(prefixes, ~ coasts::cloud_object_name(
      prefix = paste(conf$export$file_prefix, .x, sep = "_"),
      version = "latest",
      extension = "tsv",
      provider = conf$public_storage$google$key,
      options = conf$public_storage$google$options
    )) %>%
    do.call("rbind", .) %>%
    as.character() %>%
    unique()

  logger::log_info("Retrieving public data to release...")
  purrr::map(files_names,
    coasts::download_cloud_file,
    provider = conf$public_storage$google$key,
    options = conf$public_storage$google$options
  )

  data_description <- generate_description()


  logger::log_info("Generating README...")
  rmarkdown::render(
    input = system.file("export/README.Rmd", package = "peskas.timor.data.pipeline")
  )

  logger::log_info("Generating metadata...")

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


#' Upload files to Dataverse
#'
#' This function upload a list files in a specific Dataverse repository.
#'
#' @param file_list Paths indicating the files to be uploaded to Dataverse.
#' @param key API token associated to the Dataverse account.
#' @param dataverse A character string specifying the Dataverse ID.
#' @param server A character string specifying a Dataverse server.
#'
#' @keywords export
#' @export
#'
#' @examples
#' \dontrun{
#' upload_files(
#'   file_list = c("file1.tsv", "file2.tsv"),
#'   key = "my_key",
#'   dataverse = "my_dataverse",
#'   server = "dataverse.example.com"
#' )
#' }
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



#' Publish latest dataset created
#'
#' The function converts from "draft" to "public" the latest dataset uploaded
#' in a specific Dataverse repository.
#'
#' @inheritParams upload_files
#'
#' @keywords export
#' @export
#'
#' @examples
#' \dontrun{
#' publish_last_dataset(key = "my_key", dataverse = "my_dataverse", server = "dataverse.example.com")
#' }
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
#' @keywords export
#' @export
#'
generate_description <- function(...) {
  trips_dat <- readr::read_tsv(grep("timor_trips", list.files(), value = TRUE)) %>% dplyr::select(-.data$landing_catch)
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
        "Name of the landing site",
        "Timor municipality",
        "Ecological habitat where the capture was made",
        "Price in USD of the catch",
        "Gear used to propel a watercraft, can be motorised or unmotorised",
        "Tool or method used to catch fish",
        "The size of holes in fishing net (mm)",
        "Fuel used (L)",
        "Refers to the methods of preserving the catch inshore and offshore",
        "Number of children involved in the fishing trip",
        "Number of adult males involved in the fishing trip",
        "Number of adult females involved in the fishing trip",
        "Number of gleaners",
        "Duration of fishing, measured in time (hours)",
        "Start of the fishing trip",
        "End of the fishing trip",
        "Fisherman happiness (1 to 5)"
      ),
      "Aquatic Food Ontology ref." = c(
        "-",
        "-",
        "-",
        "-",
        "aqfo 00000030",
        "aqfo 00000034",
        "aqfo 00000023",
        "aqfo 00002202",
        "aqfo 00002351",
        "aqfo 00002220",
        "aqfo 00002340",
        "aqfo 00002352",
        "aqfo 00002207",
        "-",
        "-",
        "-",
        "-",
        "aqfo 00002011",
        "-",
        "-",
        "-"
      )
    )

  catch_tab <-
    tibble::tibble(
      Variable = names(catch_dat),
      Description = c(
        "Unique identifier of a fishing trip",
        "ISSCAAP codes identifying the taxa group (https://www.fao.org/fishery/en/collection/asfis/en)",
        "Refers to the action the fisher will take with the catch (e.g. for sale, self-sustaining or both)",
        "Type of length used for catch measurement. TL: total length, FL: fork length, ShL: shell length, CW: carapace width",
        "Average length of the fishes in the catch (cm)",
        "Refers to the number of individual fish in a catch",
        "Fish caught by fishing operations (gr)",
        rep("Content in grams", 7)
      ),
      "Aquatic Food Ontology ref." = c(
        "-",
        "-",
        "aqfo 00000040",
        "-",
        "-",
        "-",
        "aqfo 00002074",
        "aqfo 00002088",
        rep("-", 6)
      )
    )

  aggr_tab <-
    tibble::tibble(
      Variable = names(aggr_dat),
      Description = c(
        "Period aggregated data refer to",
        "Total number of landings in the period of reference",
        "Proportion of landings involving woman",
        "Mean fuel used (L)",
        "Number of PDS tracks recorded",
        "Number of trips matched with PDS tracks",
        "Proportion between total trips and matched PDS tracks",
        "Mean catch price from all fishing trips in the reference period (USD)",
        "Mean catch weight from all fishing trips in the reference period (kg)",
        "Mean number of landings per boat in the period of reference",
        "Total national revenue estimated in the reference period (USD)",
        "Total national catch estimated in the reference period (kg)",
        "Mean catch price per kg in the reference period (USD)",
        "Total revenue aggregated from landing sites (USD)",
        "Total catch aggregated from landing sites (kg)"
      ),
      "Aquatic Food Ontology ref." = c(
        "-",
        "-",
        "-",
        "aqfo 00002352",
        "-",
        "-",
        "-",
        "aqfo 00002202",
        "aqfo 00002088",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-"
      )
    )

  list(
    trips_tab = trips_tab,
    catch_tab = catch_tab,
    aggr_tab = aggr_tab,
    time_range = time_range
  )
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
