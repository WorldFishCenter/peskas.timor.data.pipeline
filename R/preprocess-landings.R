#' Pre-process Timor Updated Landings Survey
#'
#' Downloads raw structured data from cloud storage services and pre-process
#' into a binary format that is easier to deal with in R.
#'
#'
#' This function downloads the landings data from a given version (specified in
#' the config file `conf.yml`.The parameters needed are:
#'
#' ```
#' surveys:
#'   landings:
#'     <version>:
#'       raw:
#'         file_prefix:
#'         version:
#' storage:
#'   google:
#'     key:
#'     options:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a threshold for the logging infrastructure. See [logger::log_levels] for more details
#' @keywords workflow
#' @return no outputs. This function is used for it's side effects
#' @export
#'
preprocess_updated_landings <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  conf <- read_config()

  landings_raw <- get_raw_landings(conf, "v3") %>%
    clean_updated_landings() %>%
    dplyr::select(-c(dplyr::contains("stock_photo")))

  logger::log_info("Nesting landings attachment fields")
  landings_nested_attachments <- pt_nest_attachments(landings_raw)

  logger::log_info("Nesting landings species fields")
  landngs_nested_species <-
    landings_nested_attachments %>%
    # fill with taxon code 0 when there is no catch
    dplyr::mutate(dplyr::across(dplyr::contains("species_group/species"), ~
      dplyr::case_when(
        .data$catch_outcome == "0" ~ "0",
        TRUE ~ .x
      ))) %>%
    pt_nest_species() %>%
    dplyr::mutate(species_group = purrr::map2(
      .data$species_group, .data$`group_conservation_trading/food_or_sale`,
      ~ dplyr::mutate(.x, food_or_sale = .y)
    )) %>%
    dplyr::select(-c(.data$`group_conservation_trading/food_or_sale`))



  preprocessed_filename <- paste(conf$surveys$landings_3$file_prefix, "preprocessed", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(
    x = landngs_nested_species,
    file = preprocessed_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  coasts::upload_cloud_file(
    file = preprocessed_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}


#' Pre-process Timor Landings Survey data (step 1)
#'
#' Downloads raw structured data from cloud storage services and pre-process
#' into a binary format that is easier to deal with in R.
#'
#' In order to not exceed CPU memory limits in Docker containers, the
#' preprocessing of raw landings data was splitted in two containers
#' (two separate jobs in GitHub actions), this function process the first half
#' of raw data, while the function `preprocess_landings_step_2` process the second
#' half.
#'
#' This function downloads the landings data from a given version (specified in
#' the config file `conf.yml`.The parameters needed are:
#'
#' ```
#' surveys:
#'   landings:
#'     <version>:
#'       raw:
#'         file_prefix:
#'         version:
#' storage:
#'   google:
#'     key:
#'     options:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a threshold for the logging infrastructure. See [logger::log_levels] for more details
#' @keywords workflow
#' @return no outputs. This function is used for it's side effects
#' @export
#'
preprocess_landings_step_1 <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  conf <- read_config()

  landings_raw <- get_raw_landings(conf, "v2")

  # split data
  half_data <- round(nrow(landings_raw) / 2, 0)
  landings_raw <- landings_raw[1:half_data, ]

  logger::log_info("Nesting landings attachment fields")
  landings_nested_attachments <- pt_nest_attachments(landings_raw)

  logger::log_info("Nesting landings species fields")
  landngs_nested_species <- pt_nest_species(landings_nested_attachments)

  preprocessed_filename <- paste(conf$surveys$landings_2$file_prefix, "step_1", "preprocessed", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(
    x = landngs_nested_species,
    file = preprocessed_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  coasts::upload_cloud_file(
    file = preprocessed_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}



#' Pre-process Timor Landings Survey data (step 2)
#'
#' Downloads raw structured data from cloud storage services and pre-process
#' into a binary format that is easier to deal with in R.
#'
#' In order to not exceed CPU memory limits in Docker containers, the
#' preprocessing of raw landings data was splitted in two containers
#' (two separate jobs in GitHub actions), this function process the second half
#' of raw data, while the function `preprocess_landings_step_1` process the first half.
#'
#' This function downloads the landings data from a given version (specified in
#' the config file `conf.yml`.The parameters needed are:
#'
#' ```
#' surveys:
#'   landings:
#'     <version>:
#'       raw:
#'         file_prefix:
#'         version:
#' storage:
#'   google:
#'     key:
#'     options:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a threshold for the logging infrastructure. See [logger::log_levels] for more details
#' @keywords workflow
#' @return no outputs. This function is used for it's side effects
#' @export
#'
preprocess_landings_step_2 <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  conf <- read_config()

  landings_raw <- get_raw_landings(conf, "v2")

  preprocessed_step_1 <- download_versioned_rds(
    prefix = paste(conf$surveys$landings_2$file_prefix, "step_1", "preprocessed", sep = "_"),
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    version = conf$surveys$landings_2$version$preprocess
  )

  # get ids of batch 2 to download and process
  batch_2_ids <- setdiff(landings_raw$`_id`, preprocessed_step_1$`_id`)

  landings_raw <-
    landings_raw %>%
    dplyr::filter(.data$`_id` %in% batch_2_ids)

  logger::log_info("Nesting landings attachment fields")
  landings_nested_attachments <- pt_nest_attachments(landings_raw)

  logger::log_info("Nesting landings species fields")
  landings_nested_species <- pt_nest_species(landings_nested_attachments)

  # bind processed batch 1 and 2
  preprocessed <-
    dplyr::bind_rows(
      preprocessed_step_1,
      landings_nested_species
    )

  preprocessed_filename <- paste(conf$surveys$landings_2$file_prefix, "preprocessed", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(
    x = preprocessed,
    file = preprocessed_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  coasts::upload_cloud_file(
    file = preprocessed_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}
