#' Select server from kobo
#'
#' Specifies the Host URL of the API to use
#'
#' @param api Either "kobo", "kobohr", "ona", or a custom (full) URL. API URLs
#'   are made available for KoBo Toolbox ("kobo",
#'   \url{https://kc.kobotoolbox.org/api/v1/}), KoBo Humanitarian Response
#'   ("kobohr", \url{https://kc.humanitarianresponse.info/api/v1/}), Ona ("ona",
#'   \url{https://api.ona.io/api/v1/}) and Unhcr ("unhcr",
#'   \url{https://kobocat.unhcr.org/api/v1/}) . For your own installation, or
#'   other installations using the same API but accessed at a different URL,
#'   enter the full URL.
#' @param version Wether using the API "v1" or "v2"
#'
#' @author Ananda Mahto
#'
#' @return An URL
#' @export
#'
#' @examples
#'
#' get_host_url("unhcr")
#' get_host_url("https://kobocat.unhcr.org/api/v1/")
get_host_url <- function(api, version = "v1") {
  if (api %in% c("kobo", "kobohr", "ona", "unhcr")) {
    if (version == "v1") {
      switch(api,
        kobo = "https://kc.kobotoolbox.org/api/v1",
        kobohr = "https://kc.humanitarianresponse.info/api/v1",
        ona = "https://api.ona.io/api/v1",
        unhcr = "https://kobocat.unhcr.org/api/v1"
      )
    } else if (version == "v2") {
      switch(api,
        kobo = "https://kc.kobotoolbox.org/api/v2",
        kobohr = "https://kobo.humanitarianresponse.info/api/v2"
      )
    }
  } else {
    api
  }
}

#' Download kobo data in as json using the v1 API
#'
#' @param path string with path to file where API request should be saved
#' @param id survey id. Usually a 6 digit number. See [this support
#'   page](https://support.kobotoolbox.org/pulling_data_into_powerbi.html) for
#'   an example on how this can be obtained
#' @param token access token for the account e.g. "Token XXXXXXX"
#' @param api
#' @param overwrite Will only overwrite existing path if TRUE.

#'
#' @inheritParams get_host_url
#'
#' @return The file path
#' @export
#'
#' @examples
#' \dontrun{
#' retrieve_survey_data("test.json", id = 753491, token = "XXX")
#' file.remove("test.json")
#' }
retrieve_survey_data <- function(path, id = NULL, token = NULL,
                                 api = "kobohr",
                                 overwrite = TRUE) {
  api_limit <- 30000

  request_url <- paste(get_host_url(api, "v1"),
    "data", id,
    sep = "/"
  )

  message(request_url)

  # Function to get a page of survey
  retrieve_survey_page <- function(start = 0) {
    message("Retrieving survey. Starting at record ", start)

    resp <- httr::RETRY(
      verb = "GET",
      url = request_url,
      config = httr::add_headers(Authorization = token),
      query = list(
        start = start,
        limit = api_limit
      ),
      times = 12,
      pause_cap = 180
    )

    if (!(resp$status_code %in% 200:299)) {
      stop("Unsuccessful response from server")
    }

    content <- httr::content(resp)
    if ("results" %in% names(content)) content <- content$results
    content
  }

  results <- list()
  get_next <- TRUE
  start <- 0

  while (get_next) {
    this_page <- retrieve_survey_page(start)
    results <- c(results, this_page)
    if (length(this_page) < api_limit) {
      get_next <- FALSE
    } else {
      start <- start + api_limit
    }
  }

  # Check that submissions are unique in case there is overlap in the pagination
  if (dplyr::n_distinct(purrr::map_dbl(results, ~ .$`_id`)) != length(results)) {
    stop("Number of submission ids not the same as number of records")
  }

  jsonlite::write_json(results, path, auto_unbox = TRUE)
  path
}

#' Download survey metadata
#'
#' @param id
#' @param token
#' @param api
#' @inheritParams retrieve_survey_data
#'
#' @return A list with survey metadata
#' @export
#'
#' @examples
#' \dontrun{
#' # It's only possible to donwload survey metadata with the account Token
#' retrieve_survey_metadata(753491, token = "123XXXXXX")
#' }
#'
retrieve_survey_metadata <- function(id = NULL, token = NULL, api = "kobohr") {
  # If the token is not provided is not possible to get the
  no_token <- length(token) == 0 | is.null(token)
  if (no_token) {
    stop("Token is required")
  }

  survey_basic_metadata <- retrieve_basic_survey_metadata(id, token, api)

  # the version 2 of the api returns much richer information about the surveys
  httr::GET(
    url = paste(get_host_url(api, version = "v2"), "assets",
      survey_basic_metadata$id_string,
      sep = "/"
    ),
    config = httr::add_headers(Authorization = token)
  ) %>%
    httr::content() %>%
    c(survey_basic_metadata)
}

# Help function to retrieve basic metadata from the v1 api - Same params as in
# retrieve_survey_metadata
retrieve_basic_survey_metadata <- function(id, token, api) {
  assets_v1_raw <- httr::GET(
    url = paste(get_host_url(api, version = "v1"), "data", sep = "/"),
    config = httr::add_headers(Authorization = token)
  )

  purrr::keep(httr::content(assets_v1_raw), ~ .$id == id)[[1]]
}


#' Download survey
#'
#' Download multiple survey artefacts (data and metadata) at once. Progress
#' through the function is tracked using the package *logger*.
#'
#' @param prefix name to be used as the prefix of the file names to be
#'   downloaded. Can be a path.
#' @param api
#' @param id
#' @param token
#' @param format Either "csv" or "json"
#' @param metadata whether to download metadata as well as data
#' @param append_version whether to append versioning information to the
#'   filename using [add_version].
#' @inheritParams retrieve_survey_data
#'
#' @return a character vector with paths of the downloaded files
#' @export
#'
#' @examples
#' \dontrun{
#' # It's only possible to donwload survey metadata with the account Token
#' retrieve_survey(
#'   prefix = "my-survey", api = "kobohr", id = 753491,
#'   token = "123XXXXXX"
#' )
#' # To download in a different path
#' dir.create("my-data-dir")
#' retrieve_survey(
#'   prefix = "my-data-dir/my-survey", api = "kobohr", id = 753491,
#'   token = "123XXXXXX"
#' )
#' }
retrieve_survey <- function(prefix, api, id, token, format = c("csv", "json"),
                            metadata = TRUE, append_version = TRUE) {
  metadata_filename <- paste(prefix, "metadata", sep = "_")
  data_filename <- paste(prefix, "raw", sep = "_")

  if (isTRUE(append_version)) {
    metadata_filename <- add_version(metadata_filename, "json")
    csv_filename <- add_version(data_filename, "csv")
    json_filename <- add_version(data_filename, "json")
  } else {
    metadata_filename <- paste0(metadata_filename, ".json")
    csv_filename <- paste0(data_filename, ".csv")
    json_filename <- paste0(data_filename, ".json")
  }

  filenames <- character()

  if (isTRUE(metadata)) {
    logger::log_info("Downloading survey metadata as {metadata_filename}...")
    retrieve_survey_metadata(id, token = token, api = api) %>%
      jsonlite::write_json(metadata_filename)
    logger::log_success("Metadata download succeeded")
    filenames <- c(filenames, metadata_filename)
  }

  logger::log_info("Downloading survey json data as {json_filename}...")
  retrieve_survey_data(json_filename, id, token, api)
  logger::log_success("Survey json data download succeeded")

  if ("csv" %in% format) {
    logger::log_info("Converting json data to CSV as {csv_filename}...")
    survey_json_to_csv(json_filename, csv_filename)
    filenames <- c(filenames, csv_filename)
  }

  if ("json" %in% format) {
    filenames <- c(filenames, json_filename)
  } else {
    logger::log_info("Removing temporary json {json_filename}...")
    file.remove(json_filename)
  }

  filenames
}

# Convert a Kobo survey in json format to a csv format that respects the
# conventions from the data that could have been downloaded using the csv
# request in the API v1
survey_json_to_csv <- function(json_path, csv_path) {
  survey <- jsonlite::read_json(json_path)
  survey_df <- purrr::map_dfr(survey, flatten_row)
  readr::write_csv(survey_df, csv_path)
  csv_path
}

flatten_row <- function(x) {
  x %>%
    # Each row is composed of several fields
    purrr::imap(flatten_field) %>%
    rlang::squash() %>%
    tibble::as_tibble()
}

flatten_field <- function(x, p) {
  # If the field is a simple vector do nothing but if the field is a list we
  # need more logic
  if (inherits(x, "list")) {
    if (length(x) > 0) {
      if (purrr::vec_depth(x) == 2) {
        # If the field-list has named elements is we just need to rename the list
        x <- list(x) %>%
          rlang::set_names(p) %>%
          unlist() %>%
          as.list()
      } else {
        # If the field-list is an "array" we need to iterate over its children
        x <- purrr::imap(x, rename_child, p = p)
      }
    }
  } else {
    if (is.null(x)) x <- NA
  }
  x
}

# Appends parent name or number to element
rename_child <- function(x, i, p) {
  if (length(x) == 0) {
    if (is.null(x)) x <- NA
    x <- list(x)
    x <- rlang::set_names(x, paste(p, i - 1, sep = "."))
  } else {
    if (inherits(i, "character")) {
      x <- rlang::set_names(x, paste(p, i, sep = "."))
    } else if (inherits(i, "integer")) {
      x <- rlang::set_names(x, paste(p, i - 1, names(x), sep = "."))
    }
  }
  x
}
