#' Retrieve Data from Kobotoolbox API
#'
#' This function retrieves survey data from Kobotoolbox API for a specific asset.
#' It supports pagination and handles both JSON and XML formats.
#'
#' @param prefix name to be used as the prefix of the file names to be
#'   downloaded. Can be a path.
#' @param assetid The asset ID of the Kobotoolbox form.
#' @param url The URL of Kobotoolbox (default is "eu.kobotoolbox.org").
#' @param uname Username for Kobotoolbox account.
#' @param pwd Password for Kobotoolbox account.
#' @param encoding Encoding to be used for data retrieval (default is "UTF-8").
#' @param format Format of the data to retrieve, either "json" or "xml" (default is "json").
#'
#' @return A list containing all retrieved survey results.
#' @keywords ingestion
#' @details
#' The function uses pagination to retrieve large datasets, with a limit of 30,000 records per request.
#' It continues to fetch data until all records are retrieved or an error occurs.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' kobo_data <- get_kobo_data(
#'   preifx = "my-survey",
#'   assetid = "your_asset_id",
#'   uname = "your_username",
#'   pwd = "your_password"
#' )
#' }
get_kobo_data <- function(prefix = NULL, assetid = NULL, url = "eu.kobotoolbox.org", uname = NULL, pwd = NULL, encoding = "UTF-8", format = "json") {
  if (!is.character(url)) stop("URL entered is not a string")
  if (!is.character(uname)) stop("uname (username) entered is not a string")
  if (!is.character(pwd)) stop("pwd (password) entered is not a string")
  if (!is.character(assetid)) stop("assetid entered is not a string")
  if (is.null(url) | url == "") stop("URL empty")
  if (is.null(uname) | uname == "") stop("uname (username) empty")
  if (is.null(pwd) | pwd == "") stop("pwd (password) empty")
  if (is.null(assetid) | assetid == "") stop("assetid empty")
  if (!format %in% c("json", "xml")) stop("format must be either 'json' or 'xml'")

  base_url <- paste0("https://", url, "/api/v2/assets/", assetid, "/data.", format)

  message("Starting data retrieval from ", base_url)

  get_page <- function(url, limit = 30000, start = 0) {
    full_url <- paste0(url, "?limit=", limit, "&start=", start)

    message("Retrieving page starting at record ", start)

    respon.kpi <- tryCatch(
      expr = {
        httr2::request(full_url) |>
          httr2::req_auth_basic(uname, pwd) |>
          httr2::req_perform()
      },
      error = function(x) {
        message("Error on page starting at record ", start, ". Please try again or check the input parameters.")
        return(NULL)
      }
    )

    if (!is.null(respon.kpi)) {
      content_type <- httr2::resp_content_type(respon.kpi)

      if (grepl("json", content_type)) {
        message("Successfully retrieved JSON data starting at record ", start)
        return(httr2::resp_body_json(respon.kpi, encoding = encoding))
      } else if (grepl("xml", content_type)) {
        message("Successfully retrieved XML data starting at record ", start)
        return(httr2::resp_body_string(respon.kpi, encoding = encoding))
      } else if (grepl("html", content_type)) {
        warning("Unexpected HTML response for start ", start, ". Unable to parse.")
        return(NULL)
      } else {
        warning("Unexpected content type: ", content_type, " for start ", start, ". Unable to parse.")
        return(NULL)
      }
    } else {
      return(NULL)
    }
  }

  all_results <- list()
  start <- 0
  limit <- 30000
  get_next <- TRUE

  while (get_next) {
    page_results <- get_page(base_url, limit, start)

    if (is.null(page_results)) {
      message("Error occurred. Stopping data retrieval.")
      break
    }

    new_results <- page_results$results
    all_results <- c(all_results, new_results)

    message("Total records retrieved so far: ", length(all_results))

    if (length(new_results) < limit) {
      message("Retrieved all available records.")
      get_next <- FALSE
    } else {
      start <- start + limit
    }
  }

  message("Data retrieval complete. Total records retrieved: ", length(all_results))

  # Check for unique submission IDs
  submission_ids <- sapply(all_results, function(x) x$`_id`)
  if (length(unique(submission_ids)) != length(all_results)) {
    warning("Number of unique submission IDs does not match the number of records. There may be duplicates.")
  }

  data_filename <- paste(prefix, "raw", sep = "_")
  csv_filename <- add_version(data_filename, "csv")

  logger::log_info("Converting json data to CSV as {csv_filename}...")
  survey_df <- purrr::map_dfr(all_results, flatten_row)
  readr::write_csv(survey_df, csv_filename)
  csv_filename
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
