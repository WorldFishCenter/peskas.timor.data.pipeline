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
get_kobo_data <- function(
  prefix = NULL,
  assetid = NULL,
  url = "eu.kobotoolbox.org",
  uname = NULL,
  pwd = NULL,
  encoding = "UTF-8",
  format = "json",
  limit = 1000,
  since_id = NULL,
  retry_times = 3,
  progress = TRUE
) {
  if (!is.character(url)) {
    stop("URL entered is not a string")
  }
  if (!is.character(uname)) {
    stop("uname (username) entered is not a string")
  }
  if (!is.character(pwd)) {
    stop("pwd (password) entered is not a string")
  }
  if (!is.character(assetid)) {
    stop("assetid entered is not a string")
  }
  if (is.null(url) | url == "") {
    stop("URL empty")
  }
  if (is.null(uname) | uname == "") {
    stop("uname (username) empty")
  }
  if (is.null(pwd) | pwd == "") {
    stop("pwd (password) empty")
  }
  if (is.null(assetid) | assetid == "") {
    stop("assetid empty")
  }
  if (!format %in% c("json", "xml")) {
    stop("format must be either 'json' or 'xml'")
  }
  if (!is.numeric(limit) || limit < 1 || limit > 1000) {
    stop("`limit` must be a number between 1 and 1000.")
  }
  limit <- as.integer(limit)

  base_url <- paste0(
    "https://",
    url,
    "/api/v2/assets/",
    assetid,
    "/data.",
    format
  )

  if (progress) {
    message("Starting data retrieval from ", base_url)
  }

  # --- Page fetcher (adapted from v1) ---
  get_page <- function(page_url) {
    respon.kpi <- tryCatch(
      expr = {
        httr2::request(page_url) |>
          httr2::req_auth_basic(uname, pwd) |>
          httr2::req_retry(max_tries = retry_times) |>
          httr2::req_error(is_error = \(resp) FALSE) |>
          httr2::req_perform()
      },
      error = function(e) {
        warning("Request failed: ", conditionMessage(e))
        return(NULL)
      }
    )

    if (is.null(respon.kpi)) {
      return(NULL)
    }

    status <- httr2::resp_status(respon.kpi)
    if (status >= 400) {
      warning(
        "HTTP error ",
        status,
        " when fetching: ",
        page_url,
        "\nBody: ",
        tryCatch(httr2::resp_body_string(respon.kpi), error = function(e) {
          "(unable to read body)"
        })
      )
      return(NULL)
    }

    content_type <- httr2::resp_content_type(respon.kpi)

    if (grepl("json", content_type)) {
      return(httr2::resp_body_json(respon.kpi, encoding = encoding))
    } else if (grepl("xml", content_type)) {
      return(httr2::resp_body_string(respon.kpi, encoding = encoding))
    } else {
      warning("Unexpected content type: ", content_type)
      return(NULL)
    }
  }

  # --- Build initial URL ---
  initial_url <- paste0(base_url, "?limit=", limit, "&start=0")

  if (!is.null(since_id)) {
    query_json <- paste0('{"_id":{"$gte":', since_id, '}}')
    initial_url <- paste0(
      initial_url,
      "&query=",
      utils::URLencode(query_json, reserved = TRUE)
    )
  }

  # --- Pagination loop using `next` field ---
  all_results <- list()
  current_url <- initial_url
  page_num <- 1L

  repeat {
    if (progress) {
      message("Fetching page ", page_num, "...")
    }

    page_data <- get_page(current_url)

    if (is.null(page_data)) {
      warning("Failed to retrieve page ", page_num, ". Stopping.")
      break
    }

    new_results <- page_data$results
    if (is.null(new_results) || length(new_results) == 0) {
      if (progress) {
        message("No results on page ", page_num, ". Done.")
      }
      break
    }

    all_results <- c(all_results, new_results)

    if (progress) {
      message(
        "Page ",
        page_num,
        ": retrieved ",
        length(new_results),
        " records (total: ",
        length(all_results),
        " / ",
        if (!is.null(page_data$count)) page_data$count else "unknown",
        ")"
      )
    }

    next_url <- page_data$`next`
    if (is.null(next_url) || identical(next_url, "")) {
      if (progress) {
        message("No more pages. Retrieval complete.")
      }
      break
    }

    current_url <- next_url
    page_num <- page_num + 1L
  }

  if (progress) {
    message("Data retrieval complete. Total records: ", length(all_results))
  }

  # --- Duplicate check ---
  if (length(all_results) > 0) {
    submission_ids <- vapply(
      all_results,
      function(x) if (!is.null(x$`_id`)) x$`_id` else NA_integer_,
      integer(1)
    )
    n_unique <- length(unique(submission_ids[!is.na(submission_ids)]))
    if (n_unique != length(all_results)) {
      warning(
        "Found ",
        length(all_results) - n_unique,
        " duplicate submission IDs out of ",
        length(all_results),
        " records."
      )
    }
  }

  # --- CSV export (original v2 logic) ---
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
    if (is.null(x)) {
      x <- NA
    }
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
