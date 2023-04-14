#' Get records from an Airtable table
#'
#' Downloads an Airtable table into a list of records
#'
#' Airtable API returns one page of records at a time. Each page will contain
#' `pageSize` (a query parameter) records, which is 100 by default. Internally,
#' this function will automatically download all records until `maxRecords` has
#' been reached.
#'
#' @param table name of the table (string)
#' @param base_id id of the Airtable base where the table is located
#' @param api_key API key for Airtable. By default it looks at the
#'   `AIRTABLE_KEY` environment variable.
#' @param query additional parameters for the airtable query like for example
#'   fields, view, pageSize, maxRecords, sort, filterByFormula, cellFormat,
#'   timeZone, userLocale, etc... See the airtable documentation for more
#'   details.
#'
#' @return a list with a results field containing all records
#' @seealso air_records_to_tibble
#' @export
#'
#' @examples
#' \dontrun{
#' # Get all records for the table boats
#' air_get_records(table = "boats", base_id = "appjEVaN8kBNXAWak")
#'
#' # Get records only in a specified view
#' air_get_records("boats", "appjEVaN8kBNXAWak",
#'   query = list(view = "Boats with wrong registration")
#' )
#'
#' # Get only the first 5 records
#' air_get_records("boats", "appjEVaN8kBNXAWak", query = list(maxRecords = 5))
#' }
#'
air_get_records <- function(table, base_id,
                            api_key = Sys.getenv("AIRTABLE_KEY"),
                            query = list()) {
  this_response <- httr::GET(
    url = "https://api.airtable.com",
    path = c("v0", base_id, table),
    httr::add_headers(Authorization = api_key),
    query = query
  )

  this_content <- httr::content(this_response)
  multipage_response <- "offset" %in% names(this_content)

  # If it's a multipage response call this function recursivelly
  if (isTRUE(multipage_response)) {
    out <- list(records = list())
    # The Airtable API limit queries to 5 per second, this delay ensures we stay
    # within these limits
    Sys.sleep(1 / 4)
    query$offset <- this_content$offset
    out$records <- c(
      this_content$records,
      air_get_records(table, base_id, api_key, query)$records
    )
  } else {
    out <- this_content
  }
  out
}

#' Format Airtable records to a data frame (tibble)
#'
#' Transforms a list of records obtained from the airtable api (for example
#' using `air_get_records`) into a data frame.
#'
#' Fields that have a only one item are stored as a vector while fields that
#' accept multiple items are stored as a nested list column
#'
#' @param records List of records from an airtable table
#'
#' @return a tibble
#' @export
#'
#' @examples
#' \dontrun{
#' # Get all records for the table boats
#' air_get_records(table = "boats", base_id = "appjEVaN8kBNXAWak") %>%
#'   air_record_to_tibble()
#' }
air_records_to_tibble <- function(records) {
  # Determine the maximum length of a record
  max_field_length <- purrr::map_dfr(
    .x = records$records,
    .f = field_length
  ) %>%
    purrr::map(max, na.rm = TRUE)
  # Convert each record to a data frame and put them together by row
  purrr::map_dfr(
    .x = records$records,
    .f = record_to_data_frame,
    max_field_length
  )
}

# Iterate over all fiealds in a record to determine its length
field_length <- function(this_record) {
  purrr::map(this_record$fields, length)
}

# Iterate over each record and convert it to an one-row data frame. Use the
# field length information to determine whether the field should be nested
record_to_data_frame <- function(this_record, max_field_length) {
  # Records contain multiple fields
  from_fields <- purrr::imap(
    .x = this_record$fields,
    .f = extract_value,
    max_field_length
  ) %>%
    purrr::compact() %>%
    tibble::as_tibble()
  # But record also contain information not visible as fields
  from_others <- purrr::discard(this_record, ~ length(.) > 1) %>%
    tibble::as_tibble()

  dplyr::bind_cols(from_others, from_fields)
}

# If the maximum length of fields is 1 just get the value as a vector, otherwise
# as a list that can be nested in the tibble
extract_value <- function(x, y, max_field_length) {
  if (max_field_length[y][[1]] > 1) {
    list(c(unlist(x)))
  } else {
    unlist(x)
  }
}

#' Format a data frame (tibble) as Airtable records
#'
#' Gets a data frame and converts it into a list that can be passed to
#' `air_upload_records()` to be uploaded to Airtable. This is required because
#' the Airtable API does not accept tabular data and has certain restrictions
#' about the format of the body request
#'
#' @param this_tibble A data frame with the fields to be uploaded to Airtable
#' @param id_fields Character vector with the name of the column storing the id
#' @param link_fields Character vector with the name of the columns that are
#'   link fields (fields that contain the id of another row's table)
#' @param max_records Records are grouped into batches of max_records. Defaults
#'   to 10 which is the current limit imposed by the Airtable API.
#'
#' @return a list with records
#' @export
#'
air_tibble_to_records <- function(this_tibble, id_fields = NULL, link_fields = NULL,
                                  max_records = 10) {
  # Convert data frame to list. It it's not a link field, unbox it
  as_list_air <- function(..., link_fields = NULL) {
    dots <- rlang::list2(...)

    if (!is.null(link_fields)) {
      purrr::imap(dots, function(x, name) {
        if (name %in% link_fields) {
          x
        } else {
          jsonlite::unbox(x)
        }
      })
    } else {
      purrr::map(dots, jsonlite::unbox)
    }
  }
  records <- this_tibble %>%
    dplyr::select(-!!id_fields) %>%
    purrr::pmap(as_list_air,
      link_fields = link_fields
    ) %>%
    purrr::map(~ list(fields = .))

  # Fill list with blocks
  n_partitions <- length(records) %/% max_records + 1
  body <- purrr::list_along(1:n_partitions)
  for (i in 1:length(records)) {
    this_block <- ((i - 1) %/% max_records) + 1
    this_index <- ifelse(i %% max_records == 0, max_records, i %% max_records)
    body[[this_block]]$records[[this_index]] <- records[[i]]
    if (!is.null(id_fields)) {
      body[[this_block]]$records[[this_index]]$id <- jsonlite::unbox(this_tibble[[id_fields]][i])
    }
  }

  body
}

#' Upload and create Airtable records to a base
#'
#' Takes a list created by `tibble_to_air_records()` and uploads it to Airtable.
#' If `request_type` is set to "update" the body list must have an id field for
#' each record. Each element of body must be up to 10 records long.
#'
#' @param body list with the records, as formatted by `tibble_to_air_records()`
#' @param table
#' @param base_id
#' @param api_key
#' @param request_type Whether to create a new record or update an existing one.
#'   If updating an existing one, an `id_field` needs to be specified when
#'   calling `tibble_to_air_records()`
#' @inheritParams air_get_records
#' @return List of request responses
#' @export
air_upload_records <- function(body, table, base_id,
                               api_key = Sys.getenv("AIRTABLE_KEY"),
                               request_type = c("create", "update")) {
  if (request_type[1] == "create") {
    curl_fun <- httr::POST
  } else if (request_type[1] == "update") {
    curl_fun <- httr::PATCH
  }

  post_request <- function(this_body) {
    Sys.sleep(1 / 4)
    response <- curl_fun(
      url = "https://api.airtable.com",
      path = c("v0", base_id, table),
      httr::add_headers(Authorization = api_key),
      body = jsonlite::toJSON(this_body),
      httr::content_type("application/json")
    )
    logger::log_info(
      "Uploading records. Status code: ",
      httr::status_code(response)
    )
    response
  }

  purrr::map(body, post_request)
}
