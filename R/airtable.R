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
#'
#' # Get all records for the table boats
#' air_get_records(table = "boats", base_id = "appjEVaN8kBNXAWak")
#'
#' # Get records only in a specified view
#' air_get_records("boats", "appjEVaN8kBNXAWak",
#'                 query = list(view = "Boats with wrong registration"))
#'
#' # Get only the first 5 records
#' air_get_records("boats", "appjEVaN8kBNXAWak", query = list(maxRecords = 5))
#'
air_get_records <- function(table, base_id,
                          api_key = Sys.getenv("AIRTABLE_KEY"),
                          query = list()){

  this_response <- httr::GET(url = "https://api.airtable.com",
                        path = c("v0", base_id, table),
                        httr::add_headers(Authorization = api_key),
                        query = query)

  this_content <- httr::content(this_response)
  multipage_response <- "offset" %in% names(this_content)

  # If it's a multipage response call this function recursivelly
  if (isTRUE(multipage_response)) {
    out <- list(records = list())
    # The Airtable API limit queries to 5 per second, this delay ensures we stay
    # within these limits
    Sys.sleep(1/4)
    query$offset <- this_content$offset
    out$records <- c(this_content$records,
                     air_get_records(table, base_id, api_key, query)$records)
  } else {
    out <- this_content
  }
  out
}

#' Format Airtable records to a data frame (tibble)
#'
#' @param records List of records from an airtable table
#'
#' @return a tibble
#' @export
#'
#' @examples
air_records_to_tibble <- function(records){
  # Determine the maximum length of a record
  max_field_length <- purrr::map_dfr(.x = records$records,
                                     .f = field_length) %>%
    purrr::map(max, na.rm = TRUE)
  # Convert each record to a data frame and put them together by row
  purrr::map_dfr(.x = records$records,
                 .f = record_to_data_frame,
                 max_field_length)
}

# Iterate over all fiealds in a record to determine its length
field_length <- function(this_record){
  purrr::map(this_record$fields, length)
}

# Iterate over each record and convert it to an one-row data frame. Use the
# field length information to determine whether the field should be nested
record_to_data_frame <- function(this_record, max_field_length){
  # Records contain multiple fields
  from_fields <- purrr::imap(.x = this_record$fields,
                             .f = extract_value,
                             max_field_length) %>%
    tibble::as_tibble()
  # But record also contain information not visible as fields
  from_others <- purrr::discard(this_record, ~ length(.) > 1) %>%
    tibble::as_tibble()

  dplyr::bind_cols(from_others, from_fields)
}

# If the maximum length of fields is 1 just get the value as a vector, otherwise
# as a list that can be nested in the tibble
extract_value <- function(x, y, max_field_length){
  if (max_field_length[y][[1]] > 1){
    list(c(unlist(x)))
  } else {
    unlist(x)
  }
}
