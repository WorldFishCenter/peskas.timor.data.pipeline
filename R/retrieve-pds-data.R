#' Download pelagic data system trips in as csv or json
#'
#' @param path String with path to file where API request should be saved
#' @param secret Access secret code for the account
#' @param token Access token for the account
#' @param start_date Start date from which download trips information
#' @param end_date Last date to download trips information
#' @param overwrite Will only overwrite existing path if TRUE.
#'
#' @return The file path
#' @export
#'
#' @examples
#' retrieve_pds_trips_data("test.csv")
#' file.remove("test.csv")
retrieve_pds_trips_data <- function(path, secret = NULL, token = NULL, start_date = NULL,
                                    end_date = NULL, overwrite = TRUE) {
  request_url <- paste("https://analytics.pelagicdata.com/api", token,
    "v1/trips", start_date, end_date,
    sep = "/"
  )

  # Temporarily download the data as a CSV
  temp_csv <- tempfile(fileext = ".csv")

  httr::GET(
    url = request_url,
    config = httr::add_headers("X-API-SECRET" = secret),
    query = list(
      deviceInfo = TRUE,
      withLastSeen = TRUE
    ),
    httr::write_disk(temp_csv, overwrite = TRUE)
  )

  # Read the CSV into a data frame
  trips_data <- readr::read_csv(temp_csv)

  # Write the data frame to a Parquet file
  arrow::write_parquet(
    x = trips_data,
    sink = path,
    compression = "lz4",
    compression_level = 12
  )

  # Remove the temporary CSV file
  unlink(temp_csv)

  # Return the Parquet file path
  path
}

#' Download pelagic data system trips
#'
#' @param prefix Name to be used as the prefix of the file names to be
#'   downloaded. Can be a path.
#' @param secret
#' @param token
#' @param start_date
#' @param end_date
#' @param append_version whether to append versioning information to the
#'   filename using [add_version].
#' @inheritParams retrieve_pds_trips_data
#'
#' @return A character vector with paths of the downloaded files
#' @export
#'
#' @examples
#' \dontrun{
#' retrieve_pds_trips(
#'   prefix = "my-trips", secret = "abcXXXXXX",
#'   token = "123XXXXXX", start_date = "2018-07-01",
#'   end_date = Sys.Date()
#' )
#' # To download in a different path
#' dir.create("my-data-dir")
#' retrieve_pds_trips(
#'   prefix = "my-data-dir/my-trips", secret = "abcXXXXXX",
#'   token = "123XXXXXX", start_date = "2018-07-01",
#'   end_date = Sys.Date()
#' )
#' }
retrieve_pds_trips <- function(prefix, secret = NULL, token = NULL, start_date = "2018-07-01",
                               end_date = Sys.Date(),
                               append_version = TRUE) {
  data_filename <- paste(prefix, sep = "_")

  if (isTRUE(append_version)) {
    parquet_filename <- add_version(data_filename, "parquet")
  } else {
    parquet_filename <- paste0(data_filename, ".parquet")
  }


  logger::log_info("Downloading trips parquet data as {parquet_filename}...")
  retrieve_pds_trips_data(
    parquet_filename, secret, token,
    start_date, end_date
  )
  logger::log_success("Trips parquet data download succeeded")

  parquet_filename
}


#' Download pelagic data system tracks in as parquet
#'
#' @param path String with path to file where API request should be saved
#' @param secret Access secret code for the account
#' @param token Access token for the account
#' @param id Trip unique id from which download tracks data
#' @param overwrite Will only overwrite existing path if TRUE.
#'
#' @return The file path
#' @export
#'
#' @examples
#' retrieve_pds_tracks_data("test.parquet")
#' file.remove("test.parquet")
retrieve_pds_tracks_data <- function(path, secret = NULL, token = NULL,
                                     id = NULL, overwrite = TRUE) {
  # Temporary file to save the CSV
  temp_csv <- tempfile(fileext = ".csv")

  request_url <- paste("https://analytics.pelagicdata.com/api", token,
                       "v1/trips", id, "points",
                       sep = "/"
  )

  httr::GET(
    url = request_url,
    config = httr::add_headers("X-API-SECRET" = secret),
    query = list(format = "csv"),
    httr::write_disk(temp_csv, overwrite = TRUE)
  )

  # Read the CSV file and write it as Parquet
  data <- readr::read_csv(temp_csv)
  arrow::write_parquet(
    x = data,
    sink = path,
    compression = "lz4",
    compression_level = 12
  )

  # Optionally, delete the temporary CSV file
  unlink(temp_csv)

  path
}

#' Download pelagic data system tracks
#'
#' @param prefix Name to be used as the prefix of the file names to be
#'   downloaded. Can be a path.
#' @param secret
#' @param token
#' @param id
#' @inheritParams retrieve_pds_tracks_data
#'
#' @return A character vector with paths of the downloaded files
#' @export
#'
#' @examples
#' \dontrun{
#' retrieve_pds_tracks(
#'   prefix = "my-tracks", secret = "abcXXXXXX",
#'   token = "123XXXXXX", id = "2327722"
#' )
#' # To download in a different path
#' dir.create("my-data-dir")
#' retrieve_pds_tracks(
#'   prefix = "my-data-dir/my-tracks", secret = "abcXXXXXX",
#'   token = "123XXXXXX", id = "2327722"
#' )
#' }
retrieve_pds_tracks <- function(prefix, secret, token, id) {
  data_filename <- paste(prefix, "raw", id, ".parquet", sep = "_")

  filenames <- character()

  logger::log_info("Downloading tracks parquet data as {data_filename}...")
  retrieve_pds_tracks_data(path = data_filename, secret, token, id)
  logger::log_success("Tracks parquet data download succeeded")
  filenames <- c(filenames, data_filename)

  filenames
}
