#' Ingest Pelagic Data System trips data
#'
#' Downloads Pelagic Data System (pds) trips information and uploads it to cloud
#' storage services.
#'
#' This function downloads trips information from Pelagic Data System devices.
#' Afterwards it uploads this information to cloud services. File names used
#' contain a versioning string that includes the date-time and, if available,
#' the first 7 digits of the git commit sha. This is acomplished
#' using [add_version()]
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' pds:
#'   trips:
#'     token:
#'     secret:
#'     file_prefix:
#' pds_storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#'
#' @keywords workflow
#'
#' @return No output. This funcrion is used for it's side effects
#' @export
#'
ingest_pds_trips <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  file_list <- retrieve_pds_trips(
    prefix = pars$pds$trips$file_prefix,
    secret = pars$pds$trips$secret,
    token = pars$pds$trips$token
  )

  logger::log_info("Uploading files to cloud...")
  # Iterate over multiple storage providers if there are more than one
  purrr::map(pars$storage, ~ upload_cloud_file(file_list, .$key, .$options))

  logger::log_success("File upload succeded")
}

#' Ingest Pelagic Data System tracks data
#'
#' Downloads Pelagic Data System (pds) trips information and uploads it to cloud
#' storage services.
#'
#' The function  downloads and uploads only tracks data that are not yet
#' stored in the bucket.
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' pds:
#'   tracks:
#'     token:
#'     secret:
#'     file_prefix:
#' pds_storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#'
#' @keywords workflow
#'
#' @return No output. This funcrion is used for it's side effects
#' @export
#' @importFrom rlang .data
#'
ingest_pds_tracks <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  pds_trips_csv <-
    cloud_object_name(
      prefix = pars$pds$trips$file_prefix,
      provider = pars$storage$google$key,
      extension = "csv",
      options = pars$storage$google$options
    )
  logger::log_info("Retrieving {pds_trips_csv}")
  # get trips data frame
  pds_trips_mat <- download_cloud_file(
    name = pds_trips_csv,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  # extract unique trip identifiers
  trips_ID <- readr::read_csv(
    pds_trips_mat,
    col_types = readr::cols_only(Trip = readr::col_character())
  ) %>%
    magrittr::extract2("Trip") %>%
    unique()

  if (isTRUE(pars$pds$tracks$compress)) ext <- "csv.gz" else ext <- "csv"

  # list id tracks already in bucket
  file_list_id <- cloud_object_name(
    prefix = pars$pds$tracks$file_prefix,
    provider = pars$pds_storage$google$key,
    extension = ext,
    options = pars$pds_storage$google$options
  ) %>%
    stringr::str_extract("[[:digit:]]+") %>%
    as.character()

  process_track <- function(id, pars) {
    path <- paste0(pars$pds$tracks$file_prefix, "-", id) %>%
      add_version(extension = "csv")
    on.exit(file.remove(path))

    retrieve_pds_tracks_data(path,
      secret = pars$pds$trips$secret,
      token = pars$pds$trips$token,
      id = id
    )

    if (isTRUE(pars$pds$tracks$compress)) {
      logger::log_info("Compressing file...")
      csv_path <- path
      path <- paste0(path, ".gz")
      readr::read_csv(csv_path,
        col_types = readr::cols(.default = readr::col_character())
      ) %>%
        readr::write_csv(path)
      on.exit(file.remove(csv_path, path))
    }

    logger::log_info("Uploading {path} to cloud...")
    # Iterate over multiple storage providers if there are more than one
    purrr::map(pars$pds_storage, ~ purrr::walk(
      .x = path,
      .f = ~ insistent_upload_cloud_file(
        file = .,
        provider = pars$pds_storage$google$key,
        options = pars$pds_storage$google$options
      )
    ))
    logger::log_success("File upload succeded")
  }

  tracks_to_download <- trips_ID[!(trips_ID %in% file_list_id)]
  if (isTRUE(pars$pds$tracks$multisession$parallel)) {
    future::plan(future::multisession,
      workers = pars$pds$tracks$multisession$n_sessions
    )
  }
  furrr::future_walk(tracks_to_download, process_track, pars, .progress = TRUE)

  # Store names of pds-tracks (useful for map generation)
  tracks_names <-
    googleCloudStorageR::gcs_list_objects(pars$pds_storage$google$options$bucket) %>%
    dplyr::select(.data$name) %>%
    dplyr::mutate(Trip = stringr::str_extract(.data$name, "[[:digit:]]+"))

  tracks_names_filename <-
    pars$pds$tracks$bucket_content$file_prefix %>%
    add_version(extension = "rds")

  readr::write_rds(
    x = tracks_names,
    file = tracks_names_filename
  )

  logger::log_info("Uploading {tracks_names_filename} to cloud sorage")
  upload_cloud_file(
    file = tracks_names_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}

#' Insistent version of `upload_cloud_file()`
#'
#' Just like `upload_cloud_file()`, this function takes a vector of tracks files
#' as argument and upload them to the cloud. The function uses
#' [purrr::insistently] in order to continue to upload files despite stale OAuth
#' token.
#'
#' @param delay he time interval to suspend execution for, in seconds.
#' @param ... Inputs to `upload_cloud_file()`
#'
#' @return No output. This function is used for it's side effects
#' @export
#'
insistent_upload_cloud_file <- function(..., delay = 3) {
  purrr::insistently(upload_cloud_file,
    rate = purrr::rate_backoff(
      pause_cap = 60 * 5,
      max_times = 10
    ),
    quiet = F
  )(...)
  Sys.sleep(delay)
}

#' Insistent version of `download_cloud_file()`
#'
#' Just like `download_cloud_file()`, this function takes a vector of tracks files
#' as argument and download them to the cloud. The function uses
#' [purrr::insistently] in order to continue to download files despite stale OAuth
#' token.
#'
#' @param delay he time interval to suspend execution for, in seconds.
#' @param ... Inputs to `download_cloud_file()`
#'
#' @return No output. This function is used for it's side effects
#' @export
#'
insistent_download_cloud_file <- function(..., delay = 3) {
  purrr::insistently(download_cloud_file,
    rate = purrr::rate_backoff(
      pause_cap = 60 * 5,
      max_times = 10
    ),
    quiet = F
  )(...)
  Sys.sleep(delay)
}



#' Ingest tracks data as a single file
#'
#' This function uploads two files: `data`, the complete tracks in a single rds
#' file and `trips`, a vector containing unique the trips from `data` useful to
#' take track of the synchronization status of `data`.
#'
#' @param pars The configuration file.
#' @param data An rds file containing tracks data.
#' @param trips A vector of unique Trips from the argument `data`.
#'
#' @return No output. This function is used for it's side effects
#' @export
#'
ingest_complete_tracks <- function(pars, data = NULL, trips = NULL) {
  c(
    pars$pds$tracks$complete$file_prefix,
    paste(pars$pds$tracks$complete$file_prefix, "trips", sep = "_")
  ) %>%
    purrr::map_chr(add_version, extension = "rds") %T>%
    purrr::walk2(
      list(data, trips),
      ~ readr::write_rds(.y, .x, compress = "gz")
    ) %>%
    purrr::walk(upload_cloud_file,
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    )
}


#' Generate and ingest Timor maps
#'
#' This function downloads pds tracks coordinates, generates a png image
#' showing the map of Timor divided by municipalities including the tracks paths,
#' and upload it to cloud storage. It also upload the data frame splitted by grids
#' to produce leaflet maps in the portal.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#' threshold for the logging infrastructure. See [logger::log_levels] for more
#' details
#' @return No output. This function is used for it's side effects.
#' @export
#'
ingest_pds_map <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  logger::log_info("Retrieving PDS tracks")

  tracks <- get_sync_tracks(pars) %>%
    dplyr::filter(.data$Lng > 124.03 & .data$Lng < 127.29 & .data$Lat > -9.74 & .data$ Lat < -7.98) # exclude track points outside borders

  logger::log_info("Retrieving merged trips")

  merged_trips <-
    get_merged_trips(pars) %>%
    dplyr::filter(!is.na(.data$landing_id) & !is.na(.data$tracker_trip_id)) %>%
    tidyr::unnest(.data$landing_catch, keep_empty = T) %>%
    tidyr::unnest(.data$length_frequency, keep_empty = T) %>%
    dplyr::group_by(.data$landing_id) %>%
    dplyr::mutate(n_fishermen = .data$fisher_number_child + .data$fisher_number_man + .data$fisher_number_woman) %>%
    dplyr::summarise(
      gear_type = dplyr::first(.data$gear_type),
      region = dplyr::first(.data$reporting_region),
      trip = dplyr::first(.data$tracker_trip_id),
      duration = dplyr::first(.data$trip_duration),
      n_fishermen = dplyr::first(.data$n_fishermen),
      landing_value = dplyr::first(.data$landing_value),
      weight = sum(.data$weight, na.rm = TRUE) / 1000
    ) %>%
    dplyr::mutate(
      CPE = (.data$weight / .data$n_fishermen) / .data$duration,
      RPE = (.data$landing_value / .data$n_fishermen) / .data$duration
    )

  logger::log_info("Opening shapefiles ...")
  timor_nation <- system.file("report/timor_shapefiles/tls_admbnda_adm0_who_ocha_20200911.shp",
    package = "peskas.timor.data.pipeline"
  ) %>%
    sf::st_read()

  timor_regions <- system.file("report/timor_shapefiles/tls_admbnda_adm1_who_ocha_20200911.shp",
    package = "peskas.timor.data.pipeline"
  ) %>%
    sf::st_read()

  ### produce png map
  merged_trips_ids <-
    merged_trips %>%
    magrittr::extract2("trip") %>%
    unique()

  tracks_ids <-
    tracks %>%
    dplyr::rename(trip = .data$Trip) %>%
    dplyr::filter(.data$trip %in% merged_trips_ids)

  # Convert to grids to fill
  degx <- degy <- 0.001 # define grid size
  gridx <- seq(min(tracks_ids$Lng), max(tracks_ids$Lng) + degx, by = degx)
  gridy <- seq(min(tracks_ids$Lat), max(tracks_ids$Lat) + degy, by = degy)

  tracks_grid <-
    tracks_ids %>%
    dplyr::mutate(
      cell = paste(findInterval(.data$Lng, gridx),
        findInterval(.data$Lat, gridy),
        sep = ","
      )
    ) %>%
    dplyr::group_by(.data$cell) %>%
    dplyr::summarise(
      Lat = mean(.data$Lat),
      Lng = mean(.data$Lng),
      trips = dplyr::n()
    ) %>%
    dplyr::filter(.data$trips > 0)

  logger::log_info("Generating png file")

  map <-
    ggplot2::ggplot() +
    ggplot2::theme_void() +
    ggplot2::geom_sf(data = timor_nation, size = 0.4, color = "#963b00", fill = "white") +
    ggplot2::geom_sf(data = timor_regions, size = 0.1, color = "black", fill = "grey", linetype = 2, alpha = 0.1) +
    ggplot2::geom_point(tracks_grid,
      mapping = ggplot2::aes(x = .data$Lng, y = .data$Lat, color = .data$trips),
      size = 0.01, alpha = 0.5
    ) +
    ggplot2::geom_sf_text(
      data = timor_regions, ggplot2::aes(label = .data$ADM1_EN), size = 2.8,
      fontface = "bold"
    ) +
    ggplot2::annotate(
      geom = "text", y = -8.16, x = 125.45, label = "Atauro",
      size = 2.8, fontface = "bold"
    ) +
    ggplot2::scale_colour_viridis_c(
      begin = 0.1,
      trans = "log2",
      breaks = c(4, 7500),
      labels = c("Low fishing\nactivity", "High fishing\nactivity")
    ) +
    ggplot2::labs(
      x = "",
      y = "",
      fill = "",
      title = "",
      color = ""
    ) +
    ggplot2::coord_sf(
      xlim = c(124.0363, 127.2961),
      ylim = c(-9.511914, -8.139941)
    ) +
    ggplot2::theme(
      legend.position = "top",
      legend.key.height = ggplot2::unit(0.4, "cm"),
      legend.key.width = ggplot2::unit(1.5, "cm")
    )

  map_filename <-
    paste(pars$pds$tracks$map$png$file_prefix, pars$pds$tracks$map$png$extension, sep = ".")

  ggplot2::ggsave(
    filename = map_filename,
    plot = map,
    width = 7,
    height = 4,
    bg = NULL,
    dpi = pars$pds$tracks$map$png$dpi_resolution
  )
  logger::log_info("Uploading {map_filename} to cloud sorage")
  upload_cloud_file(
    file = map_filename,
    provider = pars$public_storage$google$key,
    options = pars$public_storage$google$options
  )

  ### produce indicators map grid

  tracks_ids_summarised <-
    tracks_ids %>%
    dplyr::group_by(.data$trip) %>%
    dplyr::summarise(
      Lat = stats::median(.data$Lat),
      Lng = stats::median(.data$Lng)
    )

  landings_geo <-
    merged_trips %>%
    dplyr::left_join(tracks_ids_summarised, by = "trip")

  degx <- degy <- 0.1 # define grid size (0.1 is 11.1 km)
  gridx <- seq(min(tracks_ids_summarised$Lng), max(tracks_ids_summarised$Lng) + degx, by = degx)
  gridy <- seq(min(tracks_ids_summarised$Lat), max(tracks_ids_summarised$Lat) + degy, by = degy)

  logger::log_info("Generating indicators data frame...")

  tracks_grid <-
    landings_geo %>%
    dplyr::filter(!is.na(.data$Lat)) %>%
    dplyr::mutate(
      cell = paste(findInterval(.data$Lng, gridx),
        findInterval(.data$Lat, gridy),
        sep = ","
      )
    ) %>%
    dplyr::mutate(
      CPE = dplyr::case_when(is.infinite(.data$CPE) ~ NA_real_, TRUE ~ .data$CPE),
      RPE = dplyr::case_when(is.infinite(.data$RPE) ~ NA_real_, TRUE ~ .data$RPE)
    ) %>%
    dplyr::group_by(.data$region) %>%
    dplyr::mutate(
      region_cpe = round(mean(.data$CPE, na.rm = TRUE), 2),
      region_rpe = round(mean(.data$RPE, na.rm = TRUE), 2)
    ) %>%
    dplyr::group_by(.data$cell) %>%
    dplyr::summarise(
      region = dplyr::first(.data$region),
      Lat = stats::median(.data$Lat),
      Lng = stats::median(.data$Lng),
      weight = sum(.data$weight, na.rm = T),
      trips = dplyr::n(),
      trips_log = log(.data$trips+1),
      region_cpe = dplyr::first(.data$region_cpe),
      region_rpe = dplyr::first(.data$region_rpe),
      CPE = round(stats::median(.data$CPE, na.rm = TRUE), 2),
      RPE = round(stats::median(.data$RPE, na.rm = TRUE), 2),
      CPE_log = round(stats::median(log(.data$CPE + 1), na.rm = TRUE), 2),
      RPE_log = round(stats::median(log(.data$RPE + 1), na.rm = TRUE), 2)
    ) %>%
    dplyr::ungroup()

  map_grid_name <-
    paste(pars$pds$tracks$map$map_grid$file_prefix) %>%
    add_version(extension = pars$pds$tracks$map$map_grid$extension)

  readr::write_rds(tracks_grid, map_grid_name)

  logger::log_info("Uploading {map_grid_name} to cloud sorage")
  upload_cloud_file(
    file = map_grid_name,
    provider = pars$public_storage$google$key,
    options = pars$public_storage$google$options
  )
}
