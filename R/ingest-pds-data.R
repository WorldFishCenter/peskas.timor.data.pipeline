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
    dplyr::mutate(n_fishermen = .data$fisher_number_child + .data$fisher_number_man + .data$fisher_number_woman) %>%
    tidyr::unnest(.data$landing_catch, keep_empty = T) %>%
    tidyr::unnest(.data$length_frequency, keep_empty = T) %>%
    dplyr::mutate(length = ifelse(.data$individuals == 0, NA_real_, .data$length)) %>%
    dplyr::group_by(.data$landing_id, .data$landing_date) %>%
    dplyr::arrange(dplyr::desc(.data$weight), .by_group = TRUE) %>%
    dplyr::summarise(
      gear_type = dplyr::first(.data$gear_type),
      region = dplyr::first(.data$reporting_region),
      trip = dplyr::first(.data$tracker_trip_id),
      duration = dplyr::first(.data$trip_duration),
      n_fishermen = dplyr::first(.data$n_fishermen),
      landing_value = dplyr::first(.data$landing_value),
      catch_taxon = dplyr::first(.data$catch_taxon),
      weight = sum(.data$weight, na.rm = TRUE) / 1000,
      length = mean(.data$length, na.rm = TRUE)
    ) %>%
    dplyr::mutate(remove_label = dplyr::case_when(!.data$catch_taxon == "0" & .data$weight == 0
    ~ "remove", TRUE ~ "keep")) %>%
    dplyr::filter(.data$remove_label == "keep") %>%
    dplyr::select(-.data$remove_label) %>%
    dplyr::mutate(
      CPE = (.data$weight / .data$n_fishermen) / .data$duration,
      RPE = (.data$landing_value / .data$n_fishermen) / .data$duration
    ) %>%
    dplyr::ungroup()

  logger::log_info("Opening shapefiles ...")
  timor_nation <- system.file("report/timor_shapefiles/tls_admbnda_adm0_who_ocha_20200911.shp",
    package = "peskas.timor.data.pipeline"
  ) %>%
    sf::st_read()

  timor_regions <- system.file("report/timor_shapefiles/tls_admbnda_adm1_who_ocha_20200911.shp",
    package = "peskas.timor.data.pipeline"
  ) %>%
    sf::st_read()

  merged_trips_ids <-
    merged_trips %>%
    magrittr::extract2("trip") %>%
    unique()

  tracks_ids <-
    tracks %>%
    dplyr::rename(trip = .data$Trip) %>%
    dplyr::filter(.data$trip %in% merged_trips_ids)

  # Produce png map
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
    ggplot2::geom_point(tracks_grid,
      mapping = ggplot2::aes(x = .data$Lng, y = .data$Lat, color = .data$trips),
      size = 0.01, alpha = 0.5
    ) +
    ggplot2::geom_sf(data = timor_nation, size = 0.4, color = "#963b00", fill = "white") +
    ggplot2::geom_sf(data = timor_regions, size = 0.1, color = "black", fill = "grey", linetype = 2, alpha = 0.1) +
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
      breaks = c(2, 7200),
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
      month_date = lubridate::floor_date(.data$landing_date, unit = "month"),
      month_date = as.Date(.data$month_date, tz = "Asia/Dili"),
      gear_type = stringr::str_to_sentence(.data$gear_type),
      region_cpe = round(mean(.data$CPE, na.rm = TRUE), 2),
      region_rpe = round(mean(.data$RPE, na.rm = TRUE), 2)
    ) %>%
    dplyr::group_by(.data$cell, .data$month_date, .data$gear_type, .data$catch_taxon) %>%
    dplyr::summarise(
      region = dplyr::first(.data$region),
      Lat = stats::median(.data$Lat),
      Lng = stats::median(.data$Lng),
      # weight = sum(.data$weight, na.rm = T),
      # trips = dplyr::n(),
      # trips_log = log(.data$trips + 1),
      region_cpe = dplyr::first(.data$region_cpe),
      region_rpe = dplyr::first(.data$region_rpe),
      length = mean(.data$length, na.rm = TRUE),
      CPE = round(mean(.data$CPE, na.rm = TRUE), 2),
      RPE = round(mean(.data$RPE, na.rm = TRUE), 2),
      CPE_log = round(mean(log(.data$CPE + 1), na.rm = TRUE), 2),
      RPE_log = round(mean(log(.data$RPE + 1), na.rm = TRUE), 2)
    ) %>%
    convert_taxa_names(pars) %>%
    dplyr::filter(!is.na(.data$catch_taxon)) %>%
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




#' Convert taxa codes to common names
#'
#' @param data A dataframe with taxa codes under a column named "catch_taxon"
#' @param pars The config file
#'
#' @return A dataframe with taxa common names
#' @export
#'
convert_taxa_names <- function(data, pars) {
  catch_types <-
    peskas.timor.data.pipeline::get_preprocessed_metadata(pars)$catch_types %>%
    dplyr::filter(!.data$catch_name_en %in% c("Herring", "Unknown", "Surgeonfish", "Bannerfish", "No catch")) %>%
    dplyr::select(
      catch_taxon = .data$interagency_code,
      "Common name" = .data$catch_name_en
    ) %>%
    dplyr::mutate("Common name" = dplyr::case_when(
      catch_taxon == "RAX" ~ "Short mackerel",
      catch_taxon == "CGX" ~ "Jacks/Trevally",
      catch_taxon == "CLP" ~ "Sardines",
      catch_taxon == "TUN" ~ "Tuna/Bonito",
      catch_taxon == "SNA" ~ "Snapper",
      TRUE ~ .data$`Common name`
    ))
  data %>%
    dplyr::left_join(catch_types, by = "catch_taxon") %>%
    dplyr::mutate(fish_group = dplyr::case_when(
      catch_taxon %in% c("COZ") ~ "Molluscs",
      catch_taxon %in% c("PEZ") ~ "Shrimps",
      catch_taxon %in% c("MZZ") ~ "Unknown",
      catch_taxon %in% c("SLV", "CRA") ~ "Crustaceans",
      catch_taxon %in% c("OCZ", "IAX") ~ "Cephalopods",
      catch_taxon %in% c("SKH", "SRX") ~ "Sharks and rays",
      catch_taxon %in% c("SNA", "GPX", "PWT", "SUR", "GRX", "MUI", "BGX") ~ "Large demersals",
      catch_taxon %in% c("CGX", "TUN", "BEN", "LWX", "BAR", "SFA", "CBA", "DOX", "ECN", "DOS") ~ "Large pelagics",
      catch_taxon %in% c("YDX", "SPI", "EMP", "SUR", "TRI", "MOJ", "WRA", "MOO", "BWH", "LGE", "MOB", "MHL", "GOX", "THO", "IHX", "APO", "IHX", "PUX", "DRZ") ~ "Small demersals",
      catch_taxon %in% c("RAX", "SDX", "CJX", "CLP", "GZP", "FLY", "KYX", "CLP", "MUL", "DSF", "MIL", "THF") ~ "Small pelagics",
      TRUE ~ NA_character_
    )) %>%
    dplyr::select(-.data$catch_taxon) %>%
    dplyr::rename(catch_taxon = .data$`Common name`)
}

#' Ingest a Kepler.gl map
#'
#' This function use the python library Kepler.gl \url{https://docs.kepler.gl/docs/keplergl-jupyter} to generate and upload a
#' map of PDS tracks around Timor. It uses [reticulate::import_from_path] to load
#' and run the python script `kepler_mapper.py`.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#' threshold for the logging infrastructure. See [logger::log_levels] for more
#' details.
#'
#' @return Nothing. This function upload to GCS.
#' @export
#'
ingest_kepler_tracks <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  logger::log_info("Getting PDS tracks...")

  pars <- read_config()
  tracks <- get_full_tracks(pars)

  counts <-
    tracks %>%
    dplyr::filter(!.data$Lat < -15) %>%
    dplyr::mutate(
      Lat = round(.data$Lat, 2),
      Lng = round(.data$Lng, 2)
    ) %>%
    dplyr::group_by(.data$Lat, .data$Lng) %>%
    dplyr::count() %>%
    dplyr::rename("GPS tracks" = .data$n)

  timor_nation <- system.file("report/timor_shapefiles/tls_admbnda_adm0_who_ocha_20200911.shp",
    package = "peskas.timor.data.pipeline"
  ) %>%
    sf::st_read() %>%
    dplyr::mutate(area = sf::st_area(.data$geometry))

  coordinates_sf <-
    sf::st_as_sf(counts[1:2],
      coords = c("Lng", "Lat"),
      crs = sf::st_crs(timor_nation)
    )

  logger::log_info("Dropping on land PDS tracks")
  points <-
    coordinates_sf %>%
    dplyr::mutate(on_land = lengths(sf::st_within(coordinates_sf, timor_nation))) %>%
    dplyr::mutate(
      Lng = sf::st_coordinates(.data$geometry)[, 1],
      Lat = sf::st_coordinates(.data$geometry)[, 2]
    ) %>%
    sf::st_drop_geometry() %>%
    dplyr::filter(-.data$on_land == 0) %>%
    dplyr::select(-.data$on_land) %>%
    dplyr::left_join(counts)

  readr::write_csv(points, "kepler_tracks.csv")

  logger::log_info("Generating Kepler map")
  kepler_mapper("kepler_tracks.csv")

  logger::log_info("Uploading kepler_pds_map.html to cloud sorage")
  upload_cloud_file(
    file = "kepler_pds_map.html",
    provider = pars$public_storage$google$key,
    options = pars$public_storage$google$options
  )
}

#' Generate a Kepler.gl map
#'
#' This function is a R wrapper of `kepler_mapper.py`, a python script function
#' aimed to elaborate produce a self-contained map (in html) using the
#' Kepler.gl python library \url{https://docs.kepler.gl/docs/keplergl-jupyter}.
#'
#' @param data_path Data to add to map.
#'
#' @return A self-contained map in html.
#' @export
#'
kepler_mapper <- function(data_path = NULL) {
  python_path <- system.file(package = "peskas.timor.data.pipeline")
  kepler_mapper_py <- reticulate::import_from_path(
    module = "kepler_mapper",
    path = python_path
  )
  py_function <- kepler_mapper_py$kepler_map
  py_function(data_path)
}
