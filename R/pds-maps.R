#' PDS-derived map products
#'
#' Everything in this file is Timor-only and downstream of PDS, not PDS
#' ingestion: migration Phase 7 deleted `ingest_pds_trips()`,
#' `ingest_pds_tracks()` and `preprocess_pds_trips()` in favour of
#' `coasts::ingest_pds_trips()`, `coasts::ingest_pds_tracks()` and
#' `coasts::preprocess_pds_tracks()`, which is how Mozambique, Kenya and
#' Zanzibar have always done it — none of them carries any PDS code.
#'
#' What is left are the two map products the portal path reads
#' (`indicators_gridded`, `tracks-map`) plus the Kepler map and the taxa-name
#' lookup they share with `format_public_data()`. Neither map function is wired
#' into a workflow: `indicators_gridded.rds` was last written 2024-07-27 and
#' `tracks-map.png` 2021-12-11, and `portal-indicators_grid.json` — regenerated
#' from that stale rds on every run — is one of the objects the portal
#' excludes. They are retained rather than ported to coasts' H3 output because
#' `export_files()` still reads both; the decision belongs to Phase 8's portal
#' gate.
#'
#' @name pds-maps
NULL




#' Ingest tracks data as a single file
#'
#' This function uploads two files: `data`, the complete tracks in a single rds
#' file and `trips`, a vector containing unique the trips from `data` useful to
#' take track of the synchronization status of `data`.
#'
#' @param conf The configuration file.
#' @param data An rds file containing tracks data.
#' @param trips A vector of unique Trips from the argument `data`.
#'
#' @return No output. This function is used for it's side effects
#' @export
#'
ingest_complete_tracks <- function(conf, data = NULL, trips = NULL) {
  c(
    conf$pds$tracks$complete$file_prefix,
    paste(conf$pds$tracks$complete$file_prefix, "trips", sep = "_")
  ) %>%
    purrr::map_chr(add_version, extension = "rds") %T>%
    purrr::walk2(
      list(data, trips),
      ~ readr::write_rds(.y, .x, compress = "gz")
    ) %>%
    purrr::walk(
      coasts::upload_cloud_file,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
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

  conf <- read_config()

  logger::log_info("Retrieving PDS tracks")

  tracks <- get_sync_tracks(conf) %>%
    dplyr::filter(
      .data$Lng > 124.03 &
        .data$Lng < 127.29 &
        .data$Lat > -9.74 &
        .data$Lat < -7.98
    ) # exclude track points outside borders

  logger::log_info("Retrieving merged trips")

  merged_trips <-
    get_merged_trips(conf) %>%
    dplyr::filter(!is.na(.data$landing_id) & !is.na(.data$tracker_trip_id)) %>%
    dplyr::mutate(
      n_fishermen = .data$fisher_number_child +
        .data$fisher_number_man +
        .data$fisher_number_woman
    ) %>%
    tidyr::unnest(.data$landing_catch, keep_empty = T) %>%
    tidyr::unnest(.data$length_frequency, keep_empty = T) %>%
    dplyr::mutate(
      length = ifelse(.data$number_of_fish == 0, NA_real_, .data$length)
    ) %>%
    dplyr::group_by(.data$landing_id, .data$landing_date) %>%
    dplyr::arrange(dplyr::desc(.data$catch), .by_group = TRUE) %>%
    dplyr::summarise(
      gear_type = dplyr::first(.data$gear),
      region = dplyr::first(.data$municipality),
      trip = dplyr::first(.data$tracker_trip_id),
      duration = dplyr::first(.data$trip_length),
      n_fishermen = dplyr::first(.data$n_fishermen),
      landing_value = dplyr::first(.data$catch_price),
      catch_taxon = dplyr::first(.data$catch_taxon),
      weight = sum(.data$catch, na.rm = TRUE) / 1000,
      length = mean(.data$length, na.rm = TRUE)
    ) %>%
    dplyr::mutate(
      remove_label = dplyr::case_when(
        !.data$catch_taxon == "0" & .data$weight == 0 ~ "remove",
        TRUE ~ "keep"
      )
    ) %>%
    dplyr::filter(.data$remove_label == "keep") %>%
    dplyr::select(-.data$remove_label) %>%
    dplyr::mutate(
      CPE = (.data$weight / .data$n_fishermen) / .data$duration,
      RPE = (.data$landing_value / .data$n_fishermen) / .data$duration
    ) %>%
    dplyr::ungroup()

  logger::log_info("Opening shapefiles ...")
  timor_nation <- system.file(
    "report/timor_shapefiles/tls_admbnda_adm0_who_ocha_20200911.shp",
    package = "peskas.timor.data.pipeline"
  ) %>%
    sf::st_read()

  timor_regions <- system.file(
    "report/timor_shapefiles/tls_admbnda_adm1_who_ocha_20200911.shp",
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
      cell = paste(
        findInterval(.data$Lng, gridx),
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
    ggplot2::geom_point(
      tracks_grid,
      mapping = ggplot2::aes(x = .data$Lng, y = .data$Lat, color = .data$trips),
      size = 0.01,
      alpha = 0.5
    ) +
    ggplot2::geom_sf(
      data = timor_nation,
      size = 0.4,
      color = "#963b00",
      fill = "white"
    ) +
    ggplot2::geom_sf(
      data = timor_regions,
      size = 0.1,
      color = "black",
      fill = "grey",
      linetype = 2,
      alpha = 0.1
    ) +
    ggplot2::geom_sf_text(
      data = timor_regions,
      ggplot2::aes(label = .data$ADM1_EN),
      size = 2.8,
      fontface = "bold"
    ) +
    ggplot2::annotate(
      geom = "text",
      y = -8.16,
      x = 125.45,
      label = "Atauro",
      size = 2.8,
      fontface = "bold"
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
    paste(
      conf$pds$tracks$map$png$file_prefix,
      conf$pds$tracks$map$png$extension,
      sep = "."
    )

  ggplot2::ggsave(
    filename = map_filename,
    plot = map,
    width = 7,
    height = 4,
    bg = NULL,
    dpi = conf$pds$tracks$map$png$dpi_resolution
  )
  logger::log_info("Uploading {map_filename} to cloud sorage")
  coasts::upload_cloud_file(
    file = map_filename,
    provider = conf$public_storage$google$key,
    options = conf$public_storage$google$options
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
  gridx <- seq(
    min(tracks_ids_summarised$Lng),
    max(tracks_ids_summarised$Lng) + degx,
    by = degx
  )
  gridy <- seq(
    min(tracks_ids_summarised$Lat),
    max(tracks_ids_summarised$Lat) + degy,
    by = degy
  )

  logger::log_info("Generating indicators data frame...")

  tracks_grid <-
    landings_geo %>%
    dplyr::filter(!is.na(.data$Lat)) %>%
    dplyr::mutate(
      cell = paste(
        findInterval(.data$Lng, gridx),
        findInterval(.data$Lat, gridy),
        sep = ","
      )
    ) %>%
    dplyr::mutate(
      CPE = dplyr::case_when(
        is.infinite(.data$CPE) ~ NA_real_,
        TRUE ~ .data$CPE
      ),
      RPE = dplyr::case_when(
        is.infinite(.data$RPE) ~ NA_real_,
        TRUE ~ .data$RPE
      )
    ) %>%
    dplyr::group_by(.data$region) %>%
    dplyr::mutate(
      month_date = lubridate::floor_date(.data$landing_date, unit = "month"),
      month_date = as.Date(.data$month_date, tz = "Asia/Dili"),
      gear_type = stringr::str_to_sentence(.data$gear_type),
      region_cpe = round(mean(.data$CPE, na.rm = TRUE), 2),
      region_rpe = round(mean(.data$RPE, na.rm = TRUE), 2)
    ) %>%
    dplyr::group_by(
      .data$cell,
      .data$month_date,
      .data$gear_type,
      .data$catch_taxon
    ) %>%
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
    convert_taxa_names(conf) %>%
    dplyr::filter(!is.na(.data$catch_taxon)) %>%
    dplyr::ungroup()

  map_grid_name <-
    paste(conf$pds$tracks$map$map_grid$file_prefix) %>%
    add_version(extension = conf$pds$tracks$map$map_grid$extension)

  readr::write_rds(tracks_grid, map_grid_name)

  logger::log_info("Uploading {map_grid_name} to cloud sorage")
  coasts::upload_cloud_file(
    file = map_grid_name,
    provider = conf$public_storage$google$key,
    options = conf$public_storage$google$options
  )
}


#' Convert taxa codes to common names
#'
#' @param data A dataframe with taxa codes under a column named "catch_taxon"
#' @param conf The config file
#'
#' @return A dataframe with taxa common names
#' @export
#'
convert_taxa_names <- function(data, conf) {
  catch_types <-
    peskas.timor.data.pipeline::get_preprocessed_sheets(conf)$catch_types %>%
    dplyr::filter(
      !.data$catch_name_en %in%
        c("Herring", "Unknown", "Surgeonfish", "Bannerfish", "No catch")
    ) %>%
    dplyr::select(
      catch_taxon = .data$interagency_code,
      "Common name" = .data$catch_name_en
    ) %>%
    dplyr::mutate(
      "Common name" = dplyr::case_when(
        catch_taxon == "RAX" ~ "Short mackerel",
        catch_taxon == "CGX" ~ "Jacks/Trevally",
        catch_taxon == "CLP" ~ "Sardines",
        catch_taxon == "TUN" ~ "Tuna/Bonito",
        catch_taxon == "SNA" ~ "Snapper",
        TRUE ~ .data$`Common name`
      )
    )
  data %>%
    dplyr::left_join(catch_types, by = "catch_taxon") %>%
    dplyr::mutate(
      fish_group = dplyr::case_when(
        catch_taxon %in% c("COZ") ~ "Molluscs",
        catch_taxon %in% c("PEZ") ~ "Shrimps",
        catch_taxon %in% c("MZZ") ~ "Unknown",
        catch_taxon %in% c("SLV", "CRA") ~ "Crustaceans",
        catch_taxon %in% c("OCZ", "IAX") ~ "Cephalopods",
        catch_taxon %in% c("SKH", "SRX") ~ "Sharks and rays",
        catch_taxon %in%
          c(
            "SNA",
            "GPX",
            "PWT",
            "SUR",
            "GRX",
            "MUI",
            "BGX"
          ) ~ "Large demersals",
        catch_taxon %in%
          c(
            "CGX",
            "TUN",
            "BEN",
            "LWX",
            "BAR",
            "SFA",
            "CBA",
            "DOX",
            "ECN",
            "DOS"
          ) ~ "Large pelagics",
        catch_taxon %in%
          c(
            "YDX",
            "SPI",
            "EMP",
            "SUR",
            "TRI",
            "MOJ",
            "WRA",
            "MOO",
            "BWH",
            "LGE",
            "MOB",
            "MHL",
            "GOX",
            "THO",
            "IHX",
            "APO",
            "IHX",
            "PUX",
            "DRZ"
          ) ~ "Small demersals",
        catch_taxon %in%
          c(
            "RAX",
            "SDX",
            "CJX",
            "CLP",
            "GZP",
            "FLY",
            "KYX",
            "CLP",
            "MUL",
            "DSF",
            "MIL",
            "THF"
          ) ~ "Small pelagics",
        TRUE ~ NA_character_
      )
    ) %>%
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

  conf <- read_config()
  tracks <- get_full_tracks(conf)

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

  timor_nation <- system.file(
    "report/timor_shapefiles/tls_admbnda_adm0_who_ocha_20200911.shp",
    package = "peskas.timor.data.pipeline"
  ) %>%
    sf::st_read() %>%
    dplyr::mutate(area = sf::st_area(.data$geometry))

  coordinates_sf <-
    sf::st_as_sf(
      counts[1:2],
      coords = c("Lng", "Lat"),
      crs = sf::st_crs(timor_nation)
    )

  logger::log_info("Dropping on land PDS tracks")
  points <-
    coordinates_sf %>%
    dplyr::mutate(
      on_land = lengths(sf::st_within(coordinates_sf, timor_nation))
    ) %>%
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
  coasts::upload_cloud_file(
    file = "kepler_pds_map.html",
    provider = conf$public_storage$google$key,
    options = conf$public_storage$google$options
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

#' Extract Timor Geographical Boundaries
#'
#' This function extracts the geographical boundaries of Timor, including national boundaries,
#' administrative regions, and special cases for Atauro and Dili. It uses shapefiles stored within
#' the 'peskas.timor.data.pipeline' package to perform this extraction. Logging is incorporated to
#' inform the user of the process status and any potential threshold-based logging details.
#'
#' @param log_threshold The logging threshold level as defined by the `logger` package. This parameter
#'        allows the user to set the verbosity of logging output. The default level is `logger::DEBUG`.
#'
#' @return An `sf` object containing the geographical boundaries of Timor's regions, including adjustments
#'         for specific areas like Atauro and Dili. The returned object includes region names and their
#'         corresponding geometries.
#'
#' @export
#'
#' @examples
#' # Assuming 'peskas.timor.data.pipeline' package is installed and loaded
#' timor_boundaries <- get_timor_boundaries()
#' plot(timor_boundaries$geometry)
#'
get_timor_boundaries <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  logger::log_info("Extracting Timor shape boundaries...")

  timor_nation <- sf::read_sf(system.file(
    "report/timor_shapefiles/tls_admbnda_adm0_who_ocha_20200911.shp",
    package = "peskas.timor.data.pipeline"
  ))
  timor_atauro <-
    sf::read_sf(system.file(
      "report/timor_shapefiles/tls_admbnda_adm2_who_ocha_20200911.shp",
      package = "peskas.timor.data.pipeline"
    )) %>%
    dplyr::filter(.data$ADM2_EN == "Atauro") %>%
    dplyr::rename(region = .data$ADM2_EN) %>%
    dplyr::select(-c("ADM0_EN", "ADM0_PCODE"))

  timor_dili <-
    sf::read_sf(system.file(
      "report/timor_shapefiles/tls_admbnda_adm2_who_ocha_20200911.shp",
      package = "peskas.timor.data.pipeline"
    )) %>%
    dplyr::rename(region = .data$ADM1_EN) %>%
    dplyr::filter(.data$region == "Dili" & !.data$ADM2_EN == "Atauro") %>%
    dplyr::summarise(region = "Dili")

  timor_region <-
    sf::read_sf(system.file(
      "report/timor_shapefiles/tls_admbnda_adm1_who_ocha_20200911.shp",
      package = "peskas.timor.data.pipeline"
    )) %>%
    dplyr::rename(region = .data$ADM1_EN) %>%
    dplyr::mutate(
      region = dplyr::case_when(
        .data$region == "Laut\u00E9m" ~ "Lautem",
        .data$region == "Liqui\u00E7\u00E1" ~ "Liquica",
        .data$region == "Oecussi" ~ "Oecusse",
        TRUE ~ .data$region
      )
    ) %>%
    dplyr::filter(!.data$region %in% c("Ermera", "Aileu", "Atauro", "Dili")) %>%
    dplyr::bind_rows(timor_atauro) %>%
    dplyr::bind_rows(timor_dili) %>%
    dplyr::select(.data$region, .data$geometry)
  timor_region
}
