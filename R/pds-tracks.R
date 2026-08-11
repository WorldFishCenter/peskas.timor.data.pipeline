#' Extract per-trip descriptors from PDS tracks
#'
#' The one PDS product `coasts` has no equivalent for, and the reason Timor
#' still has a track-preprocessing step at all after migration Phase 7 moved
#' ingestion to `coasts::*`. `coasts::preprocess_pds_tracks()` aggregates track
#' points into 500 m and 1 km spatial grid cells for the shared effort
#' products; [validate_pds_trips()] needs something different — one row per
#' trip carrying the geometry and signal-quality measures its alerts are built
#' on:
#'
#' * `start_end_distance` — metres between the first and last point of a trip,
#'   which is how a one-way trip is spotted (alert 12).
#' * `outliers_proportion` — percentage of points faster than 30 m/s, and
#'   `timetrace_dispersion` — the standard deviation of the interval between
#'   consecutive points (together, alert 13).
#' * `start_lat` / `start_lng` / `end_lat` / `end_lng`, which
#'   [merge_consecutive_trips()] needs to decide whether two trips of the same
#'   boat are really one.
#'
#' The step is incremental: only trips absent from the previous descriptors
#' artefact are read. Both workflow steps run, coasts' for the grids and this
#' one for the descriptors.
#'
#' The parameters needed are:
#'
#' ```
#' pds:
#'   pds_tracks:
#'     file_prefix:
#'     descriptors:
#'       file_prefix:
#'       version:
#' pds_storage:
#'   google:
#'     key:
#'     options:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#' @keywords workflow
#' @return no outputs. This function is used for it's side effects
#' @export
#'
describe_pds_tracks <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()
  country_opts <- coasts::resolve_storage_opts(conf, "country")

  pds_trips <- get_pds_trips(conf)

  logger::log_info("Listing stored tracks...")
  tracks_list <- coasts::cloud_object_names(
    prefix = conf$pds$pds_tracks$file_prefix,
    provider = conf$pds_storage$google$key,
    options = coasts::resolve_storage_opts(conf, "pds"),
    extension = "parquet"
  )

  described <- tryCatch(
    get_track_descriptors(conf),
    error = function(e) {
      logger::log_info("No existing descriptors artefact found")
      NULL
    }
  )

  trips_to_describe <- setdiff(unique(pds_trips$Trip), described$Trip)
  logger::log_info(
    "{NROW(described)} trips already described, \\
     {length(trips_to_describe)} to read"
  )

  if (length(trips_to_describe) == 0) {
    logger::log_info("No new tracks to describe")
    return(invisible())
  }

  future::plan(future::multisession)
  new_descriptors <- furrr::future_map_dfr(
    trips_to_describe,
    get_tracks_descriptors,
    conf,
    tracks_list,
    .progress = TRUE
  )
  future::plan(future::sequential)

  descriptors <- dplyr::bind_rows(described, new_descriptors)

  coasts::upload_parquet_to_cloud(
    data = descriptors,
    prefix = conf$pds$pds_tracks$descriptors$file_prefix,
    provider = conf$storage$google$key,
    options = country_opts
  )
  logger::log_success("Wrote descriptors for {nrow(descriptors)} trips")
}

#' Extract summaries from one pds track
#'
#' The unit of work [describe_pds_tracks()] maps over. See there for what the
#' descriptors are and why they exist.
#'
#' @param Trip A pds trip id.
#' @param tracks_list Character vector of track object names in the PDS bucket,
#'   as returned by [coasts::cloud_object_names()].
#' @param conf The configuration file.
#'
#' @return A one-row tibble of descriptors, or a zero-row tibble if the trip has
#'   no stored track.
#' @export
#'
get_tracks_descriptors <- function(Trip, conf, tracks_list) {
  # Nothing may escape this function as a bare condition. `furrr` maps it with
  # `conf` as an argument, and R deparses the call when it prints a deferred
  # warning or an unhandled error — which puts the **whole resolved config**,
  # service-account private key and all, into the job log. That is the leak
  # Phase 3 fixed in `read_config()`, arriving through a different door: the
  # `Community` column of a PDS track carries an unquoted comma blob, so
  # `read_csv()` warned on essentially every one of them. Muffle at the source.
  withCallingHandlers(
    describe_one_track(Trip, conf, tracks_list),
    warning = function(w) {
      logger::log_debug("Trip {Trip}: {conditionMessage(w)}")
      invokeRestart("muffleWarning")
    }
  )
}

describe_one_track <- function(Trip, conf, tracks_list) {
  # Since Phase 7 the track object name is exactly `<prefix>_<trip>.parquet`,
  # with no version string, so this is an equality test rather than the
  # substring match the versioned `pds-track-<trip>__<version>__.csv.gz` names
  # needed — and it cannot mistake trip 1234 for trip 123.
  track_file <- sprintf(
    "%s_%s.parquet",
    conf$pds$pds_tracks$file_prefix,
    as.character(Trip)
  )

  # A trip whose track never made it into the bucket — the PDS API can retire a
  # trip, or refuse its points — contributes no descriptors rather than aborting
  # the whole parallel map. `validate_pds_trips()` left-joins these, so the trip
  # survives with NA quality columns and is retried next run.
  if (!track_file %in% tracks_list) {
    logger::log_warn("No stored track for trip {Trip}")
    return(tibble::tibble())
  }

  track <- tryCatch(
    coasts::download_cloud_file(
      name = track_file,
      provider = conf$pds_storage$google$key,
      options = coasts::resolve_storage_opts(conf, "pds")
    ) %>%
      arrow::read_parquet(),
    error = function(e) {
      logger::log_warn("Trip {Trip}: {conditionMessage(e)}")
      NULL
    }
  )

  if (is.null(track) || nrow(track) == 0) {
    return(tibble::tibble())
  }

  file.remove(track_file)

  tibble::tibble(
    Trip = Trip,
    Boat = unique(track$Boat),
    start_end_distance = geosphere::distm(
      c(track[1, ]$Lng, track[1, ]$Lat),
      c(track[nrow(track), ]$Lng, track[nrow(track), ]$Lat),
      fun = geosphere::distGeo
    )[1],
    outliers_proportion = dplyr::filter(track, .data$`Speed (M/S)` > 30) %>%
      nrow() /
      nrow(track) *
      100,
    timetrace_dispersion = stats::sd(diff(track$Time)),
    start_lat = dplyr::first(track$Lat),
    start_lng = dplyr::first(track$Lng),
    end_lat = dplyr::last(track$Lat),
    end_lng = dplyr::last(track$Lng)
  )
}

# Download the per-trip track descriptors written by describe_pds_tracks().
get_track_descriptors <- function(conf) {
  coasts::download_parquet_from_cloud(
    prefix = conf$pds$pds_tracks$descriptors$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country"),
    version = conf$pds$pds_tracks$descriptors$version
  )
}
