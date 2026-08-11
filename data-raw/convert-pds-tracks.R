# Convert Timor's PDS track objects to the cross-country layout
#
# Run once per environment. Migration Phase 7 moved PDS ingestion to
# `coasts::ingest_pds_trips()` / `coasts::ingest_pds_tracks()`, the same three
# calls Mozambique, Kenya and Zanzibar make. coasts names a track
#
#     pds-tracks_<trip_id>.parquet
#
# where Timor had accumulated 99,219 objects named
#
#     pds-track-<trip_id>__<YYYYMMDDHHMMSS>_<sha7>__.csv.gz
#
# and it recovers "which tracks do I already have" with a regex that only
# matches the former. Pointed at Timor's bucket unconverted it recognises **0**
# of 99,219 and re-fetches the entire history from the PDS API — measured, not
# feared. This script converts in place instead: one GCS read and one GCS write
# per object, **no PDS API traffic**, after which `ingest_pds_tracks()` fetches
# only genuinely new trips.
#
# It also seeds the descriptors artefact `describe_pds_tracks()` maintains from
# the old `pds-track_preprocessed__*.rds`, so that step stays incremental
# instead of re-reading every track.
#
# The old `.csv.gz` objects are left in place — no lifecycle policy exists on
# any Peskas bucket and nothing in this repo deletes objects. They are dead once
# this has run; deleting them is migration Phase 11's job, together with the
# other orphaned prefixes in AUDIT §2.
#
# Usage:
#     Rscript data-raw/convert-pds-tracks.R
#     R_CONFIG_ACTIVE=production Rscript data-raw/convert-pds-tracks.R
#
# Safe to re-run: objects whose parquet already exists are skipped, so an
# interrupted run resumes.

library(peskas.timor.data.pipeline)

logger::log_threshold(logger::INFO)
conf <- read_config()
pds_opts <- coasts::resolve_storage_opts(conf, "pds")
country_opts <- coasts::resolve_storage_opts(conf, "country")

old_prefix <- "pds-track-"
new_prefix <- conf$pds$pds_tracks$file_prefix # pds-tracks

logger::log_info("Listing {pds_opts$bucket}...")
stored <- coasts::cloud_object_names(
  prefix = "pds-track",
  provider = conf$pds_storage$google$key,
  options = pds_opts
)

csv_gz <- grep("\\.csv\\.gz$", stored, value = TRUE)
already <- grep("\\.parquet$", stored, value = TRUE)
logger::log_info("{length(csv_gz)} csv.gz, {length(already)} parquet")

# One trip can carry several versioned csv.gz objects; keep the first, which is
# what `get_tracks_descriptors()` always read (GCS lists lexicographically).
todo <- tibble::tibble(name = csv_gz) |>
  dplyr::mutate(trip = stringr::str_extract(.data$name, "[[:digit:]]+")) |>
  dplyr::group_by(.data$trip) |>
  dplyr::slice(1) |>
  dplyr::ungroup() |>
  dplyr::mutate(target = sprintf("%s_%s.parquet", new_prefix, .data$trip)) |>
  dplyr::filter(!.data$target %in% already)

logger::log_info("{nrow(todo)} tracks to convert")

convert_one <- function(name, target) {
  tryCatch(
    {
      coasts::download_cloud_file(
        name = name,
        provider = conf$pds_storage$google$key,
        options = pds_opts
      )
      # Guess types, exactly as `coasts::get_trip_points()` does when it parses
      # the same CSV off the API, so a converted track is indistinguishable
      # from a freshly ingested one.
      readr::read_csv(name, show_col_types = FALSE, progress = FALSE) |>
        arrow::write_parquet(
          sink = target,
          compression = "lz4",
          compression_level = 12
        )
      coasts::insistent_upload_cloud_file(
        file = target,
        provider = conf$pds_storage$google$key,
        options = pds_opts
      )
      unlink(c(name, target))
      TRUE
    },
    error = function(e) {
      logger::log_warn("{name}: {conditionMessage(e)}")
      unlink(c(name, target))
      FALSE
    }
  )
}

# Oversubscribed on purpose: every unit of work is four network round trips and
# almost no computation, so the useful worker count is set by latency, not by
# cores. Measured on 14 cores: 14 workers convert ~280 objects/min (6 h for
# 102k), 56 workers ~4x that.
options(parallelly.maxWorkers.localhost = 8)
future::plan(future::multisession, workers = 56)
ok <- furrr::future_map2_lgl(
  todo$name,
  todo$target,
  convert_one,
  .progress = TRUE
)
future::plan(future::sequential)

logger::log_success("Converted {sum(ok)} tracks, {sum(!ok)} failed")

# Seed the descriptors artefact from the old preprocessed-tracks rds.
descriptors_prefix <- conf$pds$pds_tracks$descriptors$file_prefix
seeded <- coasts::cloud_object_names(
  prefix = descriptors_prefix,
  provider = conf$storage$google$key,
  options = country_opts,
  extension = "parquet"
)

if (length(seeded) > 0) {
  logger::log_info("{descriptors_prefix} already exists, not seeding")
} else {
  old <- coasts::cloud_object_name(
    prefix = "pds-track_preprocessed",
    provider = conf$storage$google$key,
    extension = "rds",
    options = country_opts
  )
  logger::log_info("Seeding {descriptors_prefix} from {old}")
  coasts::download_cloud_file(
    name = old,
    provider = conf$storage$google$key,
    options = country_opts
  )
  descriptors <- readr::read_rds(old) |>
    dplyr::mutate(Trip = as.integer(.data$Trip), Boat = as.integer(.data$Boat))
  coasts::upload_parquet_to_cloud(
    data = descriptors,
    prefix = descriptors_prefix,
    provider = conf$storage$google$key,
    options = country_opts
  )
  unlink(old)
  logger::log_success("Seeded {nrow(descriptors)} trip descriptors")
}
