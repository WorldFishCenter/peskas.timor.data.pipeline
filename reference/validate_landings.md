# Validate landings

Downloads the weighted long catch table from cloud storage, runs every
validator in `R/validation-functions.R`, and publishes two things: the
validated landings artefacts, and the per-submission flags the
enumerators act on.

## Usage

``` r
validate_landings(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure. See
  [logger::log_levels](https://daroczig.github.io/logger/reference/log_levels.html)
  for more details

## Value

no outputs. This function is used for it's side effects

## Details

By default outlier identification uses the median absolute deviation
(MAD).

## Outputs

- `<surveys.landings.validated.file_prefix>__*.rds` — the **nested**
  artefact, one row per submission with the `landing_catch` list-column.
  This is the portal's input and its column names are load-bearing:
  [`format_public_data()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/format_public_data.md)
  reads `municipality`, `landing_site`, `propulsion_gear`,
  `trip_length`, `catch_preservation` and the `fisher_number_*` trio by
  name. Unchanged by migration Phase 5.

- `<...>_long__*.parquet` — the same content in the **flat long** shape,
  one row per (submission, catch, length bin), under the standard column
  names. Written for migration Phase 6's API export; nothing reads it
  yet.

- `<surveys.landings.validation.flags.file_prefix>__*.parquet` — a
  versioned snapshot of the flags.
  [`coasts::mdb_collection_push()`](https://rdrr.io/pkg/coasts/man/mdb_collection_push.html)
  replaces a collection wholesale, so this is the only history of what
  was flagged when.

## Flags sink

Flags go to the **shared** cross-country validation database
(`storage.mongodb.databases.validation`), one `surveys_flags-<asset_id>`
collection per live form plus the matching
`enumerators_stats-<asset_id>`. v1 is frozen and gets neither. This
replaced the Google Sheets `flags` tab in migration Phase 5.

Where a token is configured, the current KoBoToolbox validation status
of the already-flagged submissions is read first, so an approval an
enumerator entered by hand is preserved rather than overwritten. Writing
a status *back* to KoBoToolbox is
[`sync_validation_status()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/sync_validation_status.md),
which the recurring pipeline deliberately does not call.
