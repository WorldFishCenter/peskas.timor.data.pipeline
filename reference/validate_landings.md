# Validate landings

Downloads the weighted long catch table, runs every validator in
`R/validation-functions.R`, and publishes the validated landings and the
per-submission flags. Outlier identification uses the median absolute
deviation by default.

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

## Outputs

- `<...>_validated_long__*.parquet` — one row per (submission, catch,
  length bin) under the standard column names.

- `<surveys.landings.validation.flags.file_prefix>__*.parquet` — a
  versioned snapshot of the flags.
  [`coasts::mdb_collection_push()`](https://rdrr.io/pkg/coasts/man/mdb_collection_push.html)
  replaces a collection wholesale, so this is the only history of what
  was flagged when.

## Flags sink

Flags go to the shared cross-country validation database, one
`surveys_flags-<asset_id>` collection per live form plus the matching
`enumerators_stats-<asset_id>`; v1 is frozen and gets neither.

Where a token is configured the current KoBoToolbox validation status is
read first, so an approval entered by hand is preserved rather than
overwritten.
