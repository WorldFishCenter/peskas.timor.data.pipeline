# Validate Pelagic Data System trips

Reads the trips parquet written by
[`coasts::ingest_pds_trips()`](https://rdrr.io/pkg/coasts/man/ingest_pds_trips.html)
and the per-trip descriptors written by
[`describe_pds_tracks()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/describe_pds_tracks.md),
merges trips that are really one, and flags anomalous duration, distance
and signal quality.

## Usage

``` r
validate_pds_trips(log_threshold = logger::DEBUG)
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

Kept as a Timor function in migration Phase 7 — `coasts` has no
consecutive-trip merging and no distance or outlier logic — and listed
as an upstream candidate for Phase 10.

The parameters needed in the config file are `pds.pds_trips.*`,
`pds.pds_tracks.descriptors.*` and the `validation.pds_trips`
coefficients.
