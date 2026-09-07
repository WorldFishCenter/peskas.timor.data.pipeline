# Extract per-trip descriptors from PDS tracks

The one PDS product `coasts` has no equivalent for, and the reason Timor
still has a track-preprocessing step at all after migration Phase 7
moved ingestion to `coasts::*`.
[`coasts::preprocess_pds_tracks()`](https://rdrr.io/pkg/coasts/man/preprocess_pds_tracks.html)
aggregates track points into 500 m and 1 km spatial grid cells for the
shared effort products;
[`validate_pds_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_pds_trips.md)
needs something different — one row per trip carrying the geometry and
signal-quality measures its alerts are built on:

## Usage

``` r
describe_pds_tracks(log_threshold = logger::DEBUG)
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

- `start_end_distance` — metres between the first and last point of a
  trip, which is how a one-way trip is spotted (alert 12).

- `outliers_proportion` — percentage of points faster than 30 m/s, and
  `timetrace_dispersion` — the standard deviation of the interval
  between consecutive points (together, alert 13).

- `start_lat` / `start_lng` / `end_lat` / `end_lng`, which
  [`merge_consecutive_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_consecutive_trips.md)
  needs to decide whether two trips of the same boat are really one.

The step is incremental: only trips absent from the previous descriptors
artefact are read. Both workflow steps run, coasts' for the grids and
this one for the descriptors.

The parameters needed are:

    pds:
      pds_tracks:
        file_prefix:
        descriptors:
          file_prefix:
          version:
    pds_storage:
      google:
        key:
        options:

Progress through the function is tracked using the package *logger*.
