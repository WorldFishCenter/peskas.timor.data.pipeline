# Ingest landings surveys from KoboToolbox

Downloads the live landing-survey forms from KoboToolbox, flattens each
submission into one row, and uploads the result as a versioned parquet
object to the country bucket.

## Usage

``` r
ingest_landings(versions = c("v2", "v3"), log_threshold = logger::DEBUG)
```

## Arguments

- versions:

  Character vector of form versions to ingest. Defaults to the live
  forms, `c("v2", "v3")`.

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure. See
  [logger::log_levels](https://daroczig.github.io/logger/reference/log_levels.html).

## Value

No output. This function is used for its side effects.

## Details

Only the **live** forms are ingested. `v1` (SSF Landings) received its
last submission on 2020-08-28 and is frozen: its preprocessed output was
snapshot once, out of band, and is read from
`surveys.landings.v1.frozen` thereafter.

The parameters needed in `config.yml` are:

    ingestion:
      landings:
        <version>:
          asset_id:
          username:
          password:
    surveys:
      landings:
        <version>:
          raw:
            file_prefix:
    storage:
      google:
        key:
        options:
