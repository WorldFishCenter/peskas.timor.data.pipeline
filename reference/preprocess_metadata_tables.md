# Preprocess metadata tables

Takes the ingested Google Sheets metadata tables, parses them and writes
one versioned preprocessed list for the rest of the pipeline to read.

## Usage

``` r
preprocess_metadata_tables(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure. See
  [logger::log_levels](https://daroczig.github.io/logger/reference/log_levels.html)
  for more details

## Details

Six tables are left, all with live readers: `devices`
([`validate_imeis()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_imeis.md)),
`catch_types`, `stations`, `reporting_unit`, `habitat` and
`conservation`. `vms_installs`, `centro_pescas`, `boats`,
`fishing_vessel_statistics` and `registered_boats` went in migration
Phase 11 — the frame is authoritative for the last of those
(`get_registered_boats()`) and the other four had no reader.
`morphometric_table` went on 2026-09-05, when its 559 curated rows
became a package snapshot (`inst/extdata/morphometric-coefficients.csv`)
so that
[`calculate_weights()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/calculate_weights.md)
reads no Google Sheet.

This function requires no arguments because it retrieves the parameters
from `conf.yml`. The fields required are:

    metadata:
      spreadsheet:
        name:
      version:
        preprocess:
    storage:
      storage_name:
        key:
        options:
          project:
          bucket:
          service_account_key:
