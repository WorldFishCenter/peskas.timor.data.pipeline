# Ingest metadata tables

Metadata tables are manually updated in a Airtable. This function
downloads the table and uploads it to a drive location. These tables
include information about boats, devices, municipalities, etc.

## Usage

``` r
ingest_metadata_tables(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure. See
  [logger::log_levels](https://daroczig.github.io/logger/reference/log_levels.html)
  for more details

## Details

The parameters needed in `conf.yml` are:

    metadata:
      airtable:
        base_id:
        name:
        api_key:
        tables:
          -
          -
    storage:
      storage_name:
        key:
        options:
          project:
          bucket:
          service_account_key:

## Examples

``` r
if (FALSE) { # \dontrun{
ingest_metadata_tables()
} # }
```
