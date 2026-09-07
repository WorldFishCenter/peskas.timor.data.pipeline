# Format public data

Format the merged trips into files that are fit for distribution.
Specifically, this function produces a trips and a catch table as well
as tables containing aggregated data. These files are stored both as tsv
files and rds in a storage bucket with public access.

## Usage

``` r
format_public_data(log_threshold = logger::DEBUG)
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

\#'The parameters needed in the config file are those required for
[`merge_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_trips.md)
in addition to:

    public_storage:
     google:
       key: gcs
       options:
         project:
         bucket:
         service_account_key:
    export:
     file_prefix:
