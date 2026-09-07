# Download and read a versioned RDS object from public cloud storage

Retrieves an `.rds` file from the configured public Google cloud storage
using an exact prefix match, downloads it locally, reads it into R, and
deletes the temporary local file. The function also attaches a
`data_last_updated` attribute parsed from the object filename (expected
to contain a timestamp as the 4th underscore-separated token in
`%Y%m%d%H%M` format).

## Usage

``` r
get_file(prefix)
```

## Arguments

- prefix:

  Character. Prefix used to locate the cloud object (exact match).

## Value

The R object read from the RDS file, with an extra attribute
`data_last_updated` (POSIXlt) indicating the timestamp extracted from
the filename.

## Details

Requires configuration from
[`read_config()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/read_config.md).
Uses
[`coasts::cloud_object_name()`](https://rdrr.io/pkg/coasts/man/cloud_object_name.html)
to resolve the object name and
[`coasts::download_cloud_file()`](https://rdrr.io/pkg/coasts/man/download_cloud_file.html)
to download it. Assumes filenames follow a convention like
`..._<timestamp>_...` where `<timestamp>` is in `%Y%m%d%H%M` format and
located at position 4 after splitting on `_`.

## See also

read_config, coasts::cloud_object_name, coasts::download_cloud_file

## Examples

``` r
if (FALSE) { # \dontrun{
x <- get_file("timor_aggregated")
attr(x, "data_last_updated")
} # }
```
