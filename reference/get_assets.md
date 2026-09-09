# Download the Airtable frame mapping tables for Timor

Wraps
[`coasts::get_assets()`](https://rdrr.io/pkg/coasts/man/get_assets.html),
which downloads the shared snapshot and returns only the rows belonging
to Timor's survey forms.

## Usage

``` r
get_assets(conf)
```

## Arguments

- conf:

  The configuration file.

## Value

A named list of five tibbles: `taxa`, `gear`, `vessels`, `sites`, `geo`.
