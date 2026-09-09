# Download the validated landings in the flat long shape

The same content as
[`get_validated_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_validated_landings.md),
one row per (submission, catch, length bin) under the standard column
names and with `catch_kg` in kilograms. Read by
[`export_api_validated()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_api_validated.md).

## Usage

``` r
get_validated_landings_long(conf)
```

## Arguments

- conf:

  The configuration file.

## Value

A tibble, one row per (submission, catch, length bin).
