# Download Peskas validated landings, in the nested portal shape

One row per submission, with the catch as the `landing_catch` /
`length_frequency` list-columns that
[`merge_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_trips.md),
[`format_public_data()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/format_public_data.md)
and
[`estimate_fishery_indicators()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/estimate_fishery_indicators.md)
read.

## Usage

``` r
get_validated_landings(conf)
```

## Arguments

- conf:

  The configuration file.

## Value

A tibble of validated survey landings, one row per submission.

## Details

This is a view over
[`get_validated_landings_long()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_validated_landings_long.md),
re-nested on read; there is no separate stored artefact in this shape.
