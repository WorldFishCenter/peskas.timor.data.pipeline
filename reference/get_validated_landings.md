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

Until migration Phase 8 this read a second stored artefact,
`timor-landings-merged_validated__*.rds`, written by
[`validate_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landings.md)
beside the long parquet. That artefact is gone: this is now a **view**
over
[`get_validated_landings_long()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_validated_landings_long.md),
rebuilt by `nest_landing_catch()`. The two were proven interchangeable
before the switch — 97,360 submissions and 1,648,016 catch rows compared
column by column, every column equal, with 1,599 catch weights differing
by at most 2.9e-11 g (one ULP of the grams → kg → grams round trip) and
the national total unchanged to 20 significant digits.
