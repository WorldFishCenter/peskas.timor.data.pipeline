# Download the merged landings with catch weights

The flat long catch table of
[`merge_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_landings.md)
with `weight` (grams) and the seven per-catch nutrient columns added by
[`calculate_weights()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/calculate_weights.md).

## Usage

``` r
get_weighted_landings(conf)
```

## Arguments

- conf:

  The configuration file.

## Value

A tibble, one row per (submission, catch, length bin).
