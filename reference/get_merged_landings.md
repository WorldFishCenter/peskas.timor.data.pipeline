# Download the merged landings

The merged table is the flat long catch table produced by
[`merge_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_landings.md):
one row per (submission, catch, length bin), parquet since migration
Phase 4.

## Usage

``` r
get_merged_landings(conf)
```

## Arguments

- conf:

  The configuration file.

## Value

A tibble of merged landings.
