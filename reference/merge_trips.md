# Merge trips

Merges validated landings and the validated pds trips into one single
data frame. Merging is done only when there is a single landing and
tracked trip per day. Days are determined using the end of the tracking
trip.

## Usage

``` r
merge_trips()
```

## Value

no outputs. This function is used for it's side effects

## Details

The parameters needed in the config file are those required for
[`validate_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landings.md),
and
[`validate_pds_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_pds_trips.md)combined,
as well as merged_trips.
