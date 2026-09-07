# Download Peskas surveys and PDS data

Download validated Peskas surveys and PDS data merged by temporal
matching

## Usage

``` r
get_merged_trips(conf, ...)
```

## Arguments

- conf:

  The configuration file

- ...:

  Any additional parameters, passed on to
  [`coasts::cloud_object_name()`](https://rdrr.io/pkg/coasts/man/cloud_object_name.html)
  — for example `version` or `exact_match`.

## Value

A dataframe of validated survey landings and PDS trips.
