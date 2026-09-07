# Extract Timor Geographical Boundaries

This function extracts the geographical boundaries of Timor, including
national boundaries, administrative regions, and special cases for
Atauro and Dili. It uses shapefiles stored within the
'peskas.timor.data.pipeline' package to perform this extraction. Logging
is incorporated to inform the user of the process status and any
potential threshold-based logging details.

## Usage

``` r
get_timor_boundaries(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging threshold level as defined by the `logger` package. This
  parameter allows the user to set the verbosity of logging output. The
  default level is
  [`logger::DEBUG`](https://daroczig.github.io/logger/reference/log_levels.html).

## Value

An `sf` object containing the geographical boundaries of Timor's
regions, including adjustments for specific areas like Atauro and Dili.
The returned object includes region names and their corresponding
geometries.

## Examples

``` r
# Assuming 'peskas.timor.data.pipeline' package is installed and loaded
timor_boundaries <- get_timor_boundaries()
#> INFO [2026-09-07 07:17:42] Extracting Timor shape boundaries...
plot(timor_boundaries$geometry)

```
