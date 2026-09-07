# Validate pds trips duration and distance

This function takes pds trips data and returns validated trip duration
and distance.

## Usage

``` r
validate_pds_data(
  data,
  max_hrs = NULL,
  min_hrs = NULL,
  km = NULL,
  se_km = NULL,
  outl = NULL,
  timet = NULL
)
```

## Arguments

- data:

  A data frame containing pds trips

- max_hrs:

  Upper limit of trip duration (hours) to be considered a valid catch
  session.

- min_hrs:

  Lower limit of trip duration (hours) to be considered a valid catch
  session.

- km:

  Limit of trip distance traveled (Km) to be considered a valid catch
  session.

- se_km:

  Distance between the start and end point of a trip to be considered a
  valid catch session.

- outl:

  Limit of speed outlier points in each trip to be considered a good
  quality pds track.

- timet:

  Limit of signal trace dispersion to be considered a good quality pds
  track.

## Value

A list containing data frames with validated catch duration and catch
distance traveled

## Examples

``` r
if (FALSE) { # \dontrun{
conf <- read_config()
pds_trips <- get_pds_trips(conf)
validate_pds(pds_trips)
} # }
```
