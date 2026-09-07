# Merge short intervalled trips

PDS data are quite patchy, sometimes an actual single trip is recorded
as 2 different trips. This function calculates the temporal and the
geographical distance between two consecutive trips for each boat,
merging those consecutive trips that start before 180 minutes from the
following within a range of 2 km.

## Usage

``` r
merge_consecutive_trips(
  x,
  consecutive_time = NULL,
  consecutive_distance = NULL
)
```

## Arguments

- x:

  A data frame containing raw pds trips data.

- consecutive_time:

  Time between consecutive trips (hrs).

- consecutive_distance:

  Distance between consecutive trips (Km).

## Value

A dataframe with pds trips including a new column `associated_to`
indicating the consecutive trips to be merged.
