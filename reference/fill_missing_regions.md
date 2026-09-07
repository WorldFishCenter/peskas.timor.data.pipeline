# Fill missing regions

Replace empty region data based on boats geographic activity (trough
tracker imeis)

## Usage

``` r
fill_missing_regions(trips = NULL)
```

## Arguments

- trips:

  Dataframe with Timor validated trips.

## Value

A dataframe with filled regions (where possible).

## Examples

``` r
if (FALSE) { # \dontrun{
fill_missing_regions(trips)
} # }
```
