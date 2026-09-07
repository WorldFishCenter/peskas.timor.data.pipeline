# Format aggregated time-binned datasets for portal output

Adds human-readable date labels (day/week/month/year) to aggregated
datasets. Supports two modes:

- Municipal mode (`municipal = TRUE`): expects a single data frame with
  `date_bin_start` and adds `month` and `year` columns.

- National mode (`municipal = FALSE`): expects a list of data frames
  (e.g., `day`, `week`, `month`, `year`), converts each to `data.table`,
  sets `n_boats` to `national_boats`, and adds formatted date label
  columns.

## Usage

``` r
format_aggregated_data(aggregated, municipal = FALSE, national_boats = NULL)
```

## Arguments

- aggregated:

  A data frame (municipal mode) or a list of data frames (national mode)
  containing a `date_bin_start` column.

- municipal:

  Logical. If `TRUE`, treat `aggregated` as a single municipal table;
  otherwise treat it as a list of time-binned tables.

- national_boats:

  Numeric/integer. Total number of boats to assign to the `n_boats`
  column in national mode.

## Value

The formatted `aggregated` object: a `data.table` (municipal mode) or a
list of `data.table`s (national mode) with added/updated label columns.

## Details

In national mode, formatted labels are:

- `day`: `"%d %b %y"`

- `week`: `"%d %b %y"`

- `month`: `"%B %Y"` plus `year` as `"%Y"`

- `year`: `"%Y"`

## See also

data.table::as.data.table

## Examples

``` r
if (FALSE) { # \dontrun{
agg <- get_file("timor_aggregated")
agg2 <- format_aggregated_data(agg, national_boats = 123)

m <- get_file("timor_municipal_aggregated")
m2 <- format_aggregated_data(m, municipal = TRUE)
} # }
```
