# Validate the internal consistency of a landing

A landing must agree with itself: a recorded no-catch cannot carry
individuals or revenue, and a recorded catch cannot lack both.

## Usage

``` r
validate_landing_regularity(landings)
```

## Arguments

- landings:

  The long weighted catch table.

## Value

A list with `regularity_alerts` (one row per submission) and
`regular_landings` (the long table, with the offending rows'
measurements blanked and `alert_number` set).
