# Generate an alert vector based on the `univOutl::LocScaleB()` function

Generate an alert vector based on the
[`univOutl::LocScaleB()`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)
function

## Usage

``` r
alert_outlier(
  x,
  no_alert_value = NA_real_,
  alert_if_larger = no_alert_value,
  alert_if_smaller = no_alert_value,
  ...
)
```

## Arguments

- x:

  numeric vector where outliers will be checked

- no_alert_value:

  value to put in the output when there is no alert (x is within bounds)

- alert_if_larger:

  alert for when x is above the bounds found by
  [`univOutl::LocScaleB()`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)

- alert_if_smaller:

  alert for when x is below the bounds found by
  [`univOutl::LocScaleB()`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)

- ...:

  arguments for
  [`univOutl::LocScaleB()`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)

## Value

a vector of the same lenght as x
