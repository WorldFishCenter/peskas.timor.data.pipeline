# Validate surveys' catch parameters

Uses univariate techniques (see
[univOutl::LocScaleB](https://rdrr.io/pkg/univOutl/man/LocScaleB.html))
to identify outliers in the number of individuals recorded per (gear,
taxon) group, and blanks the weight and nutrients derived from an
outlying count.

## Usage

``` r
validate_catch_params(landings = NULL, k_ind = NULL)
```

## Arguments

- landings:

  The long catch table from
  [`validate_landing_regularity()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landing_regularity.md),
  carrying `alert_number`.

- k_ind:

  Extension of bounds for the number of individuals

## Value

A list with `alerts` (one row per submission) and `catch` (the long
table with outlying counts and their weights blanked).

## Details

Grouping is on the resolved `gear` label rather than the raw gear code;
the two are one-to-one over all nine codes the live forms use, so the
partition is identical.

## Examples

``` r
if (FALSE) { # \dontrun{
conf <- read_config()
landings <- get_weighted_landings(conf)
validate_catch_params(validate_landing_regularity(landings)$regular_landings)
} # }
```
