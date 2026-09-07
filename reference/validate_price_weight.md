# Outlier identification of the price-weight relation

Flags submissions whose revenue per kilo of estimated catch falls
outside the configured band, and propagates the regularity, revenue and
individuals-count alerts onto both the price and the catch frames so
that a single alert number per submission survives the assembly.

## Usage

``` r
validate_price_weight(
  catch_params = NULL,
  price_alerts = NULL,
  non_regular_ids = NULL,
  cook_dist = NULL,
  price_weight_min = NULL,
  price_weight_max = NULL
)
```

## Arguments

- catch_params:

  Output of
  [`validate_catch_params()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_catch_params.md):
  `alerts` (one row per submission) and `catch` (the long table).

- price_alerts:

  The data frame of price alerts.

- non_regular_ids:

  The data frame of landings regularity alerts.

- cook_dist:

  Unused. See above.

- price_weight_min:

  Min price per weight value threshold.

- price_weight_max:

  Max price per weight value threshold.

## Value

A list with `alerts` (one row per submission, carrying the validated
`catch_price`) and `catch` (the long table with blanked measurements).

## Details

The relationship between weight and price is mostly linear, and this
function used to also compute a per-submission Cook's distance from a
`log(price) ~ log(weight)` fit. That term has been commented out of the
threshold for as long as the fixed band has existed, so the fit — a
[`stats::lm()`](https://rdrr.io/r/stats/lm.html) plus a `broom`
augmentation over ~90k submissions, one of the two most expensive steps
in validation — was deleted in migration Phase 5. The `cook_dist`
parameter is retained because `config.yml` still supplies it.
