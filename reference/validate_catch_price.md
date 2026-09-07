# Validate surveys' total catch values

Flags implausibly large revenue. The
[`univOutl::LocScaleB()`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)
route is kept in place, commented out, because the fixed threshold
replaced it deliberately.

## Usage

``` r
validate_catch_price(regular_landings, method = NULL, k = NULL)
```

## Arguments

- regular_landings:

  The long catch table from
  [`validate_landing_regularity()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landing_regularity.md),
  carrying `catch_price` and the `alert_number` raised so far.

- method:

  character identifying how to estimate the scale of the distribution.
  Available choices are:

  `method='IQR'` for using the Inter-Quartile Range, i.e. Q3-Q1;

  `method='IDR'` for using the Inter-Decile Range; i.e. P90-P10

  `method='MAD'` for using the Median Absolute Deviation;

  `method='Gini'` robust scale estimate based on Gini's Mean Difference
  (see [`GiniMd`](https://rdrr.io/pkg/Hmisc/man/GiniMd.html));

  `method='ScaleTau2'` robust tau-estimate of univariate scale, as
  proposed by Maronna and Zamar (2002) (see
  also[`scaleTau2`](https://rdrr.io/pkg/robustbase/man/scaleTau2.html));

  `method='Qn'` for using the Qn estimator proposed by Rousseeuw and
  Croux (1993) (see also
  [`Qn`](https://rdrr.io/pkg/robustbase/man/Qn.html));

  `method='Sn'` for using the Sn estimator proposed by Rousseeuw and
  Croux (1993) (see also
  [`Sn`](https://rdrr.io/pkg/robustbase/man/Sn.html)).

  When `method='dQ'` the estimated scale for the left tail is
  (Q2-Q1)/0.6745, while for the right tail it is considered
  (Q3-Q2)/0.6745 (Q2 is the median); this double estimate should be able
  to account for slight skewness.

  When `method='dD'` the estimated scale for the left tail is
  (P50-P10)/1.2816, while for the right tail it is considered
  (P90-P50)/1.2816 (P50 is the median); this double estimate should be
  able to account for skewness.

  Finally, when `method='AdjOut'`, bounds are based on the adjusted
  outlyingness method as proposed by Hubert and Van der Veeken (2008).

- k:

  Nonnegative constant that determines the extension of bounds. Commonly
  used values are 2, 2.5 and 3 (default).

## Value

A data frame containing validated catch values, one row per submission.
