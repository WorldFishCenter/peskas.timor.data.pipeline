# Validate the recorded number of gleaners

Validate the recorded number of gleaners

## Usage

``` r
validate_gleaners(submissions, method, k_gleaners)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

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

- k_gleaners:

  Extension of bounds for the number of gleaners.

## Value

A tibble: `submission_id`, `n_gleaners`, `alert_number`.
