# Resolve the recorded catch-preservation method

Maps the recorded code to its label. Raises no alert.

## Usage

``` r
validate_conservation(submissions)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

## Value

A tibble: `submission_id`, `conservation_place`, `alert_number`.
