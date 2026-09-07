# Carry through the recorded happiness rating

Raises no alert; it is here so the rating reaches the validated artefact
alongside everything else.

## Usage

``` r
validate_happiness(submissions)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

## Value

A tibble: `submission_id`, `happiness`, `alert_number`.
