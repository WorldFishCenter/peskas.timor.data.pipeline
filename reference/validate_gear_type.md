# Validate the recorded gear type

Reads the `gear` label
[`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md)
resolves from the PESKAS \| FRAME frame. Note the frame's labels are
Title Case where the retired Google Sheets `gear_types` table was lower
case, and `MC` is "Gleaning" where the Sheets said "manual collection".

## Usage

``` r
validate_gear_type(submissions)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

## Value

A tibble: `gear_type`, `alert_number`, `submission_id`.
