# Resolve the recorded catch-preservation method

Conservation has no frame equivalent, so the Google Sheets
`conservation` table stays. Raises no alert.

## Usage

``` r
validate_conservation(submissions, metadata_conservation)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

- metadata_conservation:

  The Google Sheets `conservation` table.

## Value

A tibble: `submission_id`, `conservation_place`, `alert_number`.
