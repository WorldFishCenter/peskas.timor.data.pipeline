# Validate the recorded habitat

Habitat is the one label with no frame equivalent, so
[`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md)
resolves it from the Google Sheets `habitat` table. This validator
therefore only has to notice a code that table does not cover — the
seven-way `case_when()` it used to duplicate was verified identical to
the resolved column over all 97,347 submissions before being deleted.

## Usage

``` r
validate_habitat(submissions)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

## Value

A tibble: `submission_id`, `habitat_code`, `habitat_type`,
`alert_number`.
