# Validate the recorded habitat

Habitat is the one label with no frame equivalent, so
[`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md)
resolves it from a fixed code-to-label lookup. This validator therefore
only has to notice a code the lookup does not cover.

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
