# Validate the recorded vessel type

Reads the `vessel_type` label
[`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md)
resolves from the Airtable frame. Both codes the live forms use resolve,
so alert 12 is currently never raised.

## Usage

``` r
validate_vessel_type(submissions)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

## Value

A tibble: `vessel_type`, `alert_number`, `submission_id`.
