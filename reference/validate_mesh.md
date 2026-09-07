# Validate the recorded mesh size

The unit conversion moved to
[`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md)
in migration Phase 5; `mesh_size` reaches this function in millimetres.

## Usage

``` r
validate_mesh(submissions, mesh_limit)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

- mesh_limit:

  Largest plausible mesh size, in millimetres.

## Value

A tibble: `submission_id`, `mesh_size`, `alert_number`.
