# Validate the recorded mesh size

`mesh_size` reaches this function in millimetres, converted by
[`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md).

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
