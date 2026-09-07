# Reshape one raw landings table into the flat long catch table

The whole of preprocessing for a single form version, as a pure function
so it can be exercised without cloud storage.

## Usage

``` r
reshape_landings(raw, version, labels)
```

## Arguments

- raw:

  A raw landings table as written by
  [`ingest_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/ingest_landings.md).

- version:

  Form version, `"v2"` or `"v3"`.

- labels:

  Output of
  [`survey_labels()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/survey_labels.md).

## Value

A tibble, one row per (submission, catch, length bin).
