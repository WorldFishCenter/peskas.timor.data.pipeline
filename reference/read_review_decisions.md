# Read reviewers' decisions on the live forms

Decisions made in the Peskas Management Platform (which writes them to
the flags collection) and in KoBoToolbox, read with
[`coasts::review_decisions()`](https://rdrr.io/pkg/coasts/man/review_decisions.html)
before a run's push replaces the collection. v1 is frozen and has
neither. Reading is safe from any environment; writing back to
KoBoToolbox is
[`sync_validation_status()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/sync_validation_status.md).

## Usage

``` r
read_review_decisions(conf)
```

## Arguments

- conf:

  The configuration file.

## Value

A tibble, one row per reviewed submission: `submission_id`,
`validation_status`, `validated_at`, `validated_by`.
