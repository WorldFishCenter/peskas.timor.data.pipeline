# Validate the tracker IMEIs recorded on a survey

Enumerators record as many digits of the tracker IMEI as they can read,
so a recorded value is matched against the deployed devices by
**suffix**. A value short enough to be ambiguous, or matching no device,
is flagged.

## Usage

``` r
validate_imeis(submissions, deployed_imeis)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

- deployed_imeis:

  Character vector of deployed device IMEIs.

## Value

A tibble: `imei`, `alert_number`, `submission_id`.

## Details

The roster spans every device ever deployed, since a submission from any
year can reference one.
