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

The deployed-device list still comes from the Google Sheets `devices`
table rather than the frame's `pds_devices`: measured 2026-08-10, the
frame carries 442 Timor devices against the Sheets' 595, and switching
would take alert 3 from 824 to 1,475 submissions and strip the resolved
IMEI — hence the matched trip — from 651 of them.
