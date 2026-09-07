# Validate the recorded landing site

Alert 16 fires when the recorded station code is in neither reference
source.

## Usage

``` r
validate_sites(submissions, metadata_stations, metadata_reporting_units)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

- metadata_stations:

  The Google Sheets `stations` table.

- metadata_reporting_units:

  The Google Sheets `reporting_unit` table.

## Value

A tibble: `submission_id`, `station_code`, `station_name`,
`reporting_region`, `alert_number`.

## Details

Unlike gear and vessels this validator **keeps** its Google Sheets join,
and deliberately. The published `municipality` is Timor's own reporting
unit, which is not the frame's `gaul_1_name` — Atauro is its own
reporting unit while GAUL puts it inside Dili — and
[`format_public_data()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/format_public_data.md)
hardcodes both the reporting-unit names and five Sheets spellings of
`landing_site` when it classifies the north coast. Measured 2026-08-10:
11 of 40 site names differ between the two sources, two of them among
those five. Reconciling the labels is migration Phase 8's, together with
the portal parity gate; moving them here would silently reclassify
published aggregates.
