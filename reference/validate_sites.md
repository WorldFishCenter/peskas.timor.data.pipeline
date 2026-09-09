# Validate the recorded landing site

Resolves the site name and reporting region from the Airtable frame by
site code. Alert 16 fires when the recorded code matches no known site.

## Usage

``` r
validate_sites(submissions, frame_sites, frame_geo)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

- frame_sites:

  The frame `sites` table from
  [`get_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_assets.md).

- frame_geo:

  The frame `geo` table from
  [`get_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_assets.md).

## Value

A tibble: `submission_id`, `station_code`, `station_name`,
`reporting_region`, `alert_number`.
