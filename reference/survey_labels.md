# Label lookup tables for preprocessing

Taxa, gear, vessels and landing sites come from the PESKAS \| FRAME
assets snapshot; habitat and the per-taxon `length_type` have no frame
equivalent and come from the Google Sheets. Every frame table is
narrowed to Timor with
[`timor_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/timor_assets.md)
first — the snapshot is cross-country and all 56 of Timor's
`alpha3_code`s are used by another country too.

## Usage

``` r
survey_labels(conf)
```

## Arguments

- conf:

  The configuration file.

## Value

A named list of lookup tibbles keyed on the raw form value.
