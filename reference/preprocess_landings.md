# Pre-process the landing surveys into a flat long catch table

Reads the raw parquet written by
[`ingest_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/ingest_landings.md)
and turns each form version into **one row per (submission, catch,
length bin)** — the flat long shape the other country pipelines use —
resolving every code the form records into its standard label along the
way.

## Usage

``` r
preprocess_landings(versions = c("v2", "v3"), log_threshold = logger::DEBUG)
```

## Arguments

- versions:

  Character vector of form versions to pre-process. Defaults to the live
  forms, `c("v2", "v3")`. `v1` is frozen — see
  `data-raw/freeze-landings-v1.R`.

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure. See
  [logger::log_levels](https://daroczig.github.io/logger/reference/log_levels.html).

## Value

No output. This function is used for its side effects.

## Details

The parameters needed in `config.yml` are:

    surveys:
      landings:
        <version>:
          raw:
            file_prefix:
            version:
          preprocessed:
            file_prefix:
    storage:
      google:
        key:
        options:

## Column set

The table is a **superset**. Alongside the standard columns
(`submission_id`, `landing_date`, `gaul_*`, `landing_site`, `n_fishers`,
`trip_duration`, `gear`, `vessel_type`, `habitat`, `catch_outcome`,
`n_catch`, `catch_taxon`, `scientific_name`, `length`, `catch_price`,
`mesh_size`, `n_gleaners`, `fuel`, `happiness`, …) it carries every raw
KoBo column, reconciled across form versions exactly as
`merge_versions()` used to reconcile them after the merge. Nothing reads
the raw columns any more —
[`validate_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landings.md)
moved onto the standard names in migration Phase 5 — and they are
dropped with the other legacy passthrough in Phase 11.

The raw form codes are kept beside their resolved labels
(`landing_site_code`, `gear_code`, `vessel_code`, `habitat_code`)
because validation needs both: an unrecognised code is exactly what
alerts 12, 14, 16 and 19 report.

## Labels

Taxa, gear, vessels and landing sites are resolved from the PESKAS \|
FRAME assets snapshot, which is authoritative wherever it overlaps the
Google Sheets tables. Habitat has no frame equivalent and stays on the
Sheets, as does the per-taxon `length_type` (descriptive only — see
[`join_weights()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/join_weights.md)).
The snapshot is cross-country and carries no `country` column, so every
read is narrowed with
[`timor_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/timor_assets.md).
