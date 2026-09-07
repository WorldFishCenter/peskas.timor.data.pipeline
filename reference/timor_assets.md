# Keep only Timor's rows of an assets table

The snapshot carries no `country` column, so rows are selected by the
record ids of Timor's two KoBo forms (`metadata.airtable.form_ids`).
Tables whose `form_id` lists several forms are matched if any of them is
Timor's.

## Usage

``` r
timor_assets(x, conf)
```

## Arguments

- x:

  An asset table from
  [`get_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_assets.md),
  carrying a `form_id` column.

- conf:

  The configuration file.

## Value

`x`, filtered to Timor.
