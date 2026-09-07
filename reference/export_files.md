# Export portal datasets as versioned JSON files to public cloud storage

Orchestrates the portal data export workflow:

1.  Downloads multiple precomputed objects from public cloud storage via
    [`get_file()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_file.md).

2.  Standardizes column names via
    [`rename_ontology()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/rename_ontology.md).

3.  Formats time-binned tables via
    [`format_aggregated_data()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/format_aggregated_data.md).

4.  Computes summary tables (`estimated_tons`, `estimated_revenue`) and
    a curated `summary_data` list for portal use.

5.  Writes each object to pretty-printed JSON with versioned filenames
    and uploads them to public cloud storage.

## Usage

``` r
export_files()
```

## Value

Invisibly returns `NULL`. Called for its side effects (JSON creation and
upload).

## Details

The exported JSON objects are exactly the seven the live portal
consumes: `aggregated`, `taxa_aggregated`, `municipal_aggregated`,
`municipal_taxa`, `nutrients_aggregated`, `data_last_updated` and
`summary_data`. The contract is **discovery-based** —
`peskas.timor.portal.v2/scripts/fetchData.js` lists the bucket for the
`portal-` prefix and keeps the newest version of each — so renaming or
dropping one of these seven does not fail a build, it silently removes a
page from the live site.

The function expects `summary_data` (downloaded) to contain fields such
as `n_surveys`, `catch_norm`, `catch_price_norm`, `nutrients_per_catch`,
`nutrients_norm`, `conservation`, `cpue_df`, and `timor_shape`.

Assumes a configured public Google cloud storage provider in
`conf$public_storage$google` and relies on helper functions such as
[`add_version()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/add_version.md),
[`coasts::upload_cloud_file()`](https://rdrr.io/pkg/coasts/man/upload_cloud_file.html),
and JSON serialization via `toJSON()`.

## See also

get_file, rename_ontology, format_aggregated_data,
coasts::upload_cloud_file, add_version

## Examples

``` r
if (FALSE) { # \dontrun{
export_files()
} # }
```
