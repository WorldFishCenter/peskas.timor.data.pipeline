# Publish the validated landings on Harvard Dataverse

Releases the table the Peskas Fishery Data API serves for Timor-Leste
(the `trips-validated` parquet written by
[`export_api_validated()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_api_validated.md))
as a new version of one Dataverse dataset.

## Usage

``` r
upload_dataverse(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure. See
  [logger::log_levels](https://daroczig.github.io/logger/reference/log_levels.html)
  for more details

## Value

Invisibly, the DOI of the dataset.

## Details

Two files are released, both built here:

- `timor_landings.csv`, the API table as it is, with empty cells for
  missing values.

- `README.md`, filled in from `inst/export/README.md`. Its column table
  is read from the API's `/metadata/landings` endpoint at run time, so
  no column is described in this package, and a column the API does not
  describe stops the release.

The dataset record (title, authors, licence, links) is
`inst/export/dataset-fields.json`. It is sent on every run, so edit it
there: a change made on the Dataverse website is overwritten by the next
release.

`export_dataverse$dataset_doi` in `inst/config.yml` names the dataset:

- **Empty**: the dataset is created as an unpublished draft and its DOI
  is logged. Review the draft, put the DOI in the config, and run again.

- **Set**: the files whose content changed are replaced and a new major
  version is published. Outside the `production` configuration the draft
  is left unpublished.

The calls follow the Dataverse native API,
<https://guides.dataverse.org/en/latest/api/native-api.html>.
