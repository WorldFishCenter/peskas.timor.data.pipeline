# Download the Airtable frame assets snapshot

Reads the versioned `assets__*.rds` written by
[`ingest_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/ingest_assets.md)
from the shared coasts hub bucket. The snapshot is **cross-country**:
use
[`timor_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/timor_assets.md)
to narrow a table to Timor's rows.

## Usage

``` r
get_assets(conf)
```

## Arguments

- conf:

  The configuration file

## Value

A named list of asset tables (`taxa`, `gear`, `vessels`, `sites`, `geo`,
`forms`, `devices`, `frame`).
