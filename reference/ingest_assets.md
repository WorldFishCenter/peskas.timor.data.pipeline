# Ingest the Airtable frame assets snapshot

Thin wrapper over
[`coasts::ingest_assets()`](https://rdrr.io/pkg/coasts/man/ingest_assets.html),
which pulls taxa, gears, vessels, landing sites, districts, forms, PDS
devices and the fleet frame from the PESKAS \| FRAME base and writes one
versioned `assets__*.rds`.

## Usage

``` r
ingest_assets(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure. See
  [logger::log_levels](https://daroczig.github.io/logger/reference/log_levels.html).

## Value

No output. This function is used for its side effects.

## Details

Airtable is authoritative wherever it overlaps the Google Sheets
metadata tables (taxa, gears, vessels, landing sites, districts, PDS
devices); the Sheets keep only the five tables the frame does not cover.

Until coasts 4.7.0 the delegated call wrote the snapshot to the country
bucket while every reader resolved the hub, so this function mirrored it
afterwards. COASTS-TODO C11 fixed that upstream —
[`coasts::ingest_assets()`](https://rdrr.io/pkg/coasts/man/ingest_assets.html)
now writes the hub itself — and migration Phase 11 deleted the mirror.
