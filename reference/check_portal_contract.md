# Check the portal JSON contract

The portal contract is *discovery-based*: `peskas.timor.portal.v2` lists
`gs://public-timor` for the `portal-` prefix and keeps the newest
version of each name. A renamed object or a dropped key does not fail a
build — it silently disappears from the live site. So this asserts on
**names** first, then on keys, nesting, column sets and column types.

## Usage

``` r
check_portal_contract(
  baseline,
  candidate,
  allow_dropped = character(),
  numbers = FALSE,
  conf = NULL
)
```

## Arguments

- baseline, candidate:

  Each a directory of `portal-*.json` files, a bucket (`gs://<bucket>`
  for the newest set, `gs://<bucket>@<version>` to pin one by any
  substring of the `<timestamp>_<sha>` version — pin the **full**
  version, since a bare sha is shared by every run of that commit), or
  `"latest"`, the newest set in whichever public bucket the active
  config resolves.

- allow_dropped:

  Object names whose removal is intended, so a missing object is a note
  rather than a failure.

- numbers:

  Also print a per-column numeric summary.

- conf:

  The configuration file. Read when a bucket has to be resolved.

## Value

A character vector of structural failures, empty when the contract
holds. The full report is printed.

## Details

Row counts and column order are reported, never failed: the portal reads
an array of objects by key, and row counts are data. Numeric summaries
are printed only when `numbers = TRUE`, and never fail — accepting or
rejecting a moved number needs a human.

`inst/tinytest/test_portal_contract.R` runs this against the frozen
shape on every pipeline run.

## Limitation

A bucket set is the newest version of each name, which is what the
portal sees — so if
[`export_files()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_files.md)
stopped writing an object, the previous version stays newest and the
name check cannot fire. That is why dropping `indicators_grid` was
harmless. Comparing two local directories catches it.
