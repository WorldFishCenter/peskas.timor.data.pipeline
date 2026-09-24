# peskas.timor.data.pipeline

R package implementing the Peskas Timor-Leste small-scale-fisheries data
pipeline: it ingests KoBoToolbox landing surveys and Pelagic Data
Systems (PDS) GPS tracker data, preprocesses and validates them, models
fishery indicators, and publishes JSON to a public GCS bucket consumed
by the live portal (`peskas.timor.portal.v2`). It also publishes to
Harvard Dataverse and emails reports.

Timor was the first Peskas pipeline and has since been aligned to the
shared conventions. **Where it still differs, the difference is usually
deliberate and documented; check before “fixing” it.**

Ecosystem context (other repos, data flow, cross-repo contracts): see
PESKAS.md, loaded via CLAUDE.local.md.

## Commands

``` bash
Rscript -e 'devtools::load_all()'    # also document(), check(), test()
docker build -f Dockerfile.prod --build-arg COASTS_REF=<tag> -t peskas-timor .
```

The `inst/tinytest/` suites assert against cloud artefacts and run as
steps **inside** the pipeline workflow, not under `devtools::test()`.
Each step must check the result itself; see
`.claude/rules/validation.md`.

`ARG COASTS_REF` has **no default** in `Dockerfile.prod`, so a local
build must pass it; the workflow passes the latest coasts release.

## Rules that apply everywhere

- **Namespace every hub call `coasts::`.** Timor has no storage layer of
  its own and does not re-export those names.
- The resolved config carries the service-account key, the Airtable PAT,
  the Dataverse token and the blastula credentials. Pass
  `log_threshold = logger::INFO` to every `coasts::` workflow call.
- **Never write to a production bucket** from an ad-hoc script or a
  local session. `.Renviron` pins `R_CONFIG_ACTIVE=default` and
  overrides the command line.
- **Config variables are named `conf`**, matching the other pipelines.
  `pars` was the old name; do not reintroduce it.
- **Never delete an assertion to make a change pass.** Update the
  expectation deliberately, and say so. Tests are Timor’s advantage over
  the other pipelines.
- Comments never reference the migration or a phase number.

## Things that break silently

All have happened here; detail in the named `.claude/rules/` file.

|  |  |
|----|----|
| A **renamed or dropped portal object** disappears from the live site with no error — the portal discovers files by prefix. `test_portal_contract.R` guards it on every run | `portal-contract.md` |
| **[`coasts::cloud_object_name()`](https://rdrr.io/pkg/coasts/man/cloud_object_name.html) returns one name.** Using it to enumerate a bucket reads as “no tracks stored” and re-fetches the entire PDS history | `storage.md` |
| An **unpinned FishBase release** changed published catch on a container rebuild, dropping a taxon worth 5% of landed weight from the portal | `taxa-and-weights.md` |
| An **unset `metadata.fishbase.fao_areas`** silently filters Timor on the Indian Ocean pair the WIO repos use | `config-and-secrets.md` |
| **[`tinytest::run_test_file()`](https://rdrr.io/pkg/tinytest/man/run_test_file.html) sets no exit status** — the test steps reported success regardless of what they found | `validation.md` |
| An **allowlist** of PDS customers discards the trip history of any tracker later reassigned. `pds$exclude_customers` selects trips; `pds$customers` is a separate device roster for `generate_fleet_analysis()` and is correctly an allowlist | `pds.md` |
| **`harmonise_v2()`/`harmonise_v3()` are not `map_surveys()`** — they reconcile form versions, not labels | `surveys.md` |

## Pipeline

`.github/workflows/data-pipeline.yaml` is the only workflow that
produces data; read it for triggers and the exact `needs:` graph. In
outline:

    build-container
    ├── ingest-landings -> preprocess-landings -> merge-landings -> validate-landings
    └── ingest-pds-data -> preprocess-pds-data -> validate-pds-data

    validate-landings ├── export-api
                      └── merge-trips (also needs validate-pds-data)
                            ├── model-indicators
                            └── export-trips (also needs model-indicators)

    export-api + preprocess-pds-data -> summarize-model-data -> export-surveys-portal

`summarize-model-data` and `export-surveys-portal` are the shared coasts
chain (`summarize_data()` -\> `generate_fleet_analysis()` -\>
`export_portal()`), as in the other three countries: they put Timor on
the coasts portal and fill the `dashboard` MongoDB. **Neither feeds
`peskas.timor.portal.v2`**; that is `export-trips`, which writes
`portal-*.json` to the public bucket.

`data-report`, `dataverse-upload` and `validation-email-sender` run in
the pipeline’s container. Two things these must not do again: **build
their own image**, and **assume a referenced function is exercised**.

## Local notes

`notes/` (gitignored) holds the 2026 alignment record: `STATE.md`
decision log, `COASTS-TODO.md` open hub items. A clone lacks it, so
never cite it from a committed file.
