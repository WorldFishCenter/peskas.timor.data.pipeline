# CLAUDE.md

R package implementing the Peskas Timor-Leste small-scale-fisheries data
pipeline: it ingests KoBoToolbox landing surveys and Pelagic Data Systems (PDS)
GPS tracker data, preprocesses and validates them, models fishery indicators,
and publishes JSON to a public GCS bucket consumed by the live portal
(`peskas.timor.portal.v2`). It also publishes to Harvard Dataverse and emails
reports.

Timor was the first Peskas pipeline and predates the conventions the other
country pipelines (Kenya, Mozambique, Zanzibar) share. It has since been
aligned to them — shared `peskas.coasts` hub, `.env` secrets, parquet
interchange, cross-country API. **Where it still differs, the difference is
usually deliberate and documented; check before "fixing" it.**

## Commands

```r
devtools::load_all()
devtools::document()     # roxygen -> man/ + NAMESPACE
devtools::check()        # baseline: 0 errors, 0 warnings, 3 NOTEs
devtools::test()         # testthat, 40 assertions
```

```bash
docker compose up                                   # RStudio on :8802
docker build -f Dockerfile.prod --build-arg COASTS_REF=<tag> -t peskas-timor .
```

The five `inst/tinytest/` suites assert against cloud artefacts and run as
steps **inside** the pipeline workflow, not under `devtools::test()`:

```r
tinytest::run_test_file(system.file("tinytest/test_validated_landings.R",
                                    package = "peskas.timor.data.pipeline"))
```

`ARG COASTS_REF` has **no default** in either Dockerfile — a local build must
pass it. The workflow resolves the latest `peskas.coasts` release and passes it
in, so a build always records which hub release it used. Keep `Dockerfile` and
`Dockerfile.prod` in step. **coasts >= 4.11.0 is a hard floor.**

## Rules that apply everywhere

- **Namespace every hub call `coasts::`.** Timor has no storage layer of its
  own and does not re-export those names.
- **Never log the resolved config.** It carries the service-account key, the
  Airtable PAT, the Dataverse token and the blastula credentials. Pass
  `log_threshold = logger::INFO` to every `coasts::` workflow call.
- **Never write to a production bucket** from an ad-hoc script or a local
  session. `.Renviron` pins `R_CONFIG_ACTIVE=default` and overrides the command
  line; `production` is CI-on-`main` only.
- **Config variables are named `conf`**, matching the other pipelines. `pars`
  was the old name; do not reintroduce it.
- **Never delete an assertion to make a change pass.** Update the expectation
  deliberately, and say so.
- Every workflow function follows the same shape: `read_config()` ->
  authenticate -> download latest inputs -> process -> `logger::log_info()` ->
  `add_version()` -> upload.
- Comments explain *why*, in at most two sentences, and never reference the
  migration or a phase number.
- Run `devtools::document()` after touching roxygen; `man/` and `NAMESPACE` are
  committed.

## Things that break silently

These have all happened here. Each has a file under `.claude/rules/` with the
detail, loaded when you open the code it concerns.

| | |
|---|---|
| A **renamed or dropped portal object** disappears from the live site with no error — the portal discovers files by prefix. `test_portal_contract.R` guards it on every run | `portal-contract.md` |
| **`coasts::cloud_object_name()` returns one name.** Using it to enumerate a bucket reads as "no tracks stored" and re-fetches the entire PDS history | `storage.md` |
| An **unpinned FishBase release** changed published catch on a container rebuild, dropping a taxon worth 5% of landed weight from the portal | `taxa-and-weights.md` |
| An **unset `metadata.fishbase.fao_areas`** silently filters Timor on the Indian Ocean pair the WIO repos use | `config-and-secrets.md` |
| **`tinytest::run_test_file()` sets no exit status** — the test steps reported success regardless of what they found | `validation.md` |
| An **allowlist** of PDS customers discards the trip history of any tracker later reassigned | `pds.md` |
| **`harmonise_v2()`/`harmonise_v3()` are not `map_surveys()`** — they reconcile form versions, not labels | `surveys.md` |

## Pipeline

`.github/workflows/data-pipeline.yaml`, twelve jobs, every 2 days plus on every
push (~1h30m). It is the only workflow that produces data, and a push to any
non-`main` branch runs the whole thing against the `-dev` buckets, which is the
integration test.

```
build-container
├── ingest-landings   -> preprocess-landings
└── ingest-pds-data   -> preprocess-pds-data -> validate-pds-data

merge-landings -> validate-landings ├── export-api
                                    └── merge-trips ├── model-indicators
                                                    └── export-trips
```

Eight other workflows: `R-CMD-check`, `pkgdown`, `test-coverage`,
`pr-commands`, `release` (cuts a release from the top block of `NEWS.md` on a
push to `main`), and three that run in the pipeline's container —
`data-report`, `dataverse-upload`, `validation-email-sender`. Two things the
non-pipeline workflows must not do again: **build their own image**, and
**assume a referenced function is exercised**.

## Local notes

`notes/` is gitignored: the record of the 2026 alignment work, where `STATE.md`
is the decision log and `COASTS-TODO.md` the open items for `peskas.coasts` and
the other country pipelines. A clone will not have it and nothing in the
package reads it.
