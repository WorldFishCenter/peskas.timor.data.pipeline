# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

> **This repo is mid-migration.** It is being aligned to the harmonized Peskas
> standard (shared `peskas.coasts` hub, `.env` secrets, parquet interchange,
> cross-country API). Before doing anything substantive, read in order:
> [.claude/migration/PLAN.md](.claude/migration/PLAN.md),
> [.claude/migration/STRUCTURAL-DIFF.md](.claude/migration/STRUCTURAL-DIFF.md),
> [.claude/migration/AUDIT.md](.claude/migration/AUDIT.md),
> [.claude/migration/STATE.md](.claude/migration/STATE.md).
> **One migration phase per session — never two.** End every session by appending
> a STATE.md entry.
>
> Everything below documents the repo **as it is today** (pre-migration), not the
> target state. Where the target differs, the plan says so.

---

## Project overview

R package implementing the Peskas Timor-Leste small-scale-fisheries data
pipeline. It ingests KoBoToolbox landing surveys and Pelagic Data Systems (PDS)
GPS tracker data, preprocesses and validates them, models fishery indicators,
and publishes JSON to a public GCS bucket consumed by the live portal
(`peskas.timor.portal.v2`). It also publishes datasets to Harvard Dataverse and
emails reports.

Timor was the **first** Peskas pipeline and predates the conventions the other
country pipelines share. Expect divergence from Mozambique / Kenya / Zanzibar to
be structural, not cosmetic.

## Repo layout

| Path | What |
|---|---|
| [R/](R/) | 32 source files, verb-noun naming (see module map below) |
| [inst/conf.yml](inst/conf.yml) | the config file (note: `conf.yml`, not `config.yml`) |
| `auth/` | 15 plaintext credential files, gitignored, read by the `local:` config env |
| [inst/tinytest/](inst/tinytest/) | 4 assertion suites, run as steps **inside** the pipeline workflow |
| [inst/report/](inst/report/) | Rmd reports + `generate_*.R` drivers, shapefiles, bib, css |
| [inst/export/](inst/export/) | Dataverse dataset metadata (README.Rmd, dataset-fields.json, PNGs) |
| [.github/workflows/](.github/workflows/) | 11 workflows — **only 2 are healthy**, see below |
| `peskas.mozambique.data.pipeline/` | untracked reference copy of the standard; read constantly, copy sparingly. Removed in migration Phase 11 |

## Module map (`R/`)

Ingestion
- [ingest-landings.R](R/ingest-landings.R) — `ingest_landings_v1v3()`, `ingest_landings_v2()`
- [retrieve-survey-data.R](R/retrieve-survey-data.R) — Timor's own `get_kobo_data()` (writes files to disk) plus `flatten_row()`/`flatten_field()`/`rename_child()`
- [ingest-metadata-tables.R](R/ingest-metadata-tables.R) — 15 Google Sheets metadata tables
- [ingest-pds-data.R](R/ingest-pds-data.R) — 858 lines: PDS trips + tracks, kepler map, retry wrappers
- [retrieve-pds-data.R](R/retrieve-pds-data.R) — PDS API client

Preprocessing
- [clean-raw-data.R](R/clean-raw-data.R), [preprocess-landings.R](R/preprocess-landings.R) (`step_1`/`step_2`), [pt_nest_species.R](R/pt_nest_species.R), [pt_nest_attachments.R](R/pt_nest_attachments.R), [merge-landings.R](R/merge-landings.R), [preprocess-metadata-tables.R](R/preprocess-metadata-tables.R)
- [calculate-weights.R](R/calculate-weights.R) — rfishbase / morphometric length-weight

Validation
- [validate-landings.R](R/validate-landings.R) — orchestrator, writes flags to Google Sheets
- [validation-functions.R](R/validation-functions.R) — 19 validators (IMEI deployment, landing regularity, mesh, gleaners, fuel, conservation, happiness, …). **This is deeper than any other country pipeline — preserve it.**
- [validate-pds-trips.R](R/validate-pds-trips.R) — consecutive-trip merging, distance/outlier logic; no `coasts` equivalent exists

Merge / model / export
- [merge-trips.R](R/merge-trips.R), [estimate-catch.R](R/estimate-catch.R), [model-catch.R](R/model-catch.R) (glmmTMB), [calculate-nutrients.R](R/calculate-nutrients.R) (nutrients + RDI)
- [format-public-data.R](R/format-public-data.R) — 1200 lines, the largest file; builds every portal object
- [export.R](R/export.R) — `export_files()` serializes and uploads the `portal-*.json` set
- [export-dataverse.R](R/export-dataverse.R), [send-email.R](R/send-email.R)

Infrastructure
- [cloud-storage.R](R/cloud-storage.R) (261 l) + [get-cloud-files.R](R/get-cloud-files.R) (318 l) — own GCS layer; deleted in Phase 2 in favour of `coasts::*`
- [airtable.R](R/airtable.R) — `air_*` API, **entirely different** from the standard's `airtable_to_df`/`bulk_update_airtable`
- [utils.R](R/utils.R) — `add_version()`, `read_config()`
- [google-drive.R](R/google-drive.R), [utils-pipe.R](R/utils-pipe.R), [utils-tidy-eval.R](R/utils-tidy-eval.R), [globals.R](R/globals.R)

## Configuration

`inst/conf.yml`, read via `read_config()` → `config::get(config = Sys.getenv("R_CONFIG_ACTIVE", "default"))`.

Three environments:

| env | storage buckets | secrets from |
|---|---|---|
| `default` | `timor-dev`, `pds-timor-dev`, `public-timor-dev` | `Sys.getenv()` |
| `production` | `timor`, `pds-timor`, `public-timor` | `Sys.getenv()` |
| `local` | inherits `development` **(which does not exist)** | `readLines("auth/<file>")` |

`.Renviron` currently sets `R_CONFIG_ACTIVE=local`, so a local run uses a
*different config branch* than CI — a recurring source of "works in CI, not
locally". CI sets `R_CONFIG_ACTIVE=production` only on `main`; **any push to a
non-main branch runs the whole pipeline against the `-dev` buckets.** That is the
integration-test mechanism for the migration.

### Environment variables (CI) → `auth/` files (local)

| env var | `auth/` file | notes |
|---|---|---|
| `KOBO_USERNAME` / `KOBO_PASSWORD` | `kobo-username` / `kobo-password` | basic auth against `eu.kobotoolbox.org` |
| `KOBO_PESKAS1/2/3` | `peskas-timor-v1/2/3-id` | asset ids; renamed to `KOBO_ASSET_ID_V*` in Phase 1 |
| `GCP_SA_KEY` | `gcp-sa-peskas_ingestion-key.json` | full JSON; GH secret is named `PESKAS_DATAINGESTION_GCS_KEY` |
| `GOOGLE_SHEET_ID` | `metadata-sheet-id` | metadata tables |
| `VALID_SHEET_ID` | `valid-sheet-id` | validation flags sheet |
| `PDS_TOKEN` / `PDS_SECRET` | `pds-token` / `pds-secret` | GH secrets `PESKAS_PDS_TOKEN` / `PESKAS_PDS_SECRET` |
| `AIRTABLE_KEY` | `airtable-key` | renamed to `AIRTABLE_TOKEN` in Phase 1 |
| `DATAVERSE_TOKEN` | `dataverse-token` | GH secret `PESKAS_DATAVERSE_TOKEN` |
| `PESKAS_GMAIL_KEY` | `peskas-gmail-key`, `blastula_cred_file` | two files, one config key |

There is **no `.env` file and no `dotenv`** yet; that arrives in Phase 1.

## Storage

Google Cloud Storage, project `peskas`, service account
`data-ingestion@peskas.iam.gserviceaccount.com` (it can read/write objects but
**cannot** `storage.buckets.list`).

Versioned object naming, from `add_version()`:

```
<prefix>__<YYYYMMDDHHMMSS>_<git-sha7>__.<ext>
```

The sha comes from `git2r::sha(git2r::last_commit())`, falling back to
`$GITHUB_SHA` inside containers. `cloud_object_name(version = "latest")` resolves
the newest.

| bucket | contents |
|---|---|
| `timor` / `timor-dev` | surveys and derived tables: raw `.csv`, everything downstream `.rds` |
| `pds-timor` / `pds-timor-dev` | one gzipped CSV per GPS trip: `pds-track-<trip_id>__*__.csv.gz` |
| `public-timor` / `public-timor-dev` | `portal-*.json` — the live portal contract |

**No lifecycle policy is set.** Every pipeline run appends new versions and
nothing is ever deleted; `gs://timor` holds ~33k objects and `gs://pds-timor`
~98k. See [.claude/migration/AUDIT.md](.claude/migration/AUDIT.md) for the full
prefix inventory including orphaned prefixes.

Interchange format today is `.rds` with **nested list-columns**
(`landing_catch`, `length_frequency`). The migration flips this to flat long
parquet.

## Portal contract (do not break)

[export.R](R/export.R) writes nine objects as `portal-<name>__*__.json` to
`public-timor`:

```
aggregated  taxa_aggregated  municipal_aggregated  municipal_taxa
nutrients_aggregated  data_last_updated  indicators_grid
label_groups_list  summary_data
```

`peskas.timor.portal.v2/scripts/fetchData.js` lists `portal-*` in the bucket,
keeps the newest version of each, and **excludes** `indicators_grid.json`,
`label_groups_list.json`, `pars.json`, `taxa_names.json`,
`var_dictionary.json` — so seven of the nine are live. The portal discovers
files dynamically, meaning a **renamed or dropped object silently disappears
from the site**. Migration Phase 8 gates on a structural + numeric diff against
the golden snapshot.

## Pipeline DAG

From [.github/workflows/data-pipeline.yaml](.github/workflows/data-pipeline.yaml)
(every 2 days at 00:00 UTC, plus on every push; ~1h30m):

```
build-container
├── ingest-preprocess-metadata-tables   ingest_metadata_tables → preprocess_metadata_tables → ingest_rfish_table
├── ingest-preprocess-landings-v1-v3    ingest_landings_v1v3 → preprocess_legacy_landings → preprocess_updated_landings
├── ingest-preprocess-v2-landings-step1 ingest_landings_v2 → preprocess_landings_step_1
│   └── step2                           preprocess_landings_step_2
└── ingest-pds-data                     ingest_pds_trips → ingest_pds_tracks
    └── preprocess-pds-data             preprocess_pds_trips → preprocess_pds_tracks
        └── validate-pds-data           validate_pds_trips        [tinytest]

merge-landings   merge_landings → calculate_weights
└── validate-landings  validate_landings                          [tinytest]
    └── merge-trips    merge_trips                                [tinytest]
        ├── model-indicators  estimate_fishery_indicators
        └── export-trips      format_public_data → export_files   [tinytest] → enumerators report
```

## Development commands

```r
devtools::load_all()     # load
devtools::document()     # roxygen → man/ + NAMESPACE
devtools::check()        # R CMD check
devtools::install()

# tinytest suites (the ones CI actually runs)
tinytest::run_test_file(system.file("tinytest/test_validated_landings.R",  package = "peskas.timor.data.pipeline"))
tinytest::run_test_file(system.file("tinytest/test_validated_pds_trips.R", package = "peskas.timor.data.pipeline"))
tinytest::run_test_file(system.file("tinytest/test_merged_trips.R",        package = "peskas.timor.data.pipeline"))
tinytest::run_test_file(system.file("tinytest/test_public_data.R",         package = "peskas.timor.data.pipeline"))
```

```bash
docker compose up          # RStudio on :8802, DISABLE_AUTH=true
docker build -f Dockerfile.prod -t peskas-timor .
```

`Dockerfile.prod`: `rocker/geospatial:4.4`, ~70 packages via `install2.r`,
`rfishbase` pinned to 5.0.1, `ggchicklet` + `glmmTMB` from GitHub, then
`COPY . /home` + `remotes::install_local()`. Image is pushed to
`ghcr.io/worldfishcenter/peskas.timor.data.pipeline/r-runner-peskas-timor:latest`.

## CI health — most workflows are dead

Verified 2026-07-31 via `gh api .../actions/workflows`:

| workflow | state | last green |
|---|---|---|
| `data-pipeline.yaml` | active | 2026-07-31 ✅ |
| `data-report.yaml` | disabled (inactivity) | 2026-06-01 |
| `check-standard.yaml` (R-CMD-check) | active | never in recent history — fails in <15 s |
| `pkgdown.yaml` | active | fails |
| `test-coverage.yaml` | active | fails in <10 s |
| `dataverse-upload.yaml` | disabled (inactivity) | fails since ≥2026-04 |
| `form-summary.yaml` | disabled (inactivity) | fails since ≥2025-08 |
| `upload-matched-trips.yaml` | disabled (inactivity) | fails since ≥2025-08 |
| `validation-email-sender.yaml` | disabled (inactivity) | fails since ≥2025-09 |
| `keplergl-map.yaml` | disabled (manually) | fails since 2026-02 |
| `pr-commands.yaml` | active | no runs |

The four monthly/weekly ones plus `keplergl-map` still build via the retired
`docker.pkg.github.com` registry and `whoan/docker-build-with-cache-action@v5`.
**In practice only `data-pipeline.yaml` produces data.** Do not assume a
function is exercised just because a workflow references it.

## Conventions and gotchas

- Every workflow function follows: `read_config()` → authenticate → download
  latest inputs → process → `logger::log_info()` → `add_version()` → upload.
- Logging is `logger`; the `log_threshold` argument on workflow functions sets
  the level.
- `pt_nest_species()` / `pt_nest_attachments()` build the nested list-columns
  the `.rds` interchange format depends on.
- `pars$...$version$preprocess: latest` appears on `landings_1/2/3`, `pds.trips`,
  `pds.tracks`, `metadata` and `validation`. The unified template dropped this
  field; do not drop it here without deciding per key.
- Timezone handling was fixed in commit `15f6b18` — re-verify it after any
  rewrite of the export path.
- **Known live bug:** `get_preprocessed_metadata()` is defined twice, in
  [get-cloud-files.R:110](R/get-cloud-files.R#L110) (correct, reads
  `pars$metadata$google_sheets$name`) and
  [validate-landings.R:373](R/validate-landings.R#L373) (broken, reads the
  removed `pars$metadata$airtable$name`). Collation order means the broken one
  wins. Several other helpers in `validate-landings.R` reference config keys
  that no longer exist. Catalogued in AUDIT.md.
- Tests are Timor's advantage over the other pipelines. **Never delete an
  assertion to make a change pass** — update the expectation deliberately.
