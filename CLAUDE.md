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
> Everything below documents the repo **as it is today**, not the target state.
> Where the target differs, the plan says so. Phases completed so far:
> **0, 1, 2** — so config, secrets, the container and the **storage layer** are
> already on the standard; ingestion, preprocessing, validation, PDS and export
> are not.

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
| [R/](R/) | 30 source files, verb-noun naming (see module map below) |
| [inst/config.yml](inst/config.yml) | the config file. A **superset**: harmonized keys plus every legacy key, so old code keeps running mid-migration |
| [inst/config_template.yml](inst/config_template.yml) | Timor's copy of the cross-country spec, with its deviations recorded |
| `.env` | local secrets, gitignored. Template: [.env.example](.env.example). Replaced the old `auth/` directory in Phase 1 |
| [inst/tinytest/](inst/tinytest/) | 4 assertion suites, run as steps **inside** the pipeline workflow |
| [inst/report/](inst/report/) | Rmd reports + `generate_*.R` drivers, shapefiles, bib, css |
| [inst/export/](inst/export/) | Dataverse dataset metadata (README.Rmd, dataset-fields.json, PNGs) |
| [.github/workflows/](.github/workflows/) | 11 workflows — **only 2 are healthy**, see below |
| `peskas.mozambique.data.pipeline/` | untracked reference copy of the standard; read constantly, copy sparingly. Removed in migration Phase 11 |

## Module map (`R/`)

Ingestion
- [ingest-landings.R](R/ingest-landings.R) — `ingest_landings_v1v3()`, `ingest_landings_v2()`
- [retrieve-survey-data.R](R/retrieve-survey-data.R) — Timor's own `get_kobo_data()` (writes files to disk) plus `flatten_row()`/`flatten_field()`/`rename_child()`
- [ingest-metadata-tables.R](R/ingest-metadata-tables.R) — 15 Google Sheets metadata tables. Six of them are superseded in Phase 3 by the Airtable frame (see below); five stay
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
- [get-cloud-files.R](R/get-cloud-files.R) — 13 `get_*` accessors over one internal `download_versioned_rds()` helper. Timor's own GCS layer (`cloud-storage.R`, `google-drive.R`) was **deleted in Phase 2**; everything delegates to `coasts::*`
- [utils.R](R/utils.R) — `add_version()`, `read_config()`, `load_dotenv()`
- [peskas.timor.data.pipeline-package.R](R/peskas.timor.data.pipeline-package.R) — package-level roxygen block
- [utils-pipe.R](R/utils-pipe.R), [utils-tidy-eval.R](R/utils-tidy-eval.R), [globals.R](R/globals.R)

## Configuration

`inst/config.yml`, read via `read_config()`, which calls `load_dotenv()` and then
`config::get(config = Sys.getenv("R_CONFIG_ACTIVE", "default"))`.

Two environments — the `local:` environment was deleted in Phase 1:

| env | storage buckets | api / hub buckets |
|---|---|---|
| `default` | `timor-dev`, `pds-timor-dev`, `public-timor-dev` | `peskas-api-dev`, `peskas-coasts-dev` |
| `production` | `timor`, `pds-timor`, `public-timor` | `peskas-api-prod`, `peskas-coasts` |

Secrets come from the environment in both cases: from `.env` locally (loaded by
`load_dotenv()`), from the workflow environment in CI. **Local and CI now
resolve the same config branch and differ only by `R_CONFIG_ACTIVE`.**
`.Renviron` sets `default`. CI sets `R_CONFIG_ACTIVE=production` only on `main`;
**any push to a non-main branch runs the whole pipeline against the `-dev`
buckets.** That is the integration-test mechanism for the migration.

The file is a **superset**: harmonized keys (`country`, `ingestion`,
`surveys.landings.{v1,v2,v3}`, `api`, `storage.google.options_{coasts,api}`)
sit alongside every legacy key the current functions still read
(`surveys.landings_{1,2,3}`, `pds.{trips,tracks}`, `models`, `export*`, …).
Legacy keys are marked `# [legacy]` and are deleted in Phase 11. Do not remove
one before the phase that removes its last reader.

`storage.mongodb` is **declared and inert** since Phase 2. It has no reader yet
— the validation flags sink decision is Phase 5. It was safe to declare only
because Phase 2 narrowed the six call sites that used to do
`purrr::map(pars$storage, ~ upload_cloud_file(files, .$key, .$options))`, which
treated every child of `storage` as a storage *provider*. **Nothing walks the
children of `storage` or `pds_storage` any more; keep it that way** — always
address a provider explicitly as `pars$storage$google`.

### Environment variables

Copy [.env.example](.env.example) to `.env` and fill it in. Both JSON-valued
entries must be minified onto one line — dotenv parses line by line.

| env var | GitHub secret | notes |
|---|---|---|
| `KOBO_USERNAME` / `KOBO_PASSWORD` | same | basic auth against `eu.kobotoolbox.org` |
| `KOBO_TOKEN` | *not set in CI yet* | token auth, used by the new `ingestion` block |
| `KOBO_ASSET_ID_V1/2/3` | *not set in CI yet* | new names, read by `ingestion.landings.<v>` |
| `KOBO_PESKAS1/2/3` | same | legacy names, read by `surveys.landings_{1,2,3}`; secrets renamed in Phase 9 |
| `GCP_SA_KEY` | `PESKAS_DATAINGESTION_GCS_KEY` | full service-account JSON, minified |
| `GOOGLE_SHEET_ID` | same | metadata tables |
| `VALID_SHEET_ID` | same | validation flags sheet |
| `PDS_TOKEN` / `PDS_SECRET` | `PESKAS_PDS_TOKEN` / `PESKAS_PDS_SECRET` | |
| `DATAVERSE_TOKEN` | `PESKAS_DATAVERSE_TOKEN` | |
| `PESKAS_GMAIL_KEY` | same | the serialized blastula credentials JSON, **not** a bare app password |
| `AIRTABLE_TOKEN` | same | the **bare** `pat…` — coasts prepends `Bearer `. Needs frame-base read access + `schema.bases:read` |
| `AIRTABLE_BASE_ID_FRAME` | same | PESKAS \| FRAME, `appMMEJYlJdfSJEjm` |

**Before Phase 3 ingestion can run in CI**, `KOBO_ASSET_ID_V1/2/3` must be
added to the workflow `env:` block — the existing `KOBO_PESKAS*` secrets can
supply the values. `KOBO_TOKEN` has no secret yet and is optional (basic auth
works). The stale `AIRTABLE_KEY` secret still exists and is deleted in Phase 9;
nothing maps it any more.

### Reference data — two sources, one of them authoritative

There are **two unrelated Airtables** in this repo's history; do not conflate
them.

1. `R/airtable.R` — Timor's own `air_*` client, orphaned (it read
   `validation.airtable.*`, a key that no longer exists). **Deleted in
   Phase 1**, with its only consumer `ingest_validation_tables()` and
   `inst/airtable/edit-submission-link.js`.
2. **PESKAS | FRAME** (`appMMEJYlJdfSJEjm`) — the cross-country harmonization
   layer, mapping each country's raw form labels to `standard_name` /
   `alpha3_code` / FAO codes. `coasts::ingest_assets()` snapshots it to
   `assets__*.rds` and the rest of the coasts pipeline reads that snapshot.
   Timor's rows were populated 2026-07-30/31 (57 taxa, 7 gears, 2 vessels, 40
   sites, 457 pds_devices). Adopted in Phase 3 by calling `coasts::` directly
   — Timor keeps **no** local copy of the Airtable module, unlike Mozambique,
   so its config uses the hub's key paths (`airtable.token`,
   `airtable.frame.base_id`) rather than Moz's `metadata.airtable.*`.

Where the frame and the Google Sheets tables overlap, **Airtable is
authoritative** (PLAN §2.5): taxa, gears, vessels, landing_sites,
districts/regions, pds_devices. The Sheets keep only `morphometric_table`,
`habitat`, `conservation`, `fishing_vessel_statistics`, `registered_boats`.
This is not cosmetic — without it the Phase 6 API export would be
schema-correct but full of untranslated Tetum labels.

## Storage

Google Cloud Storage, project `peskas`, service account
`data-ingestion@peskas.iam.gserviceaccount.com` (it can read/write objects but
**cannot** `storage.buckets.list`).

Versioned object naming, from `add_version()`:

```
<prefix>__<YYYYMMDDHHMMSS>_<git-sha7>__.<ext>
```

The sha comes from `git2r::sha(git2r::last_commit())`, falling back to
`$GITHUB_SHA` inside containers.
`coasts::cloud_object_name(version = "latest")` resolves the newest.

### The layer is `coasts::*` (since Phase 2)

`R/cloud-storage.R` and `R/google-drive.R` are gone. Every call site is
namespaced `coasts::{cloud_storage_authenticate, upload_cloud_file,
download_cloud_file, cloud_object_name}`. **Always write the `coasts::` prefix**
— Timor no longer exports these names, so an unqualified call would resolve off
the search path or not at all.

Two things Timor keeps, deliberately:

- **`insistent_upload_cloud_file()` / `insistent_download_cloud_file()`** —
  `purrr::insistently()` retry wrappers used on the PDS paths. `coasts` has no
  retry logic anywhere; these are an upstream candidate for Phase 10.
- **`add_version()`** — a naming helper, not a storage function. `coasts` exports
  a body-identical copy, but delegating would mean editing ~40 call sites plus
  three `inst/report/` drivers to remove an exported name for no behavioural
  gain. Dedupe when it is upstreamed, not before.

**`coasts::cloud_object_name()` is not a drop-in for Timor's deleted version.**
The signatures match, but coasts returns `selected_rows$name[1]` where Timor
returned the whole vector. Every Timor call site was audited against the dev
buckets and only one relied on the vector: the track enumeration in
`ingest_pds_tracks()`, which needs ~96k names. It now calls
`googleCloudStorageR::gcs_list_objects()` directly — a bucket scan, which is
what it always was. Never use `cloud_object_name()` to enumerate a bucket.

Prefer `coasts::resolve_storage_opts(pars, type)` over reaching into
`pars$storage$google$options_*` by hand. It knows `"coasts"` (hub, falling back
to `options`), `"country"` and `"pds"` — but **not** Timor's `public_storage`,
which is read directly in `get_public_files()` / `get_tracks_map()`.

| bucket | contents |
|---|---|
| `timor` / `timor-dev` | surveys and derived tables: raw `.csv`, everything downstream `.rds` |
| `pds-timor` / `pds-timor-dev` | one gzipped CSV per GPS trip: `pds-track-<trip_id>__*__.csv.gz` |
| `public-timor` / `public-timor-dev` | `portal-*.json` — the live portal contract |
| `peskas-coasts` / `peskas-coasts-dev` | the shared cross-country hub (`options_coasts`). **Read *and* written** by coasts: `assets__*`, `taxa-fishbase-enriched`, H3 effort/CPUE grids, and per-country `*_fishery_metrics` / `*_monthly_summaries_map`. Both are live — `default` must stay on `-dev` |
| `peskas-api-prod` / `peskas-api-dev` | cross-country API parquet (`options_api`), live for Kenya/Moz/Zanzibar; Timor lands in Phase 6 |

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

`Dockerfile.prod`: `rocker/geospatial:4.5`, packages via `install2.r`
(`rfishbase` unpinned — the old 5.0.1 pin conflicted with `coasts`),
`ARG COASTS_REF=v4.5.0` → `install_github('WorldFishCenter/peskas.coasts')`,
`ggchicklet` + `glmmTMB` from GitHub, then `COPY . /home` +
`remotes::install_local()`. Image is pushed to
`ghcr.io/worldfishcenter/peskas.timor.data.pipeline/r-runner-peskas-timor:latest`.
`Dockerfile` (dev, used by `docker-compose.yaml`) mirrors the same package set
and the same `COASTS_REF`. Keep the two in step.

**`devtools::check()` baseline** (measured 2026-07-31 against the Phase 0 and
Phase 1 commits, and again after Phase 2): 1 WARNING (undocumented
`get_kobo_data()` arguments), 5 NOTEs, and one pre-existing testthat failure —
`test-pre-process-landings.R:16`, `nrow(nested$_attachments[[1]])` is 3, not 2
(`FAIL 1 | WARN 9 | SKIP 0 | PASS 9`; the 9 warnings are all `.data`-in-
tidyselect deprecations from `pt_nest_*`). Do not read those as a regression.

The unused-Imports NOTE now names **`arrow` only** — `coasts` dropped out of it
in Phase 2 when the storage calls became real `coasts::*` calls. That NOTE is
the canary for whether `coasts` is genuinely wired in; if `coasts` ever
reappears in it, the delegation has been undone. `arrow` clears in Phase 3/4
when parquet lands.

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
- `pars$...$version$preprocess: latest` is read on `landings_1/2/3`,
  `pds.trips`, `pds.tracks`, `metadata` and `validation`. The unified template
  dropped this field; it is kept on each of those legacy keys and deliberately
  **not** re-added in the new tree, which uses the per-stage `version:` field.
  Every reader is a legacy-key reader and moves across in Phases 3–7.
- Timezone handling was fixed in commit `15f6b18` — re-verify it after any
  rewrite of the export path.
- **Known live bugs** (AUDIT.md §8). Fixed in Phase 1: the duplicate
  `get_preprocessed_metadata()` in `validate-landings.R` that shadowed the
  correct definition at [get-cloud-files.R:110](R/get-cloud-files.R#L110), and
  the `local:` config env inheriting a non-existent `development` env. Still
  open: two dead helpers in
  [validate-landings.R](R/validate-landings.R) (`get_validation_tables()`,
  `get_preprocessed_landings()`) read config keys that no longer exist — they
  are uncalled, and are deleted in Phase 11;
  `ingest_rfish_table()` is `continue-on-error` despite being a hard dependency
  two jobs later (Phase 9). **AUDIT §8.5 is stale**: `export_files()` already
  passes basenames as `name` and normalises correctly — verified in Phase 2. The
  45 leaked absolute-path objects in `public-timor` date from January 2026 and
  are historical residue, not a live bug. Deleting them is Phase 11.
- Tests are Timor's advantage over the other pipelines. **Never delete an
  assertion to make a change pass** — update the expectation deliberately.
