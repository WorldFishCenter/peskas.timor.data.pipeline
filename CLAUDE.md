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
> **0, 1, 2, 3, 4, 5** — so config, secrets, the container, the **storage
> layer**, **ingestion**, **preprocessing** and **validation** are already on
> the standard; the API export, PDS and the portal are not.

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
| [R/](R/) | 29 source files, verb-noun naming (see module map below) |
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
- [ingestion.R](R/ingestion.R) — `ingest_landings()` (v2 + v3 → raw parquet via `coasts::get_kobo_data()`), `ingest_assets()` (the Airtable frame snapshot) and the `flatten_row()`/`flatten_field()`/`rename_child()` helpers. Added in Phase 3, replacing `ingest-landings.R` and `retrieve-survey-data.R`
- [ingest-metadata-tables.R](R/ingest-metadata-tables.R) — 14 Google Sheets metadata tables. Phase 4 moved the taxa/gear/vessel/site joins onto the Airtable frame but the validators still keep their own Sheets copies, so only `fao_catch` could be dropped; each remaining table is annotated in `inst/config.yml` with the phase that removes it
- [ingest-pds-data.R](R/ingest-pds-data.R) — 858 lines: PDS trips + tracks, kepler map, retry wrappers
- [retrieve-pds-data.R](R/retrieve-pds-data.R) — PDS API client

Preprocessing
- [preprocessing-surveys.R](R/preprocessing-surveys.R) — `preprocess_landings(versions = c("v2","v3"))` (raw parquet → **flat long catch parquet**), `merge_landings()`, the per-version `harmonise_*()` reconciliation, and the assets-snapshot label joins (`survey_labels()`). Added in Phase 4, replacing `clean-raw-data.R`, `preprocess-landings.R` (`step_1`/`step_2`) and `merge-landings.R`
- [survey-reshaping.R](R/survey-reshaping.R) — `reshape_species_groups()`, `expand_length_frequency()`, the bin-midpoint and free-text-trim helpers. Replaces `pt_nest_species.R` / `pt_nest_attachments.R`
- [model-taxa.R](R/model-taxa.R) — `calculate_weights()` / `join_weights()`, morphometric length-weight via `coasts::get_taxa_morphometrics()`; taxa list from the assets snapshot. Renamed from `calculate-weights.R` in Phase 4. Phase 5 deleted the re-nesting from `join_weights()`, so the weight artefact is **flat long parquet** like every stage before it
- [preprocess-metadata-tables.R](R/preprocess-metadata-tables.R) — the Google Sheets `pt_validate_*` parsers

Validation
- [validation.R](R/validation.R) — `validate_landings()` orchestrator, the MongoDB flags sink (`push_validation_flags()`), and `sync_validation_status()` (the KoBo write-back, deliberately not wired into the pipeline). Renamed from `validate-landings.R` in Phase 5
- [validation-functions.R](R/validation-functions.R) — 16 validators over the long table plus the KoBo `get_validation_status()` / `update_validation_status()` pair ported from Mozambique. **This is deeper than any other country pipeline — preserve it.** The alert codes are the contract; the file opens with the code → validator table and `inst/config.yml`'s `validation.alerts` block carries the descriptions
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
— ~~the validation flags sink decision is Phase 5~~; `validation.*` is live
since Phase 5, `pipeline.*` still has no reader. It was safe to declare only
because Phase 2 narrowed the six call sites that used to do
`purrr::map(conf$storage, ~ upload_cloud_file(files, .$key, .$options))`, which
treated every child of `storage` as a storage *provider*. **Nothing walks the
children of `storage` or `pds_storage` any more; keep it that way** — always
address a provider explicitly as `conf$storage$google`.

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
| `VALID_SHEET_ID` | same | ~~validation flags sheet~~ — no reader since Phase 5, dropped in Phase 11 |
| `MONGODB_CONNECTION_STRING_VALIDATION` | *not set in CI yet* | the flags sink. Absent → `validate_landings()` warns and only the GCS snapshot is written |
| `PDS_TOKEN` / `PDS_SECRET` | `PESKAS_PDS_TOKEN` / `PESKAS_PDS_SECRET` | |
| `DATAVERSE_TOKEN` | `PESKAS_DATAVERSE_TOKEN` | |
| `PESKAS_GMAIL_KEY` | same | the serialized blastula credentials JSON, **not** a bare app password |
| `AIRTABLE_TOKEN` | same | the **bare** `pat…` — coasts prepends `Bearer `. Needs frame-base read access + `schema.bases:read` |
| `AIRTABLE_BASE_ID_FRAME` | same | PESKAS \| FRAME, `appMMEJYlJdfSJEjm` |

`KOBO_ASSET_ID_V1/2/3` are mapped in the workflow `env:` block since Phase 3,
fed from the existing `KOBO_PESKAS*` secrets; the secrets themselves are
renamed in Phase 9. `KOBO_TOKEN` has no secret and is optional — `ingestion`
uses basic auth. The stale `AIRTABLE_KEY` secret still exists and is deleted in
Phase 9; nothing maps it any more.

**Never log the resolved config.** `read_config()` used to end with
`logger::log_debug("Running with parameters {pars}")`, and every workflow
function defaults to `log_threshold = logger::DEBUG` — so each CI job printed
the service-account private key, the Airtable PAT, the Dataverse token and the
blastula credentials into its log. GitHub Actions only masks byte-exact matches
of a registered secret, which the re-serialised JSON is not. Removed in
Phase 3; it logs the key names only.

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
   Timor's rows, re-measured 2026-08-09: **60 taxa over 56 distinct
   `alpha3_code`s** (matching `models.all_taxa` exactly), **9 gears**, 2
   vessels, 40 sites, 457 pds_devices; `survey_label` populated everywhere.
   Adopted in Phase 3 by calling `coasts::` directly — Timor keeps **no** local
   copy of the Airtable module, unlike Mozambique, so its config uses the hub's
   key paths (`airtable.token`, `airtable.frame.base_id`) rather than Moz's
   `metadata.airtable.*`.

`ingest_assets()` ([ingestion.R](R/ingestion.R)) writes the snapshot and
`get_assets()` reads it back from the **hub** bucket. Two things to know:

- The snapshot is **cross-country and carries no `country` column** — 1,609
  taxa rows across four countries, and every one of Timor's 56 codes is also
  used by another country, two of them against a different `scientific_name`.
  Always narrow with `timor_assets(x, pars)`, which filters on
  `metadata.airtable.form_ids` (the record ids of Timor's two KoBo forms — the
  only column in the snapshot that separates the countries). See COASTS-TODO
  C13.
- coasts uploads it to the **country** bucket while every reader resolves the
  **hub**, so `ingest_assets()` mirrors it to the hub afterwards. Remove that
  second upload when COASTS-TODO C11 ships.

Where the frame and the Google Sheets tables overlap, **Airtable is
authoritative** (PLAN §2.5): taxa, gears, vessels, landing_sites,
districts/regions, pds_devices. Phase 3 moved the first of those joins —
`get_taxa_list()` now reads the snapshot instead of `catch_types` + `fao_catch`
— and the rest move in Phase 4, so `metadata.google_sheets.tables` still lists
all 15. The Sheets ultimately keep only `morphometric_table`, `habitat`,
`conservation`, `fishing_vessel_statistics`, `registered_boats` — plus, for
now, `catch_types`, the only source of the per-taxon `length_type` for the five
invertebrate measures (COASTS-TODO C14).
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
`conf$storage$google$options_*` by hand. Since coasts 4.6.0 it knows
`"coasts"` (hub, falling back to `options`), `"country"`, `"pds"` and
`"public"`.

| bucket | contents |
|---|---|
| `timor` / `timor-dev` | surveys and derived tables. Raw, preprocessed, merged and the frozen v1 snapshot are all **parquet** since Phase 4 (`timor-landings-v{2,3}_{raw,preprocessed}__*.parquet`, `timor-landings-merged__*.parquet`, `timor-landings-v1-frozen__*.parquet`); the weight and validated artefacts are still `.rds` because they carry nested list-columns |
| `pds-timor` / `pds-timor-dev` | one gzipped CSV per GPS trip: `pds-track-<trip_id>__*__.csv.gz` |
| `public-timor` / `public-timor-dev` | `portal-*.json` — the live portal contract |
| `peskas-coasts` / `peskas-coasts-dev` | the shared cross-country hub (`options_coasts`). **Read *and* written** by coasts: `assets__*`, `taxa-fishbase-enriched`, H3 effort/CPUE grids, and per-country `*_fishery_metrics` / `*_monthly_summaries_map`. Both are live — `default` must stay on `-dev` |
| `peskas-api-prod` / `peskas-api-dev` | cross-country API parquet (`options_api`), live for Kenya/Moz/Zanzibar; Timor lands in Phase 6 |

**No lifecycle policy is set.** Every pipeline run appends new versions and
nothing is ever deleted; `gs://timor` holds ~33k objects and `gs://pds-timor`
~98k. See [.claude/migration/AUDIT.md](.claude/migration/AUDIT.md) for the full
prefix inventory including orphaned prefixes.

Interchange format is **flat long parquet** from raw through validated
(Phases 3, 4 and 5): one row per (submission, catch, length bin). The only
remaining `.rds` artefact on the survey path is
`timor-landings-merged_validated__*.rds`, the **nested** shape the portal reads,
and Phase 5 writes the long parquet `timor-landings-merged_validated_long__*`
beside it. Phase 6 consumes the long one; Phase 8 drops the nested one once
`format_public_data()` reads the long shape.

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
├── ingest-preprocess-metadata-tables   ingest_metadata_tables → preprocess_metadata_tables → ingest_assets
├── ingest-landings                     ingest_landings                 [v2 + v3 → raw parquet]
│   └── preprocess-landings             preprocess_landings   [v2 + v3 → long parquet]
└── ingest-pds-data                     ingest_pds_trips → ingest_pds_tracks
    └── preprocess-pds-data             preprocess_pds_trips → preprocess_pds_tracks
        └── validate-pds-data           validate_pds_trips        [tinytest]

merge-landings   merge_landings → calculate_weights
└── validate-landings  validate_landings         [flags → MongoDB, tinytest]
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

**`devtools::check()` baseline** (re-measured after Phase 4): **0 errors,
0 WARNINGs, 4 NOTEs**, and testthat is now **green**. The long-standing
`FAIL 1 | WARN 9 | PASS 9` is gone: `tests/testthat/test-pre-process-landings.R`
tested `pt_nest_attachments()` / `pt_nest_species()`, both deleted in Phase 4,
and was replaced by `test-survey-reshaping.R` (8 passing assertions over
`reshape_species_groups()` and `expand_length_frequency()`). The 9 warnings were
`.data`-in-tidyselect deprecations from `pt_nest_*` and went with it. **No
assertion was weakened to get there** — the code under test no longer exists.

Two things moved in Phase 3 and both are improvements, not drift:

- The single WARNING was "undocumented `get_kobo_data()` arguments". That
  function was Timor's own KoBo client and is gone — retrieval delegates to
  `coasts::get_kobo_data()`.
- The **unused-Imports NOTE has disappeared entirely**. It named `arrow` after
  Phase 2 and would have named `arrow` + `httr2` after Phase 3, because parquet
  I/O and KoBo retrieval both live in `coasts` now and no `R/` file references
  either namespace. Both were dropped from `Imports`. Note that this means the
  NOTE is **no longer the canary** for whether `coasts` is wired in; if the
  delegation were ever undone, `coasts` would reappear in it and the note would
  come back.

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
- **The preprocessed table is one row per (submission, catch, length bin)**, and
  it is a **superset**: the standard columns (`submission_id`, `landing_date`,
  `gaul_*`, `landing_site`, `n_fishers`, `trip_duration`, `gear`,
  `vessel_type`, `habitat`, `catch_outcome`, `n_catch`, `catch_taxon`,
  `scientific_name`, `length`, `catch_price`, …) sit beside every raw KoBo
  column, reconciled per form version by `harmonise_v2()` / `harmonise_v3()`.
  Nothing reads the raw columns since Phase 5 — validation moved onto the
  standard names — and they go with the rest of the legacy passthrough in
  Phase 11.
- **Length bins with no count are kept.** A catch expands to one row per 5 cm
  bin whether or not anybody was counted in it, because that is what the nested
  `length_frequency` has always held and it reaches the portal —
  `timor_catch` is 1.7 M rows of which 93% have no weight. Dropping them would
  change a published table.
- **v1 is frozen** (last submission 2020-08-28). It is not ingested and not
  preprocessed; `merge_landings()` reads
  `timor-landings-v1-frozen__*.parquet`, produced once per environment by
  [data-raw/freeze-landings-v1.R](data-raw/freeze-landings-v1.R). The freeze
  also converted v1's **fork lengths to total length**, so every source now
  carries TL and `join_weights()` no longer branches on `survey_version`.
  `summarise_ll_coeffs()` and `normalise_length_to_tl()` were deleted with it;
  the length-length logic now lives **inside the freeze script**, its only
  consumer. Phase 4 also moved v1's column reconciliation and its flattening to
  the long shape into that script, deliberately — the form is dead and
  `preprocess_landings()` should not carry a shape nothing will produce again.
  **The snapshot exists in `timor-dev` only** — run the script against
  `production` before the Phase 11 cutover.
- **`length_type` is descriptive, not functional.** It is not a survey field —
  the form records only counts per length bin, and `mean_length` is the bin
  midpoint. It comes from the Sheets `catch_types`, per taxon, non-`NA` for
  five invertebrates (`SLV` CL, `OCZ` ML, `IAX` ML, `CRA` CW, `COZ` ShL). Field
  practice measures those on total length (confirmed 2026-08-10), which is what
  the `OCZ`/`SLV`/`IAX`/`MOO` overrides encode, so nothing is converted and
  nothing is selected by it. If that ever changes, the place to act is
  `summarise_lw_coeffs()` — it pools coefficients across every measurement axis
  (`SLV` = 34 carapace-length studies + 19 total-length), and filtering each
  declaring taxon to its own axis would move national catch weight by +0.63%.
- Variables holding the resolved configuration are named **`conf`**, matching
  the other country pipelines. The old `pars` was renamed throughout in
  Phase 3; do not reintroduce it.
- ~~`get_raw_landings()` coerces the raw parquet to all-character~~ — deleted in
  Phase 4. Preprocessing reads the typed parquet directly. The trim it used to
  reproduce now happens deliberately in `trim_free_text()`, which also strips
  the stray leading and trailing newlines ~60 free-text answers carry.
- `conf$...$version$preprocess: latest` is read on `landings_1/2/3`,
  `pds.trips`, `pds.tracks` and `metadata` — `validation` lost its reader in
  Phase 5. The unified template dropped this field; it is kept on each of those
  legacy keys and deliberately **not** re-added in the new tree, which uses the
  per-stage `version:` field. Every reader is a legacy-key reader and moves
  across in Phases 3–7.
- Timezone handling was fixed in commit `15f6b18` — re-verify it after any
  rewrite of the export path.
- **Known live bugs** (AUDIT.md §8). Fixed in Phase 1: the duplicate
  `get_preprocessed_metadata()` in `validate-landings.R` that shadowed the
  correct definition at [get-cloud-files.R:110](R/get-cloud-files.R#L110), and
  the `local:` config env inheriting a non-existent `development` env. Fixed in
  Phase 5: `validate_catch_params()`'s positional assignment of
  `length_individuals` into a separately-derived frame, and the `isTRUE()` on a
  vector that made alerts 12–15 unreachable (both rewrites are alert-identical
  on the current data — see the STATE Phase 5 entry). ~~Still open: two dead
  helpers in `validate-landings.R`~~ — removed with the file in Phase 5;
  ~~`ingest_rfish_table()` is `continue-on-error`~~ — moot, the function and
  its workflow step are gone. **AUDIT §8.5 is stale**: `export_files()` already
  passes basenames as `name` and normalises correctly — verified in Phase 2. The
  45 leaked absolute-path objects in `public-timor` date from January 2026 and
  are historical residue, not a live bug. Deleting them is Phase 11.
- Tests are Timor's advantage over the other pipelines. **Never delete an
  assertion to make a change pass** — update the expectation deliberately.
