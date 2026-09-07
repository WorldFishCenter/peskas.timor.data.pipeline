# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this
repository.

> **This repo is mid-migration.** It is being aligned to the harmonized
> Peskas standard (shared `peskas.coasts` hub, `.env` secrets, parquet
> interchange, cross-country API). Before doing anything substantive,
> read in order:
> [.claude/migration/PLAN.md](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.claude/migration/PLAN.md),
> [.claude/migration/STRUCTURAL-DIFF.md](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.claude/migration/STRUCTURAL-DIFF.md),
> [.claude/migration/AUDIT.md](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.claude/migration/AUDIT.md),
> [.claude/migration/STATE.md](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.claude/migration/STATE.md),
> [.claude/migration/ALIGNMENT-AUDIT.md](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.claude/migration/ALIGNMENT-AUDIT.md).
> **One migration phase per session — never two.** End every session by
> appending a STATE.md entry.
>
> **`ALIGNMENT-AUDIT.md` (2026-08-18) corrects five claims made below
> and in `AUDIT.md`. Trust it over this file where they disagree.** The
> five:
> [`merge_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_trips.md)
> **does** have cross-country counterparts (Mozambique’s is
> line-for-line identical, and writes parquet); the 59-column raw KoBo
> passthrough has a live reader (`enumerators_summary.Rmd`, which runs
> every pipeline run) and is **not** deletable; the frame’s
> `pds_devices` is **not** a strict subset of the Sheets `devices`;
> `centro_pescas` contained **no** lat/lon and had no reader at all; and
> [`timor_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/timor_assets.md)
> cannot be swapped onto the snapshot’s `country` column, which is
> absent on `sites` and a record-id link on `geo` (COASTS-TODO C24).
>
> Everything below documents the repo **as it is today**, not the target
> state. Where the target differs, the plan says so. Phases completed so
> far: **0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11a** — so config, secrets,
> the container, the **storage layer**, **ingestion**,
> **preprocessing**, **validation**, the **cross-country API export**,
> **PDS**, the **country modules + portal parity**, **CI, repo metadata
> and docs**, the **upstreaming to coasts** and now the **legacy
> cleanup** are done. What is left is the **cutover (Phase 11b: freeze
> v1, convert the prod tracks, merge to `main`, first production run)**,
> then the static-asset and label-source work the alignment audit scoped
> as **Phase 12**.
>
> **Phase 11a deleted ~2,900 lines and changed no published number.**
> The KoBoToolbox validation-status functions now come from `coasts::`
> (4.7.0, C15);
> [`ingest_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/ingest_assets.md)
> no longer mirrors the snapshot to the hub (C11); the second glmmTMB
> estimator, the PDS map products and eleven other unreferenced
> functions are gone, with the five Sheets tables that had no reader and
> every `# [legacy]` config key. It touched no production bucket.
> **Phase 11b is the cutover and is a separate session** — see
> [.claude/migration/PROMPT-PHASE11B.md](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.claude/migration/PROMPT-PHASE11B.md).

------------------------------------------------------------------------

## Project overview

R package implementing the Peskas Timor-Leste small-scale-fisheries data
pipeline. It ingests KoBoToolbox landing surveys and Pelagic Data
Systems (PDS) GPS tracker data, preprocesses and validates them, models
fishery indicators, and publishes JSON to a public GCS bucket consumed
by the live portal (`peskas.timor.portal.v2`). It also publishes
datasets to Harvard Dataverse and emails reports.

Timor was the **first** Peskas pipeline and predates the conventions the
other country pipelines share. Expect divergence from Mozambique / Kenya
/ Zanzibar to be structural, not cosmetic.

## Repo layout

| Path | What |
|----|----|
| [R/](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/) | 25 source files, verb-noun naming (see module map below) |
| [inst/config.yml](https://worldfishcenter.github.io/peskas.timor.data.pipeline/inst/config.yml) | the config file. Harmonized keys only since Phase 11 — every `# [legacy]` key is gone |
| [inst/config_template.yml](https://worldfishcenter.github.io/peskas.timor.data.pipeline/inst/config_template.yml) | Timor’s copy of the cross-country spec, with its deviations recorded |
| `.env` | local secrets, gitignored. Template: [.env.example](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.env.example). Replaced the old `auth/` directory in Phase 1 |
| [inst/extdata/](https://worldfishcenter.github.io/peskas.timor.data.pipeline/inst/extdata/) | packaged reference data. One file: `morphometric-coefficients.csv`, Timor’s 559 curated length-weight rows, read by `curated_lw_coeffs()`. Note `.gitignore` and `.Rbuildignore` both blanket-exclude `*.csv` and both carry an exception for this directory — check them before adding a file here |
| [inst/tinytest/](https://worldfishcenter.github.io/peskas.timor.data.pipeline/inst/tinytest/) | 4 assertion suites, run as steps **inside** the pipeline workflow |
| [inst/report/](https://worldfishcenter.github.io/peskas.timor.data.pipeline/inst/report/) | Rmd reports + `generate_*.R` drivers, shapefiles, bib, css |
| [inst/export/](https://worldfishcenter.github.io/peskas.timor.data.pipeline/inst/export/) | Dataverse dataset metadata (README.Rmd, dataset-fields.json, PNGs) |
| [.github/workflows/](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.github/workflows/) | 9 workflows since Phase 9, see below |

## Module map (`R/`)

Ingestion -
[ingestion.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/ingestion.R)
—
[`ingest_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/ingest_landings.md)
(v2 + v3 → raw parquet via
[`coasts::get_kobo_data()`](https://rdrr.io/pkg/coasts/man/get_kobo_data.html)),
[`ingest_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/ingest_assets.md)
(the Airtable frame snapshot) and the
`flatten_row()`/`flatten_field()`/`rename_child()` helpers. Added in
Phase 3, replacing `ingest-landings.R` and `retrieve-survey-data.R` -
[ingest-metadata-tables.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/ingest-metadata-tables.R)
— **6** Google Sheets metadata tables, from twelve. Phase 4 moved the
taxa/gear/vessel/site joins onto the Airtable frame; Phase 11 dropped
the five with no reader left (`vms_installs`, `centro_pescas`, `boats`,
`fishing_vessel_statistics`, `registered_boats`), and the 2026-09-05
taxa-path session dropped `morphometric_table` — its 559 curated rows
are package data now, which is what took the last Google Sheet off the
weight path. The six that stay — `devices`, `catch_types`, `stations`,
`reporting_units`, `habitat`, `conservation` — all have a live reader
and each is annotated in `inst/config.yml` with what blocks it. Moving
them is **Phase 12**, and every one is blocked on Airtable *data*, not
code (ALIGNMENT-AUDIT §0) - **PDS has no Timor ingestion code since
Phase 7.** `ingest-pds-data.R`, `retrieve-pds-data.R` and
`preprocess_pds_trips()` are gone; the workflow calls
[`coasts::ingest_pds_trips()`](https://rdrr.io/pkg/coasts/man/ingest_pds_trips.html)
and
[`coasts::ingest_pds_tracks()`](https://rdrr.io/pkg/coasts/man/ingest_pds_tracks.html)
with `package = "peskas.timor.data.pipeline"`, exactly as Mozambique,
Kenya and Zanzibar do — none of them carries a line of PDS code either.
Everything coasts needs is in `conf$pds`. The third call those three
also make,
[`coasts::preprocess_pds_tracks()`](https://rdrr.io/pkg/coasts/man/preprocess_pds_tracks.html),
is **not** wired in, and Phase 8 decided to keep it that way: its output
feeds
[`coasts::summarize_data()`](https://rdrr.io/pkg/coasts/man/summarize_data.html),
which Timor does not use (its portal is the JSON contract) and which is
blocked for Timor anyway on COASTS-TODO C17 — so it would produce ~1.4 M
grid rows per run for no reader. C20 (its first pass reads every track
with `detectCores() - 1` workers, one on a CI runner) is the second
reason, not the first. Wire it in when C17 ships **and** a Timor
consumer exists

Preprocessing -
[preprocessing-surveys.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/preprocessing-surveys.R)
— `preprocess_landings(versions = c("v2","v3"))` (raw parquet → **flat
long catch parquet**),
[`merge_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_landings.md),
the per-version `harmonise_*()` reconciliation, and the assets-snapshot
label joins
([`survey_labels()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/survey_labels.md)).
Added in Phase 4, replacing `clean-raw-data.R`, `preprocess-landings.R`
(`step_1`/`step_2`) and `merge-landings.R` -
[survey-reshaping.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/survey-reshaping.R)
—
[`reshape_species_groups()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/reshape_species_groups.md),
[`expand_length_frequency()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/expand_length_frequency.md),
the bin-midpoint and free-text-trim helpers. Replaces
`pt_nest_species.R` / `pt_nest_attachments.R` -
[model-taxa.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/model-taxa.R)
—
[`calculate_weights()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/calculate_weights.md)
/
[`join_weights()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/join_weights.md),
morphometric length-weight via
[`coasts::get_taxa_morphometrics()`](https://rdrr.io/pkg/coasts/man/get_taxa_morphometrics.html).
Renamed from `calculate-weights.R` in Phase 4. Phase 5 deleted the
re-nesting from
[`join_weights()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/join_weights.md),
so the weight artefact is **flat long parquet** like every stage before
it. Rewritten 2026-09-05 — see **The taxa and weight path** below. It
reads **no Google Sheet**: the taxon *codes* come from the frame, the
*names* from the FAO ASFIS list in the country bucket, the curated
invertebrate coefficients from `inst/extdata/`. Coefficients are
filtered to FAO areas **57 and 71**, and `assert_taxa_coverage()` fails
the run if any taxon but `MZZ`/`SWX` resolves to nothing -
[preprocess-metadata-tables.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/preprocess-metadata-tables.R)
— the seven surviving Google Sheets `pt_validate_*` parsers, most of
them pass-throughs -
[pds-tracks.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/pds-tracks.R)
—
[`describe_pds_tracks()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/describe_pds_tracks.md) +
[`get_tracks_descriptors()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_tracks_descriptors.md),
the per-trip track descriptors (`start_end_distance`,
`outliers_proportion`, `timetrace_dispersion`, start/end coordinates)
that
[`validate_pds_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_pds_trips.md)
joins on. **The one PDS product `coasts` has no equivalent for** —
[`coasts::preprocess_pds_tracks()`](https://rdrr.io/pkg/coasts/man/preprocess_pds_tracks.html)
emits spatial grid summaries instead, and both steps run. Renamed from
`preprocess-pds-trips.R` in Phase 7, which also deleted
`preprocess_pds_trips()`: the trips artefact is read typed and
Dili-local straight from the raw parquet by `get_pds_trips()`, so there
is no preprocessed-trips stage any more, as there never was in the WIO
repos

Validation -
[validation.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/validation.R)
—
[`validate_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landings.md)
orchestrator, the MongoDB flags sink
([`push_validation_flags()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/push_validation_flags.md)),
and
[`sync_validation_status()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/sync_validation_status.md)
(the KoBo write-back, deliberately not wired into the pipeline). Renamed
from `validate-landings.R` in Phase 5 -
[validation-functions.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/validation-functions.R)
— 16 validators over the long table. **This is deeper than any other
country pipeline — preserve it.** The KoBo validation-status client that
lived here from Phase 5 was upstreamed in Phase 10 (COASTS-TODO C15) and
deleted in Phase 11; `R/validation.R` calls
[`coasts::list_validation_statuses()`](https://rdrr.io/pkg/coasts/man/list_validation_statuses.html)
and
[`coasts::update_validation_status()`](https://rdrr.io/pkg/coasts/man/update_validation_status.html).
The alert codes are the contract; the file opens with the code →
validator table and `inst/config.yml`’s `validation.alerts` block
carries the descriptions -
[validate-pds-trips.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/validate-pds-trips.R)
—
[`validate_pds_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_pds_trips.md),
consecutive-trip merging, distance/outlier logic, plus `get_pds_trips()`
(the typed, Dili-local view of the raw trips parquet). No `coasts`
equivalent exists; upstream candidate for Phase 10

Merge / model / export -
[api.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/api.R)
—
[`export_api_raw()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_api_raw.md)
/
[`export_api_validated()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_api_validated.md),
the 22-column cross-country trips table written to
`peskas-api-{dev,prod}/timor/{raw,validated}`. Added in Phase 6 and
wired into `data-pipeline.yaml` in Phase 9,
`if: !endsWith(github.ref, '/main')` — so it publishes to `-dev` only,
and deleting those two lines is Timor’s first write to
`peskas-api-prod`. The schema is the contract Kenya, Mozambique and
Zanzibar already publish; do not add, drop or reorder a column without
agreeing it across all four -
[merge-trips.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/merge-trips.R),
[model-fishery.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/model-fishery.R)
(the Phase 8 concatenation of `estimate-catch.R` and `model-catch.R`;
[`estimate_fishery_indicators()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/estimate_fishery_indicators.md)
is the workflow entry point. `model_indicators()`, the second glmmTMB
implementation with no caller, and its 674-line subgraph went in Phase
11 — with `glmmTMB` itself, which nothing in the package imports any
more),
[nutrients.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/nutrients.R)
(nutrients + RDI, renamed from `calculate-nutrients.R`; **kept, not
delegated** — see the file header).
[`merge_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_trips.md)
matches a landing to a tracked trip on `(landing_date, tracker_imei)`
and is **not**
[`coasts::merge_survey_trips()`](https://rdrr.io/pkg/coasts/man/merge_survey_trips.html),
which does a different job (COASTS-TODO C10). Its output
`all_trips__*.rds` feeds
[`format_public_data()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/format_public_data.md)
and `model-fishery.R` — changing its schema breaks the export path. It
is a **full join**, so its 176,302 rows are the landings plus the
validated tracker trips minus the **6,940** actual landing↔︎trip matches
(6,999 before Phase 7’s frame device filter; the 59 come back when the
27 missing IMEIs are added to PESKAS \| FRAME) -
[format-public-data.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/format-public-data.R)
— 1200 lines, the largest file; builds every portal object. It reads
exactly two things,
[`get_merged_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_merged_trips.md)
and
[`get_models()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_models.md),
and Phase 8 left its body alone: the nested `landing_catch` /
`length_frequency` shape it works in is now produced on *read* by
[`get_validated_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_validated_landings.md)
rather than stored -
[pds-maps.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/pds-maps.R)
— two lookups,
[`get_timor_boundaries()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_timor_boundaries.md)
and
[`convert_taxa_names()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/convert_taxa_names.md),
both called by
[`format_public_data()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/format_public_data.md).
The map products it is named for are gone: Phase 8 stopped
[`export_files()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_files.md)
reading `indicators_gridded` and found `tracks-map.png` had never had a
reader, which left `ingest_pds_map()`, `ingest_kepler_tracks()`,
`kepler_mapper()` and `ingest_complete_tracks()` unreferenced, and
**Phase 11 deleted all four** with `inst/kepler_mapper.py` and the four
accessors that fed them (`get_sync_tracks()`, `get_full_tracks()`,
`get_full_trips()`, `get_tracks_map()`) -
[export.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/export.R)
—
[`export_files()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_files.md)
serializes and uploads the `portal-*.json` set: **seven objects since
Phase 8**, exactly the seven the portal consumes -
[export-dataverse.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/export-dataverse.R),
[reports.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/reports.R)
(renamed from `send-email.R`; templates in
[inst/report/](https://worldfishcenter.github.io/peskas.timor.data.pipeline/inst/report/))

Infrastructure -
[get-cloud-files.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/get-cloud-files.R)
— 9 `get_*` accessors over one internal `download_versioned_rds()`
helper, from 13: Phase 11 deleted `get_sync_tracks()`,
`get_full_tracks()`, `get_full_trips()`, `get_tracks_map()`,
`get_validation_flags()`, `get_tracks_ids()` and the duplicate
`get_preprocessed_metadata()`. Timor’s own GCS layer (`cloud-storage.R`,
`google-drive.R`) was **deleted in Phase 2**; everything delegates to
`coasts::*` -
[utils.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/utils.R)
—
[`add_version()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/add_version.md),
[`read_config()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/read_config.md),
[`load_dotenv()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/load_dotenv.md) -
[peskas.timor.data.pipeline-package.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/peskas.timor.data.pipeline-package.R)
— package-level roxygen block -
[utils-pipe.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/utils-pipe.R),
[utils-tidy-eval.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/utils-tidy-eval.R),
[globals.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/globals.R)

## Configuration

`inst/config.yml`, read via
[`read_config()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/read_config.md),
which calls
[`load_dotenv()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/load_dotenv.md)
and then
`config::get(config = Sys.getenv("R_CONFIG_ACTIVE", "default"))`.

Two environments — the `local:` environment was deleted in Phase 1:

| env | storage buckets | api / hub buckets |
|----|----|----|
| `default` | `timor-dev`, `pds-timor-dev`, `public-timor-dev` | `peskas-api-dev`, `peskas-coasts-dev` |
| `production` | `timor`, `pds-timor`, `public-timor` | `peskas-api-prod`, `peskas-coasts` |

Secrets come from the environment in both cases: from `.env` locally
(loaded by
[`load_dotenv()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/load_dotenv.md)),
from the workflow environment in CI. **Local and CI now resolve the same
config branch and differ only by `R_CONFIG_ACTIVE`.** `.Renviron` sets
`default`. CI sets `R_CONFIG_ACTIVE=production` only on `main`; **any
push to a non-main branch runs the whole pipeline against the `-dev`
buckets.** That is the integration-test mechanism for the migration.

The file **was** a superset. Through Phases 1-10 every harmonized key
(`country`, `ingestion`, `surveys.landings.{v1,v2,v3}`, `api`,
`storage.google.options_{coasts,api}`) sat beside the legacy key it
replaced, marked `# [legacy]`. **Phase 11 deleted the legacy half** —
`surveys.kobo_*`, `surveys.landings_{1,2,3}`,
`surveys.{merged,validated}_landings`, `surveys.landings.validated`,
`pds.{trips,tracks}`, `validation.google_sheets`, `validation.version`,
`metadata.rfishtable` and `export_dataverse.metadata`, each after its
last reader. What is left is read by this package, by `coasts::` through
`read_config(package = "peskas.timor.data.pipeline")`, or is part of the
harmonized shape. **`coasts` reads this file too**, so grep the hub as
well as `R/` before removing a key.

`storage.mongodb` is **declared and inert** since Phase 2. It has no
reader yet — ~~the validation flags sink decision is Phase 5~~;
`validation.*` is live since Phase 5, `pipeline.*` still has no reader.
It was safe to declare only because Phase 2 narrowed the six call sites
that used to do
`purrr::map(conf$storage, ~ upload_cloud_file(files, .$key, .$options))`,
which treated every child of `storage` as a storage *provider*.
**Nothing walks the children of `storage` or `pds_storage` any more;
keep it that way** — always address a provider explicitly as
`conf$storage$google`.

### Environment variables

Copy
[.env.example](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.env.example)
to `.env` and fill it in. Both JSON-valued entries must be minified onto
one line — dotenv parses line by line.

| env var | GitHub secret | notes |
|----|----|----|
| `KOBO_USERNAME` / `KOBO_PASSWORD` | same | basic auth against `eu.kobotoolbox.org` |
| `KOBO_TOKEN` | *not set in CI yet* | token auth, used by the new `ingestion` block |
| `KOBO_ASSET_ID_V1/2/3` | `KOBO_PESKAS1/2/3`, mapped in the workflow `env:` block | the only asset-id names read since Phase 11 |
| `GCP_SA_KEY` | `PESKAS_DATAINGESTION_GCS_KEY` | full service-account JSON, minified |
| `GOOGLE_SHEET_ID` | same | metadata tables |
| `MONGODB_CONNECTION_STRING_VALIDATION` | *not set in CI yet* | the flags sink. Absent → [`validate_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landings.md) warns and only the GCS snapshot is written |
| `PDS_TOKEN` / `PDS_SECRET` | `PESKAS_PDS_TOKEN` / `PESKAS_PDS_SECRET` |  |
| `DATAVERSE_TOKEN` | `PESKAS_DATAVERSE_TOKEN` |  |
| `PESKAS_GMAIL_KEY` | same | the serialized blastula credentials JSON, **not** a bare app password |
| `AIRTABLE_TOKEN` | same | the **bare** `pat…` — coasts prepends `Bearer`. Needs frame-base read access + `schema.bases:read` |
| `AIRTABLE_BASE_ID_FRAME` | same | PESKAS \| FRAME, `appMMEJYlJdfSJEjm` |

`KOBO_ASSET_ID_V1/2/3` are mapped in the workflow `env:` block from the
`KOBO_PESKAS*` secrets, which keep their legacy names on purpose: secret
values are write-only, so renaming them means re-entering three asset
ids by hand for no behavioural gain. Phase 11 deleted the config keys
that read `KOBO_PESKAS<n>` directly, not the mapping. `KOBO_TOKEN` has
no secret and is optional — `ingestion` uses basic auth. **Two stale
GitHub secrets survive and deleting them is a user action**, neither
mapped by any workflow: `AIRTABLE_KEY` and `VALID_SHEET_ID`, the
latter’s sheet having lost its last reader in Phase 5 and its config key
in Phase 11.

**Never log the resolved config.**
[`read_config()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/read_config.md)
used to end with `logger::log_debug("Running with parameters {pars}")`,
and every workflow function defaults to `log_threshold = logger::DEBUG`
— so each CI job printed the service-account private key, the Airtable
PAT, the Dataverse token and the blastula credentials into its log.
GitHub Actions only masks byte-exact matches of a registered secret,
which the re-serialised JSON is not. Removed in Phase 3; it logs the key
names only.

**[`coasts::read_config()`](https://rdrr.io/pkg/coasts/man/read_config.html)
still has that line**, and every coasts workflow function still defaults
to
[`logger::DEBUG`](https://daroczig.github.io/logger/reference/log_levels.html)
— so calling one with the default threshold leaks everything Timor
stopped leaking in Phase 3. **Every `coasts::` workflow call in
`data-pipeline.yaml` must pass `log_threshold = logger::INFO`**; the two
PDS ingestion steps do. Filed as COASTS-TODO C21, and live in
Mozambique, Kenya and Zanzibar today.

### Reference data — two sources, one of them authoritative

There are **two unrelated Airtables** in this repo’s history; do not
conflate them.

1.  `R/airtable.R` — Timor’s own `air_*` client, orphaned (it read
    `validation.airtable.*`, a key that no longer exists). **Deleted in
    Phase 1**, with its only consumer `ingest_validation_tables()` and
    `inst/airtable/edit-submission-link.js`.
2.  **PESKAS \| FRAME** (`appMMEJYlJdfSJEjm`) — the cross-country
    harmonization layer, mapping each country’s raw form labels to
    `standard_name` / `alpha3_code` / FAO codes.
    [`coasts::ingest_assets()`](https://rdrr.io/pkg/coasts/man/ingest_assets.html)
    snapshots it to `assets__*.rds` and the rest of the coasts pipeline
    reads that snapshot. Timor’s rows, re-measured 2026-08-09: **60 taxa
    over 56 distinct `alpha3_code`s** (matching `models.all_taxa`
    exactly), **9 gears**, 2 vessels, 40 sites, 457 pds_devices;
    `survey_label` populated everywhere. Adopted in Phase 3 by calling
    `coasts::` directly — Timor keeps **no** local copy of the Airtable
    module, unlike Mozambique, so its config uses the hub’s key paths
    (`airtable.token`, `airtable.frame.base_id`) rather than Moz’s
    `metadata.airtable.*`.

[`ingest_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/ingest_assets.md)
([ingestion.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/ingestion.R))
writes the snapshot and
[`get_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_assets.md)
reads it back from the **hub** bucket. Two things to know:

- The snapshot is **cross-country and carries no `country` column** —
  1,609 taxa rows across four countries, and every one of Timor’s 56
  codes is also used by another country, two of them against a different
  `scientific_name`. Always narrow with `timor_assets(x, pars)`, which
  filters on `metadata.airtable.form_ids` (the record ids of Timor’s two
  KoBo forms — the only column in the snapshot that separates the
  countries). See COASTS-TODO C13.
- coasts uploads it to the **country** bucket while every reader
  resolves the **hub**, so
  [`ingest_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/ingest_assets.md)
  mirrors it to the hub afterwards. Remove that second upload when
  COASTS-TODO C11 ships.

Where the frame and the Google Sheets tables overlap, **Airtable is
authoritative** (PLAN §2.5): taxa, gears, vessels, landing_sites,
districts/regions, pds_devices. Phase 3 moved the first of those joins —
`get_taxa_list()` now reads the snapshot instead of `catch_types` +
`fao_catch` — and the rest move in Phase 4, so
`metadata.google_sheets.tables` still lists all 15. The Sheets
ultimately keep only `morphometric_table`, `habitat`, `conservation`,
`fishing_vessel_statistics`, `registered_boats` — plus, for now,
`catch_types`, the only source of the per-taxon `length_type` for the
five invertebrate measures (COASTS-TODO C14). This is not cosmetic —
without it the Phase 6 API export would be schema-correct but full of
untranslated Tetum labels.

**`pds_devices` is the exception, and it is incomplete.** The frame is
now authoritative on the PDS side — `conf$pds$customers`
(`MAF / WorldFish`, `Traders`, `FSSP2: Traders`) is what
[`coasts::ingest_pds_trips()`](https://rdrr.io/pkg/coasts/man/ingest_pds_trips.html)
filters the API response by, and it is how every country claims its
devices out of the shared PDS account. Measured 2026-08-11 against the
449 IMEIs Timor’s token returns, that list covers 422; the **27 it
misses are in no frame customer at all** and are worth 2,791 trips,
14,519 tracked hours and 59 of the 6,999 landing↔︎trip matches. The IMEIs
are listed in the Phase 7 STATE entry — adding them to Airtable recovers
the data on the next run.

On the **survey** side the same gap is why
`metadata.google_sheets.tables` still lists `devices`: the frame’s 442
Timor rows are a strict subset of the Sheets’ 595, so
[`validate_imeis()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_imeis.md)
would take alert 3 from 824 to 1,475 submissions and strip the resolved
`tracker_imei` — hence the matched trip — from 651 of them. Switch it
only once the frame is complete.

### The taxa and weight path

Rewritten **2026-09-05** at the user’s instruction that it be aligned to
the WIO packages. Three sources, and after this change **none of them is
a Google Sheet** —
[`calculate_weights()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/calculate_weights.md)
was the weight path’s last Sheets reader.

| what | where from |
|----|----|
| which taxon *codes* Timor has | the PESKAS \| FRAME assets snapshot (authoritative for taxa, PLAN §2.5) |
| each code’s *scientific name* | the FAO **ASFIS** list, `gs://timor{,-dev}/asfis__*.parquet`, joined on `Alpha3_Code` — the same object and access path Mozambique uses |
| the curated invertebrate coefficients | `inst/extdata/morphometric-coefficients.csv` |

**ASFIS was adopted for alignment, not for effect.** Measured before the
switch: 55 of Timor’s 56 codes carry an identical `scientific_name` in
the frame and in ASFIS, the exception being `MZZ` (frame `Osteichthyes`,
ASFIS `Actinopterygii`), and both expand to the same 50 codes over the
same species. Join on `Alpha3_Code`, never `Taxonomic_Code`.

**FAO areas 57 and 71**, from `metadata.fishbase.fao_areas`, with
`filter_by_area = TRUE`. That key **must** exist:
[`coasts::resolve_fao_areas()`](https://rdrr.io/pkg/coasts/man/resolve_fao_areas.html)
falls back to `c(51, 57)`, the Indian Ocean pair the WIO repos use, so
an unset key filters Timor on one wrong area and misses the Pacific one
silently. It was absent and read as `NULL` from Phase 3 until this
session, harmless only because the filter was off.

**`taxa_search_aliases()` is the load-bearing part.**
`expand_taxonomic_info()` matches the FishBase backbone at species /
genus / family / order / class only, and FAO names several taxa at ranks
that backbone has no column for. Aliases are **additive** — extra search
names for a code, never a replacement — so nothing that already resolves
can regress and **no published taxon code changes**. This is
deliberately *not* Mozambique’s approach, which recodes `catch_taxon` in
the data (`TUN` → `TUS`, `SKH` → `CVX`, `CLP` → `ANX`) and thereby
renames the published taxon.

| code | ASFIS name | why it fails | alias |
|----|----|----|----|
| `TUN` | `Thunnini` | tribe — no such rank | the tribe’s 5 genera |
| `SKH` | `Selachimorpha (Pleurotremata)` | superorder | `Carcharhiniformes` |
| `LGE` | `Leiognathus equulus` | binomial FishBase has revised | `Leiognathidae` |
| `CLP` | `Clupeidae` | **stale family**, see below | `Dorosomatidae` too |

`TUN` is **56% of landed weight**, so its pool is a fishery decision,
taken with the user on 2026-09-05: the tribe *Thunnini* is *Allothunnus,
Auxis, Euthynnus, Katsuwonus, Thunnus*, which is what Timor’s “Tunas
nei” lands. Mozambique’s `TUS` would drop skipjack and frigate tuna; the
ASFIS family `Scombridae` would pull in the mackerels Timor codes
separately as `RAX`. At 25 cm: 249 g for the tribe, 249 g for `TUS`, 172
g for `Scombridae`.

`CLP` is **not** a rank problem. FishBase’s 2022 revision moved the
tropical sardines — *Sardinella*, *Amblygaster*, *Herklotsichthys*,
*Nematalosa*, *Tenualosa* — out of `Clupeidae` into `Dorosomatidae`,
while FAO still files them all under `CLUPEIDAE`. FishBase’s `Clupeidae`
now holds 15 mostly temperate species, so Timor’s second-largest taxon
(26% of landed weight) was priced off **114 records of *Clupea harengus*
and 36 of *Sprattus sprattus***. Searching both families restores 123
area-57/71 records over 25 Indo-Pacific species. **Do not “fix” this in
Airtable** — it was considered and rejected: `DCX` and `CLU` are
`Clupeoidei`, a *suborder*, which matches nothing; `DAG` is the
freshwater Lake Tanganyika sardine; `SIX` (*Sardinella* spp) resolves
but narrows to 8 species, drops the herrings the label names, and
renames a published taxon key.

**Coefficients are restated on a total-length basis before pooling**
(added 2026-09-06, after Zanzibar 4.9.0). A published `W = a * L^b` is
fitted on whichever axis the study used, and for Timor’s taxa only 1,824
of 3,702 matched pairs are `TL` — 990 are `FL`, 460 `SL`. Pooling them
as published and applying the result to a TL measurement overestimates
weight, because FL and SL are shorter (medians 0.958 TL and 0.827 TL).
`get_taxa_morphometrics()` already returned `length_length` and it was
being discarded; it is now used to convert. Four things to know:

- **POPLL fits `Length1 = aL + bL * Length2` — the second column is the
  predictor.** coasts’ roxygen states this backwards (COASTS-TODO
  **C28**). Getting it wrong inverts every ratio.
- Substituting `L_type ~= ratio * TL` gives `W = a * ratio^b * TL^b`, so
  **`b` is unchanged and only `a` is rescaled**.
- **`length_types = NULL` is passed deliberately.** The coasts default
  keeps only `TL`/`FL` pairs, which would drop 1,427 of 2,642 usable
  conversions — all the `SL` ones, where the correction is largest.
- **The 285 pairs with no conversion are kept as published, not
  dropped**, and the curated rows are bound on afterwards so they are
  never restated. Dropping the unconvertible takes `MOO` down 92% and
  `SFA` 78%. Zanzibar converts only for taxa that would otherwise have
  nothing; Timor converts everything it can, which is why no `Type`
  filter is needed.

Effect measured in isolation: national catch **−10.9%**, price/kg
**+11.7%**, nutrient supply **−11.9%**; `CJX` −37%, `LWX` −19%, `FLY`
−15%, `CGX` −14%, `CLP` −12%, `TUN` −8.5%; `GZP` and `MOO` unchanged,
being curated-driven.

**`rescue_by_common_name()` is gone**, and it was not a like-for-like
removal — it was the *only* source of coefficients for `TUN`. It ran
[`rfishbase::common_to_sci()`](https://docs.ropensci.org/rfishbase/reference/common_to_sci.html)
on the literal strings `"Tuna"`, `"Shark"` and `"Garfish"`, a substring
match on common names, so `SKH`’s pool contained *Pangasius
sanitwongsei* (a Mekong catfish) and the aquarium bala shark, and
`GZP`’s was topped by driftfishes and scads at 185–328 g while the
actual garfish sit at 17–21 g. It also never touched the *expansion*,
only the coefficients — which is why **`TUN`, 51% of national catch,
contributed nothing to any published nutrient figure** for the life of
the pipeline. Fixing the alias fixed the nutrients as a side effect.

**Two guards, and they exist because of a measured failure.** See
COASTS-TODO **C25** before trusting any single run’s numbers:

- `assert_taxa_coverage()` **errors** when a taxon resolves to no
  coefficient pair. `MZZ` (`Actinopterygii`, which FishBase files as
  `Teleostei`) and `SWX` (`Algae`) are the two documented exemptions —
  neither has ever had a coefficient here, and both weigh zero.
- [`get_nutrients_table()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_nutrients_table.md)
  **warns** for taxa with no nutrient values. Currently `CUX` and `GZP`,
  together 0.5% of catch. A warning rather than an error because
  [`get_fao_composition()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_fao_composition.md)
  legitimately does not cover every invertebrate.

## Storage

Google Cloud Storage, project `peskas`, service account
`data-ingestion@peskas.iam.gserviceaccount.com` (it can read/write
objects but **cannot** `storage.buckets.list`).

Versioned object naming, from
[`add_version()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/add_version.md):

    <prefix>__<YYYYMMDDHHMMSS>_<git-sha7>__.<ext>

The sha comes from `git2r::sha(git2r::last_commit())`, falling back to
`$GITHUB_SHA` inside containers.
`coasts::cloud_object_name(version = "latest")` resolves the newest.

### The layer is `coasts::*` (since Phase 2)

`R/cloud-storage.R` and `R/google-drive.R` are gone. Every call site is
namespaced
`coasts::{cloud_storage_authenticate, upload_cloud_file, download_cloud_file, cloud_object_name}`.
**Always write the `coasts::` prefix** — Timor no longer exports these
names, so an unqualified call would resolve off the search path or not
at all.

One thing Timor keeps, deliberately:

- **[`add_version()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/add_version.md)**
  — a naming helper, not a storage function. `coasts` exports a
  body-identical copy, but delegating would mean editing ~40 call sites
  plus three `inst/report/` drivers to remove an exported name for no
  behavioural gain. Dedupe when it is upstreamed, not before.

The retry wrappers are **not** one of them any more: `coasts` 4.6.0
shipped `insistent_upload_cloud_file()` /
`insistent_download_cloud_file()` (COASTS-TODO C5) and Timor’s local
copies are gone. The one remaining call site,
[data-raw/convert-pds-tracks.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/data-raw/convert-pds-tracks.R),
is written
[`coasts::insistent_upload_cloud_file()`](https://rdrr.io/pkg/coasts/man/insistent_upload_cloud_file.html)
— keep the prefix.

**[`coasts::cloud_object_name()`](https://rdrr.io/pkg/coasts/man/cloud_object_name.html)
is not a drop-in for Timor’s deleted version.** The signatures match,
but coasts returns `selected_rows$name[1]` where Timor returned the
whole vector. Every Timor call site was audited against the dev buckets
and only one relied on the vector: the ~100k track names in the PDS
bucket. Use
**[`coasts::cloud_object_names()`](https://rdrr.io/pkg/coasts/man/cloud_object_names.html)**
(plural, 4.6.0, with `latest_only`) for that —
[`get_tracks_descriptors()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_tracks_descriptors.md)
and `data-raw/convert-pds-tracks.R` both do (`get_tracks_ids()` did too,
until Phase 11 deleted it as unreachable). Never use the singular to
enumerate a bucket; it silently returns one name, and on the PDS path
that reads as “no tracks stored” and re-fetches the entire history from
the API.

Listing that bucket is not cheap — ~100k objects at 1,000 per request,
three to thirty minutes depending on how the API feels. Do it once per
function and pass the vector down, as
[`describe_pds_tracks()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/describe_pds_tracks.md)
does.

Prefer `coasts::resolve_storage_opts(pars, type)` over reaching into
`conf$storage$google$options_*` by hand. Since coasts 4.6.0 it knows
`"coasts"` (hub, falling back to `options`), `"country"`, `"pds"` and
`"public"`.

| bucket | contents |
|----|----|
| `timor` / `timor-dev` | surveys and derived tables. Raw, preprocessed, merged and the frozen v1 snapshot are all **parquet** since Phase 4 (`timor-landings-v{2,3}_{raw,preprocessed}__*.parquet`, `timor-landings-merged__*.parquet`, `timor-landings-v1-frozen__*.parquet`); the weight artefact is parquet since Phase 5, and since **Phase 8 the validated artefact is written once**, `timor-landings-merged_validated_long__*.parquet` — the nested `.rds` twin has no writer any more, it is rebuilt on read by [`get_validated_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_validated_landings.md) |
| `pds-timor` / `pds-timor-dev` | one parquet per GPS trip, `pds-tracks_<trip_id>.parquet`, written by [`coasts::ingest_pds_tracks()`](https://rdrr.io/pkg/coasts/man/ingest_pds_tracks.html). **Not versioned** — a re-ingest overwrites. The 103,373 `pds-track-<trip_id>__*__.csv.gz` objects of the old family were converted in place by [data-raw/convert-pds-tracks.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/data-raw/convert-pds-tracks.R) in Phase 7, not re-fetched. Phase 11a deleted the dev bucket’s copies; the ~98k in `pds-timor` are Phase 11b’s, after the production conversion |
| `public-timor` / `public-timor-dev` | `portal-*.json` — the live portal contract |
| `peskas-coasts` / `peskas-coasts-dev` | the shared cross-country hub (`options_coasts`). **Read *and* written** by coasts: `assets__*`, `taxa-fishbase-enriched`, H3 effort/CPUE grids, and per-country `*_fishery_metrics` / `*_monthly_summaries_map`. Both are live — `default` must stay on `-dev` |
| `peskas-api-prod` / `peskas-api-dev` | cross-country API parquet (`options_api`), live for Kenya/Moz/Zanzibar. Timor joined in Phase 6 and publishes to **`-dev` only** so far: `timor/{raw,validated}/trips-{raw,validated}__*.parquet`. The service account has object create/delete on **both** buckets (verified 2026-08-11 via `testIamPermissions`), so the first prod write is a decision, not a permission |

**No lifecycle policy is set.** Every pipeline run appends new versions
and nothing is ever deleted; `gs://timor` holds ~33k objects and
`gs://pds-timor` ~98k. See
[.claude/migration/AUDIT.md](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.claude/migration/AUDIT.md)
for the full prefix inventory including orphaned prefixes.

Interchange format is **flat long parquet** from raw through validated
(Phases 3, 4, 5 and 8): one row per (submission, catch, length bin).
Since Phase 8 there is **no `.rds` artefact left on the survey path**.
`timor-landings-merged_validated_long__*` (40 columns since Phase 6) is
the one validated object; the nested
`timor-landings-merged_validated__*.rds` has no writer any more.
[`get_validated_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_validated_landings.md)
re-nests the parquet on read into the `landing_catch` /
`length_frequency` shape the portal path works in, so
[`format_public_data()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/format_public_data.md),
[`estimate_fishery_indicators()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/estimate_fishery_indicators.md)
and
[`merge_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_trips.md)
did not have to change. The two shapes were proven interchangeable first
— 97,360 submissions, 1,648,016 catch rows, `all.equal` TRUE, the only
difference 1,599 catch weights moving by ≤2.9e-11 g on the grams → kg →
grams round trip.

`all_trips__*.rds` stays nested `.rds`, deliberately. It has **no
cross-country counterpart** — no other country has a
[`merge_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_trips.md)
at all, and
[`coasts::merge_survey_trips()`](https://rdrr.io/pkg/coasts/man/merge_survey_trips.html)
does a different job (COASTS-TODO C10) — so flattening it would buy no
harmonization while requiring `format-public-data.R`, `model-fishery.R`
and `pds-maps.R` to be rewritten against a live portal.

On the **PDS** path Phase 7 made raw trips and the tracks parquet
(`pds-trips__*.parquet` from
[`coasts::ingest_pds_trips()`](https://rdrr.io/pkg/coasts/man/ingest_pds_trips.html),
`pds-tracks_<id>.parquet` from
[`coasts::ingest_pds_tracks()`](https://rdrr.io/pkg/coasts/man/ingest_pds_tracks.html),
`pds-tracks-descriptors__*.parquet` from
[`describe_pds_tracks()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/describe_pds_tracks.md)).
`pds-trips_validated__*.rds` is still `.rds`, and Phase 8 deliberately
left it alone rather than flipping it with the merge path as PLAN
anticipated: it carries `tracker_trip_start`/`_end` as `Asia/Dili`
POSIXct and
[`merge_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_trips.md)
derives `landing_date` from `tracker_trip_end`, so a parquet round trip
is a timezone risk on the exact code path commit `15f6b18` exists to
protect — for no gain, since only
[`merge_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_trips.md)
and `test_validated_pds_trips.R` read it and neither is cross-country.
`pds-trips_preprocessed__*` and `pds-track_preprocessed__*` have no
writer any more.

## Portal contract (do not break)

[export.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/export.R)
writes **seven** objects as `portal-<name>__*__.json` to `public-timor`:

    aggregated  taxa_aggregated  municipal_aggregated  municipal_taxa
    nutrients_aggregated  data_last_updated  summary_data

`peskas.timor.portal.v2/scripts/fetchData.js` lists `portal-*` in the
bucket, keeps the newest version of each, and **excludes**
`indicators_grid.json`, `label_groups_list.json`, `pars.json`,
`taxa_names.json`, `var_dictionary.json` — so these seven are exactly
the live set. Until Phase 8 the export also emitted `indicators_grid`
and `label_groups_list`, the two the portal excludes, rebuilt on every
run from an `indicators_gridded.rds` last written 2024-07-27. Both were
dropped; their existing versions stay in the bucket, so re-adding two
lines to
[`export_files()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_files.md)
restores the family.

The portal discovers files dynamically, meaning a **renamed or dropped
object silently disappears from the site**. The gate for that is
[data-raw/compare-portal-json.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/data-raw/compare-portal-json.R),
written in Phase 8 and reused by Phase 11: it asserts object names, then
keys, nesting, column sets and column types against the Phase 0 golden
(`reference/2026-07-31_90ede9a/`, gitignored), and prints per-column
numeric summaries against the newest `public-timor-dev` set. Row counts
and column order are reported, not failed — the portal reads an array of
objects by key. Run it before touching anything on the export path.

## Pipeline DAG

From
[.github/workflows/data-pipeline.yaml](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.github/workflows/data-pipeline.yaml)
(every 2 days at 00:00 UTC, plus on every push; ~1h30m):

    build-container
    ├── ingest-preprocess-metadata-tables   ingest_metadata_tables → preprocess_metadata_tables → ingest_assets
    ├── ingest-landings                     ingest_landings                 [v2 + v3 → raw parquet]
    │   └── preprocess-landings             preprocess_landings   [v2 + v3 → long parquet]
    └── ingest-pds-data                     coasts::ingest_pds_trips → coasts::ingest_pds_tracks
        └── preprocess-pds-data             describe_pds_tracks
            └── validate-pds-data           validate_pds_trips        [tinytest]

    merge-landings   merge_landings → calculate_weights
    └── validate-landings  validate_landings         [flags → MongoDB, tinytest]
        └── merge-trips    merge_trips                                [tinytest]
            ├── model-indicators  estimate_fishery_indicators
            └── export-trips      format_public_data → export_files   [tinytest] → enumerators report

## Development commands

``` r

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

``` bash
docker compose up          # RStudio on :8802, DISABLE_AUTH=true
docker build -f Dockerfile.prod -t peskas-timor .
```

`Dockerfile.prod`: `rocker/geospatial:4.5`, packages via `install2.r`
(`rfishbase` unpinned — the old 5.0.1 pin conflicted with `coasts`),
`ARG COASTS_REF` **with no default** →
`install_github('WorldFishCenter/peskas.coasts', ref = ...)`,
`ggchicklet` + `glmmTMB` from GitHub, then `COPY . /home` +
[`remotes::install_local()`](https://remotes.r-lib.org/reference/install_local.html).
The workflow’s “Resolve latest peskas.coasts release” step reads the
latest tag (**v4.6.0** as of 2026-08-11) and passes it in, so a build
always records which hub release it used; a local build must pass
`--build-arg COASTS_REF=<tag>` or fail. Image is pushed to
`ghcr.io/worldfishcenter/peskas.timor.data.pipeline/r-runner-peskas-timor:latest`.
`Dockerfile` (dev, used by `docker-compose.yaml`) mirrors the same
package set and the same `COASTS_REF`. Keep the two in step. **coasts ≥
4.6.0 is now a hard floor**: Phase 7’s PDS path needs
`cloud_object_names()`, `get_trip_points()`,
`resolve_storage_opts(conf, "pds")` and the `"MAF / WorldFish"`
customer.

**`devtools::check()` baseline** (re-measured after Phase 11a): **0
errors, 0 WARNINGs, 4 NOTEs**, and testthat is **green** (27
assertions). The four NOTEs are: 30 non-default Imports, installed size
(`inst/export` + `inst/report`), unverifiable file timestamps, and four
undefined globals (`sd`, `rnorm`, `if_all`, `` `Estimated revenue` ``) —
none of them new. The long-standing `FAIL 1 | WARN 9 | PASS 9` is gone:
`tests/testthat/test-pre-process-landings.R` tested
`pt_nest_attachments()` / `pt_nest_species()`, both deleted in Phase 4,
and was replaced by `test-survey-reshaping.R` (8 passing assertions over
[`reshape_species_groups()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/reshape_species_groups.md)
and
[`expand_length_frequency()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/expand_length_frequency.md)).
The 9 warnings were `.data`-in-tidyselect deprecations from `pt_nest_*`
and went with it. **No assertion was weakened to get there** — the code
under test no longer exists.

Two things moved in Phase 3 and both are improvements, not drift:

- The single WARNING was “undocumented `get_kobo_data()` arguments”.
  That function was Timor’s own KoBo client and is gone — retrieval
  delegates to
  [`coasts::get_kobo_data()`](https://rdrr.io/pkg/coasts/man/get_kobo_data.html).
- The **unused-Imports NOTE has disappeared entirely**. It named `arrow`
  after Phase 2 and would have named `arrow` + `httr2` after Phase 3,
  because parquet I/O and KoBo retrieval both live in `coasts` now and
  no `R/` file references either namespace. Both were dropped from
  `Imports`. Note that this means the NOTE is **no longer the canary**
  for whether `coasts` is wired in; if the delegation were ever undone,
  `coasts` would reappear in it and the note would come back. Phase 7
  dropped **`googleCloudStorageR`** the same way — the last two direct
  `gcs_list_objects()` calls became
  [`coasts::cloud_object_names()`](https://rdrr.io/pkg/coasts/man/cloud_object_names.html)
  — and `arrow` came *back* into use, in
  [`describe_pds_tracks()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/describe_pds_tracks.md),
  which reads the per-trip track parquet directly. Phase 11 dropped
  three more for the same reason: **`httr2`** (the KoBo validation
  client moved to `coasts::`), **`reticulate`** (`kepler_mapper()`) and
  **`glmmTMB`** (`model_indicators()`), and moved **`ggplot2`** to
  Suggests, where the Rmd reports are its only consumers. The
  `installGithub.r glmmTMB` line went out of both Dockerfiles with it —
  nothing in the package or in `coasts` imports it.

## CI — nine workflows since Phase 9, from eleven

| workflow | state | notes |
|----|----|----|
| `data-pipeline.yaml` | active | **the only one that produces data.** Green end to end |
| `R-CMD-check.yaml` | active | replaced `check-standard.yaml` in Phase 9. **One runner** (`ubuntu-latest`, release), not the five-platform matrix — deliberate, see the file header |
| `pkgdown.yaml` | active | r-lib v2 template; keyword-driven reference sections |
| `test-coverage.yaml` | active | `tests/testthat/` only; `fail_ci_if_error: false`, no `CODECOV_TOKEN` exists |
| `pr-commands.yaml` | active | `/document` and `/style`, now gated on `MEMBER`/`OWNER` |
| `release.yaml` | active | new in Phase 9. Cuts a release from the top block of `NEWS.md` on a push to `main` |
| `data-report.yaml` | disabled (inactivity) | rebuilt in Phase 9: no build job, runs in the pipeline’s container |
| `dataverse-upload.yaml` | disabled (inactivity) | same |
| `validation-email-sender.yaml` | disabled (inactivity) | same, and repointed at the Mongo flags sink |

**Re-enabling the three disabled ones is a Phase 11b action.** A
schedule fires from the **default branch**, so until the migration
branch is merged those crons would run `main`’s pre-migration code
against production.

Deleted in Phase 9: `form-summary.yaml`, `keplergl-map.yaml`,
`upload-matched-trips.yaml` — three of the four workflows that could
only build through the retired `docker.pkg.github.com` registry with
`whoan/docker-build-with-cache-action@v5`, none of which had succeeded
since 2025-08. The fourth, `validation-email-sender.yaml`, was rebuilt
instead because Phase 5 gave
[`send_validation_mail()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/send_validation_mail.md)
a working reader.

Two things the non-pipeline workflows must not do again: **build their
own image** (they each carried a duplicate `build-container` job that
has been unable to succeed since Phase 2 gave `Dockerfile.prod` an
`ARG COASTS_REF` with no default, which none of them resolved), and
**assume a referenced function is exercised** — three of these nine are
disabled and one produces data.

The three API-export steps aside, `data-pipeline.yaml` is unchanged in
substance since Phase 8: `checkout@v5`, `build-push-action@v6`,
`ubuntu-latest`, `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24`, the COASTS_REF
resolution, the four tinytest steps, and `log_threshold = logger::INFO`
on both `coasts::` calls.
[`export_api_raw()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_api_raw.md)
/
[`export_api_validated()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_api_validated.md)
were wired in in Phase 9 and carry
`if: ${{ !endsWith(github.ref, '/main') }}` — deleting those two lines
is Timor’s first write to `peskas-api-prod`, which is a separate, unmade
decision.

## Conventions and gotchas

- Every workflow function follows:
  [`read_config()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/read_config.md)
  → authenticate → download latest inputs → process →
  [`logger::log_info()`](https://daroczig.github.io/logger/reference/log_level.html)
  →
  [`add_version()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/add_version.md)
  → upload.
- Logging is `logger`; the `log_threshold` argument on workflow
  functions sets the level.
- **The preprocessed table is one row per (submission, catch, length
  bin)**, and it is a **superset**: the standard columns
  (`submission_id`, `landing_date`, `gaul_*`, `landing_site`,
  `n_fishers`, `trip_duration`, `gear`, `vessel_type`, `habitat`,
  `catch_outcome`, `n_catch`, `catch_taxon`, `scientific_name`,
  `length`, `catch_price`, …) sit beside every raw KoBo column,
  reconciled per form version by `harmonise_v2()` / `harmonise_v3()`.
  Nothing in `R/` reads the raw columns since Phase 5 — validation moved
  onto the standard names — but **`inst/report/enumerators_summary.Rmd`
  reads nine of them** (`_id`, `landing_site_name`,
  `Ita_koleta_dadus_husi_atividad`, `no_boats`, `reason_no_activity`,
  `date`, `today`, `start`, `end`) and its driver is the last step of
  the active `export-trips` job. **The passthrough is not deletable**;
  Phase 11 verified this and left it in place (ALIGNMENT-AUDIT §7).
- **Length bins with no count are kept.** A catch expands to one row per
  5 cm bin whether or not anybody was counted in it, because that is
  what the nested `length_frequency` has always held and it reaches the
  portal — `timor_catch` is 1.7 M rows of which 93% have no weight.
  Dropping them would change a published table.
- **v1 is frozen** (last submission 2020-08-28). It is not ingested and
  not preprocessed;
  [`merge_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_landings.md)
  reads `timor-landings-v1-frozen__*.parquet`, produced once per
  environment by
  [data-raw/freeze-landings-v1.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/data-raw/freeze-landings-v1.R).
  The freeze also converted v1’s **fork lengths to total length**, so
  every source now carries TL and
  [`join_weights()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/join_weights.md)
  no longer branches on `survey_version`. `summarise_ll_coeffs()` and
  `normalise_length_to_tl()` were deleted with it; the length-length
  logic now lives **inside the freeze script**, its only consumer. Phase
  4 also moved v1’s column reconciliation and its flattening to the long
  shape into that script, deliberately — the form is dead and
  [`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md)
  should not carry a shape nothing will produce again. **The snapshot
  exists in `timor-dev` only** — run the script against `production`
  before the Phase 11b cutover, **after** Phase 11a’s deletions, so the
  permanent snapshot is built by the final code.
- **`length_type` is descriptive, not functional.** It is not a survey
  field — the form records only counts per length bin, and `mean_length`
  is the bin midpoint. It comes from the Sheets `catch_types`, per
  taxon, non-`NA` for five invertebrates (`SLV` CL, `OCZ` ML, `IAX` ML,
  `CRA` CW, `COZ` ShL). Field practice measures those on total length
  (confirmed 2026-08-10), which is what the `OCZ`/`SLV`/`IAX`/`MOO`
  overrides encode, so nothing is converted and nothing is selected by
  it. If that ever changes, the place to act is `summarise_lw_coeffs()`
  — it pools coefficients across every measurement axis (`SLV` = 34
  carapace-length studies + 19 total-length), and filtering each
  declaring taxon to its own axis would move national catch weight by
  +0.63%.
- Variables holding the resolved configuration are named **`conf`**,
  matching the other country pipelines. The old `pars` was renamed
  throughout in Phase 3; do not reintroduce it.
- ~~`get_raw_landings()` coerces the raw parquet to all-character~~ —
  deleted in Phase 4. Preprocessing reads the typed parquet directly.
  The trim it used to reproduce now happens deliberately in
  `trim_free_text()`, which also strips the stray leading and trailing
  newlines ~60 free-text answers carry.
- `conf$...$version$preprocess: latest` is read on `landings_1/2/3`,
  `pds.trips`, `pds.tracks` and `metadata` — `validation` lost its
  reader in Phase 5. The unified template dropped this field; it is kept
  on each of those legacy keys and deliberately **not** re-added in the
  new tree, which uses the per-stage `version:` field. Every reader is a
  legacy-key reader and moves across in Phases 3–7.
- Timezone handling was fixed in commit `15f6b18` — re-verify it after
  any rewrite of the export path. It lives in `summarise_estimations()`
  ([format-public-data.R](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/format-public-data.R))
  and its point is that `today` must be built in the **data’s**
  timezone, not the session’s, or
  `floor_date(today, "month") == date_bin_start` silently matches
  nothing and the current month is published unscaled. Re-verified in
  Phase 8 against the parquet inputs, not assumed:
  `models$national$aggregated$landing_period` still carries
  `tzone = "Asia/Dili"`, exactly one month matches, and the emitted
  August 2026 figures are scaled by 12/31.
- **Known live bugs** (AUDIT.md §8). Fixed in Phase 1: the duplicate
  `get_preprocessed_metadata()` in `validate-landings.R` that shadowed
  the correct definition at
  [get-cloud-files.R:110](https://worldfishcenter.github.io/peskas.timor.data.pipeline/R/get-cloud-files.R#L110),
  and the `local:` config env inheriting a non-existent `development`
  env. Fixed in Phase 5:
  [`validate_catch_params()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_catch_params.md)’s
  positional assignment of `length_individuals` into a
  separately-derived frame, and the
  [`isTRUE()`](https://rdrr.io/r/base/Logic.html) on a vector that made
  alerts 12–15 unreachable (both rewrites are alert-identical on the
  current data — see the STATE Phase 5 entry). ~~Still open: two dead
  helpers in `validate-landings.R`~~ — removed with the file in Phase 5;
  ~~`ingest_rfish_table()` is `continue-on-error`~~ — moot, the function
  and its workflow step are gone. **AUDIT §8.5 is stale**:
  [`export_files()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_files.md)
  already passes basenames as `name` and normalises correctly — verified
  in Phase 2. The 45 leaked absolute-path objects in `public-timor` date
  from January 2026 and are historical residue, not a live bug. Deleting
  them is Phase 11b — they are in a production bucket. ~~**Found in
  Phase 7, open upstream:**
  [`coasts::read_config()`](https://rdrr.io/pkg/coasts/man/read_config.html)
  logs the whole resolved config at DEBUG (COASTS-TODO C21)~~ — **fixed
  upstream** in coasts PR \#11 and in every release the workflow can
  resolve. The `log_threshold = logger::INFO` at each `coasts::` call
  site stays as a regression guard, deliberately; it is still live in
  the other three repos.
- **The FishBase release is pinned in config, and it must stay that
  way.** `metadata.fishbase.db_version: "25.04"` (coasts \>= 4.10.0).
  `"latest"` is not a fixed dataset: `rfishbase` 5.0.3 moved the parquet
  host from HuggingFace (which stops at **25.04**) to Source Cooperative
  (which serves **26.06**), so a container rebuild silently moved the
  pipeline to 26.06 — in which `Caesionidae` and `Scaridae` survive as
  family names with **zero species attached**. `CJX` and `PWT` therefore
  weighed `NA`, which sums to zero, and `CJX` — 5% of landed weight and
  one of the 13 `models.modelled_taxa` — was **missing from
  `portal-taxa_aggregated`** on two runs with no error. Four things
  follow:
  - **Pass `conf` to
    [`coasts::get_taxa_morphometrics()`](https://rdrr.io/pkg/coasts/man/get_taxa_morphometrics.html).**
    Without it coasts falls back to its own
    [`read_config()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/read_config.md)
    and resolves `"latest"`, silently undoing the pin. Both Timor call
    sites pass it.
  - **Every number in `NEWS.md` was measured on 25.04.** Moving the key
    re-baselines the portal; run `data-raw/compare-portal-json.R` when
    you do.
  - The two hosts serve **identical** 25.04 (verified 2026-09-06: 25,730
    `poplw` and 27,211 `popll` rows on both), so the pin holds whichever
    `rfishbase` is installed. An earlier stopgap that pinned `rfishbase`
    to 5.0.1 in the Dockerfiles is **gone** — it pinned the host, not
    the release.
  - `assert_taxa_coverage()` stays as the backstop. It is what caught
    this. A trap worth naming: from the artefacts alone, **“varies
    between runs” and “varies between environments” look identical**.
    The first diagnosis here was “non-deterministic partial reads” and
    it was wrong. Resolve the release before concluding either.
- **`expand_taxonomic_info()` warns about names it cannot match**
  (coasts 4.10.0), and for Timor it always names six: `Algae`,
  `Brachyura`, `Actinopterygii`, `Selachimorpha (Pleurotremata)`,
  `Leiognathus equulus`, `Thunnini`. **That is expected, not a fault.**
  Four of them (`TUN`, `SKH`, `LGE`, plus `CLP`’s second family) resolve
  through `taxa_search_aliases()` instead, and `CRA` through the curated
  table; only `MZZ` and `SWX` genuinely have nothing, and both are
  exempt in `assert_taxa_coverage()`.
- Tests are Timor’s advantage over the other pipelines.- Tests are
  Timor’s advantage over the other pipelines. **Never delete an
  assertion to make a change pass** — update the expectation
  deliberately.
