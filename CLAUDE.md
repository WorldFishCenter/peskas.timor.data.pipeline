# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

> **The migration is merged and live.** `main` carries it as of **2026-09-07**
> — PR **#126**, merge commit **`0c75ee9`**, release **v4.0.0**, production run
> **34091315009** green 13/13. The repo is aligned to the harmonized Peskas
> standard (shared `peskas.coasts` hub, `.env` secrets, parquet interchange,
> cross-country API).
>
> **Phases 0-12 are complete** on the branch `migration-phase12`, which is
> **not merged**: Phase 12 moved the site and municipality labels onto the frame
> and collapsed the two rival "North Coast" definitions into one table. It has
> **not had a dev pipeline run** — the local gates all pass (both published
> figures delta 0, four tinytest suites green, `check()` at baseline) but the
> new `landing_site` labels only reach an artefact when the pipeline runs.
> **Push the branch to get that run**, then `data-raw/compare-portal-json.R`.
> What remains of the Phase 12 scope is **item C** (`devices` → `pds_devices`,
> hard-blocked on the 144 missing Airtable IMEIs). The phase also deleted the
> enumerators report, which had no consumer, and **dropped the 59-column raw
> passthrough** behind it — the preprocessed table is 39 standard columns now.
> Both of those change a stored parquet's schema, so the dev run matters.
>
> Before doing anything substantive, read
> [.claude/migration/STATE.md](.claude/migration/STATE.md) — its "Current
> position" and the **Phase 12 entry at the bottom** — then
> [.claude/migration/ALIGNMENT-AUDIT.md](.claude/migration/ALIGNMENT-AUDIT.md)
> §15, **whose Phase 12 list is now stale in both directions**: its items 1 and
> 2 shipped before Phase 11, and items 2, 3, 5, 6 and 7 shipped in Phase 12.
> [PROMPT-PHASE12.md](.claude/migration/PROMPT-PHASE12.md) is the accurate scope.
> [PLAN.md](.claude/migration/PLAN.md),
> [STRUCTURAL-DIFF.md](.claude/migration/STRUCTURAL-DIFF.md) and
> [AUDIT.md](.claude/migration/AUDIT.md) are the pre-migration record and are
> now history: useful for *why* something is shaped as it is, unreliable about
> what is there today.
> **One migration phase per session — never two.** End every session by appending
> a STATE.md entry.
>
> **What the merge published**, against the previous live portal: catch
> **-18.4%**, landing weight **-15.3%**, price/kg **+17.8%**, nutrient supply
> **-26.0%**. The seven-object contract is unchanged — the gate reports 0
> structural failures. It was also Timor's first `peskas-api-prod` write.
> `NEWS.md`'s top block attributes the movement to three separable corrections.
>
> **One thing still needs attention, and it is not code.** The three
> `disabled_inactivity` workflows **re-enabled themselves** when the merge
> pushed to the default branch — `data-report.yaml`, `dataverse-upload.yaml`,
> `validation-email-sender.yaml`. None has ever run on migrated code, and two
> send things outward (an emailed report, a Dataverse upload). Each has
> `workflow_dispatch`; **trigger them manually before their crons do.**
>
> `inst/report/data_report.Rmd`'s gear labels were fixed on 2026-09-07: its
> `factor()` levels were lowercase while `preprocess_landings()` sets `gear`
> from the frame's Title-Case `standard_name`, so all nine resolved to `NA` and
> the following `na.omit()` dropped the rows, removing the Gear axis from the
> parallel-sets plot. Unknown labels are now appended to the level set instead
> of dropped, so it cannot fail that way again. It has **not** been rendered end
> to end — the first manual dispatch above is its real test.
>
> **`ALIGNMENT-AUDIT.md` (2026-08-18) corrects five claims made below and in
> `AUDIT.md`. Trust it over this file where they disagree.** The five:
> `merge_trips()` **does** have cross-country counterparts (Mozambique's is
> line-for-line identical, and writes parquet); the 59-column raw KoBo
> passthrough had a live reader (`enumerators_summary.Rmd`) and was **not**
> deletable — **superseded: Phase 12 deleted that report and then the
> passthrough itself**; the frame's `pds_devices` is **not** a
> strict subset of the Sheets `devices`; `centro_pescas` contained **no** lat/lon
> and had no reader at all; and `timor_assets()` cannot be swapped onto the
> snapshot's `country` column, which is absent on `sites` and a record-id link on
> `geo` (COASTS-TODO C24) — **also superseded: `timor_assets()` was deleted in
> Phase 12 and `get_assets()` delegates to `coasts::get_assets()`**.
>
> Everything below documents the repo **as it is today**.

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
| [R/](R/) | 25 source files, verb-noun naming (see module map below) |
| [inst/config.yml](inst/config.yml) | the config file. Harmonized keys only since Phase 11 — every `# [legacy]` key is gone |
| [inst/config_template.yml](inst/config_template.yml) | Timor's copy of the cross-country spec, with its deviations recorded |
| `.env` | local secrets, gitignored. Template: [.env.example](.env.example). Replaced the old `auth/` directory in Phase 1 |
| [inst/extdata/](inst/extdata/) | packaged reference data. One file: `morphometric-coefficients.csv`, Timor's 559 curated length-weight rows, read by `curated_lw_coeffs()`. Note `.gitignore` and `.Rbuildignore` both blanket-exclude `*.csv` and both carry an exception for this directory — check them before adding a file here |
| [inst/tinytest/](inst/tinytest/) | 4 assertion suites, run as steps **inside** the pipeline workflow |
| [inst/report/](inst/report/) | Rmd reports + `generate_*.R` drivers, shapefiles, bib, css |
| [inst/export/](inst/export/) | Dataverse dataset metadata (README.Rmd, dataset-fields.json, PNGs) |
| [.github/workflows/](.github/workflows/) | 9 workflows since Phase 9, see below |

## Module map (`R/`)

Ingestion
- [ingestion.R](R/ingestion.R) — `ingest_landings()` (v2 + v3 → raw parquet via `coasts::get_kobo_data()`) and the `flatten_row()`/`flatten_field()`/`rename_child()` helpers. **`ingest_assets()` went in Phase 12**: Timor was the only country pipeline that wrote the cross-country assets snapshot — coasts' own pipeline writes it daily and Kenya, Mozambique and Zanzibar only read it. Timor now does the same, through `get_assets()`. Added in Phase 3, replacing `ingest-landings.R` and `retrieve-survey-data.R`
- [ingest-metadata-tables.R](R/ingest-metadata-tables.R) — **1** Google Sheets metadata table, from twelve. Phase 4 moved the taxa/gear/vessel/site joins onto the Airtable frame; Phase 11 dropped the five with no reader left (`vms_installs`, `centro_pescas`, `boats`, `fishing_vessel_statistics`, `registered_boats`), and the 2026-09-05 taxa-path session dropped `morphometric_table` — its 559 curated rows are package data now, which is what took the last Google Sheet off the weight path. Phase 12 dropped four more: `reporting_units` and `stations` (`validate_sites()` resolves both published labels from the frame now), `catch_types` (its `interagency_code` is **identical** to the frame's `alpha3_code` — 0 disagreements over the same 56 codes — and its `catch_name_en` and `length_type` had no live reader), and `habitat` + `conservation`, which are now fixed `case_when()` lookups in R (`habitat_labels()`, `conservation_labels()`) exactly as Kenya, Mozambique and Zanzibar hold theirs. **`devices` is the only table left**: the tracker IMEI roster, which must span retired devices because `validate_imeis()` expands partial IMEIs by suffix and a submission from any year can reference one
- **PDS has no Timor ingestion code since Phase 7.** `ingest-pds-data.R`, `retrieve-pds-data.R` and `preprocess_pds_trips()` are gone; the workflow calls `coasts::ingest_pds_trips()` and `coasts::ingest_pds_tracks()` with `package = "peskas.timor.data.pipeline"`, exactly as Mozambique, Kenya and Zanzibar do — none of them carries a line of PDS code either. Everything coasts needs is in `conf$pds`. The third call those three also make, `coasts::preprocess_pds_tracks()`, is **not** wired in, and Phase 8 decided to keep it that way: its output feeds `coasts::summarize_data()`, which Timor does not use (its portal is the JSON contract) and which is blocked for Timor anyway on COASTS-TODO C17 — so it would produce ~1.4 M grid rows per run for no reader. C20 (its first pass reads every track with `detectCores() - 1` workers, one on a CI runner) is the second reason, not the first. Wire it in when C17 ships **and** a Timor consumer exists

Preprocessing
- [preprocessing-surveys.R](R/preprocessing-surveys.R) — `preprocess_landings(versions = c("v2","v3"))` (raw parquet → **flat long catch parquet**), `merge_landings()`, the per-version `harmonise_*()` reconciliation, and the assets-snapshot label joins (`survey_labels()`). Added in Phase 4, replacing `clean-raw-data.R`, `preprocess-landings.R` (`step_1`/`step_2`) and `merge-landings.R`
- **`harmonise_v2()`/`harmonise_v3()` are not the counterpart of `map_surveys()`, and comparing them as such is a category error** (written down in Phase 12; ALIGNMENT-AUDIT §4 L8 measured it). The WIO `map_surveys()` (Moz `preprocessing-surveys.R:1071`, Zanzibar `preprocessing.R:731`) resolves **labels**: it joins each frame mapping and drops the raw column at every join, then renames the standard name into the standard slot. Timor's `harmonise_*()` reconcile **form versions**: they coalesce differently-spelled KoBo questions between v2 and v3 so the two can be `bind_rows()`d into one table. The WIO repos have no such function because they never merge two form versions — each form keeps its own `preprocess_*()` all the way to `merge_trips()`. Timor's actual counterpart to `map_surveys()` is `resolve_survey_labels()`, which does the same four joins in the same order. The divergence follows from PLAN §2.2 (one merged table across form versions) and is **deliberate** — do not "fix" it. `resolve_survey_labels()` does not drop the raw column at each join the way `map_surveys()` does — instead Phase 12 removed all 59 of them in one explicit `select(all_of(landing_cols()))` at the end of `reshape_landings()`, which is the shape Zanzibar uses. Same result, one place
- [survey-reshaping.R](R/survey-reshaping.R) — `reshape_species_groups()`, `expand_length_frequency()`, the bin-midpoint and free-text-trim helpers. Replaces `pt_nest_species.R` / `pt_nest_attachments.R`
- [model-taxa.R](R/model-taxa.R) — `calculate_weights()` / `join_weights()`, morphometric length-weight via `coasts::get_taxa_morphometrics()`. Renamed from `calculate-weights.R` in Phase 4. Phase 5 deleted the re-nesting from `join_weights()`, so the weight artefact is **flat long parquet** like every stage before it. Rewritten 2026-09-05 — see **The taxa and weight path** below. It reads **no Google Sheet**: the taxon *codes* come from the frame, the *names* from the FAO ASFIS list in the country bucket, the curated invertebrate coefficients from `inst/extdata/`. Coefficients are filtered to FAO areas **57 and 71**, and `assert_taxa_coverage()` fails the run if any taxon but `MZZ`/`SWX` resolves to nothing
- [preprocess-metadata-tables.R](R/preprocess-metadata-tables.R) — the seven surviving Google Sheets `pt_validate_*` parsers, most of them pass-throughs
- [pds-tracks.R](R/pds-tracks.R) — `describe_pds_tracks()` + `get_tracks_descriptors()`, the per-trip track descriptors (`start_end_distance`, `outliers_proportion`, `timetrace_dispersion`, start/end coordinates) that `validate_pds_trips()` joins on. **The one PDS product `coasts` has no equivalent for** — `coasts::preprocess_pds_tracks()` emits spatial grid summaries instead, and both steps run. Renamed from `preprocess-pds-trips.R` in Phase 7, which also deleted `preprocess_pds_trips()`: the trips artefact is read typed and Dili-local straight from the raw parquet by `get_pds_trips()`, so there is no preprocessed-trips stage any more, as there never was in the WIO repos

Validation
- **`validate_sites()` reads the PESKAS | FRAME snapshot, not the Google Sheets** (Phase 12). `preprocess_landings()` had already resolved `landing_site` and the GAUL columns from the frame via `survey_labels()`, exactly as Moz's `map_surveys()` does — and then `validate_landings()` overwrote both with the Sheets `stations`/`reporting_unit` values, which is why Timor was the only country publishing a non-frame `landing_site` (ALIGNMENT-AUDIT §8 L5). Measured before the switch: the 40 site codes match **40/40** with zero orphans either way; the recoded `reporting_region` reproduces the Sheets municipality vocabulary **exactly** for 39 of 40 sites; the exception is site 33 **Welaluhu** (Sheets `Manatuto`, frame `Manufahi / Fatuberliu`), which is one landing of 95,669 and does not move an `Area` because both sit south. **11 of 40 site names differ**, worth 53,961 landings — but `landing_site` is in **none** of the seven portal objects, so that change lands on the cross-country API only, which is the point of making it
- [validation.R](R/validation.R) — `validate_landings()` orchestrator, the MongoDB flags sink (`push_validation_flags()`), and `sync_validation_status()` (the KoBo write-back, deliberately not wired into the pipeline). Renamed from `validate-landings.R` in Phase 5
- [validation-functions.R](R/validation-functions.R) — 16 validators over the long table. **This is deeper than any other country pipeline — preserve it.** The KoBo validation-status client that lived here from Phase 5 was upstreamed in Phase 10 (COASTS-TODO C15) and deleted in Phase 11; `R/validation.R` calls `coasts::list_validation_statuses()` and `coasts::update_validation_status()`. The alert codes are the contract; the file opens with the code → validator table and `inst/config.yml`'s `validation.alerts` block carries the descriptions
- [validate-pds-trips.R](R/validate-pds-trips.R) — `validate_pds_trips()`, consecutive-trip merging, distance/outlier logic, plus `get_pds_trips()` (the typed, Dili-local view of the raw trips parquet). No `coasts` equivalent exists; upstream candidate for Phase 10

Merge / model / export
- [api.R](R/api.R) — `export_api_raw()` / `export_api_validated()`, the 22-column cross-country trips table written to `peskas-api-{dev,prod}/timor/{raw,validated}`. Added in Phase 6 and wired into `data-pipeline.yaml` in Phase 9, `if: !endsWith(github.ref, '/main')` — so it publishes to `-dev` only, and deleting those two lines is Timor's first write to `peskas-api-prod`. The schema is the contract Kenya, Mozambique and Zanzibar already publish; do not add, drop or reorder a column without agreeing it across all four
- [merge-trips.R](R/merge-trips.R), [model-fishery.R](R/model-fishery.R) (the Phase 8 concatenation of `estimate-catch.R` and `model-catch.R`; `estimate_fishery_indicators()` is the workflow entry point. `model_indicators()`, the second glmmTMB implementation with no caller, and its 674-line subgraph went in Phase 11 — with `glmmTMB` itself, which nothing in the package imports any more), [nutrients.R](R/nutrients.R) (nutrients + RDI, renamed from `calculate-nutrients.R`; **kept, not delegated** — see the file header). `merge_trips()` matches a landing to a tracked trip on `(landing_date, tracker_imei)` and is **not** `coasts::merge_survey_trips()`, which does a different job (COASTS-TODO C10). Its output `all_trips__*.rds` feeds `format_public_data()` and `model-fishery.R` — changing its schema breaks the export path. It is a **full join**, so its 176,302 rows are the landings plus the validated tracker trips minus the **6,940** actual landing↔trip matches (6,999 before Phase 7's frame device filter; the 59 come back when the 27 missing IMEIs are added to PESKAS \| FRAME)
- **`frame_reporting_region()`** ([model-fishery.R](R/model-fishery.R)) is the single definition of Timor's twelve published reporting regions from the frame's `geo`: Atauro by `gaul_2_name` (it is a GAUL sub-district of Dili that Timor reports separately), then `Lautém`/`Liquiçá`/`Oecussi` recoded to the published spellings. Four explicit cases, **not** an accent strip — `iconv(x, "UTF-8", "ASCII//TRANSLIT")` yields `Laut'em`/`Liquic'a` on macOS and matches nothing. Phase 12 consolidated it out of `get_registered_boats()`, which had the only copy, and `validate_sites()` now uses it too
- **`coasts::generate_fleet_analysis()`: assessed in Phase 12, and Timor should not call it.** It is the standard's analogue of `estimate_fishery_indicators()` — it raises sampled catch to a fleet total from a registered-boat count, the same job and the same input field (`geo.total_boats`), by a different estimator (PDS trip rates rather than glmmTMB). Three reasons, in order:
  1. **It cannot run.** Its `monthly_summaries` input is `<surveys.summaries.file_prefix>_monthly_summaries`, produced by `coasts::summarize_data()`. `conf$surveys$summaries` is `NULL` for Timor and that chain is deliberately unwired (no Timor consumer; COASTS-TODO C17, then C20) — see the PDS note above.
  2. **It would rename all twelve published regions.** It keys `boat_registry` on **`gaul_2_name`** (`assets$geo |> select("gaul_2_name", "total_boats")`). Timor's twelve `geo.total_boats` values sit one per reporting unit, but on `gaul_2` rows named `Atabae`, `Bazartete`, `Cristo Rei`, `Barique/Natarbora`… — sub-districts, not Timor's published `gaul_1`-plus-Atauro vocabulary. `frame_reporting_region()` exists precisely to bridge that.
  3. **PLAN §2.3 keeps Timor's modelling.** The portal contract is the seven JSONs, not `<country>_fishery_metrics`.
  So: **not adopted, deliberately.** Revisit only if C17 ships, a Timor consumer for `summarize_data()` appears, *and* the registry key is reconciled
- [format-public-data.R](R/format-public-data.R) — 1200 lines, the largest file; builds every portal object. It reads exactly two things, `get_merged_trips()` and `get_models()`, and Phase 8 left its body alone: the nested `landing_catch` / `length_frequency` shape it works in is now produced on *read* by `get_validated_landings()` rather than stored
- [pds-maps.R](R/pds-maps.R) — two lookups, `get_timor_boundaries()` and `convert_taxa_names()`, both called by `format_public_data()`. The map products it is named for are gone: Phase 8 stopped `export_files()` reading `indicators_gridded` and found `tracks-map.png` had never had a reader, which left `ingest_pds_map()`, `ingest_kepler_tracks()`, `kepler_mapper()` and `ingest_complete_tracks()` unreferenced, and **Phase 11 deleted all four** with `inst/kepler_mapper.py` and the four accessors that fed them (`get_sync_tracks()`, `get_full_tracks()`, `get_full_trips()`, `get_tracks_map()`)
- [export.R](R/export.R) — `export_files()` serializes and uploads the `portal-*.json` set: **seven objects since Phase 8**, exactly the seven the portal consumes
- [export-dataverse.R](R/export-dataverse.R), [reports.R](R/reports.R) (renamed from `send-email.R`; templates in [inst/report/](inst/report/))

Infrastructure
- [get-cloud-files.R](R/get-cloud-files.R) — 9 `get_*` accessors over one internal `download_versioned_rds()` helper, from 13: Phase 11 deleted `get_sync_tracks()`, `get_full_tracks()`, `get_full_trips()`, `get_tracks_map()`, `get_validation_flags()`, `get_tracks_ids()` and the duplicate `get_preprocessed_metadata()`. Timor's own GCS layer (`cloud-storage.R`, `google-drive.R`) was **deleted in Phase 2**; everything delegates to `coasts::*`
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

The file **was** a superset. Through Phases 1-10 every harmonized key
(`country`, `ingestion`, `surveys.landings.{v1,v2,v3}`, `api`,
`storage.google.options_{coasts,api}`) sat beside the legacy key it replaced,
marked `# [legacy]`. **Phase 11 deleted the legacy half** — `surveys.kobo_*`,
`surveys.landings_{1,2,3}`, `surveys.{merged,validated}_landings`,
`surveys.landings.validated`, `pds.{trips,tracks}`, `validation.google_sheets`,
`validation.version`, `metadata.rfishtable` and `export_dataverse.metadata`,
each after its last reader. What is left is read by this package, by `coasts::`
through `read_config(package = "peskas.timor.data.pipeline")`, or is part of the
harmonized shape. **`coasts` reads this file too**, so grep the hub as well as
`R/` before removing a key.

**`metadata.coast_areas` is the one site→coast table** (Phase 12), 40 entries keyed on the **frame's** site names. Two definitions of the coast lived here before — a municipality list in `export_files()` and a municipality-plus-five-site-names `case_when()` in `get_summary_data()` — and they disagreed on Lautem for the life of the pipeline, misfiling 14.25% of national revenue into South Coast until `602a110` patched one copy. Coast is a property of the **landing site**: two municipalities hold sites on both coasts (Lautem 4,299 north / 2 south, Manatuto 2,919 / 1), and latitude cannot derive it — the classes overlap, because Oecusse is an exclave whose north-facing sites sit further south than most of the south coast. `export_files()` cannot see the site (the model has collapsed the data to municipality by then), so `get_summary_data()` derives a municipality→coast map from this table by **submission-weighted majority** and passes it in the intermediate `summary_data` rds; `export_files()` hard-fails if it is absent. The derived map is `identical()` to the seven-name list `export_files()` used to hardcode, and both published figures moved by **0**. A landing site absent from the table **warns** rather than silently becoming South Coast. Note that majority is **submission**-weighted, not site-count-weighted: by site count Manatuto is 1 north / 2 south and would flip, changing `estimated_revenue`.

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
| `KOBO_ASSET_ID_V1/2/3` | `KOBO_PESKAS1/2/3`, mapped in the workflow `env:` block | the only asset-id names read since Phase 11 |
| `GCP_SA_KEY` | `PESKAS_DATAINGESTION_GCS_KEY` | full service-account JSON, minified |
| `GOOGLE_SHEET_ID` | same | metadata tables |
| `MONGODB_CONNECTION_STRING_VALIDATION` | *not set in CI yet* | the flags sink. Absent → `validate_landings()` warns and only the GCS snapshot is written |
| `PDS_TOKEN` / `PDS_SECRET` | `PESKAS_PDS_TOKEN` / `PESKAS_PDS_SECRET` | |
| `DATAVERSE_TOKEN` | `PESKAS_DATAVERSE_TOKEN` | |
| `PESKAS_GMAIL_KEY` | same | the serialized blastula credentials JSON, **not** a bare app password |
| `AIRTABLE_TOKEN` | same | the **bare** `pat…` — coasts prepends `Bearer `. Needs frame-base read access + `schema.bases:read` |
| `AIRTABLE_BASE_ID_FRAME` | same | PESKAS \| FRAME, `appMMEJYlJdfSJEjm` |

`KOBO_ASSET_ID_V1/2/3` are mapped in the workflow `env:` block from the
`KOBO_PESKAS*` secrets, which keep their legacy names on purpose: secret values
are write-only, so renaming them means re-entering three asset ids by hand for
no behavioural gain. Phase 11 deleted the config keys that read
`KOBO_PESKAS<n>` directly, not the mapping. `KOBO_TOKEN` has no secret and is
optional — `ingestion` uses basic auth. **Two stale GitHub secrets survive and
deleting them is a user action**, neither mapped by any workflow:
`AIRTABLE_KEY` and `VALID_SHEET_ID`, the latter's sheet having lost its last
reader in Phase 5 and its config key in Phase 11.

**Never log the resolved config.** `read_config()` used to end with
`logger::log_debug("Running with parameters {pars}")`, and every workflow
function defaults to `log_threshold = logger::DEBUG` — so each CI job printed
the service-account private key, the Airtable PAT, the Dataverse token and the
blastula credentials into its log. GitHub Actions only masks byte-exact matches
of a registered secret, which the re-serialised JSON is not. Removed in
Phase 3; it logs the key names only.

**`coasts::read_config()` still has that line**, and every coasts workflow
function still defaults to `logger::DEBUG` — so calling one with the default
threshold leaks everything Timor stopped leaking in Phase 3. **Every
`coasts::` workflow call in `data-pipeline.yaml` must pass
`log_threshold = logger::INFO`**; the two PDS ingestion steps do. Filed as
COASTS-TODO C21, and live in Mozambique, Kenya and Zanzibar today.

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

`coasts::ingest_assets()` writes the snapshot — **not Timor**, since Phase 12 —
and `get_assets()` reads it back from the **hub** bucket. Things to know:

- The snapshot is **cross-country and carries no `country` column** — 1,609
  taxa rows across four countries, and every one of Timor's 56 codes is also
  used by another country, two of them against a different `scientific_name`.
  `get_assets(conf)` returns them **already narrowed to Timor** — it is a
  six-line binding over `coasts::get_assets()`, which does the download, the
  form-id filtering, the column drop and the de-duplication in one call and
  returns five tables (`taxa`, `gear`, `vessels`, `sites`, `geo`). **There is no
  `timor_assets()` any more**: Phase 12 deleted it, because no other country has
  a narrowing helper — they filter during the download, and Timor was the only
  one downloading everything and filtering afterwards. Verified 60 / 9 / 2 / 40 /
  37 rows across the swap. See COASTS-TODO C13.
- **The form record ids are resolved at run time, not hardcoded** (Phase 12).
  `get_airtable_form_id()` looks each one up from the frame's `forms` table by
  the KoBo asset id already in `ingestion.landings.{v2,v3}.asset_id`, exactly as
  Kenya, Mozambique and Zanzibar do; `metadata.airtable.form_ids` is gone.
  `timor_form_ids()` is the two-element map Zanzibar writes inline; it needs no
  cache, since `get_assets()` is called once per workflow function and each is
  its own `Rscript` process. The lookup must hit the Airtable API because
  `coasts::ingest_assets()` keeps
  `airtable_id` on `geo` but **drops it from `forms`**, so the mapping is not in
  the snapshot; adding it back is a one-line hub change, COASTS-TODO **C29**.
  Beware: in `forms` the column named `form_id` holds the *KoBo asset id*, while
  in every other table `form_id` holds *Airtable record ids*.
- ~~coasts uploads it to the **country** bucket while every reader resolves the
  **hub**, so `ingest_assets()` mirrors it to the hub afterwards~~ — stale twice
  over: COASTS-TODO C11 shipped in coasts 4.7.0 and Phase 11 deleted the mirror,
  then Phase 12 deleted Timor's `ingest_assets()` entirely.

Where the frame and the Google Sheets tables overlap, **Airtable is
authoritative** (PLAN §2.5): taxa, gears, vessels, landing_sites,
districts/regions, pds_devices. Every one of those joins has moved, and as of
Phase 12 the Sheets keep **only `devices`** — see the module map above. This is
not cosmetic: without the frame the API export would be schema-correct but full
of untranslated Tetum labels.

**`pds_devices` is the exception, and it is incomplete.** The frame is now
authoritative on the PDS side — `conf$pds$customers`
(`MAF / WorldFish`, `Traders`, `FSSP2: Traders`) is what
`coasts::ingest_pds_trips()` filters the API response by, and it is how every
country claims its devices out of the shared PDS account. Measured 2026-08-11
against the 449 IMEIs Timor's token returns, that list covers 422; the **27 it
misses are in no frame customer at all** and are worth 2,791 trips, 14,519
tracked hours and 59 of the 6,999 landing↔trip matches. The IMEIs are listed in
the Phase 7 STATE entry — adding them to Airtable recovers the data on the next
run.

On the **survey** side the same gap is why `metadata.google_sheets.tables` still
lists `devices`: the frame's 442 Timor rows are a strict subset of the Sheets'
595, so `validate_imeis()` would take alert 3 from 824 to 1,475 submissions and
strip the resolved `tracker_imei` — hence the matched trip — from 651 of them.
Switch it only once the frame is complete.

### The taxa and weight path

Rewritten **2026-09-05** at the user's instruction that it be aligned to the WIO
packages. Three sources, and after this change **none of them is a Google
Sheet** — `calculate_weights()` was the weight path's last Sheets reader.

| what | where from |
|---|---|
| which taxon *codes* Timor has | the PESKAS \| FRAME assets snapshot (authoritative for taxa, PLAN §2.5) |
| each code's *scientific name* | the FAO **ASFIS** list, `gs://timor{,-dev}/asfis__*.parquet`, joined on `Alpha3_Code` — the same object and access path Mozambique uses |
| the curated invertebrate coefficients | `inst/extdata/morphometric-coefficients.csv` |

**ASFIS was adopted for alignment, not for effect.** Measured before the switch:
55 of Timor's 56 codes carry an identical `scientific_name` in the frame and in
ASFIS, the exception being `MZZ` (frame `Osteichthyes`, ASFIS
`Actinopterygii`), and both expand to the same 50 codes over the same species.
Join on `Alpha3_Code`, never `Taxonomic_Code`.

**FAO areas 57 and 71**, from `metadata.fishbase.fao_areas`, with
`filter_by_area = TRUE`. That key **must** exist: `coasts::resolve_fao_areas()`
falls back to `c(51, 57)`, the Indian Ocean pair the WIO repos use, so an unset
key filters Timor on one wrong area and misses the Pacific one silently. It was
absent and read as `NULL` from Phase 3 until this session, harmless only because
the filter was off.

**`taxa_search_aliases()` is the load-bearing part.** `expand_taxonomic_info()`
matches the FishBase backbone at species / genus / family / order / class only,
and FAO names several taxa at ranks that backbone has no column for. Aliases are
**additive** — extra search names for a code, never a replacement — so nothing
that already resolves can regress and **no published taxon code changes**. This
is deliberately *not* Mozambique's approach, which recodes `catch_taxon` in the
data (`TUN` → `TUS`, `SKH` → `CVX`, `CLP` → `ANX`) and thereby renames the
published taxon.

| code | ASFIS name | why it fails | alias |
|---|---|---|---|
| `TUN` | `Thunnini` | tribe — no such rank | the tribe's 5 genera |
| `SKH` | `Selachimorpha (Pleurotremata)` | superorder | `Carcharhiniformes` |
| `LGE` | `Leiognathus equulus` | binomial FishBase has revised | `Leiognathidae` |
| `CLP` | `Clupeidae` | **stale family**, see below | `Dorosomatidae` too |

`TUN` is **56% of landed weight**, so its pool is a fishery decision, taken with
the user on 2026-09-05: the tribe *Thunnini* is *Allothunnus, Auxis, Euthynnus,
Katsuwonus, Thunnus*, which is what Timor's "Tunas nei" lands. Mozambique's
`TUS` would drop skipjack and frigate tuna; the ASFIS family `Scombridae` would
pull in the mackerels Timor codes separately as `RAX`. At 25 cm: 249 g for the
tribe, 249 g for `TUS`, 172 g for `Scombridae`.

`CLP` is **not** a rank problem. FishBase's 2022 revision moved the tropical
sardines — *Sardinella*, *Amblygaster*, *Herklotsichthys*, *Nematalosa*,
*Tenualosa* — out of `Clupeidae` into `Dorosomatidae`, while FAO still files
them all under `CLUPEIDAE`. FishBase's `Clupeidae` now holds 15 mostly temperate
species, so Timor's second-largest taxon (26% of landed weight) was priced off
**114 records of *Clupea harengus* and 36 of *Sprattus sprattus***. Searching
both families restores 123 area-57/71 records over 25 Indo-Pacific species.
**Do not "fix" this in Airtable** — it was considered and rejected: `DCX` and
`CLU` are `Clupeoidei`, a *suborder*, which matches nothing; `DAG` is the
freshwater Lake Tanganyika sardine; `SIX` (*Sardinella* spp) resolves but
narrows to 8 species, drops the herrings the label names, and renames a
published taxon key.

**Coefficients are restated on a total-length basis before pooling** (added
2026-09-06, after Zanzibar 4.9.0). A published `W = a * L^b` is fitted on
whichever axis the study used, and for Timor's taxa only 1,824 of 3,702 matched
pairs are `TL` — 990 are `FL`, 460 `SL`. Pooling them as published and applying
the result to a TL measurement overestimates weight, because FL and SL are
shorter (medians 0.958 TL and 0.827 TL). `get_taxa_morphometrics()` already
returned `length_length` and it was being discarded; it is now used to convert.
Four things to know:

- **POPLL fits `Length1 = aL + bL * Length2` — the second column is the
  predictor.** coasts' roxygen states this backwards (COASTS-TODO **C28**).
  Getting it wrong inverts every ratio.
- Substituting `L_type ~= ratio * TL` gives `W = a * ratio^b * TL^b`, so **`b`
  is unchanged and only `a` is rescaled**.
- **`length_types = NULL` is passed deliberately.** The coasts default keeps
  only `TL`/`FL` pairs, which would drop 1,427 of 2,642 usable conversions —
  all the `SL` ones, where the correction is largest.
- **The 285 pairs with no conversion are kept as published, not dropped**, and
  the curated rows are bound on afterwards so they are never restated. Dropping
  the unconvertible takes `MOO` down 92% and `SFA` 78%. Zanzibar converts only
  for taxa that would otherwise have nothing; Timor converts everything it can,
  which is why no `Type` filter is needed.

Effect measured in isolation: national catch **−10.9%**, price/kg **+11.7%**,
nutrient supply **−11.9%**; `CJX` −37%, `LWX` −19%, `FLY` −15%, `CGX` −14%,
`CLP` −12%, `TUN` −8.5%; `GZP` and `MOO` unchanged, being curated-driven.

**`rescue_by_common_name()` is gone**, and it was not a like-for-like removal —
it was the *only* source of coefficients for `TUN`. It ran
`rfishbase::common_to_sci()` on the literal strings `"Tuna"`, `"Shark"` and
`"Garfish"`, a substring match on common names, so `SKH`'s pool contained
*Pangasius sanitwongsei* (a Mekong catfish) and the aquarium bala shark, and
`GZP`'s was topped by driftfishes and scads at 185–328 g while the actual
garfish sit at 17–21 g. It also never touched the *expansion*, only the
coefficients — which is why **`TUN`, 51% of national catch, contributed nothing
to any published nutrient figure** for the life of the pipeline. Fixing the
alias fixed the nutrients as a side effect.

**Two guards, and they exist because of a measured failure.** See COASTS-TODO
**C25** before trusting any single run's numbers:

- `assert_taxa_coverage()` **errors** when a taxon resolves to no coefficient
  pair. `MZZ` (`Actinopterygii`, which FishBase files as `Teleostei`) and `SWX`
  (`Algae`) are the two documented exemptions — neither has ever had a
  coefficient here, and both weigh zero.
- `get_nutrients_table()` **warns** for taxa with no nutrient values. Currently
  `CUX` and `GZP`, together 0.5% of catch. A warning rather than an error
  because `get_fao_composition()` legitimately does not cover every
  invertebrate.

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

One thing Timor keeps, deliberately:

- **`add_version()`** — a naming helper, not a storage function. `coasts` exports
  a body-identical copy, but delegating would mean editing ~40 call sites plus
  three `inst/report/` drivers to remove an exported name for no behavioural
  gain. Dedupe when it is upstreamed, not before.

The retry wrappers are **not** one of them any more: `coasts` 4.6.0 shipped
`insistent_upload_cloud_file()` / `insistent_download_cloud_file()` (COASTS-TODO
C5) and Timor's local copies are gone. The one remaining call site,
[data-raw/convert-pds-tracks.R](data-raw/convert-pds-tracks.R), is written
`coasts::insistent_upload_cloud_file()` — keep the prefix.

**`coasts::cloud_object_name()` is not a drop-in for Timor's deleted version.**
The signatures match, but coasts returns `selected_rows$name[1]` where Timor
returned the whole vector. Every Timor call site was audited against the dev
buckets and only one relied on the vector: the ~100k track names in the PDS
bucket. Use **`coasts::cloud_object_names()`** (plural, 4.6.0, with
`latest_only`) for that — `get_tracks_descriptors()` and
`data-raw/convert-pds-tracks.R` both do (`get_tracks_ids()` did too, until
Phase 11 deleted it as unreachable). Never use the singular to enumerate a
bucket; it silently returns one name, and on the PDS path that reads as "no
tracks stored" and re-fetches the entire history from the API.

Listing that bucket is not cheap — ~100k objects at 1,000 per request, three to
thirty minutes depending on how the API feels. Do it once per function and pass
the vector down, as `describe_pds_tracks()` does.

Prefer `coasts::resolve_storage_opts(pars, type)` over reaching into
`conf$storage$google$options_*` by hand. Since coasts 4.6.0 it knows
`"coasts"` (hub, falling back to `options`), `"country"`, `"pds"` and
`"public"`.

| bucket | contents |
|---|---|
| `timor` / `timor-dev` | surveys and derived tables. Raw, preprocessed, merged and the frozen v1 snapshot are all **parquet** since Phase 4 (`timor-landings-v{2,3}_{raw,preprocessed}__*.parquet`, `timor-landings-merged__*.parquet`, `timor-landings-v1-frozen__*.parquet`); the weight artefact is parquet since Phase 5, and since **Phase 8 the validated artefact is written once**, `timor-landings-merged_validated_long__*.parquet` — the nested `.rds` twin has no writer any more, it is rebuilt on read by `get_validated_landings()` |
| `pds-timor` / `pds-timor-dev` | one parquet per GPS trip, `pds-tracks_<trip_id>.parquet`, written by `coasts::ingest_pds_tracks()`. **Not versioned** — a re-ingest overwrites. The 103,373 `pds-track-<trip_id>__*__.csv.gz` objects of the old family were converted in place by [data-raw/convert-pds-tracks.R](data-raw/convert-pds-tracks.R) in Phase 7, not re-fetched. Phase 11a deleted the dev bucket's copies; the ~98k in `pds-timor` are Phase 11b's, after the production conversion |
| `public-timor` / `public-timor-dev` | `portal-*.json` — the live portal contract |
| `peskas-coasts` / `peskas-coasts-dev` | the shared cross-country hub (`options_coasts`). **Read *and* written** by coasts: `assets__*`, `taxa-fishbase-enriched`, H3 effort/CPUE grids, and per-country `*_fishery_metrics` / `*_monthly_summaries_map`. Both are live — `default` must stay on `-dev` |
| `peskas-api-prod` / `peskas-api-dev` | cross-country API parquet (`options_api`), live for Kenya/Moz/Zanzibar. Timor joined in Phase 6 and publishes to **`-dev` only** so far: `timor/{raw,validated}/trips-{raw,validated}__*.parquet`. The service account has object create/delete on **both** buckets (verified 2026-08-11 via `testIamPermissions`), so the first prod write is a decision, not a permission |

**No lifecycle policy is set.** Every pipeline run appends new versions and
nothing is ever deleted; `gs://timor` holds ~33k objects and `gs://pds-timor`
~98k. See [.claude/migration/AUDIT.md](.claude/migration/AUDIT.md) for the full
prefix inventory including orphaned prefixes.

Interchange format is **flat long parquet** from raw through validated
(Phases 3, 4, 5 and 8): one row per (submission, catch, length bin). Since
Phase 8 there is **no `.rds` artefact left on the survey path**.
`timor-landings-merged_validated_long__*` (40 columns since Phase 6) is the one
validated object; the nested `timor-landings-merged_validated__*.rds` has no
writer any more. `get_validated_landings()` re-nests the parquet on read into
the `landing_catch` / `length_frequency` shape the portal path works in, so
`format_public_data()`, `estimate_fishery_indicators()` and `merge_trips()` did
not have to change. The two shapes were proven interchangeable first — 97,360
submissions, 1,648,016 catch rows, `all.equal` TRUE, the only difference 1,599
catch weights moving by ≤2.9e-11 g on the grams → kg → grams round trip.

`all_trips__*.rds` stays nested `.rds`, deliberately. It has **no
cross-country counterpart** — no other country has a `merge_trips()` at all,
and `coasts::merge_survey_trips()` does a different job (COASTS-TODO C10) — so
flattening it would buy no harmonization while requiring `format-public-data.R`,
`model-fishery.R` and `pds-maps.R` to be rewritten against a live portal.

On the **PDS** path Phase 7 made raw trips and the tracks parquet
(`pds-trips__*.parquet` from `coasts::ingest_pds_trips()`,
`pds-tracks_<id>.parquet` from `coasts::ingest_pds_tracks()`,
`pds-tracks-descriptors__*.parquet` from `describe_pds_tracks()`).
`pds-trips_validated__*.rds` is still `.rds`, and Phase 8 deliberately left it
alone rather than flipping it with the merge path as PLAN anticipated: it
carries `tracker_trip_start`/`_end` as `Asia/Dili` POSIXct and `merge_trips()`
derives `landing_date` from `tracker_trip_end`, so a parquet round trip is a
timezone risk on the exact code path commit `15f6b18` exists to protect — for
no gain, since only `merge_trips()` and `test_validated_pds_trips.R` read it
and neither is cross-country. `pds-trips_preprocessed__*` and
`pds-track_preprocessed__*` have no writer any more.

## Portal contract (do not break)

[export.R](R/export.R) writes **seven** objects as `portal-<name>__*__.json` to
`public-timor`:

```
aggregated  taxa_aggregated  municipal_aggregated  municipal_taxa
nutrients_aggregated  data_last_updated  summary_data
```

`peskas.timor.portal.v2/scripts/fetchData.js` lists `portal-*` in the bucket,
keeps the newest version of each, and **excludes** `indicators_grid.json`,
`label_groups_list.json`, `pars.json`, `taxa_names.json`,
`var_dictionary.json` — so these seven are exactly the live set. Until Phase 8
the export also emitted `indicators_grid` and `label_groups_list`, the two the
portal excludes, rebuilt on every run from an `indicators_gridded.rds` last
written 2024-07-27. Both were dropped; their existing versions stay in the
bucket, so re-adding two lines to `export_files()` restores the family.

**`landing_site` appears in none of the seven** — verified against the Phase 0 golden set. It reaches only the cross-country API, which is why Phase 12 could move it onto the frame's spellings (11 of 40 names change) without touching the portal. `municipality` **is** published, as `region` in `municipal_aggregated` / `municipal_taxa`, and the frame reproduces its twelve names exactly.

The portal discovers files dynamically, meaning a **renamed or dropped object
silently disappears from the site**. The gate for that is
[data-raw/compare-portal-json.R](data-raw/compare-portal-json.R), written in
Phase 8 and reused by Phase 11: it asserts object names, then keys, nesting,
column sets and column types against the Phase 0 golden
(`reference/2026-07-31_90ede9a/`, gitignored), and prints per-column numeric
summaries against the newest `public-timor-dev` set. Row counts and column
order are reported, not failed — the portal reads an array of objects by key.
Run it before touching anything on the export path.

## Pipeline DAG

From [.github/workflows/data-pipeline.yaml](.github/workflows/data-pipeline.yaml)
(every 2 days at 00:00 UTC, plus on every push; ~1h30m):

```
build-container
├── ingest-preprocess-metadata-tables   ingest_metadata_tables → preprocess_metadata_tables
├── ingest-landings                     ingest_landings                 [v2 + v3 → raw parquet]
│   └── preprocess-landings             preprocess_landings   [v2 + v3 → long parquet]
└── ingest-pds-data                     coasts::ingest_pds_trips → coasts::ingest_pds_tracks
    └── preprocess-pds-data             describe_pds_tracks
        └── validate-pds-data           validate_pds_trips        [tinytest]

merge-landings   merge_landings → calculate_weights
└── validate-landings  validate_landings         [flags → MongoDB, tinytest]
    └── merge-trips    merge_trips                                [tinytest]
        ├── model-indicators  estimate_fishery_indicators
        └── export-trips      format_public_data → export_files   [tinytest]
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
`ARG COASTS_REF` **with no default** → `install_github('WorldFishCenter/peskas.coasts', ref = ...)`,
`ggchicklet` + `glmmTMB` from GitHub, then `COPY . /home` +
`remotes::install_local()`. The workflow's "Resolve latest peskas.coasts
release" step reads the latest tag (**v4.6.0** as of 2026-08-11) and passes it
in, so a build always records which hub release it used; a local build must pass
`--build-arg COASTS_REF=<tag>` or fail. Image is pushed to
`ghcr.io/worldfishcenter/peskas.timor.data.pipeline/r-runner-peskas-timor:latest`.
`Dockerfile` (dev, used by `docker-compose.yaml`) mirrors the same package set
and the same `COASTS_REF`. Keep the two in step. **coasts ≥ 4.6.0 is now a hard
floor**: Phase 7's PDS path needs `cloud_object_names()`, `get_trip_points()`,
`resolve_storage_opts(conf, "pds")` and the `"MAF / WorldFish"` customer.

**`devtools::check()` baseline** (re-measured after Phase 11a): **0 errors,
0 WARNINGs, 4 NOTEs**, and testthat is **green** (27 assertions).
The four NOTEs are: 30 non-default Imports, installed size (`inst/export` +
`inst/report`), unverifiable file timestamps, and four undefined globals (`sd`,
`rnorm`, `if_all`, `` `Estimated revenue` ``) — none of them new. The long-standing
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
  come back. Phase 7 dropped **`googleCloudStorageR`** the same way — the last
  two direct `gcs_list_objects()` calls became
  `coasts::cloud_object_names()` — and `arrow` came *back* into use, in
  `describe_pds_tracks()`, which reads the per-trip track parquet directly.
  Phase 11 dropped three more for the same reason: **`httr2`** (the KoBo
  validation client moved to `coasts::`), **`reticulate`** (`kepler_mapper()`)
  and **`glmmTMB`** (`model_indicators()`), and moved **`ggplot2`** to Suggests,
  where the Rmd reports are its only consumers. The `installGithub.r glmmTMB`
  line went out of both Dockerfiles with it — nothing in the package or in
  `coasts` imports it.

## CI — nine workflows since Phase 9, from eleven

| workflow | state | notes |
|---|---|---|
| `data-pipeline.yaml` | active | **the only one that produces data.** Green end to end |
| `R-CMD-check.yaml` | active | replaced `check-standard.yaml` in Phase 9. **One runner** (`ubuntu-latest`, release), not the five-platform matrix — deliberate, see the file header |
| `pkgdown.yaml` | active | r-lib v2 template; keyword-driven reference sections |
| `test-coverage.yaml` | active | `tests/testthat/` only; `fail_ci_if_error: false`, no `CODECOV_TOKEN` exists |
| `pr-commands.yaml` | active | `/document` and `/style`, now gated on `MEMBER`/`OWNER` |
| `release.yaml` | active | new in Phase 9. Cuts a release from the top block of `NEWS.md` on a push to `main` |
| `data-report.yaml` | disabled (inactivity) | rebuilt in Phase 9: no build job, runs in the pipeline's container |
| `dataverse-upload.yaml` | disabled (inactivity) | same |
| `validation-email-sender.yaml` | disabled (inactivity) | same, and repointed at the Mongo flags sink |

**Re-enabling the three disabled ones is a Phase 11b action.** A schedule fires
from the **default branch**, so until the migration branch is merged those crons
would run `main`'s pre-migration code against production.

Deleted in Phase 9: `form-summary.yaml`, `keplergl-map.yaml`,
`upload-matched-trips.yaml` — three of the four workflows that could only build
through the retired `docker.pkg.github.com` registry with
`whoan/docker-build-with-cache-action@v5`, none of which had succeeded since
2025-08. The fourth, `validation-email-sender.yaml`, was rebuilt instead because
Phase 5 gave `send_validation_mail()` a working reader.

Two things the non-pipeline workflows must not do again: **build their own
image** (they each carried a duplicate `build-container` job that has been
unable to succeed since Phase 2 gave `Dockerfile.prod` an `ARG COASTS_REF` with
no default, which none of them resolved), and **assume a referenced function is
exercised** — three of these nine are disabled and one produces data.

The three API-export steps aside, `data-pipeline.yaml` is unchanged in substance
since Phase 8: `checkout@v5`, `build-push-action@v6`, `ubuntu-latest`,
`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24`, the COASTS_REF resolution, the four
tinytest steps, and `log_threshold = logger::INFO` on both `coasts::` calls.
`export_api_raw()` / `export_api_validated()` were wired in in Phase 9 and carry
`if: ${{ !endsWith(github.ref, '/main') }}` — deleting those two lines is Timor's
first write to `peskas-api-prod`, which is a separate, unmade decision.

## Conventions and gotchas

- Every workflow function follows: `read_config()` → authenticate → download
  latest inputs → process → `logger::log_info()` → `add_version()` → upload.
- Logging is `logger`; the `log_threshold` argument on workflow functions sets
  the level.
- **The preprocessed table is one row per (submission, catch, length bin)**, and
  since Phase 12 it is **38 standard columns only** — `landing_cols()` in
  [preprocessing-surveys.R](R/preprocessing-surveys.R) is the list, and
  `reshape_landings()` ends with `select(all_of(landing_cols()))` exactly as
  Zanzibar's `preprocess_landings()` does. `merge_landings()` applies it again
  after the bind, because the **frozen v1 snapshot still carries the raw
  columns** and must not be rebuilt.
  `harmonise_v2()`/`harmonise_v3()` still reconcile the raw columns per form
  version; they are now consumed rather than stored.
  The **59-column raw passthrough is gone** (Phase 12). Its last reader was
  `enumerators_summary.Rmd`, deleted in the same phase, so ALIGNMENT-AUDIT §7's
  "not deletable" no longer holds. Dropped with it: KoBo bookkeeping nothing
  read (`_uuid`, `_xform_id_string`, `formhub/uuid`, `meta/instanceID`, the five
  `_validation_status.*`), duplicates of standard columns (`landing_site_name`
  **is** `landing_site_code`; `trip_group/gear_type` **is** `gear_code`), and
  `deviceid` — a phone IMEI riding into a stored artefact for no reader.
  **`submitted_by` is standard and stays**: it identifies the enumerator and
  `push_validation_flags()` sends it to the validation app, exactly as
  Zanzibar's `validation.R` joins it onto its flags. Only the raw
  `_submitted_by` duplicate went.
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
  `production` before the Phase 11b cutover, **after** Phase 11a's deletions,
  so the permanent snapshot is built by the final code.
- ~~**`length_type` is descriptive, not functional.**~~ — **removed in Phase 12.** It selected nothing, converted nothing, and appeared in none of the seven portal objects nor the 22 API columns. With `catch_types` gone it would have collapsed to a constant `"TL"`. If a per-axis correction is ever wanted, the place to act is `summarise_lw_coeffs()`, which pools coefficients across every measurement axis.
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
  rewrite of the export path. It lives in `summarise_estimations()`
  ([format-public-data.R](R/format-public-data.R)) and its point is that `today`
  must be built in the **data's** timezone, not the session's, or
  `floor_date(today, "month") == date_bin_start` silently matches nothing and
  the current month is published unscaled. Re-verified in Phase 8 against the
  parquet inputs, not assumed: `models$national$aggregated$landing_period`
  still carries `tzone = "Asia/Dili"`, exactly one month matches, and the
  emitted August 2026 figures are scaled by 12/31.
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
  are historical residue, not a live bug. Deleting them is Phase 11b — they are
  in a production bucket.
  ~~**Found in Phase 7, open upstream:** `coasts::read_config()` logs the whole
  resolved config at DEBUG (COASTS-TODO C21)~~ — **fixed upstream** in coasts
  PR #11 and in every release the workflow can resolve. The
  `log_threshold = logger::INFO` at each `coasts::` call site stays as a
  regression guard, deliberately; it is still live in the other three repos.
- **The FishBase release is pinned in config, and it must stay that way.**
  `metadata.fishbase.db_version: "25.04"` (coasts >= 4.10.0). `"latest"` is not
  a fixed dataset: `rfishbase` 5.0.3 moved the parquet host from HuggingFace
  (which stops at **25.04**) to Source Cooperative (which serves **26.06**), so
  a container rebuild silently moved the pipeline to 26.06 — in which
  `Caesionidae` and `Scaridae` survive as family names with **zero species
  attached**. `CJX` and `PWT` therefore weighed `NA`, which sums to zero, and
  `CJX` — 5% of landed weight and one of the 13 `models.modelled_taxa` — was
  **missing from `portal-taxa_aggregated`** on two runs with no error. Four
  things follow:
  - **Pass `conf` to `coasts::get_taxa_morphometrics()`.** Without it coasts
    falls back to its own `read_config()` and resolves `"latest"`, silently
    undoing the pin. Both Timor call sites pass it.
  - **Every number in `NEWS.md` was measured on 25.04.** Moving the key
    re-baselines the portal; run `data-raw/compare-portal-json.R` when you do.
  - The two hosts serve **identical** 25.04 (verified 2026-09-06: 25,730
    `poplw` and 27,211 `popll` rows on both), so the pin holds whichever
    `rfishbase` is installed. An earlier stopgap that pinned `rfishbase` to
    5.0.1 in the Dockerfiles is **gone** — it pinned the host, not the release.
  - `assert_taxa_coverage()` stays as the backstop. It is what caught this.
  A trap worth naming: from the artefacts alone, **"varies between runs" and
  "varies between environments" look identical**. The first diagnosis here was
  "non-deterministic partial reads" and it was wrong. Resolve the release before
  concluding either.
- **`expand_taxonomic_info()` warns about names it cannot match** (coasts
  4.10.0), and for Timor it always names six: `Algae`, `Brachyura`,
  `Actinopterygii`, `Selachimorpha (Pleurotremata)`, `Leiognathus equulus`,
  `Thunnini`. **That is expected, not a fault.** Four of them (`TUN`, `SKH`,
  `LGE`, plus `CLP`'s second family) resolve through `taxa_search_aliases()`
  instead, and `CRA` through the curated table; only `MZZ` and `SWX` genuinely
  have nothing, and both are exempt in `assert_taxa_coverage()`.
- Tests are Timor's advantage over the other pipelines.- Tests are Timor's advantage over the other pipelines. **Never delete an
  assertion to make a change pass** — update the expectation deliberately.
