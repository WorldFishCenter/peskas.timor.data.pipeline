# Aligning `peskas.timor.data.pipeline` to the harmonized Peskas standard

Status: **plan approved, not started**
Reference implementation: `peskas.mozambique.data.pipeline` (local copy at repo root, untracked + ignored)
Normative spec: `peskas.mozambique.data.pipeline/inst/config_template.yml` — the
cross-country "PESKAS UNIFIED CONFIGURATION TEMPLATE". Derive Timor's config from
**this**, not from Moz's `config.yml`.
Shared hub: `coasts` (`WorldFishCenter/peskas.coasts`, local checkout at `~/Desktop/work/wf_projects/peskas.coasts`)
Also cross-check: `peskas.kenya.data.pipeline`, `peskas.zanzibar.data.pipeline`

**Exhaustive file-by-file diff, dotfiles included: `.claude/migration/STRUCTURAL-DIFF.md`.**
Read it alongside this plan — it carries the `auth/` → `.env` credential map, the
ignore-file deltas, the CI drift table, and the list of reference-repo bugs not to
replicate.

---

## 1. Why this is not a straight port

Timor was the first pipeline and predates every convention the others share. The
divergence is structural, not cosmetic:

| Concern | Timor today | Harmonized standard | Verdict |
|---|---|---|---|
| Config file | `inst/conf.yml`; keys `surveys.landings_{1,2,3}`, `validation.*`, `models.*`, `export_dataverse.*`; a `local:` env reading `auth/*` files | `inst/config.yml`; `country:`, `ingestion.<source>`, `surveys.<source>.{raw,preprocessed,validated}`, `api.trips.{raw,validated}`, `pds`, `metadata`, `storage.google.{options,options_coasts,options_api}`, `storage.mongodb`, `pds_storage`, `validation` | rewrite |
| Secrets (local) | `auth/` directory of plaintext files, `local:` config env | `.env` + `dotenv::load_dot_env()` inside `read_config()`, `.env.example` committed, no `local:` env | replace |
| `read_config()` | hardcoded package, no dotenv | dotenv-aware; `coasts::read_config(package = "...")` accepts a package arg | align |
| Storage | own `R/cloud-storage.R` (261 l) + `R/get-cloud-files.R` (318 l) | `coasts::{upload,download}_parquet_to_cloud`, `cloud_object_name`, `upload_cloud_file`, `download_cloud_file`, `resolve_storage_opts` | delete, delegate |
| Interchange format | `.rds` with nested list-columns (`landing_catch`, `length_frequency`) | flat **long** parquet, one row per catch item, lz4 | change |
| Raw survey storage | per-batch `.json` files uploaded verbatim | one flattened parquet table (`flatten_row`/`flatten_field`/`rename_child`) | change |
| Kobo retrieval | own `get_kobo_data()` that writes files to disk | `coasts::get_kobo_data()` returning a list | delegate |
| PDS | bespoke: `ingest-pds-data.R` (858 l), `retrieve-pds-data.R`, `preprocess-pds-trips.R` | `coasts::ingest_pds_trips/ingest_pds_tracks/preprocess_pds_tracks/predict_pds_tracks` + H3 gridding | delegate (decided) |
| Validation flags | Google Sheets (`validation_alerts`, `VALID_SHEET_ID`) | MongoDB `validation-{dev,prod}` collections `flags-<asset_id>`, plus write-back of validation status to Kobo | change |
| Reference data | 15 Google Sheets metadata tables; own `air_*` Airtable client, orphaned | PESKAS \| FRAME Airtable base → `coasts::ingest_assets()` assets snapshot, read by ingestion, PDS, taxa, modelling and export | **adopt — see §2.5** |
| Cross-country API | **none** | `export_api_raw()` / `export_api_validated()` → `peskas-api-{dev,prod}` bucket, `<country>/{raw,validated}/trips-{raw,validated}` | **add — this is the real payoff** |
| Portal | `format_public_data()` + `export_files()` → `public-timor` versioned `portal-*.json`, consumed by `peskas.timor.portal.v2` (`scripts/fetchData.js`) | `coasts::summarize_data()` + `coasts::export_portal()` → MongoDB `portal-prod` | **keep Timor's JSON** (decided) |
| Modelling | `estimate_fishery_indicators()` (glmmTMB), nutrients/RDI, Dataverse | `coasts::model_cpue`, `generate_fleet_analysis` | keep Timor-only, upstream generic parts |
| `R/` layout | 32 files, verb-noun mixed | `ingestion.R`, `preprocessing-surveys.R`, `survey-reshaping.R`, `validation.R`, `validation-functions.R`, `api.R`, `export.R`, `model-taxa.R`, `merge-trips.R`, `airtable.R`, `utils.R`, `utils-pipe.R`, `globals.R`, `<pkg>-package.R` | reorganize |
| Roxygen keywords | sparse `@keywords workflow` | `workflow {ingestion,preprocessing,validation,export}`, `helper`, `storage` — drives `_pkgdown.yml` | standardize |
| Docker | `rocker/geospatial:4.4`, ~70 `install2.r` packages, `rfishbase` pinned to 5.0.1, no `coasts` | `rocker/geospatial:4.5`, lean, `ARG COASTS_REF` resolved from the latest `peskas.coasts` release at build time | rewrite |
| GH Actions | 11 workflows, `checkout@v4`, `ubuntu-22.04`, `build-push@v5` | `checkout@v5`, `ubuntu-latest`, `build-push@v6`, `release.yaml` driven by NEWS.md, `R-CMD-check.yaml` | align |
| DESCRIPTION | 20 Imports / 50 Suggests, no `Remotes:` | lean Imports incl. `coasts`; `Remotes: github::WorldFishCenter/peskas.coasts` | rewrite |
| Tests | `inst/tinytest/` (4 suites) run in CI + a `testthat` stub | Moz/Kenya/Zanzibar have **none** | **keep Timor's — do not regress to the standard here** |

Two things Timor does *better* than the reference and must survive: the tinytest
suites wired into CI, and the depth of `validation-functions.R` (IMEI deployment
checks, landing regularity, mesh, gleaners, fuel, conservation, happiness).

---

## 2. Decisions locked

1. **Portal** — keep the `public-timor` GCS JSON contract and `peskas.timor.portal.v2`
   as-is. Align everything upstream of it. The coasts/Mongo portal path is
   explicitly out of scope, but Phase 6 makes it a drop-in later by emitting the
   standard API parquet that `coasts::summarize_data()` already consumes.
2. **Legacy forms** — ~~freeze v1 and v2~~ **superseded 2026-07-31.** Any form
   still receiving submissions runs through the recurring pipeline like every
   other source, however low the rate. Measured (AUDIT §1): v2 took a
   20-submission backfill on 2026-07-27; v1's last submission was 2020-08-28.
   So: **v2 and v3 stay live, v1 is frozen.** The three forms become versioned
   sources under `surveys.landings.{v1,v2,v3}` — the template's documented
   `{source}: {version}: raw / preprocessed / validated` shape, the same one
   Zanzibar uses for `wf_v1`/`wf_v2`.
3. **Timor-only modelling** — retained as clearly-scoped country modules; generic
   pieces (nutrients/RDI, Dataverse publishing, PDS trip validation) get upstreamed
   to `coasts` in a late phase.
4. **PDS** — switch to `coasts::*`. Largest single deletion; needs a parity check
   and a compatibility shim for the products `format_public_data()` consumes.
5. **Airtable frame — adopt it. Added 2026-07-31.** This was missing from the
   plan entirely; both earlier readings ("reconcile in P8", then AUDIT §7.7
   "delete") were answering about Timor's *own* orphaned `air_*` client and
   silently skipped the standard's frame integration, which Timor has never
   had.

   The **PESKAS | FRAME** base (`appMMEJYlJdfSJEjm`) is the cross-country
   harmonization layer: it maps each country's raw form labels to
   `standard_name` / `alpha3_code` / FAO codes. `coasts::ingest_assets()`
   snapshots taxa/gears/vessels/landing_sites/forms to
   `assets__<version>__.rds`, and `conf$metadata$airtable$name` is then read by
   coasts in `ingestion.R`, `ingestion-pds.R`, `model-fishery.R`, `fishbase.R`,
   `export.R`, `predict-tracks.R` and `aggregate-effort.R`.

   **Without it, Phase 6 produces a correctly-shaped API parquet full of
   untranslated Tetum labels** — schema-conformant and useless to consumers.
   That makes this a prerequisite for the payoff, not an optional extra.

   Timor's rows exist and are current (measured 2026-07-31): 57 taxa tagged
   `form_version: PeskAAS 2` with `alpha3_code`s matching `models.all_taxa`,
   7 gears (`original_name` in Tetum → `standard_name` + `code` +
   `FAO_abbrev`), 2 vessels, 40 landing sites, 457 `pds_devices`, and
   `countries.Current Form` = the live v3 asset id.

   Decisions:
   - **Airtable is authoritative where the two overlap** — taxa, gears,
     vessels, landing_sites, districts/regions, pds_devices. The Google Sheets
     tables keep only what Airtable does not cover: `morphometric_table`,
     `habitat`, `conservation`, `fishing_vessel_statistics`,
     `registered_boats`. Timor drops from 15 metadata tables to 5.
   - Config **and** the removal of the legacy client landed in **Phase 1**
     (the deletion was pulled forward from Phase 8 at the user's request);
     ingestion lands in **Phase 3**; the joins move off the Sheets in
     **Phase 4**.
   - **Key paths follow coasts, not Mozambique.** coasts' `fetch_asset()`
     reads `conf$airtable$frame$base_id` / `conf$airtable$token` at the top
     level, while Moz nests the same values under `metadata.airtable` because
     it vendors its own copy of the Airtable module. Timor imports coasts and
     calls it directly, so it keeps no local copy. `conf$metadata$airtable$name`
     / `$assets` (the snapshot prefix) are provided too — coasts reads both
     spellings.
   - **No `AIRTABLE_BASE_ID_ASSETS`.** It exists in Mozambique's `.env` but is
     referenced by nothing in coasts or in any country package. The assets
     snapshot is built from the frame base.
   - Credentials, resolved 2026-07-31: the working PAT has read access to
     PESKAS \| FRAME and PESKAS \| TRACKS. It must be stored **bare** (`pat…`)
     — `coasts::airtable_to_df()` does `paste("Bearer", token)`, whereas the
     retired `air_get_records()` passed the value verbatim as the header, so
     credentials inherited from `auth/` carry a stale `Bearer ` prefix. The
     `AIRTABLE_KEY` GitHub secret still carries it.

### Sub-decision still open (raise at Phase 5)

Validation flags sink: stay on Google Sheets, or move to MongoDB like the others?
Moving matches the standard and unlocks the shared validation UI, but needs a new
`MONGODB_CONNECTION_STRING_VALIDATION` secret and a rewrite of the weekly
`send_validation_mail()` reader. Recommendation: move.

---

## 3. Target architecture

```
KoBo v2+v3 ─coasts::get_kobo_data─▶ raw parquet ──preprocess_landings()──▶ preprocessed parquet (long, catch-level)
                                                          │                        │
frozen v1 parquet ────────────────────────────────────────┘                        ├──▶ export_api_raw()      ──▶ peskas-api-*/timor/raw
                                                                                   │
                                                            validate_landings() ──▶ validated parquet
                                                                                   │
                                                                                   ├──▶ export_api_validated() ──▶ peskas-api-*/timor/validated
PDS ──coasts::ingest_pds_trips/tracks──▶ preprocess_pds_tracks ──┐                 │
                                                                 └── merge_trips() ┘
                                                                          │
                                          estimate_fishery_indicators() ──┤   (Timor-only)
                                                                          ▼
                                             format_public_data() + export_files() ──▶ public-timor/portal-*.json
                                                                          └──▶ upload_dataverse(), reports, emails
```

The `peskas-api-*` branch is new and is what makes Timor a peer of the other
countries without touching its portal.

---

## 4. Phases

Each phase is **one fresh Claude session**. Do not combine.

| # | Phase | Scope | Risk | Est. sessions |
|---|---|---|---|---|
| 0 | Groundwork & audit | docs, `CLAUDE.md`, bucket/asset inventory, immediate hygiene fixes | none | 1 |
| 1 | Foundations | `config.yml` superset, `read_config()`, DESCRIPTION, Docker, `.env` | low | 1 |
| 2 | Storage delegation | delete `cloud-storage.R`/`get-cloud-files.R`, call `coasts::*` | medium | 1 |
| 3 | Ingestion | `ingestion.R`, v2+v3 live, freeze v1, metadata | medium | 1 |
| 4 | Preprocessing | `preprocessing-surveys.R` + `survey-reshaping.R` + `model-taxa.R` | **high** | 2 |
| 5 | Validation | `validation.R` + `validation-functions.R`, flags sink | high | 1 |
| 6 | API + merge | `api.R`, standard-schema export, `merge_trips()` | medium | 1 |
| 7 | PDS switch | delegate to `coasts`, parity check, shim for portal products | **high** | 1–2 |
| 8 | Country modules | rename/rewire modelling, nutrients, Dataverse, reports; portal JSON parity | **high** | 1–2 |
| 9 | CI / repo / docs | workflows, pkgdown, README, NEWS, release automation | low | 1 |
| 10 | Upstream to coasts | separate PRs in the `peskas.coasts` repo | medium | 1–2 |
| 11 | Cutover | strip legacy config keys and dead code, full green dev run, merge to main | medium | 1 |

---

### Phase 0 — Groundwork & audit

No behaviour change. Produces the map everything else navigates by.

- Write `CLAUDE.md` at repo root (Timor has none) modelled on Moz's: architecture,
  dev commands, config, env vars, storage layout, conventions. Every later session
  boots from this.
- Write `.claude/migration/STATE.md` (handoff log, template in §6).
- **Immediate hygiene (do now, they will bite otherwise):**
  - ~~Add `peskas.mozambique.data.pipeline/` to `.gitignore` and `.Rbuildignore`~~ — done.
    Make the `.Rbuildignore` entry an anchored regex like every other line:
    `^peskas\.mozambique\.data\.pipeline/`.
  - **`.dockerignore` is the live gap.** It contains only `.github` and `.git`.
    `.gitignore` does not affect the Docker build context, and `Dockerfile.prod`
    ends with `COPY . /home` — so a **local** `docker build` copies the 21 MB
    reference tree, including `peskas.mozambique.data.pipeline/.env` with live
    Mozambique credentials (KoBo token/password, two MongoDB connection strings,
    GCP SA key, Airtable token, PDS token/secret), into an image layer. CI is
    unaffected (`actions/checkout` materialises tracked files only). Fix list in
    STRUCTURAL-DIFF §2.
  - Add to `.Rbuildignore`: `^\.env$`, `^\.env\.example$`, `^\.claude$`,
    `^CLAUDE\.md$`, `^\.Renviron$`, `^.*\.parquet$`, `^.*\.rds$`, `^reference$`.
  - Add `*.parquet` to `.gitignore` — the migration moves everything to parquet and
    it is not currently ignored. Change `.env*` to `.env` / `.env.local` /
    `.env.*.local` so `.env.example` can be committed.
  - Rotate the `ANTHROPIC_API_KEY` duplicated in both repos' untracked `.Renviron`
    (verified never committed in either), and move it to `~/.Renviron`.
  - Delete `inst/__pycache__/` and `.Rhistory`.
- **Audit, recorded in `.claude/migration/AUDIT.md`:**
  - Last submission date + row count per Kobo asset (`KOBO_PESKAS1/2/3`) — confirms
    the v1/v2 freeze is safe and dates the cutoff.
    → **Done. It did not confirm it:** v2 was still receiving submissions, which
    is what superseded decision §2. See AUDIT §1.
  - Full object-prefix inventory of `timor`, `pds-timor`, `public-timor` (and the
    `-dev` twins): which prefixes are live, which are orphaned.
  - Which GH secrets each of the 11 workflows actually needs.
  - Exact list of `portal-*.json` files `peskas.timor.portal.v2/scripts/fetchData.js`
    downloads — this is the frozen output contract for Phase 8.
- **Snapshot golden outputs** to a local (gitignored) `reference/` dir: current
  validated landings, merged trips, and every `portal-*.json`. These are the
  regression baseline for Phases 4, 5 and 8.

---

### Phase 1 — Foundations

The key trick that makes the rest safely incremental: the new config is written as a
**superset**, keeping the legacy keys the current functions read. Old code keeps
running while new code uses new keys. Legacy keys are deleted in Phase 11.

- `inst/conf.yml` → `inst/config.yml`, derived from `inst/config_template.yml`
  (`country: timor`, `ingestion`, `surveys.<source>.{raw,preprocessed,validated}`,
  `api.trips`, `pds`, `metadata`, `storage.google.{options,options_coasts,options_api}`,
  `storage.mongodb`, `pds_storage`, `validation`) **plus** every current key
  verbatim. `coasts::read_config()` accepts either filename.
  - The template deliberately removed the per-source `version: preprocess: latest`
    field and warns that pipelines reading `version$preprocess` must re-add it.
    Timor uses that pattern on `landings_1/2/3`, `pds.trips`, `pds.tracks`,
    `metadata` and `validation` — decide per key here, don't drop silently.
- Ship Timor's own `inst/config_template.yml`.
- **`auth/` → `.env`**: 15 plaintext credential files read by the `local:` config env
  via `readLines()` get replaced by `.env` + `dotenv`. Delete the `local:` env and
  the `auth/` directory, commit `.env.example`. Full file→env-var mapping in
  STRUCTURAL-DIFF §1, including the GH secret renames
  (`KOBO_PESKAS1/2/3` → `KOBO_ASSET_ID_V1/2/3`, `AIRTABLE_KEY` → `AIRTABLE_TOKEN`).
  After this, local and CI differ only by `R_CONFIG_ACTIVE`, not by config branch.
- `.Renviron`: `R_CONFIG_ACTIVE=local` → `default` (the `local:` env is gone).
  Note Moz sets `dev`, which does not exist in its own config — do not copy.
- `.Rprofile` is currently **empty**; adopt the standard's `use_prod()` /
  `use_default()` / `show_env()` helpers and startup banner.
- `R/utils.R`: dotenv-aware `read_config()`, keep `add_version()` (already byte-identical
  to the standard), add `load_dotenv()`.
- `DESCRIPTION`: lean Imports (`arrow`, `config`, `dotenv`, `dplyr`, `furrr`, `future`,
  `git2r`, `glue`, `httr2`, `janitor`, `jsonlite`, `logger`, `lubridate`, `magrittr`,
  `purrr`, `readr`, `rlang`, `sf`, `stringr`, `tibble`, `tidyr`, `tidyselect`,
  `univOutl`, `coasts`) + Timor extras (`glmmTMB`, `mongolite`, `dataverse`,
  `blastula`, `googlesheets4`); `Remotes: github::WorldFishCenter/peskas.coasts`
  **and** `github::WorldFishCenter/ssf-ai-toolkit/Rplug@plug-R` (coasts imports
  `ssfaitk`). Add `Config/testthat/edition: 3`.
  **Pin coasts to a release tag for the duration of the migration**, not `latest`.
  Keep Timor's Title/Description/Authors — Moz's are unedited `usethis` boilerplate.
- `Dockerfile.prod`: `rocker/geospatial:4.5`, lean install list, `ARG COASTS_REF`.
  Resolve the `rfishbase 5.0.1` pin against what coasts needs — likely conflicting.
- Add `R/peskas.timor.data.pipeline-package.R` and align `R/globals.R`.
- Verify: `devtools::load_all()`, `devtools::document()`, `devtools::check()`;
  `config::get()` returns both old and new trees.

---

### Phase 2 — Storage delegation

- Delete `R/cloud-storage.R`. Replace all call sites with `coasts::cloud_storage_authenticate`,
  `upload_cloud_file`, `download_cloud_file`, `cloud_object_name`,
  `upload_parquet_to_cloud`, `download_parquet_from_cloud`.
- Collapse `R/get-cloud-files.R` — most helpers become one-line `coasts::download_parquet_from_cloud()`
  calls. Artifacts still stored as `.rds` keep working via `download_cloud_file()` +
  `readr::read_rds()`; formats flip in Phase 4.
- Keep `insistent_upload_cloud_file()` / `insistent_download_cloud_file()` retry
  wrappers if coasts has no equivalent — good candidates to upstream (Phase 10).
- Wire `coasts` into `Dockerfile.prod` and add the "Resolve peskas.coasts release"
  step to the workflow.
- Verify: package loads; one manual round-trip against `timor-dev`.

---

### Phase 3 — Ingestion

Rescoped 2026-07-31 by decision §2 and AUDIT §7.1. Smaller than originally
budgeted: the freeze shrinks to v1 only, and the JSON→flat conversion is already
done in the current code.

- New `R/ingestion.R`: one `ingest_landings()` looping over the **live** sources
  (`v2`, `v3`) — `coasts::get_kobo_data()` →
  `coasts::upload_parquet_to_cloud(prefix = conf$surveys$landings$<v>$raw$file_prefix)`.
  Both sources run every pipeline run; v2's low rate is not a reason to
  special-case it.
  - **Not a semantic change.** `get_kobo_data()` already flattens via
    `flatten_row()`/`flatten_field()`/`rename_child()` and writes one flat table
    ([retrieve-survey-data.R:225-231](../../R/retrieve-survey-data.R#L225-L231));
    the per-batch JSON objects stopped 2024-09-17. The only change is CSV →
    parquet plus delegating the retrieval to `coasts`.
- **Freeze v1 only** (last submission 2020-08-28, nothing in six years): a one-off
  script in `data-raw/` (provenance, not exported) that reads the current v1
  preprocessed output and writes `timor-landings-v1-frozen__<ts>__.parquet`.
  Assert the frozen row count matches the live v1 output before deleting
  anything. Baseline: 12,017 KoBo submissions (AUDIT §1).
- Delete `ingest_landings_v1v3()`, `ingest_landings_v2()`, `preprocess_legacy_landings()`,
  `preprocess_updated_landings()`, `merge_landings()`, `merge_versions()`,
  `R/retrieve-survey-data.R`, and the corresponding workflow jobs.
- Harmonize `ingest_metadata_tables()` toward the `get_metadata()` pattern.
- **Add `ingest_assets()`** (decision §2.5), modelled on `coasts::ingest_assets()`
  or delegating to it with `package = "peskas.timor.data.pipeline"`: pull taxa /
  gears / vessels / landing_sites / forms for Timor-Leste from the frame base and
  write one versioned `assets__*.rds` snapshot. Run it in the same workflow job as
  the metadata tables.
  - Resolve the token blocker first — see §2.5. Nothing here works until the PAT
    can read `appMMEJYlJdfSJEjm`.
  - **A coasts inconsistency to resolve here, sharper since 4.6.0.**
    `ingest_assets()` still writes the snapshot to
    `conf$storage$google$options` (the **country** bucket), while
    `ingestion-pds.R` and — as of 4.6.0 — `enrich_taxa()` both read it through
    `resolve_storage_opts(conf, "coasts")` (the **hub**). The writer and the
    readers now actively disagree. The hub holds 118 prod / 22 dev copies, so
    the hub is the de-facto home; fix `ingest_assets()` upstream (see
    COASTS-TODO C11) rather than working around it in Timor.
  - Do **not** delete the five Google Sheets tables Airtable does not cover
    (`morphometric_table`, `habitat`, `conservation`,
    `fishing_vessel_statistics`, `registered_boats`).
- **Freeze v1 with lengths normalised to total length.** New requirement, added
  2026-08-09. v1 records **fork length**; v2/v3 record TL. The weight path
  carries `summarise_ll_coeffs()` + `normalise_length_to_tl()` purely to convert
  it, for 10.4% of merged landings from a source frozen since 2020-08-28.
  Converting once at the freeze deletes both functions and removes a
  per-run FishBase round-trip.
- Verify: v2 + v3 raw parquet row counts ≥ their current submission counts
  (64,997 and 22,037 as of 2026-07-31); schema recorded in STATE. Assets
  snapshot for Timor-Leste contains **60 taxa rows over 56 distinct
  `alpha3_code`s, 9 gears, 2 vessels, 40 landing sites** (re-measured
  2026-08-09 — the earlier "57 taxa / 7 gears" is stale), every `alpha3_code`
  is present in `models.all_taxa` with zero difference either way, and
  `survey_label` is populated on every taxa, gear and vessel row.
- Once the snapshot exists, point `get_taxa_list()`
  ([calculate-weights.R](../../R/calculate-weights.R)) at it instead of the
  Google Sheets `catch_types` + `fao_catch` join. One-line swap, flagged in the
  weight-path commit.

---

### Phase 4 — Preprocessing (2 sessions)

The largest genuinely Timor-specific rewrite. Split:

**4a — reshaping to long format.** Replace `R/clean-raw-data.R`, `R/preprocess-landings.R`
(`step_1`/`step_2`), `R/pt_nest_species.R`, `R/pt_nest_attachments.R` with
`R/preprocessing-surveys.R` + `R/survey-reshaping.R` producing a flat long
catch-level table. Target columns, matching the standard:

```
submission_id, survey_id, landing_date, submission_date,
gaul_1_code, gaul_1_name, gaul_2_code, gaul_2_name, landing_site,
n_fishers (or the men/women/children triple), trip_duration,
gear, vessel_type, habitat, catch_outcome,
n_catch, catch_taxon / alpha3_code, scientific_name, length, catch_kg, catch_price
```

Timor's v3 form differs from the ADNAP form — expect real work in the nested
species/length-frequency unnesting. Moz's `reshape_species_groups()`,
`expand_length_frequency()` and `process_over100_length_groups()` are the shape to
follow, not code to copy.

**Label joins move to the assets snapshot.** Wherever preprocessing currently
joins a Google Sheets table to resolve a code to a name — catch types, gear
types, vessel types, stations/centro_pescas, reporting units — join the Phase 3
assets snapshot instead (decision §2.5) and emit the standard
`standard_name` / `alpha3_code` columns. Diff the resulting label distribution
against the golden snapshot before accepting: a mapping that silently drops a
gear will look like a clean run.

**4b — weights and taxa. ~~Pending~~ mostly DONE, pulled forward 2026-08-09**
(commit `a2c2881`). The coefficient path already runs on
`coasts::get_taxa_morphometrics()` with Mozambique's aggregation (geometric
mean of `a`, arithmetic mean of `b`) and Mozambique's `W = a * L^b * N`.
`get_catch_types()`, `get_fish_length()`, `retrieve_lengths()`,
`get_rfish_table()`, `ingest_rfish_table()` and the `taxize`/GBIF dependency
are all deleted. Measured against the golden snapshot: coefficients now cover
53 of 56 taxa (was 45), species expanded 693 → 5,259, total catch weight
**−15.4%** — expected, since the old code took the 90th percentile of
per-species weights.

What remains for 4b:

- Rename `R/calculate-weights.R` → `R/model-taxa.R` with the rest of the
  Phase 4 file reorganisation.
- Delete `summarise_ll_coeffs()` and `normalise_length_to_tl()` once Phase 3
  freezes v1 in total length.
- Point `get_taxa_list()` at the assets snapshot (Phase 3 dependency).
- Compare `R/calculate-nutrients.R` against `enrich_taxa()`'s nutrient columns
  and delete Timor's if they agree — **not** upstream it, contrary to §10.
- `metadata.fishbase.taxa_enriched.file_prefix` is still not in the config, and
  is only needed if Timor ever calls `enrich_taxa()` itself. It does not today:
  it calls `get_taxa_morphometrics()` with its own taxa list.

Verify against the Phase 0 golden snapshot: row counts, column set, and per-column
summary stats (mean/median/NA-rate) for `catch_kg`, `catch_price`, `length`.
Investigate any delta over ~1%.

---

### Phase 5 — Validation

- `R/validate-landings.R` → `R/validation.R`; keep and rename `R/validation-functions.R`.
- Preserve every Timor validator. Tag them `@keywords validation`.
- Resolve the flags-sink sub-decision. If MongoDB: add
  `MONGODB_CONNECTION_STRING_VALIDATION`, push via `coasts::mdb_collection_push()`
  to `flags-<asset_id>`, adopt `get_validation_status()` / `update_validation_status()`
  Kobo write-back, and rewrite `send_validation_mail()` to read from Mongo.
- Keep `inst/tinytest/test_validated_landings.R` passing — update expectations for
  the new schema rather than deleting assertions.
- Verify: flagged-submission count within a few % of the golden snapshot; every flag
  code still produced.

---

### Phase 6 — API contract + merge_trips

The interoperability payoff. Small, high value, low risk.

- New `R/api.R`: `export_api_raw()` and `export_api_validated()` writing to
  `conf$api$trips$*$cloud_path` (`timor/raw`, `timor/validated`) in the
  `peskas-api-{dev,prod}` bucket, with **exactly** the Moz output schema
  (`survey_id, trip_id, landing_date, gaul_1_*, gaul_2_*, landing_site, n_fishers,
  trip_duration_hrs, gear, vessel_type, catch_habitat, catch_outcome, n_catch,
  catch_taxon, scientific_name, length_cm, catch_kg, catch_price, tot_catch_kg,
  tot_catch_price`). Do not improvise field names — diff against
  `peskas.mozambique.data.pipeline/R/api.R` line by line.
- Rewrite `merge_trips()` in the standard shape (or adopt `coasts::merge_survey_trips`).
- Add `timor` to the `api.trips` block of `peskas.coasts/inst/conf.yml` (coasts PR).
- Verify: `coasts::summarize_data(package = "peskas.timor.data.pipeline")` runs
  end-to-end against dev without error. Not wired into the workflow yet — this is
  purely a proof the contract holds.

---

### Phase 7 — PDS switch

- Replace `R/ingest-pds-data.R`, `R/retrieve-pds-data.R`, `R/preprocess-pds-trips.R`
  with `coasts::ingest_pds_trips/ingest_pds_tracks/preprocess_pds_tracks`
  (`package = "peskas.timor.data.pipeline"`), driven by `conf$pds`.
- Add the Timor PDS customer name to the coasts customer list (coasts PR).
- **Keep `validate_pds_trips()`** — consecutive-trip merging, distance/outlier logic.
  coasts has no equivalent; upstream candidate.
- **Compatibility shim (the risk):** `format_public_data()`/`export_files()` consume
  `indicators_gridded` and `tracks-map` from the old track products. Either
  regenerate them from the coasts H3 output or retain `ingest_pds_map()` /
  `ingest_kepler_tracks()` as Timor-only. Decide with the Phase 0 inventory in hand.
- Verify: trip count and total tracked hours parity, old vs new, over a fixed date
  window. Investigate any delta over ~1%.

---

### Phase 8 — Country modules & portal parity

- Rename/rewire, no logic change where avoidable:
  `estimate-catch.R` + `model-catch.R` → `R/model-fishery.R`;
  `calculate-nutrients.R` → `R/nutrients.R`;
  `export-dataverse.R` stays; `send-email.R` + `inst/report/` → `R/reports.R`.
- ~~Delete `R/airtable.R` and `inst/airtable/edit-submission-link.js`.~~
  **Done in Phase 1**, pulled forward at the user's request — see §2.5 and the
  Phase 1 STATE entry. Nothing Airtable-related is left for this phase except
  dropping the `AIRTABLE_KEY` secret from the workflows (Phase 9), which is
  where the other secret renames live.
- Point `format_public_data()` at the new validated/merged parquet.
- **Hard gate:** the emitted `portal-*.json` files must match the Phase 0 golden
  snapshot structurally (same keys, same nesting, same types) and numerically within
  rounding. The portal is live and pinned to this shape.
- Carry forward the timezone fix from commit `15f6b18` explicitly — verify it is
  still applied after the rewrite.

---

### Phase 9 — CI, repo, docs

- Rewrite `.github/workflows/data-pipeline.yaml`: job naming and layout mirroring
  Moz, `checkout@v5`, `build-push@v6`, `ubuntu-latest`, `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24`,
  COASTS_REF resolution, `Rscript -e 'coasts::...(package = "peskas.timor.data.pipeline")'`
  for delegated steps. Keep the tinytest steps.
- Audit the other 10 workflows against the Phase 0 secret inventory; retire dead ones.
  Apply the GH secret renames from STRUCTURAL-DIFF §1.
- **The four generic workflows are on pre-2022 r-lib templates** (`checkout@v2`,
  `setup-pandoc@v1`, hand-rolled `.github/depends.Rds` caching, `macOS-latest`
  runners). Replace with the current r-lib templates; rename
  `check-standard.yaml` → `R-CMD-check.yaml`.
  **Hard requirement:** `coasts` is not on CRAN, so `R-CMD-check`, `pkgdown` and
  `test-coverage` all need
  `extra-packages: ... github::WorldFishCenter/peskas.coasts, github::WorldFishCenter/ssf-ai-toolkit/Rplug@plug-R`
  or they fail on the first run.
- Add `release.yaml` (GitHub release from NEWS.md). Timor's NEWS heading format is
  already compatible. **Fix the bug while porting**: the reference emits
  `tag_name=v$version` then uses `v${{ ...tag_name }}`, producing `vv2.8.0`.
- `_pkgdown.yml`: keyword-driven reference sections (`workflow`, `ingestion`,
  `preprocessing`, `validation`, `export`, `helper`, `storage`) replacing Timor's
  `matches("cloud")` / `matches("get")` name-pattern sections, which will mis-sort
  after the Phase 2–8 renames. Keep Timor's `url:` — Moz's points at Malawi.
- `NEWS.md`: `# peskas.timor.data.pipeline 4.0.0` with the migration changelog;
  bump DESCRIPTION to match (release.yaml parses NEWS.md).
- Refresh `README.Rmd`/`README.md`.

---

### Phase 10 — Upstream to coasts

Runs in the `peskas.coasts` repo, one PR per item:

- nutrients / RDI computation (Timor's is the only implementation; Kenya and
  Zanzibar would use it)
- Dataverse publishing
- `validate_pds_trips()` consecutive-trip merging and outlier logic
- the richer validators (landing regularity, mesh, gleaners, fuel, conservation)
- retry wrappers for cloud upload/download

Then Timor depends on the new coasts release and deletes its local copies.

---

### Phase 11 — Cutover

- Strip legacy keys from `config.yml`; delete `auth/`, `docs/`, `inst/__pycache__`,
  `cran-comments.md`, dead code.
- Unpin `coasts` from the migration tag → resolve latest release at build time.
- Remove the `peskas.mozambique.data.pipeline/` reference copy.
- Full green run on the phase branch (dev buckets), golden-output diff on every
  portal file, then merge to `main` and watch one production run end to end.

---

## 5. Verification strategy

- **Dev/prod isolation is free.** Every workflow sets `R_CONFIG_ACTIVE=production`
  only on `main`. Any push to a phase branch runs the whole pipeline against
  `timor-dev` / `pds-timor-dev` / `public-timor-dev`. Use pushes as the integration
  test; never merge to `main` mid-migration.
- **Golden-output diffing.** Phase 0 snapshots the current outputs. Every
  behavioural phase diffs against them: row count, column set, per-column
  summary stats. Record deltas in STATE.md even when accepted.
- **Keep tinytest green** at every phase; update expectations deliberately, never
  delete assertions to make a phase pass.
- `devtools::document()` + `devtools::check()` at the end of every session.

## 6. Risks

| Risk | Mitigation |
|---|---|
| Portal JSON contract drift breaks the live site | Golden diffs are a hard gate in Phase 8; portal repo untouched |
| v1 freeze silently drops historical rows | Assert frozen parquet row count == current v1 output before deleting anything. Risk much reduced since 2026-07-31: v2 is no longer frozen, and v2 was the only one still receiving submissions |
| `coasts` ships a breaking change mid-migration | Pin to a release tag in Phase 1; unpin only in Phase 11 |
| `rfishbase 5.0.1` pin conflicts with coasts | Resolve in Phase 1; if unresolvable, keep Timor's taxa code off the shared helper |
| Timezone regression | Commit `15f6b18` fixed this; re-verify explicitly in Phase 8 |
| Timor v3 form shape defeats a Moz-shaped reshaper | Phase 4 is budgeted at 2 sessions and treats Moz as a pattern, not a source to copy |
| Moz reference copy leaks into the build | Fixed in Phase 0 hygiene |

## 7. Working agreement across sessions

- Branch: `feat/align-coasts` off the current `feat-upgrade`. One sub-branch per
  phase, merged into it. `main` untouched until Phase 11.
- **One phase per fresh session.** Never two.
- Every session starts by reading, in order: `CLAUDE.md`, this file,
  `.claude/migration/STRUCTURAL-DIFF.md`, `.claude/migration/STATE.md`,
  `.claude/migration/AUDIT.md`.
- **The reference repo has bugs — do not replicate them.** Catalogued in
  STRUCTURAL-DIFF: unedited DESCRIPTION boilerplate, `_pkgdown.yml` pointing at
  Malawi, `.Rbuildignore` naming the Kenya `.Rproj`, `R_CONFIG_ACTIVE=dev` with no
  `dev` config env, the `vv` tag in `release.yaml`, and leaked `.parquet`/`.rds` run
  artifacts committed at its repo root. Copy patterns, not files.
- Every session ends by appending a STATE.md entry:

```markdown
## Phase N — <name> — YYYY-MM-DD
Branch: feat/align-coasts-phaseN
Done: ...
Verified: ... (commands run, deltas observed)
Deferred: ...
Open questions for the next session: ...
Files added / removed / renamed: ...
```

- The reference copy at `peskas.mozambique.data.pipeline/` stays until Phase 11.
  Read it constantly; copy from it sparingly.
