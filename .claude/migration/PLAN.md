# Aligning `peskas.timor.data.pipeline` to the harmonized Peskas standard

Status: **Phases 0–9 complete** (2026-08-12). Phase 10 next.
Progress and every measured delta: `.claude/migration/STATE.md`.
Reference implementation: `peskas.mozambique.data.pipeline` (local copy at repo root, untracked + ignored)
Normative spec: `peskas.mozambique.data.pipeline/inst/config_template.yml` — the
cross-country "PESKAS UNIFIED CONFIGURATION TEMPLATE". Derive Timor's config from
**this**, not from Moz's `config.yml`.
Shared hub: `coasts` (`WorldFishCenter/peskas.coasts`, local checkout at `~/Desktop/work/wf_projects/peskas.coasts`)
Also cross-check: `peskas.kenya.data.pipeline`, `peskas.zanzibar.data.pipeline`

Companion documents, read in this order: `STRUCTURAL-DIFF.md` (file-by-file
diff), `AUDIT.md` (Phase 0 measurements), `STATE.md` (per-phase handoff log,
**including the addenda at the end**), `COASTS-4.6.0.md` and `COASTS-TODO.md`
(what the shared hub does and still owes us).

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

### Sub-decision — resolved 2026-08-10

6. **Validation flags sink — MongoDB.** Decided by the user; Google Sheets is
   retired at Phase 5. Timor follows the Mozambique/Kenya/Zanzibar layout
   exactly, which is what `inst/config.yml` has already declared since Phase 2:

   - Flags go to the **shared** validation database,
     `validation-dev` / `validation-prod`, not to a Timor-only database. That is
     what puts Timor into the cross-country validation UI the other three use.
   - **One collection per live form**, `surveys_flags-<asset_id>`, as
     `coasts::mdb_collection_push()` is called elsewhere. v1 is frozen and gets
     no collection and no write-back.
   - The dedicated `timor-dev` / `timor-prod` databases the user is provisioning
     serve the pipeline/export collections, exactly as `mozambique-*` does.

   Consequences: a new `MONGODB_CONNECTION_STRING_VALIDATION` secret (`.env` +
   GitHub), the `get_validation_status()` / `update_validation_status()` KoBo
   write-back ported from Mozambique — they are **not** in coasts — and a
   rewrite of `send_validation_mail()` to read from Mongo. AUDIT §5 shows that
   mail has not run successfully since ≥2025-09, so there is no working Sheets
   reader to preserve.

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
| 0 | Groundwork & audit | docs, `CLAUDE.md`, bucket/asset inventory, immediate hygiene fixes | none | 1 ✅ |
| 1 | Foundations | `config.yml` superset, `read_config()`, DESCRIPTION, Docker, `.env` | low | 1 ✅ |
| 2 | Storage delegation | delete `cloud-storage.R`/`get-cloud-files.R`, call `coasts::*` | medium | 1 ✅ |
| 3 | Ingestion | `ingestion.R`, v2+v3 live, freeze v1, metadata | medium | 1 ✅ |
| 4 | Preprocessing | `preprocessing-surveys.R` + `survey-reshaping.R` + `model-taxa.R` | **high** | 2 ✅ (1 used) |
| 5 | Validation | `validation.R` + `validation-functions.R`, flags sink | high | 1 ✅ |
| 6 | API + merge | `api.R`, standard-schema export, `merge_trips()` | medium | 1 ✅ |
| 7 | PDS switch | delegate to `coasts`, parity check, shim for portal products | **high** | 1–2 ✅ (1 used) |
| 8 | Country modules | rename/rewire modelling, nutrients, Dataverse, reports; portal JSON parity | **high** | 1–2 ✅ (1 used) |
| 9 | CI / repo / docs | workflows, pkgdown, README, NEWS, release automation | low | 1 ✅ |
| 10 | Upstream to coasts | separate PRs in the `peskas.coasts` repo | medium | 1–2 |
| 11 | Cutover | strip legacy config keys and dead code, full green dev run, merge to main | medium | 1 |
| 12 | Static assets & label sources | move `registered_boats` to the frame, reconcile the site/municipality labels, retire the remaining Sheets tables | **high** | 1–2 |

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

### Phase 3 — Ingestion ✅ done 2026-08-09/10

Shipped as scoped, with three deviations recorded in the STATE Phase 3 entry
and its follow-up. What Phase 4 inherits:

- **`R/ingestion.R`** — `ingest_landings(versions = c("v2","v3"))` writing raw
  **parquet** (`coasts::get_kobo_data()` → `flatten_row()` →
  `upload_parquet_to_cloud()`), `ingest_assets()`, the flatten helpers, and the
  internal `get_raw_landings()` reader. `ingest-landings.R` and
  `retrieve-survey-data.R` are gone.
- **Raw is parquet, and provably the same table.** `get_raw_landings()` coerces
  to all-character and reproduces readr's trim / `""`→`NA` rules, because
  preprocessing was written against `read_csv(col_types = col_character())`.
  Verified over every submission of both live forms: 13 differing cells out of
  ~26M, all of them the parquet being closer to the source JSON. **Phase 4
  deletes this shim** when it moves preprocessing to a typed long table.
- **v1 is frozen.** Not ingested, not preprocessed. `merge_landings()` reads
  `timor-landings-v1-frozen__*.rds`, produced by
  `data-raw/freeze-landings-v1.R`. The freeze also converted v1's fork lengths
  to total length, so `join_weights()` no longer branches on `survey_version`
  and the length-length path is deleted. **Phase 4 does not have to port v1's
  form shape** — one fewer legacy reshaper.
- **Assets snapshot exists**, written to the coasts hub by `ingest_assets()`
  and read by `get_assets()`. Verified: 60 taxa / 56 codes, 9 gears, 2 vessels,
  40 sites, `survey_label` populated everywhere, codes matching
  `models$all_taxa` exactly. `get_taxa_list()` already reads it.
- **The snapshot is cross-country and has no `country` column.** Narrow every
  read with `timor_assets(x, conf)`, which filters on
  `metadata.airtable.form_ids`. See COASTS-TODO C13.
- **`conf`, not `pars`**, throughout the package.
- CI: `KOBO_ASSET_ID_V{1,2,3}` mapped; one `ingest-landings` job now feeds both
  preprocessing branches; `ingest_assets()` runs with the metadata tables.
- A live secrets leak was found and fixed in `read_config()` — see the STATE
  entry. **Rotation is still a user action.**

Numerically inert: 97,328 submissions compared against the previous dev run,
**0.000000%** weight delta, 0 submissions differing by more than 1e-6 kg.

---

### Phase 4 — Preprocessing ✅ done 2026-08-10

Shipped as scoped, in one session rather than two, with the deviations recorded
in the STATE Phase 4 entry. What Phase 5 inherits:

- **`R/preprocessing-surveys.R`** — `preprocess_landings(versions = c("v2","v3"))`
  writing **flat long catch parquet** (one row per submission × catch × length
  bin), `merge_landings()` reduced to a bind, the per-version `harmonise_*()`
  reconciliation, and `survey_labels()` / `resolve_catch_taxa()` /
  `resolve_survey_labels()` — the label joins, off the assets snapshot.
- **`R/survey-reshaping.R`** — `reshape_species_groups()`,
  `expand_length_frequency()`, `bin_midpoint()`, `trim_free_text()`.
- `R/calculate-weights.R` → **`R/model-taxa.R`**. `clean-raw-data.R`,
  `preprocess-landings.R`, `pt_nest_species.R`, `pt_nest_attachments.R`,
  `merge-landings.R` and `get_raw_landings()` are gone.
- **`join_weights()` is the only remaining bridge to the old format.** It renames
  the catch columns back, re-nests `species_group` / `length_individuals`, and
  drops the standard submission columns again, so the weight artefact is exactly
  what it was minus `_attachments`. **Phase 5's first job is to delete that
  bridge**: point the validators at the merged long parquet, drop the raw KoBo
  column names, and the nesting goes with them.
- The label joins are **duplicated for one phase, deliberately**: preprocessing
  resolves gear / vessel / site / taxa from the frame, while
  `validate_gear_type()`, `validate_vessel_type()` and `validate_sites()` keep
  their Google Sheets joins because they also emit alert codes 12–16. Phase 5
  moves them onto the preprocessed columns, and that is what lets
  `metadata.google_sheets.tables` finally shrink — every entry is annotated in
  `inst/config.yml` with the phase that removes it.
- v1's column reconciliation and flattening live in
  `data-raw/freeze-landings-v1.R`, which now writes **parquet**. The production
  freeze is still un-run.
- Nutrients: **keep Timor's.** `coasts::enrich_taxa()` emits six nutrients
  (Calcium, Iron, Omega3, Protein, VitaminA, Zinc) against Timor's seven, has no
  Selenium, does no unit conversion, and has no FAO food-composition override for
  the invertebrates FishBase cannot estimate (`OCZ`, `IAX`, `COZ`, `PEZ`, `CRA`,
  `SLV`) or the hardcoded `FLY` row. They do not agree, so the PLAN §4b
  "delete Timor's if they agree" test fails and `R/calculate-nutrients.R` stays.
  It is renamed to `R/nutrients.R` in Phase 8 as planned.

Numerically inert where it matters: 97,328 submissions, **5,196,740 kg**,
**0.000000%** weight delta, 0 submissions differing by more than 1e-6 kg. Catch
rows fell 1,751,969 → 1,647,260 by removing 104,709 phantom no-catch rows — see
the STATE entry.

### Phase 5 — Validation ✅ done 2026-08-10

Shipped as scoped, with three measured deviations recorded in the STATE Phase 5
entry: only two of the five `[phase 5]` metadata tables could leave the Sheets
(`devices` is worth 651 unmatched trackers, `stations`/`reporting_units` are the
*published* site and municipality labels and belong to Phase 8's portal gate);
the KoBo status read is a single bulk query rather than Mozambique's per-submission
loop; and `KOBO_TOKEN` turned out to be the wrong credential, so the client uses
basic auth and needs no new secret. Byte-identical flags across 97,347
submissions bar one deliberate data correction, validated catch weight unchanged,
all four tinytest suites green for the first time in the migration.

- `R/validate-landings.R` → `R/validation.R`; keep and rename `R/validation-functions.R`.
- Preserve every Timor validator. Tag them `@keywords validation`.
- **Move the validators onto the merged long parquet.** Phase 4 left
  `join_weights()` re-nesting `species_group` / `length_individuals` purely so
  the validators could keep reading raw KoBo column names.
  `standard_survey_cols()` is the exact list they should move onto and the exact
  list `join_weights()` drops; delete both halves together.
- **Flags sink: MongoDB** (decided, §2.6). Add
  `MONGODB_CONNECTION_STRING_VALIDATION`, push via
  `coasts::mdb_collection_push()` to `surveys_flags-<asset_id>` in the shared
  `validation-{dev,prod}` database — one collection per live form, none for the
  frozen v1 — port `get_validation_status()` / `update_validation_status()` from
  Mozambique (they are not in coasts), and rewrite `send_validation_mail()` to
  read from Mongo.
- Move the gear / vessel / site / habitat alerts (codes 12–16, 19) onto the
  labels `preprocess_landings()` already resolves, which is what lets the seven
  `# [phase 5]`-annotated tables leave `metadata.google_sheets.tables`.
- Keep `inst/tinytest/test_validated_landings.R` passing — update expectations for
  the new schema rather than deleting assertions.
- Verify against the **Phase 4** baseline, not the Phase 0 golden: the golden
  predates commit `a2c2881`'s −15.4% weight rewrite, which moves alerts 17 and 11.
  Flagged count within a few % of 13,388 / 97,328, all 37 alert combinations, all
  12 observed codes still produced.

---

### Phase 6 — API contract + merge_trips ✅ done 2026-08-11

Shipped, with three deviations recorded in the STATE Phase 6 entry. What
Phase 7 inherits:

- **`R/api.R`** — `export_api_raw()` / `export_api_validated()`, publishing the
  22-column schema to `peskas-api-dev/timor/{raw,validated}`. The schema was
  read off the live prod objects and **asserted** against Mozambique's, not
  diffed against its source. Raw reads the weight parquet, validated the long
  validated parquet; both emit 144,291 rows over 97,347 trips, summing to
  5,197,093.9 kg and 964,937.9 kg — their sources exactly.
- **`long_validated_landings()` is now a superset of the nested artefact**, and
  the API export is a projection of it. `nest_landing_catch()` selects its
  columns before nesting so the portal's shape cannot drift with it.
- **`merge_trips()` was not touched, deliberately.** `coasts::merge_survey_trips()`
  does a different job (COASTS-TODO C10) and the 22-column contract has no slot
  for a PDS trip id, so `trip_id` is `TRIP_<submission_id>` as in all three
  other countries. `all_trips` is unchanged at 175,089 × 26, 84,741 matched.
- ~~Add `timor` to the `api.trips` block of `peskas.coasts/inst/conf.yml`~~ —
  landed in coasts 4.6.0 (C7). No coasts PR was needed.
- **`coasts::summarize_data()` does not run end to end for Timor**, and the API
  parquet is not why: it also reads `asfis` and the PDS grid summaries from the
  *country* bucket, where Timor has neither (COASTS-TODO **C17**). What was
  verified instead: the API read, the trip and taxon collapses, and
  `coasts::calculate_fishery_metrics()` run for real on Timor's parquet.
- Not wired into `data-pipeline.yaml`, as scoped. Nothing written to
  `peskas-api-prod`.

---

### Phase 7 — PDS switch ✅ done 2026-08-11

Shipped as scoped. Timor now carries **no PDS ingestion or preprocessing code**,
exactly like Mozambique, Kenya and Zanzibar — all three call
`coasts::ingest_pds_trips/ingest_pds_tracks/preprocess_pds_tracks(package = ...)`
from their workflow YAML and none has a line of PDS R code. What Phase 8
inherits:

- `R/ingest-pds-data.R`, `R/retrieve-pds-data.R` and `preprocess_pds_trips()`
  are gone. `R/pds-tracks.R` keeps `describe_pds_tracks()` — the per-trip track
  descriptors `validate_pds_trips()` joins on, which coasts has no equivalent
  for (`coasts::preprocess_pds_tracks()` emits spatial grid summaries instead).
  `R/pds-maps.R` keeps the map products.
- **The track object family was renamed and converted in place**, not re-fetched:
  103,373 `pds-track-<id>__*__.csv.gz` → `pds-tracks_<id>.parquet` by
  `data-raw/convert-pds-tracks.R`. Straight delegation would have re-fetched
  98,472 tracks from the PDS API (COASTS-TODO C18). Run it against
  `production` before Phase 11, like the v1 freeze.
- **The frame is now authoritative for PDS devices**, via `conf$pds$customers`.
  It costs 2,791 trips and 59 of the 6,999 matches until 27 missing IMEIs are
  added to PESKAS | FRAME — listed in the STATE entry. The **survey**-side
  `devices` table stays on Google Sheets for the same reason.
- **The shim was not needed.** `ingest_pds_map()` / `ingest_kepler_tracks()` are
  retained Timor-only and untouched; `indicators_gridded` and `tracks-map` still
  resolve and still feed `export_files()`. Neither is in a workflow, so neither
  regenerates — the decision is Phase 8's portal gate, unchanged by this phase.

---

### Phase 8 — Country modules & portal parity ✅ done 2026-08-12

Shipped as scoped, in one session, with four decisions recorded in the STATE
Phase 8 entry. What Phase 9 inherits:

- **The renames landed**, in their own commit and with no logic change:
  `estimate-catch.R` + `model-catch.R` → `R/model-fishery.R`,
  `calculate-nutrients.R` → `R/nutrients.R`, `send-email.R` → `R/reports.R`,
  `export-dataverse.R` unchanged. `format-public-data.R` and `export.R` were
  **not** merged, against STRUCTURAL-DIFF §4: it would make a 1,636-line file
  and this plan's own Phase 8 bullet never asked for it.
- **The nested validated artefact is gone.**
  `timor-landings-merged_validated__*.rds` has no writer;
  `get_validated_landings()` re-nests the long parquet on read. Proven
  interchangeable first — `all.equal` TRUE over 97,360 submissions and
  1,648,016 catch rows, and `merge_trips()` re-run on the view produced an
  `all_trips` `all.equal` to the stored one. So the survey path is parquet end
  to end and `format-public-data.R` did not have to be rewritten.
- **`all_trips` stays nested `.rds`**, deliberately: it has no cross-country
  counterpart, so flattening it would buy no harmonization and would mean
  rewriting three files against a live portal. Same reasoning kept
  `pds-trips_validated__*.rds` as `.rds`, where a parquet round trip would also
  put `Asia/Dili` POSIXct through arrow on the one path `15f6b18` protects.
- **The export emits seven objects, not nine.** `portal-indicators_grid` and
  `portal-label_groups_list` — the two the portal excludes, rebuilt every run
  from a 2024-07-27 `indicators_gridded.rds` — were dropped, which is the
  "drop the dependency" branch of the choice below.
- **`coasts::preprocess_pds_tracks()` was not wired in**, and the reason is no
  longer C20: its output has no reader in Timor and `summarize_data()` is
  blocked on C17 anyway, so it would produce ~1.4 M grid rows per run for
  nothing.
- **The gate is `data-raw/compare-portal-json.R`**, reusable in Phase 11.
- The `15f6b18` timezone fix was re-verified against the parquet inputs rather
  than assumed: `landing_period` still carries `tzone = "Asia/Dili"`, exactly
  one month matches `floor_date(today, "month")`, and the current month is
  published scaled by `elapsed / days_in_month`.
- ~~Delete `R/airtable.R` and `inst/airtable/edit-submission-link.js`.~~
  **Done in Phase 1.** The remaining Airtable item is dropping the
  `AIRTABLE_KEY` secret from the workflows, which is Phase 9.

The gate as it was restated on 2026-08-12, for the record: **structural**
against the Phase 0 golden (`reference/2026-07-31_90ede9a/`) — object names,
keys, nesting, types; **numeric** against the newest `public-timor-dev` set,
never the golden, which predates `a2c2881`'s −15.4% weight rewrite, Phase 4's
104,709 removed phantom rows and Phase 7's trip-population change.

---

### Phase 9 — CI, repo, docs ✅ done 2026-08-12

Shipped as scoped, with three deviations recorded in the STATE Phase 9 entry.
**Eleven workflows became nine.** What Phases 10 and 11 inherit:

- `data-pipeline.yaml` was **edited, not rewritten**: versions, runners,
  `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24`, three `env:` changes and two new steps.
  The job graph, the tinytest steps and `log_threshold = logger::INFO` are
  untouched. Moz's "job naming and layout" was deliberately not copied — the
  difference is a YAML round trip, and this is the only workflow producing data.
- **The API exports are wired in, guarded.** `export_api_raw()` in
  `merge-landings`, `export_api_validated()` in `validate-landings` after its
  tinytest, both `if: ${{ !endsWith(github.ref, '/main') }}`. That guard is what
  keeps "wire it in" and "write to `peskas-api-prod`" two decisions rather than
  one; Phase 11 either deletes the two lines deliberately or says why not.
- **Deleted:** `form-summary`, `keplergl-map`, `upload-matched-trips` — retired
  registry, no successful run since 2025-08, and in `keplergl-map`'s case no live
  function. **Rebuilt:** `validation-email-sender`, because Phase 5 gave it a
  working Mongo reader. The three surviving scheduled workflows lost the
  duplicate `build-container` job that had been unable to succeed since Phase 2
  gave `Dockerfile.prod` an `ARG COASTS_REF` with no default.
- **Re-enabling the three `disabled_inactivity` workflows is a Phase 11 action.**
  A cron fires from the default branch, so enabling them before the merge runs
  pre-migration code against production.
- `check-standard.yaml` → `R-CMD-check.yaml` plus `pkgdown` / `test-coverage` /
  `pr-commands` on the r-lib v2 templates, each carrying the two GitHub
  `extra-packages`. **None of the three can run from a phase branch** — they
  trigger on `main` and on PRs to it — so their first real run is the Phase 11
  PR, and a draft PR is the way to find out early.
- `release.yaml` added with the reference's `vv` tag bug fixed. **It fires on the
  Phase 11 merge** and will cut `v4.0.0` from the top of NEWS.md.
- `_pkgdown.yml` is keyword-driven, and 35 roxygen blocks were tagged to make it
  so. A new exported function with no `@keywords` now fails
  `pkgdown::check_pkgdown()` and the `pkgdown` workflow.
- `VALID_SHEET_ID` is no longer passed by any workflow, so Phase 11 can drop
  `validation.google_sheets` and the secret without checking CI first. The
  `KOBO_PESKAS*` → `KOBO_ASSET_ID_V*` secret rename was **declined by the user**
  and retires in Phase 11 with the legacy config keys.

**State on entry, re-measured 2026-08-12** (after Phase 8): 11 workflows, of
which `data-pipeline.yaml` and four generic ones are `active` and six are
`disabled_inactivity` / `disabled_manually`. Only `data-pipeline.yaml` produces
data, and it is green — three consecutive end-to-end runs on Phase 8 code.

- Rewrite `.github/workflows/data-pipeline.yaml`: job naming and layout mirroring
  Moz, `checkout@v5`, `build-push@v6`, `ubuntu-latest`, `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24`.
  COASTS_REF resolution is **already there** since Phase 2, as are the
  `Rscript -e 'coasts::...(package = "peskas.timor.data.pipeline")'` PDS steps
  since Phase 7. Keep the tinytest steps.
  - **Non-negotiable while rewriting:** every `coasts::` workflow call keeps
    `log_threshold = logger::INFO`. The upstream fix for COASTS-TODO C21 landed
    2026-08-12 but Timor's container resolves the latest coasts *release* at
    build time, so the argument stays until that fix is tagged. Dropping it
    reprints the service-account key into the job log.
  - The `export-trips` job now emits **seven** `portal-*` objects. Anything that
    changes it must pass `data-raw/compare-portal-json.R` — see Phase 8.
- **Carried over, decide here:** whether `export_api_raw()` / `export_api_validated()`
  join the workflow. They have been run only by hand since Phase 6 and publish to
  `peskas-api-dev` only. Wiring them in and the first `peskas-api-prod` write are
  two separate decisions; only the first is Phase 9's.
- Audit the other 10 workflows against the Phase 0 secret inventory; retire dead ones.
  Four of them (`form-summary`, `keplergl-map`, `validation-email-sender`,
  `upload-matched-trips`) still build through the retired
  `docker.pkg.github.com` registry with `whoan/docker-build-with-cache-action@v5`
  and cannot work at all. `keplergl-map` has no live function behind it either:
  Phase 8 left `ingest_kepler_tracks()` unreferenced.
- Apply the GH secret renames from STRUCTURAL-DIFF §1. **Two are user actions,
  not agent actions:** secret *values* are write-only, so `KOBO_PESKAS1/2/3` →
  `KOBO_ASSET_ID_V1/2/3` needs the user to create the new secrets. Deleting the
  stale `AIRTABLE_KEY` does not need a value and can be done with `gh`, but it is
  outward-facing — confirm first. `AIRTABLE_TOKEN` already exists.
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
  `matches("cloud")` / `matches("get")` name-pattern sections, which mis-sort
  after the Phase 2–8 renames. Keep Timor's `url:` — Moz's points at Malawi.
  `pkgdown::check_pkgdown()` passes today; it must still pass afterwards, and it
  fails on any exported topic that no section claims.
- `NEWS.md`: add `# peskas.timor.data.pipeline 4.0.0` with the migration
  changelog. **DESCRIPTION is already at 4.0.0** (Phase 1); NEWS still tops out
  at 3.3.0, so it is NEWS that has to catch up, not the other way round.
  `release.yaml` parses NEWS.md, so write it before adding that workflow.
- Refresh `README.Rmd`/`README.md`.
- **Do not touch** the export path, `R/` layout or config keys. Phase 9 is CI and
  docs; the legacy-key strip and dead-code removal are Phase 11, and the list of
  what is now unreferenced is in the Phase 8 STATE entry.

---

### Phase 10 — Upstream to coasts

**Re-scoped 2026-08-12, after Phase 9.** This phase runs in the
`peskas.coasts` repo (`~/Desktop/work/wf_projects/peskas.coasts`), not this one.
It is the only phase whose deliverable is somebody else's repo, and the only one
where "do nothing" is a defensible outcome per item — an item that no second
country would use is not worth a hub PR.

**One item was already delivered without a PR.** Retry wrappers landed in coasts
4.6.0 as `insistent_upload_cloud_file()` / `insistent_download_cloud_file()`
(COASTS-TODO C5), and Timor's local copies are gone. `resolve_storage_opts()`
learned `"public"` and `"api"` (C6, C16), and Timor is registered in the hub's
`api.trips` block and PDS customer list (C7). **Do not re-do these.**

The candidates, each with the question that decides it:

| Candidate | Where it lives now | The question |
|---|---|---|
| nutrients / RDI | `R/nutrients.R` | `coasts::enrich_taxa()` emits six nutrients against Timor's seven, no selenium, no unit conversion, no FAO override for the six invertebrates FishBase cannot estimate. Upstreaming means changing a function three countries already call — is it an extension or a fork? |
| Dataverse publishing | `R/export-dataverse.R` | Nobody else publishes to Dataverse. Does a second country want it, or is this Timor-only by nature? |
| `validate_pds_trips()` merging + outlier logic | `R/validate-pds-trips.R` | No coasts equivalent exists and `coasts::merge_survey_trips()` does a different job (C10). This is the strongest candidate |
| the richer validators | `R/validation-functions.R` | Landing regularity, mesh, gleaners, fuel, conservation, happiness. Deeper than any other country's. Which are Timor-specific by *form*, not by intent? |
| `get_validation_status()` / `update_validation_status()` | `R/validation-functions.R` | **C15, and the clearest win**: every country writing flags to the shared validation database needs them, Moz's copy has two live bugs Timor fixed (404-throws, per-submission loop at 20 min vs 70 s), and Timor's is the corrected implementation |

Read COASTS-TODO.md in full first: C11, C13, C17, C18, C19, C20 and C12 are all
open, and two of them (C17, C20) are what keep `preprocess_pds_tracks()` unwired
in Timor. Some are better fixed while in the hub repo than filed again.

Then Timor depends on the new coasts release and deletes its local copies —
which is a Phase 11 edit here, not a Phase 10 one.

---

### Phase 11 — Cutover, split into 11a and 11b

**Split 2026-08-18 at the production line.** 11a touches no production bucket and
is done; 11b is the cutover and is a separate session
([`PROMPT-PHASE11B.md`](PROMPT-PHASE11B.md)). Sending them together was what the
split was written to prevent.

**11a — done 2026-08-18, `11f4081`:**

- Legacy keys stripped from `config.yml` — every `# [legacy]` marker is gone.
- Dead code deleted: 25 functions, −3,597 net lines, including
  `model_indicators()`'s 674-line subgraph and the PDS map products.
- The KoBoToolbox validation-status client and the `ingest_assets()` hub mirror
  now come from `coasts::` (C15, C11) — the deletions are what made the
  delegation real, since a package's own definitions win over its imports.
- Five Sheets metadata tables dropped, seven kept.
- `cran-comments.md`, `docs/` and the `peskas.mozambique.data.pipeline/`
  reference copy removed. `auth/` and `inst/__pycache__` no longer existed.
- `coasts` was already unpinned (the workflow resolves the latest release).
- Full green run on the phase branch against the `-dev` buckets, and the portal
  gate re-run: **zero change to the seven published objects.**

**11b — the cutover, not yet started:** the v1 freeze and the PDS track
conversion against `production`, the merge to `main`, one watched production run,
`public-timor`'s 45 leaked objects and re-enabling the three
`disabled_inactivity` workflows.

**Scope is constrained by the 2026-08-18 alignment audit.**
[`ALIGNMENT-AUDIT.md`](ALIGNMENT-AUDIT.md) §13 is the operative list of what may
and may not be deleted here. In particular Phase 11 must **not** remove the
59-column raw KoBo passthrough (`enumerators_summary.Rmd` reads nine of those
columns and runs on every pipeline run), must **not** swap `timor_assets()` onto
the assets snapshot's `country` column (it is absent on `sites` and is a record-id
link on `geo`), and must **not** flatten `all_trips__*.rds`. Seven of the twelve
Google Sheets metadata tables still have live readers and stay. All three
constraints were honoured in 11a.

---

### Phase 12 — Static assets & label sources

Everything the alignment audit found that is not a pure deletion. **After the
cutover, deliberately**: each item below moves a published number, and doing that
inside Phase 11 puts a portal regression and a config migration in one diff with
no way to bisect them.

Three items are gated on Airtable data entry by the user, not on code — see
`ALIGNMENT-AUDIT.md` §14.

1. `registered_boats` → the frame's `geo.total_boats`. Ten of twelve values are
   already identical; the two that are not move national published catch by
   **−4.74%**. Gated on Airtable task B.
2. Reconcile the two conflicting "North Coast" definitions
   (`format-public-data.R:785` vs `export.R:377`, disagreeing on Manatuto —
   14.97% of national revenue) into one config list read by both call sites.
3. `timor_assets()` off hardcoded Airtable record ids, onto Mozambique's
   `get_airtable_form_id()`. Behaviour-neutral.
4. `devices` → the frame's `pds_devices`. Gated on Airtable task A; today the
   switch costs 651 resolved trips and takes alert 3 from 824 to 1,475.
5. `stations` / `reporting_units` — the accent decision, the Atauro-vs-GAUL rule,
   the Welaluhu conflict, and the fifteen hardcoded spellings across three files.
   Largest and last; gated on `data-raw/compare-portal-json.R`.
6. Document `coasts::generate_fleet_analysis()` (the standard's counterpart to
   `estimate_fishery_indicators()`, which no phase has examined) and the
   `harmonise_*()` vs `map_surveys()` distinction. Writing only.
7. File COASTS-TODO C22 (upstream `merge_trips()` — three countries have it),
   C23 (a hub home for curated length-weight coefficients) and C24
   (`geo.country` is a record-id link, same defect C13 fixed on `taxa`).

Out of scope: `coasts::summarize_data()` and the grid summaries (still no Timor
consumer), `morphometric_table` (needs C23 upstream first), and anything touching
the seven portal objects' names or keys.

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
| v1 freeze silently drops historical rows | ~~Assert frozen row count~~ **closed.** Asserted in the freeze script (10,117, the 12,017 KoBo submissions minus the 1,900 Kg-variant rows `clean_legacy_landings()` always dropped), and the frozen lengths were proven bit-identical to the old per-run conversion. Residual risk is operational, not numeric: the snapshot exists in `timor-dev` only — **run `data-raw/freeze-landings-v1.R` against `production` before Phase 11** |
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
  `.claude/migration/STRUCTURAL-DIFF.md`, `.claude/migration/STATE.md`
  (**including the addenda and follow-ups at the end**),
  `.claude/migration/AUDIT.md`, `.claude/migration/COASTS-4.6.0.md`,
  `.claude/migration/COASTS-TODO.md`.
- **Naming:** the resolved configuration is `conf`, never `pars` (renamed
  throughout in Phase 3). Storage functions are always written `coasts::`.
- **Never log the resolved config.** It carries the service-account key and
  every token, and workflow functions default to `log_threshold = DEBUG`.
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

- ~~The reference copy at `peskas.mozambique.data.pipeline/` stays until
  Phase 11.~~ Removed in Phase 11a. Re-clone it if a session needs to read the
  standard again.
  Read it constantly; copy from it sparingly.
