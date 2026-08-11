# Migration state log

Handoff file for the `peskas.timor.data.pipeline` → harmonized-standard migration.
Read `.claude/migration/PLAN.md` then `.claude/migration/STRUCTURAL-DIFF.md` first.
Append one entry per completed phase, newest at the bottom.

## Current position

- **Phase:** 6 **complete** (2026-08-11). Phase 7 (PDS) not started.
- **Branches:** Phase 0 = `494a8d0`, Phase 1 = `ea7f253`, Phase 2 = `c6af91a`
  (+ `a2c2881` weight rewrite, `7902012` docs), Phase 3 = `a89f96e` (+ `0e8ab28`
  docs), Phase 4 = `ad58a87` (+ `7549763`, `36edc13`, `2814dff` docs).
  Phase 5 = `75985a8` (+ `992bc6d` docs).
  Phase 6 = **`ec0b7e5`** (+ `a16d60e` docs) on `feat/align-coasts-phase6`,
  **pushed**. Two green end-to-end CI runs: **31436031588** on the Phase 5 code
  — the first to exercise Phases 3, 4 and 5 at all, including the MongoDB flags
  sink and all four tinytest suites — and **31439673841** on the Phase 6 code,
  which produced the widened 40-column long artefact from CI.
- **Environment:** `gs://timor-dev` seeded from prod run `90ede9a`. Timor now
  **publishes** to `peskas-api-dev/timor/{raw,validated}`; nothing has been
  written to `peskas-api-prod`, though the service account can. `coasts` is
  unpinned; locally installed release is **4.6.0**.
- **Read before Phase 7:** the Phase 6 entry's "Findings that change later
  phases", and COASTS-TODO **C17** — `summarize_data()` reads `asfis` and the
  PDS grid summaries from the country bucket, where Timor has neither, which is
  what stops it running end to end. Also note that `devtools::load_all()` reaches
  neither `coasts::read_config(package = )` nor `furrr`/`future` workers — test
  either with `devtools::install()` + `library()`; and that the four tinytest
  suites need `dotenv::load_dot_env('<repo>/.env')` in the same `Rscript` call,
  because tinytest runs them from inside the installed library.
- ~~**Action for the user, to close Phase 5:** the
  `MONGODB_CONNECTION_STRING_VALIDATION` GitHub secret~~ — **done 2026-08-10**,
  and exercised in CI by run 31436031588. `KOBO_TOKEN` is **not** needed — the
  KoBo client uses basic auth, whose secrets already exist.
- ~~**Open since Phase 0 (AUDIT §6): can the ingestion service account write to
  `peskas-api-prod`?**~~ — **yes**, verified 2026-08-11 through the bucket
  `testIamPermissions` endpoint, without writing anything:
  `storage.objects.{create,delete,get,list}` on both API buckets.
- **Action for the user, security:** `read_config()` was printing the full GCP
  service-account private key, the Airtable PAT, the Dataverse token and the
  blastula Gmail credentials into every CI job log. Fixed in Phase 3, but the
  values are in the logs of every past `data-pipeline.yaml` run. Rotating them
  and purging old run logs is recommended and has **not** been done.
- **Action for the user, before Phase 11 cutover:** run
  `R_CONFIG_ACTIVE=production Rscript data-raw/freeze-landings-v1.R`. The frozen
  v1 snapshot `merge_landings()` depends on exists in `timor-dev` only, and it
  now also carries the Phase 5 correction to submission `16182387`'s
  landing date.
- ~~Phase 3 prerequisite: `KOBO_ASSET_ID_V1/2/3` in the workflow `env:`
  block~~ — done in Phase 3.
- ~~**Blocking sub-decision:** validation flags sink~~ — resolved 2026-08-10,
  **MongoDB**, and **shipped in Phase 5**: the shared `validation-dev` /
  `validation-prod` database, one `surveys_flags-<asset_id>` and one
  `enumerators_stats-<asset_id>` per live form, none for the frozen v1. Verified
  live against `validation-dev`. See PLAN §2.6 and the Phase 5 entry.

## Open hygiene items

- [x] `peskas.mozambique.data.pipeline/` in `.gitignore` and `.Rbuildignore`
      (anchored regex)
- [x] `.dockerignore` rewritten — excludes the reference tree, `auth`, `.env`,
      `.Renviron`, `docs`, `reference`, `*.parquet`, `*.rds`, `*.tar.gz`
- [x] `*.parquet` + `*.geojson` + `/.quarto/` in `.gitignore`; `.env*` narrowed to
      `.env` / `.env.local` / `.env.*.local` so `.env.example` is committable;
      `reference/` ignored
- [x] `.Rbuildignore` additions (`^\.env$`, `^\.env\.example$`, `^\.claude$`,
      `^CLAUDE\.md$`, `^\.Renviron$`, `^\.venv$`, `^reference$`, `^.*\.parquet$`,
      `^.*\.rds$`)
- [x] `inst/__pycache__/` deleted; `.Rhistory` absent
- [ ] **`ANTHROPIC_API_KEY` rotation — user action, still open.** Same key value
      in `./.Renviron` and `peskas.mozambique.data.pipeline/.Renviron`; not in
      `~/.Renviron`; verified never committed in either repo. Rotate in the
      Anthropic console, put the new value in `~/.Renviron` only, then delete the
      line from both project files. Details in AUDIT.md §10.

## Entry template

```markdown
## Phase N — <name> — YYYY-MM-DD
Branch: feat/align-coasts-phaseN
Done: ...
Verified: ... (commands run, deltas observed vs golden snapshot)
Deferred: ...
Open questions for the next session: ...
Files added / removed / renamed: ...
```

---

<!-- entries below -->

## Phase 0 — Groundwork & audit — 2026-07-31

Branch: `feat/align-coasts` (off `feat-upgrade`; committed locally, **not pushed**)

**Done**

- `CLAUDE.md` written at repo root and tracked. Documents the repo *as it is*,
  with a banner pointing every future session at the migration docs. Includes the
  module map, the real config/env-var mapping, storage layout, the portal
  contract, the pipeline DAG, and the CI-health table.
- `.claude/migration/AUDIT.md` written — the substantive deliverable. Ten
  sections, all measured against production with live credentials.
- Hygiene: all items closed except the `ANTHROPIC_API_KEY` rotation, which needs
  console access (see above).
- Golden snapshot captured to the gitignored `reference/2026-07-31_90ede9a/`
  (105 MB, 24 artefacts, all from the same production run — commit `90ede9a`).
  Structural + numeric baselines recorded in AUDIT §9.

**Verified** (commands run, all read-only against production)

- KoBo: `GET /api/v2/assets/{id}` and `/data/` with sort+query for all three
  assets → counts, first/last submission, per-year distribution.
- GCS: `gcloud storage ls --recursive` over nine buckets (SA
  `data-ingestion@peskas`, isolated `CLOUDSDK_CONFIG`; it lacks
  `storage.buckets.list`, so bucket names were probed explicitly).
  248,120 objects enumerated and grouped by prefix.
- GitHub: `gh api .../actions/workflows`, `.../actions/secrets`,
  `gh run list` per workflow, `gh run view --log-failed` on the last
  `feat-upgrade` push run.
- R: `readRDS()` over the snapshot for row/column/type baselines and per-column
  summary stats.

**Deltas / findings that change the plan** — full detail in AUDIT §7

1. Raw survey storage is **already** flat CSV via `flatten_row()`; the JSON-blob
   era ended 2024-09-17. Phase 3 is smaller than budgeted.
2. **v2 is not inactive** — a 20-submission backfill landed 2026-07-27 carrying
   2023 landing dates. The freeze needs a submission-id watermark, not a date.
3. Phase 7's `indicators_gridded` / `tracks-map` shim is very likely
   unnecessary: those products are 2 and 5 years stale, and their only portal
   output is excluded by `fetchData.js`.
4. **Dev is not a free integration test.** `gs://timor-dev` holds 11 objects and
   nothing downstream of `merge_landings` has ever been produced there. Budget
   dev seeding in Phase 1 or 2.
5. ~~`gs://peskas-api` (prod) does not exist~~ — **withdrawn same day, this
   finding was wrong.** The bucket is `peskas-api-prod`; the probe used
   `gs://peskas-api` and read the 404 as absence. The prod API is live for
   Kenya, Mozambique and Zanzibar. See the corrected AUDIT §6.
6. The weekly validation email has been dead since ≥2025-09.
7. `AIRTABLE_KEY` is passed to two workflows and read by nothing; `R/airtable.R`
   and `inst/airtable/edit-submission-link.js` are both dead. Verdict is
   **delete**, not "reconcile in P8".

**Live bugs catalogued** (AUDIT §8): duplicate `get_preprocessed_metadata()` where
the broken definition wins by collation order; two more dead helpers in
`validate-landings.R` reading removed config keys; `ingest_rfish_table()`
`continue-on-error` masking a hard dependency; `local: inherits: development`
pointing at a non-existent env; `export_files()` uploading unnormalised object
names (45 leaked objects in `public-timor`).

**Deferred**

- `ANTHROPIC_API_KEY` rotation (user action).
- Root-causing the 1,900-row gap between 99,051 KoBo submissions and 97,151
  merged rows. Recorded so a Phase 4 delta of that size is not misread.
- Deleting orphaned bucket prefixes and the stale `*-dev` mirrors → Phase 11.
- No `devtools::check()` run: Phase 0 changed no R code, and the audit shows
  `R-CMD-check` has been red on `main` for months for unrelated reasons
  (pre-2022 r-lib templates). Fixing it is Phase 9.

**Open questions — resolved 2026-07-31**

- **v2 freeze → don't freeze v2.** Any form still receiving submissions runs
  through the recurring pipeline like the others, however low the rate. v1 (last
  submission 2020-08-28) is frozen; v2 and v3 stay live as versioned sources
  `surveys.landings.{v1,v2,v3}` — the template's documented
  `{source}: {version}: raw / preprocessed / validated` shape, as Zanzibar uses
  for `wf_v1`/`wf_v2`. No `_id` watermark and no reconciliation job needed.
  Applied to PLAN §2, the Phase 3 scope, the Phase 3 row in the phase table, the
  architecture diagram, the risk row, and STRUCTURAL-DIFF §1.
- **Dev seeding → the user is copying the latest prod version of each prefix
  into `timor-dev` by hand.** No pipeline change required. Once that lands, a
  branch push becomes a usable phase gate as PLAN §5 assumes. `pds-timor-dev`
  already holds 96,393 track files (stale to 2024-07) and does not need seeding.

- **API buckets → resolved.** Correct names are **`peskas-api-prod`** and
  **`peskas-api-dev`**; the Phase 0 "prod bucket does not exist" finding was a
  bad probe and is withdrawn. The prod API is **live for Kenya, Mozambique and
  Zanzibar** (727 objects, current to 2026-07-31). The user created empty
  `timor/{raw,validated}` markers in both buckets on 2026-07-31.
  Consequences for Phase 6: Timor is the **last** country in, not the first, so
  read a real `mozambique/validated/trips-validated` parquet out of
  `peskas-api-prod` and conform to its observed schema rather than inferring one
  from Moz's `R/api.R`; publish to dev first, since live consumers will pick up
  anything that lands in prod. Still to check: whether the ingestion SA can
  **write** to `peskas-api-prod` (it can read). AUDIT §6.
- **Dev seeding → done, 2026-07-31.** The user copied the latest production
  version of all 21 live `gs://timor` prefixes (run `90ede9a`, 2026-07-31) into
  `gs://timor-dev`. Branch pushes are now a usable phase gate.
  Caveat when reading results: dev outputs are derived from seeded prod state,
  so they are not independently reproducible from scratch.

**Still open for the next session**

1. The remaining AUDIT §7 corrections are **not yet applied to PLAN.md**. Items
   1 (raw already flat), 2 (v2 live), 4 (dev seeding) and 5 (`peskas-api`) are
   resolved and, where they changed scope, folded in. Items 3 (Phase 7 shim
   likely unnecessary) and 6 (validation email dead) are still only recorded in
   AUDIT.md. Fold them in before scoping Phases 5 and 7.

**Files added / removed / renamed**

- added: `CLAUDE.md`, `.claude/migration/AUDIT.md`
- modified: `.gitignore`, `.Rbuildignore`, `.dockerignore`,
  `.claude/migration/STATE.md`
- removed: `inst/__pycache__/kepler_mapper.cpython-38.pyc`
- untracked, gitignored: `reference/2026-07-31_90ede9a/` (golden snapshot)

---

## Phase 1 — Foundations — 2026-07-31

Branch: `feat/align-coasts-phase1` (off `feat/align-coasts` at `494a8d0`;
**uncommitted**, held for review at the user's request)

**Done**

*Configuration*

- `inst/conf.yml` → `inst/config.yml`, rewritten from
  `inst/config_template.yml` (the unified template) as a **superset**. New
  harmonized keys: `country: timor`, `ingestion.landings.{v1,v2,v3}`,
  `surveys.landings.{v1,v2,v3}.{raw,preprocessed,validated}` plus
  `merged`/`weight`/`validated`/`validation.flags`, `pds.{token,secret,
  pds_trips,pds_tracks}`, `api.trips.{raw,validated}`,
  `storage.google.{options_coasts,options_api}`. Every legacy key kept
  verbatim and marked `# [legacy]`.
- New-tree `file_prefix` values deliberately reuse the object families already
  in `gs://timor` (`timor-landings-v3_raw`, `timor-landings-v3_preprocessed`,
  …). The migration changes the serialization format, not the storage layout,
  so eleven years of history stays addressable and the seeded dev bucket stays
  usable as a phase gate.
- Buckets, all verified to exist by listing them with the ingestion SA:
  `timor` / `timor-dev`, `pds-timor` / `pds-timor-dev`,
  `public-timor` / `public-timor-dev`, `peskas-api-prod` / `peskas-api-dev`,
  `peskas-coasts` / `peskas-coasts-dev`. **`peskas-coasts-dev` does exist** —
  AUDIT §2 omitted it because it was never probed. `options_coasts` therefore
  follows the Moz pattern (dev bucket in `default`, prod bucket in
  `production`) rather than pointing both at `peskas-coasts`.
- `api.trips.{raw,validated}.cloud_path` = `timor/raw` / `timor/validated`;
  both markers re-confirmed present in `peskas-api-dev` and `peskas-api-prod`.
- KoBo asset ids re-verified against `eu.kobotoolbox.org` before being written
  into the config comments: v1 `aur3fK7mtJem5Cg8Wi2SPd` (SSF Landings),
  v2 `aaztUDtRzb9SpSV7i9iptb` (peskAAS), v3 `aEoWV7aprG47Q4uTpaopgD`
  (PeskAAS 2) — all three deployed and active, matching AUDIT §1.
- Timor's own `inst/config_template.yml` shipped: the cross-country spec with
  Timor's six deviations recorded explicitly (no dashboard MongoDB database,
  extra `public_storage`, Timor-only top-level blocks, the much deeper
  `validation` block, singular `pds-track` prefix, no `metadata.airtable`).

*`version: preprocess:` — decided per key, nothing dropped silently*

The template removed this field and warns pipelines that read
`version$preprocess` to re-add it. Measured readers:

| key | readers | decision |
|---|---|---|
| `surveys.landings_{1,2,3}` | `preprocess-landings.R` ×5, `merge-landings.R` ×3 | **kept** on the legacy key |
| `pds.trips` | `preprocess-pds-trips.R:40`, `validate-pds-trips.R:211` | **kept** |
| `pds.tracks` | `preprocess-pds-trips.R:270` | **kept** |
| `metadata` | `preprocess-metadata-tables.R:43` | **kept** |
| `validation` | `validate-landings.R:344` | **kept** |
| `surveys.landings` | `validate-landings.R:361`, in the dead `get_preprocessed_landings()` | **not added** — the key never existed and the helper is uncalled |

It is **not** re-added in the new tree: every reader is a legacy-key reader,
and harmonized code reads the per-stage `version:` field instead. The field
therefore lives exactly where it is read, and disappears with the legacy keys
in Phase 11.

*Secrets*

- `auth/` (15 plaintext files) → `.env` + `dotenv`. `.env.example` committed
  with every key documented; `.env` generated and gitignored. Before deleting
  `auth/`, every file's value was asserted present in `.env` —
  14 carried verbatim (the SA key JSON-equal after minification), and the
  15th, `auth/peskas-gmail-key`, is an orphan: an 18-char Gmail app password
  that **no config key ever read** (the `local:` env mapped `peskas_mail.key`
  to `auth/blastula_cred_file`, a 498-char serialized blastula credentials
  JSON). It was preserved as a commented entry in `.env` and then `auth/` was
  removed.
- `local:` config environment deleted. `.Renviron`: `R_CONFIG_ACTIVE=local` →
  `default`. Local and CI now resolve the same branch and differ only by
  `R_CONFIG_ACTIVE`. This also closes AUDIT §8.4 (`local: inherits:
  development`, a non-existent env).
- `read_config()` is dotenv-aware, accepts `config.yml` with `conf.yml` as a
  fallback, and a new exported `load_dotenv()` mirrors the standard.

*Package and container*

- `DESCRIPTION`: 3.3.0 → **4.0.0**; `Remotes:
  github::WorldFishCenter/peskas.coasts@v4.5.0` (the migration pin) and
  `github::WorldFishCenter/ssf-ai-toolkit/Rplug@plug-R`; `coasts`, `arrow`,
  `dotenv` added to Imports; `Config/testthat/edition: 3`; `BugReports:` added.
  Imports were **not** trimmed to the standard's lean list — packages the
  not-yet-migrated code calls (`googleCloudStorageR`, `googledrive`, `httr`,
  `data.table`, `reticulate`, …) stay until the phase that deletes their call
  sites. Same superset principle as the config. Title/Description/Authors kept
  — the reference's are unedited `usethis` boilerplate.
- `Dockerfile.prod`: `rocker/geospatial:4.4` → **4.5**, `ARG COASTS_REF=v4.5.0`
  → `install_github('WorldFishCenter/peskas.coasts')`. The default value means
  a plain `docker build` and the unchanged workflow both still work; the
  "resolve latest release" step arrives with the unpin in Phase 11.
- `Dockerfile` (dev) moved to 4.5 with the same `COASTS_REF`, so a local
  RStudio session runs against the same coasts release as CI.
- **rfishbase pin resolved: dropped.** It was pinned to 5.0.1 via
  `remotes::install_version()`; `coasts` imports rfishbase unpinned. Checked
  the CRAN 5.0.3 NAMESPACE directly: all six entry points Timor uses —
  `common_to_sci()`, `species()`, `country()`, `length_weight()`,
  `length_length()`, `estimate()` — are still exported, and coasts' `fb_tbl()`
  exists in both. So Timor's taxa code can stay on the shared helpers; no
  fallback needed. Both Dockerfiles now install rfishbase from CRAN.
- Added `R/peskas.timor.data.pipeline-package.R`; `R/globals.R` needed no
  change (`importFrom(rlang,.data)` was already in NAMESPACE).
- `.Rprofile` was empty; now carries `use_prod()` / `use_default()` /
  `show_env()` and a startup banner that warns when `.env` is missing. No
  `use_local()` — the environment is gone.
- `.Rproj`: `LineEndingConversion: Posix`. `.Rbuildignore`: dropped the now
  meaningless `^auth$`.

*Airtable frame — scope added mid-phase, 2026-07-31*

Raised by the user after the phase was otherwise complete: other Peskas
packages carry static reference data in Airtable, and Timor's had already been
entered. The plan did not account for it — it had the opposite verdict.

- Root cause: "Airtable" meant two unrelated things and the docs merged them.
  Timor's own orphaned `air_*` client (delete — verdict unchanged) versus the
  standard's **PESKAS | FRAME** integration, which Timor has never had. Both
  STRUCTURAL-DIFF §4 ("reconcile in P8") and AUDIT §7.7 ("delete") were
  answering about the first and silently skipped the second.
- Verified against the base rather than the docs (`appMMEJYlJdfSJEjm`):
  57 Timor taxa tagged `form_version: PeskAAS 2` with `alpha3_code`s matching
  `models.all_taxa`; 7 gears mapping Tetum `original_name` → `standard_name` /
  `code` / `FAO_abbrev`; 2 vessels; 40 landing sites; 457 `pds_devices`;
  `countries.Current Form` = the live v3 asset id. Taxa and gears created
  2026-07-30, **vessels 2026-07-31 09:14** — after the Phase 0 audit ran,
  which is part of why it read the base as irrelevant.
- Why it is load-bearing: `conf$metadata$airtable$name` is read by coasts in
  `ingestion.R`, `ingestion-pds.R`, `model-fishery.R`, `fishbase.R`,
  `export.R`, `predict-tracks.R`, `aggregate-effort.R`. Without the snapshot,
  Phase 6 emits a schema-correct API parquet full of untranslated Tetum
  labels — conformant and useless. It is a prerequisite for the payoff.
- **Decisions taken** (user, 2026-07-31): Airtable is **authoritative where it
  overlaps** — taxa, gears, vessels, landing_sites, districts/regions,
  pds_devices — with the Google Sheets keeping the five tables it does not
  cover (`morphometric_table`, `habitat`, `conservation`,
  `fishing_vessel_statistics`, `registered_boats`), so Timor goes from 15
  metadata tables to 5. Config lands in Phase 1, ingestion in Phase 3, the
  joins move off the Sheets in Phase 4, the legacy client dies in Phase 8.
- **Done in this phase:** `metadata.airtable.{name,assets}` and a top-level
  `airtable.{token,frame}` block in `config.yml`; the same in
  `config_template.yml` with the reasoning; `AIRTABLE_BASE_ID_FRAME` in `.env`
  and `.env.example`. PLAN §2.5 written, Phase 3/4/8 scopes updated, AUDIT §7.7
  corrected in place, STRUCTURAL-DIFF §1 and §4 updated, CLAUDE.md gained a
  "Reference data" section.
- **Key paths follow coasts, not Mozambique — deliberate.** `coasts::fetch_asset()`
  reads `conf$airtable$frame$base_id` / `conf$airtable$token` at the top level;
  Moz nests the same values under `metadata.airtable` because
  `peskas.mozambique.data.pipeline/R/airtable.R` is a **vendored copy** of
  coasts' module (same functions, same layout) wired to its own key paths.
  Timor imports `coasts` and calls it directly, so it keeps no copy and uses
  the hub's paths — PLAN §7, copy patterns not files. `metadata.airtable.name`
  / `$assets` (the snapshot prefix) are provided as well; coasts reads both
  spellings.
- **No `AIRTABLE_BASE_ID_ASSETS`.** It arrived in `.env` from Mozambique's, but
  a grep of Moz's `R/`, `inst/` and `.github/` and of coasts finds no reader,
  and the working PAT cannot see that base. Commented out in `.env` with the
  reason, and `.env.example` says explicitly not to add it.
- **Checked, not assumed:** Moz's `frame.tables.countries: Countries` looked
  like a casing bug against the base's lowercase `countries` table. The
  Airtable REST API resolves table names case-insensitively — both return 200.
  Not a bug; no correction filed.

*Stale Airtable implementation removed (user request, pulled forward from Phase 8)*

Timor's own `air_*` client and everything that hung off it. All of it was
orphaned: `ingest_validation_tables()` was exported but called by no workflow
and no R code, and read `pars$validation$airtable$*`, a config key that has not
existed for years — it would have failed at `add_version(NULL, "rds")`.

| removed | why |
|---|---|
| `R/airtable.R` (4 exported `air_*` fns + 3 internal helpers) | superseded by `coasts::airtable_to_df()` etc. |
| `ingest_validation_tables()` in `R/ingest-metadata-tables.R` | only consumer of `air_get_records()`; read a dead config key |
| `get_validation_tables()` in `R/validate-landings.R` | AUDIT §8.2, dead |
| `get_preprocessed_landings()` in `R/validate-landings.R` | AUDIT §8.2, dead |
| `inst/airtable/edit-submission-link.js` | AUDIT §7.8 — old kobo host, hardcoded v2 asset |
| `matches("air")` section in `_pkgdown.yml` | would otherwise select nothing and break the pkgdown build |

`document()` dropped five NAMESPACE exports and five man pages. A repo-wide
grep for `air_get_records|air_records_to_tibble|air_tibble_to_records|air_upload_records|ingest_validation_tables`
now returns nothing outside the migration docs.

This closes AUDIT §8.2 and §7.8 and empties Phase 8's Airtable bullet — the
only Airtable item left anywhere is dropping the `AIRTABLE_KEY` secret from the
workflows, which stays in Phase 9 with the other secret renames.
- Adding `metadata.airtable` is only safe **because** this phase deleted the
  broken duplicate `get_preprocessed_metadata()` that read
  `metadata$airtable$name`. Had the bug still been live, this block would have
  changed its behaviour. Re-verified after the change that
  `validation$airtable` is still NULL, so the two remaining dead helpers are
  unaffected.

*Credentials — resolved locally, one item left for CI*

The PAT inherited from `auth/airtable-key` could not read the frame base
(`INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND` against both `appMMEJYlJdfSJEjm` and
`/v0/meta/bases`). The user supplied a working one; verified read access to
PESKAS | FRAME and PESKAS | TRACKS.

It must be stored **bare** (`pat…`). The old `auth/airtable-key` held
`Bearer pat…` because the retired `air_get_records()` passed the value verbatim
as the Authorization header, whereas `coasts::airtable_to_df()` does
`paste("Bearer", token)`.

**Resolved in CI, 2026-07-31:** the user created the `AIRTABLE_TOKEN` and
`AIRTABLE_BASE_ID_FRAME` repository secrets, and both are now mapped in
`data-pipeline.yaml`'s `env:` block. The dead `AIRTABLE_KEY` mapping was
removed from `data-pipeline.yaml` and `data-report.yaml` — nothing reads it now
that `R/airtable.R` is gone. The secret itself is deleted in Phase 9 with the
other renames.

*Live bug fixed*

- **AUDIT §8.1 — duplicate `get_preprocessed_metadata()`.** The unexported,
  broken definition in `validate-landings.R` (reading the removed
  `pars$metadata$airtable$name`) shadowed the correct exported one at
  `get-cloud-files.R:110` by collation order. Deleted, with a comment left in
  its place. Verified after the change:
  `body(get_preprocessed_metadata)` now reads
  `pars$metadata$google_sheets$name`. No NAMESPACE or man/ change — only one
  of the two was ever exported. This is also why no `metadata.airtable` block
  was added to the config: doing so would have quietly revived the broken
  reader instead of removing it.
- AUDIT §8.2 (`get_validation_tables()`, `get_preprocessed_landings()`) left
  as assigned. They are uncalled dead code, not duplicates; the config rewrite
  does not make deleting them any easier, and Phase 11 owns dead-code removal.

**Verified**

- `config::get()` old vs new, leaf by leaf, in **both** environments:
  **zero legacy keys lost, zero legacy values changed**; 55 new leaves in
  `default` (104 → 159 in `production`). Dead keys still resolve to NULL as
  before (`surveys$landings$file_prefix`, `metadata$airtable`,
  `validation$airtable`).
- `dotenv` round-trip: all 11 values load, including the 2,259-char minified
  service-account JSON and the 498-char blastula blob; both re-parse as JSON.
- `read_config()` end-to-end via `load_all()`: resolves `default` →
  `timor-dev` / `peskas-api-dev`, `production` → `timor` / `peskas-api-prod`,
  and picks up `.env` with no `auth/` present.
- `devtools::load_all()` clean. `devtools::document()` wrote
  `NAMESPACE` (+`export(load_dotenv)` only), `load_dotenv.Rd`,
  `peskas.timor.data.pipeline-package.Rd`.
- `devtools::check()` **against a measured baseline**: a `git worktree` of the
  Phase 0 commit `494a8d0` was checked in the same session.

  | | baseline (494a8d0) | after Phase 1 |
  |---|---|---|
  | WARNING | 1 — undocumented `get_kobo_data()` args | 1, identical |
  | testthat | `FAIL 1 \| WARN 8 \| SKIP 0 \| PASS 9` | byte-identical, same test, same warnings |
  | NOTEs | 4 | 5 |

  The pre-existing failure is
  `test-pre-process-landings.R:16`, `nrow(nested$_attachments[[1]])` = 3, not
  2 — untouched by this phase. The one new NOTE is
  `Namespaces in Imports field not imported from: 'arrow' 'coasts'`, which
  clears when Phase 2 calls `coasts::*` and Phase 3 writes parquet. The
  "Imports includes 37 non-default packages" NOTE is the cost of declaring
  what the code already uses.
- `inst/tinytest/` **not** run: those suites read pipeline artefacts from cloud
  storage and this phase changed no data path.

**Deferred, with reasons**

- **`storage.mongodb` is written but commented out.** Enabling it in isolation
  would break the live pipeline, not merely be inert: five call sites treat
  every child of `storage` as a storage *provider* —

  ```r
  purrr::map(pars$storage, ~ upload_cloud_file(files, .$key, .$options))
  ```

  `R/ingest-landings.R:68`, `R/ingest-landings.R:135`,
  `R/ingest-pds-data.R:54`, `R/preprocess-landings.R:379`,
  `R/preprocess-pds-trips.R:251`. A `mongodb` sibling makes each fire a second
  time with `key = NULL, options = NULL`, and the two multi-line ones would
  upload the same object twice. **Phase 2 must narrow all five to
  `pars$storage$google` first** — behaviour-identical today, since `google` is
  the only provider — and only then uncomment the block. The full shape is
  live in `inst/config_template.yml` so nothing has to be re-derived. Same
  applies to `pars$pds_storage` (`R/ingest-pds-data.R:168`), which stays
  single-provider for now.
- **CI env vars for the new KoBo names.** `ingestion.landings.<v>` reads
  `KOBO_ASSET_ID_V{1,2,3}` and `KOBO_TOKEN`; the workflow sets only
  `KOBO_PESKAS{1,2,3}` and there is no `KOBO_TOKEN` secret. Harmless today —
  nothing reads the new tree — but **Phase 3 must add three lines to the
  `env:` block of `data-pipeline.yaml`**, fed from the existing secrets.
  `KOBO_TOKEN` is optional; basic auth works. The secret *renames* stay in
  Phase 9. The only workflow edits made this phase were the Airtable ones
  above.
- `pds.customers` left commented in the template: Timor's PDS customer string
  is not recorded anywhere and must be read off the PDS API in Phase 7. Not
  guessed.
- `metadata.fishbase.taxa_enriched.file_prefix` not added. `coasts` reads it in
  its taxa helpers; add it in Phase 4b if those helpers are adopted.
- `mongolite` put in Suggests, not Imports — unused until the Phase 5 decision.
- `ANTHROPIC_API_KEY` rotation still open (user action, AUDIT §10).

**Open questions for the next session**

1. Phase 2 order of operations: narrow the five `purrr::map(pars$storage, ...)`
   sites **before** anything else, since both the mongodb block and the coasts
   storage delegation depend on it.
2. The locally installed `coasts` is **4.4.1**; the pin is **v4.5.0**. Install
   the pinned release locally before Phase 2 so local behaviour matches the
   container, or accept the skew knowingly.
3. Whether `peskas-coasts-dev` is the right `default` target for Timor, or
   whether Timor should read/write the prod hub bucket in both environments.
   Followed the Moz pattern for now; the hub contents are shared reference
   data, so this is worth a deliberate answer in Phase 2.

**Files added / removed / renamed**

- renamed: `inst/conf.yml` → `inst/config.yml` (content rewritten)
- also modified for the Airtable scope addition: `.claude/migration/PLAN.md`
  (new §2.5, Phase 1 table row, Phase 3/4/8 scopes),
  `.claude/migration/AUDIT.md` (§7.7 corrected in place),
  `.claude/migration/STRUCTURAL-DIFF.md` (§1 credential map, §4 verdict)
- added: `inst/config_template.yml`, `.env.example`,
  `R/peskas.timor.data.pipeline-package.R`, `man/load_dotenv.Rd`,
  `man/peskas.timor.data.pipeline-package.Rd`
- modified: `DESCRIPTION`, `Dockerfile`, `Dockerfile.prod`, `NAMESPACE`,
  `R/utils.R`, `R/validate-landings.R`, `R/ingest-metadata-tables.R`,
  `_pkgdown.yml`, `.Rprofile`, `.Rbuildignore`,
  `peskas.timor.data.pipeline.Rproj`, `man/add_version.Rd`,
  `man/read_config.Rd`, `CLAUDE.md`,
  `.github/workflows/data-pipeline.yaml` and `.github/workflows/data-report.yaml`
  (Airtable env vars only)
- removed: `auth/` (15 files, untracked — every value carried into `.env`
  first), the `local:` config environment, `R/airtable.R`,
  `inst/airtable/edit-submission-link.js`, `ingest_validation_tables()`, and
  the three dead helpers in `R/validate-landings.R`
  (`get_preprocessed_metadata()` duplicate, `get_validation_tables()`,
  `get_preprocessed_landings()`); five man pages and five NAMESPACE exports
  followed
- untracked, gitignored: `.env`; `.Renviron` modified in place

---

## Phase 2 — Storage delegation — 2026-07-31

Branch: `feat/align-coasts-phase2` (off `feat/align-coasts` at `ea7f253`;
**uncommitted**, held for review at the user's request)

**Pre-flight**

- Phase 1 (`ea7f253`) fast-forward-merged into `feat/align-coasts`.
- Pipeline run **30637659244** (the first on `rocker/geospatial:4.5` with
  `coasts` pinned at `v4.5.0`): **`build-container` green in 3m06s.** No Phase 2
  container work was needed. The only annotation is the pre-existing Node 20
  deprecation notice on `checkout@v4` / `build-push@v5` / `login@v3` /
  `setup-buildx@v3`, which Phase 9 fixes.
- Local `coasts` upgraded **4.4.1 → 4.5.0**, installed from the local checkout
  at `~/Desktop/work/wf_projects/peskas.coasts`, verified clean at tag `v4.5.0`
  (`c2bdc58`). Local and container now run the same release.

**Done**

*1. Storage-provider iteration narrowed — first, before any deletion*

Six sites, not five: the Phase 1 note missed the `pds_storage` twin at
`ingest-pds-data.R:168`. All now address `$google` explicitly.

| site | old shape | now |
|---|---|---|
| `ingest-landings.R:68` | `purrr::walk(pars$storage, ~ upload_cloud_file(...))` | single `coasts::upload_cloud_file()` |
| `ingest-landings.R:135` | same | same |
| `ingest-pds-data.R:54` | `purrr::map(pars$storage, ...)` | single call |
| `preprocess-landings.R:379` | `purrr::map(pars$storage, ...)` | single call |
| `ingest-pds-data.R:168` | `purrr::map(pars$pds_storage, ~ purrr::walk(...))` | outer provider map dropped; per-file `purrr::walk` kept |
| `preprocess-pds-trips.R:251` | `purrr::map(pars$storage, ~ purrr::walk(...))` | same |

The last two were already degenerate — the inner call hardcoded
`$google$key`/`$google$options`, so the outer map re-uploaded the *same* object
once per child of `storage`. The per-file `purrr::walk` is kept because
`insistent_upload_cloud_file()` then retries each file independently rather than
the whole batch. **`pds_storage` was fixed for consistency, not because it
needed it** — it is still single-provider, but leaving one walk-the-children
site in place would preserve exactly the footgun this step removes.

*2. `storage.mongodb` enabled*

Uncommented in `inst/config.yml` in both environments (`timor-dev` /
`validation-dev` in `default`, `timor-prod` / `validation-prod` in
`production`), and the "do not uncomment in isolation" warning replaced with a
note on why it is now safe. The stale prerequisite list in
`inst/config_template.yml` was rewritten to past tense. No `dashboard`
database — PLAN §2.1. The block has **no reader**; it is inert until Phase 5.

*3. Delegation to `coasts::*`*

- **Deleted `R/cloud-storage.R`** (261 l, 4 exported functions).
- **Deleted `R/google-drive.R`** — `authenticate_google_drive()` was exported and
  called by nothing: a repo-wide grep over `R/`, `inst/`, `tests/`, `.github/`
  found the definition and no call site. STRUCTURAL-DIFF §4 assigned it to P2.
- 84 call sites across 20 files namespaced `coasts::`, including roxygen prose
  and `\dontrun{}` examples that referenced the deleted names.
- **The three `inst/report/generate_*.R` drivers** called
  `peskas.timor.data.pipeline::upload_cloud_file()` and would have died with
  "object not found in namespace". One of them
  (`generate_enumerators_report.R`) runs in the *live* `data-pipeline.yaml`.
  All three now call `coasts::upload_cloud_file()`.
- No workflow invokes a deleted function directly — checked by extracting every
  `peskas.timor.data.pipeline::*` reference from `.github/workflows/`.
- `googleAuthR` and `googledrive` removed from `DESCRIPTION` Imports (last
  readers gone). `googleCloudStorageR` stays — four `gcs_list_objects()` sites.
- `_pkgdown.yml`: the `matches("google")` subtitle now selected nothing and
  would break the pkgdown build (same failure mode as Phase 1's
  `matches("air")`); removed. `matches("cloud")` still resolves via the two
  `insistent_*` wrappers.

*4. `R/get-cloud-files.R` collapsed*

318 l → one internal `download_versioned_rds()` (resolve → download →
`readr::read_rds()`) plus 13 thin accessors. Interchange format stays `.rds`
this phase; the helper becomes `coasts::download_parquet_from_cloud()` in
Phase 4. All 13 accessors kept — dead-code removal is Phase 11, and
`get_tracks_map()` / `get_validation_sheet()` have no callers today.

`coasts::resolve_storage_opts(pars, "country" | "pds")` adopted here rather than
`pars$storage$google$options`. It is **not** used at the other ~25 call sites:
they are rewritten in Phases 3–8 anyway, and 60 equivalent substitutions now
would be churn that makes those diffs harder to read. Not usable for
`public_storage` at all — coasts only knows `coasts`/`country`/`pds`, so
`get_public_files()` and `get_tracks_map()` read it directly, with a comment.
A `public` type is a small Phase 10 upstream.

*5. `add_version()` — kept, deliberately*

Body-identical to `coasts::add_version()` (both printed and compared). Kept in
Timor because it is a **naming** helper, not a storage function; no phase
assigns it; and delegating would mean removing an exported name that ~40 call
sites and the three `inst/report/` drivers use, for zero behavioural gain.
Dedupe when the retry wrappers go upstream in Phase 10, not before.

**The one real hazard found — `cloud_object_name()` is not a drop-in**

The brief said the five storage functions were "verified signature-for-signature
identical". The *signatures* are. Two bodies are not:

1. `coasts::cloud_object_name()` returns `selected_rows$name[1]`; Timor's
   returned the whole `selected_rows$name` vector. Timor also did `na.omit()`
   on the formatted table where coasts does `filter(!is.na(updated))` — that
   difference is immaterial, since a non-versioned object yields `NA` in `ext`
   and is dropped by the extension filter either way.
2. `coasts::upload_cloud_file()` authenticates with `force = TRUE`, so it
   re-auths per call instead of reusing a live token. Slower, more robust
   against stale OAuth; accepted.

(1) is load-bearing. Rather than reason about it, **every `cloud_object_name()`
call site was run against the dev buckets and the returned length recorded** —
29 prefix/extension/`exact_match` combinations. 27 returned exactly 1 and two
returned 0 (`pds-track-complete*`, which does not exist in dev). **Exactly one
returned more than one: the track enumeration in `ingest_pds_tracks()`, at
96,393 names.** Under coasts' `[1]` truncation, `file_list_id` would have been a
single id, `tracks_to_download <- trips_ID[!(trips_ID %in% file_list_id)]` would
have become essentially the whole trip list, and the job would have re-fetched
~96k tracks from the PDS API on every run.

That site was never a versioned-object lookup — each track has its own base name
(`pds-track-<trip_id>`), so it is a bucket scan. It now calls
`googleCloudStorageR::gcs_list_objects(bucket, prefix)` + an extension filter,
which is exactly what the tail of the *same function* already did for
`tracks_names`. Verified set-equal to the old implementation: **96,393 vs
96,393, zero elements in either direction.**

**Open question from Phase 1, answered: `peskas-coasts-dev` stays the `default` target**

Both hub buckets were inventoried with the ingestion SA. They are both **live**:

| | `peskas-coasts` | `peskas-coasts-dev` |
|---|---|---|
| objects | 4,207 | 1,824 |
| `assets__*` | 282, newest 2026-07-31 | 42, newest **2026-06-29** |
| `pds-trips` | 347, newest 2026-07-31 | 204, newest 2026-07-27 |
| `taxa-fishbase-enriched` | 118 | 22 |

Decisive point: **`options_coasts` is not a read-only reference source.** coasts
resolves `coasts_opts` and *uploads* to it in `export.R` (×3),
`model-fishery.R`, `ingestion-pds.R`, `summarize-data.R` — the bucket holds
per-country outputs (`mozambique_fishery_metrics`, `kenya_effort_gear_h3res9`,
`*_monthly_summaries_map`). Pointing `default` at `peskas-coasts` would make
every push to a phase branch write Timor outputs into the bucket the live
cross-country dashboards read, destroying the dev/prod isolation PLAN §5 uses as
its integration test. The `-dev` hub is not a stale mirror — Kenya, Mozambique
and Zanzibar write to it — so following the Moz pattern is right here, not
cargo-culted. **Keep `peskas-coasts-dev` in `default`.**

The cost is one thing Phase 3 must handle — see below.

**Verified**

- `devtools::load_all()` clean. `devtools::document()` rewrote `NAMESPACE`
  (−5 exports: the four storage functions + `authenticate_google_drive`) and
  deleted 5 man pages; 20 Rd files rewritten.
- **`devtools::check()` against the Phase 1 baseline:**

  | | baseline (`ea7f253`) | after Phase 2 |
  |---|---|---|
  | WARNING | 1 — undocumented `get_kobo_data()` args | 1, identical |
  | NOTEs | 5 | 5 |
  | unused-Imports NOTE | `'arrow' 'coasts'` | **`'arrow'` only** ✅ |
  | Imports count NOTE | 37 non-default | 35 non-default |
  | testthat | `FAIL 1 \| WARN 9 \| PASS 9` | identical |

  The `coasts` NOTE dropping is the signal the brief asked for: `coasts` is
  genuinely called now. The testthat failure is the pre-existing
  `test-pre-process-landings.R:16` (`nrow(nested$_attachments[[1]])` = 3, not 2).
  Note the Phase 1 entry recorded `WARN 8`; re-measuring `ea7f253` in this
  session gives `WARN 9`, so that was an environment difference, not a Phase 2
  regression — both trees were measured side by side and are identical.
- **`inst/tinytest/` — the real gate, run against `timor-dev`:**

  | suite | result |
  |---|---|
  | `test_merged_trips.R` | **all ok, 2/2** |
  | `test_validated_pds_trips.R` | **all ok, 7/7** |
  | `test_public_data.R` | **all ok, 1/1** |
  | `test_validated_landings.R` | 2 fails / 8 passes |

  `test_validated_landings.R` was then run against a `git worktree` of `ea7f253`
  installed into a separate library in the same session: **identical** — same
  two assertions (`<33--36>` landing dates prior to 2017, `<49--53>`
  `catch_purpose` is NULL), same 2/8 split, same four
  "Unknown or uninitialised column" warnings. Pre-existing, and a property of
  the seeded dev artefact rather than of the code: `R/pt_nest_species.R` and
  `R/pt_nest_attachments.R` are not in this phase's diff at all. **No assertion
  was touched.**
- **Round-trip against `gs://timor-dev`:** wrote a tibble, uploaded with
  `coasts::upload_cloud_file(name = <basename>)`, resolved with
  `coasts::cloud_object_name(exact_match = TRUE)` (n = 1, name matched
  `add_version()` output exactly), downloaded with
  `coasts::download_cloud_file()`, `identical()` on the round-tripped frame.
  Test object deleted from the bucket afterwards.
- **Every rewritten accessor exercised against real dev artefacts**, row counts
  matching the AUDIT §9 golden baseline exactly:
  `get_merged_trips` 174,893 × 26; `get_validated_landings` 97,151 × 19;
  `get_validated_pds_trips` 84,741 × 8; `get_validation_sheet` 97,151 × 7;
  `get_models` 2 elements (national, municipal); `get_preprocessed_sheets` 15
  tables; `get_tracks_ids` 97,825 × 2; `get_public_files` trips/catch/aggregated
  (156,370 / 1,408,300 — lower than the prod golden 171,855 / 1,721,341 because
  `public-timor-dev` is a stale mirror, not a Phase 2 delta).
- `coasts::resolve_storage_opts(pars, "country")` asserted `identical()` to
  `pars$storage$google$options`.

**Findings that change later phases**

1. **Phase 3 must run `ingest_assets()` itself.** `assets__*` in
   `peskas-coasts-dev` is newest 2026-06-29 — 32 days stale, and it *predates*
   Timor's Airtable rows (taxa/gears 2026-07-30, vessels 2026-07-31 09:14).
   Reading the existing dev snapshot would silently yield a frame with no Timor
   taxa, gears or vessels. Refreshing it is cheap; assuming it is populated is
   not.
2. **AUDIT §8.5 is stale — no fix needed.** `export_files()` already normalises:
   `purrr::walk2(files, filenames, coasts::upload_cloud_file, provider =, options =)`
   named-matches `provider`/`options` and positionally fills `file` then `name`,
   so `name` gets the basename. Verified by replaying the argument matching
   against a stub. The 45 absolute-path objects in `public-timor` are dated
   2026-01-15/18 and are historical residue from local runs. Phase 8 has nothing
   to fix; deleting the residue is Phase 11 bucket hygiene. CLAUDE.md corrected.
3. **The `setwd("../..")` in all four tinytest suites is now dead weight.** It
   dates from the `local:` config era. tinytest sets the working directory to the
   test file's own directory, so from an installed package `../..` lands in the
   R library, not the repo root — `.env` is therefore never found on a local run
   and the suites can only authenticate from real environment variables. Harmless
   in CI (the workflow supplies them). Left alone: it is a test-harness cleanup,
   not Phase 2 scope. Fold into Phase 5 or 9.
4. **`get_validated_landings()` is called as `get_validated_landings(pars)` by
   `test_validated_landings.R`**, but its only parameter is `log_threshold` and
   the body ignores it (it calls `read_config()` itself). Harmless, pre-existing,
   unchanged by this phase. Tidy in Phase 5.
5. **coasts has no retry wrappers at all** — confirmed, so
   `insistent_{upload,download}_cloud_file()` had to stay. Phase 10 upstream
   candidate, together with a `public` type for `resolve_storage_opts()`.

**Deferred, with reasons**

- **The "Resolve peskas.coasts release" workflow step** in PLAN's Phase 2 bullet
  was **not** added. `COASTS_REF` is deliberately pinned to `v4.5.0` for the
  duration of the migration (PLAN risk table); resolving the latest release at
  build time is the *opposite* of that and arrives with the unpin in Phase 11.
  `Dockerfile.prod`/`Dockerfile` were already wired to `coasts` in Phase 1, so
  the rest of that bullet was already satisfied.
- `KOBO_ASSET_ID_V1/2/3` in the workflow `env:` block — Phase 3 prerequisite,
  explicitly out of scope here.
- `coasts::{upload,download}_parquet_{to,from}_cloud()` not called yet. Nothing
  writes parquet until Phase 3/4, which is why the `arrow` unused-Imports NOTE
  survives.
- The 13th accessor set was not pruned and no legacy config key was removed —
  Phase 11 owns dead code.
- `ANTHROPIC_API_KEY` rotation still open (user action, AUDIT §10).

**Open questions for the next session**

1. None blocking. The Phase 1 hub-bucket question is answered above; the
   `purrr::map(pars$storage, ...)` hazard is closed.
2. Phase 3 should decide the `ingest_assets()` bucket inconsistency PLAN flags
   (coasts' `ingestion.R` writes the snapshot to the *country* bucket while
   `ingestion-pds.R` reads it from the *hub*). Phase 2 adds one datum: the hub is
   where 282 prod / 42 dev `assets__*` objects actually live, and coasts writes
   plenty else there, so the hub is the de-facto home.

**Files added / removed / renamed**

- removed: `R/cloud-storage.R`, `R/google-drive.R`; `man/cloud_object_name.Rd`,
  `man/cloud_storage_authenticate.Rd`, `man/download_cloud_file.Rd`,
  `man/upload_cloud_file.Rd`, `man/authenticate_google_drive.Rd`;
  5 NAMESPACE exports
- rewritten: `R/get-cloud-files.R` (318 l → 13 accessors over one internal
  `download_versioned_rds()`)
- modified (namespacing + the narrowing): `R/calculate-weights.R`,
  `R/clean-raw-data.R`, `R/estimate-catch.R`, `R/export-dataverse.R`,
  `R/export.R`, `R/format-public-data.R`, `R/ingest-landings.R`,
  `R/ingest-metadata-tables.R`, `R/ingest-pds-data.R`, `R/merge-landings.R`,
  `R/merge-trips.R`, `R/model-catch.R`, `R/preprocess-landings.R`,
  `R/preprocess-metadata-tables.R`, `R/preprocess-pds-trips.R`,
  `R/validate-landings.R`, `R/validate-pds-trips.R`
- modified: `inst/config.yml` (mongodb enabled, both envs),
  `inst/config_template.yml` (prerequisite note rewritten), `DESCRIPTION`
  (−`googleAuthR`, −`googledrive`), `NAMESPACE`, `_pkgdown.yml`
  (−`matches("google")` subtitle), `CLAUDE.md`, `.claude/migration/STATE.md`,
  20 `man/*.Rd`
- modified: `inst/report/generate_enumerators_report.R`,
  `inst/report/generate_form_summary.R`, `inst/report/generate_upload_report.R`
  (namespace only)
- **unchanged: `inst/tinytest/`, `tests/`, `R/export.R`'s serialization and
  upload path, every `portal-*.json` object name.**

---

## Addendum to Phase 2 — the `ea7f253` dev run (30637659244) — 2026-07-31

Two jobs failed on the Phase 1 push. **Neither is caused by Phase 1 or Phase 2.**
One is transient; the other is a long-standing silent data regression that
changes Phase 4b's scope and blocks branch-push integration testing for the
whole lower half of the DAG.

Job outcomes: `build-container` ✓, `ingest-preprocess-landings-v1-v3` ✓,
`ingest-preprocess-metadata-tables` ✓, `ingest-preprocess-v2-landings` ✓ (both
steps), **`ingest-pds-data` ✗**, **`merge-landings` ✗** (at `calculate_weights()`),
with `validate-*`, `merge-trips` and everything downstream skipped.

### A. `ingest_pds_trips()` — transient upstream, on an unprotected path

`retrieve_pds_trips_data()` ([retrieve-pds-data.R](../../R/retrieve-pds-data.R))
issues a single plain `httr::GET` to `analytics.pelagicdata.com` for the entire
trip history (`start_date = "2018-07-01"` → today) with `httr::write_disk()`.
No retry, no timeout handling. The error — `Recv failure: Connection reset by
peer` — is the peer dropping a large streaming response. Re-running should pass.

Structural note: Phase 2 preserved `insistent_{upload,download}_cloud_file()`,
which protect GCS uploads and per-*track* downloads, while the largest and most
failure-prone request in the pipeline has no protection. **Phase 7 will not fix
this** — grepped coasts' `pds-api.R` and `ingestion-pds.R` for
retry/`insistently`/`req_retry`: nothing, consistent with Phase 2's finding that
coasts has no retry anywhere. There is also no dev PDS, so this request hits the
live API identically in both environments. Wrapping it is a small fix; it belongs
with the Phase 10 upstream of the retry wrappers, or opportunistically in Phase 7.

### B. `get_rfish_table()` — the pin cannot resolve in dev, and the pin is load-bearing

Immediate cause: [calculate-weights.R:438](../../R/calculate-weights.R#L438)
hardcodes `version = "20241013011806_a41d5ff"`. It is the **only** hardcoded
version pin in the codebase. Measured:

| bucket | `rfish-table` versions | pinned version |
|---|---|---|
| `timor` | 1198, back to 2021-09-03 | present |
| `timor-dev` | 3 (07-30 seeding + two Phase 1 runs) | **absent** |

The dev seeding copied only the *latest* version of each prefix, so the pin
resolves to nothing → `readr::read_rds(character(0))` → `file(character(0))` →
`invalid 'description' argument`. Under Phase 2 this becomes
`coasts::cloud_object_name()` returning `NA_character_` instead of
`character(0)` — it still fails, but with a less legible error.

**This was missed by the Phase 2 audit.** The audit script passed
`version = "latest"` uniformly across all 29 call-site combinations, so
`get_rfish_table` reported n = 1; the pin was the one argument normalised away.
Any future audit of this kind must use each site's *actual* arguments.

#### The regeneration has been silently broken for ~21 months

| table | size | rows | species | mean `a` | mean `b` |
|---|---|---|---|---|---|
| pinned `20241013011806_a41d5ff` | 157.5 Kb | **5,926** | **693** | 0.02716 | 2.9781 |
| every version since | 32.8 Kb | **1,323** | **88** | 0.01724 | 3.0370 |

- The pinned 2024-10-13 object is the **last large version ever written** —
  index 1103 of 1198. Distinct historical sizes: 32.8 / 152.7 / 157.5 / 159.2 /
  166.5 / 174.6 / 177.3 / 177.7 / 179.7 Kb.
- **Nothing at all was written between 2024-10-13 and 2026-01-19** — a 15-month
  gap. That fits AUDIT §8.3: `ingest_rfish_table()` runs `continue-on-error:
  true`, so it failed silently.
- From 2026-01-19 (`70d2aba`) onward it "succeeds" but emits 32.8 Kb:
  **95 consecutive degraded versions, zero large ones.**
- Whoever added the pin pinned the last good output. Production catch weights are
  correct *only* because of it.

**Phase 1's rfishbase unpin is NOT the cause.** Three dev versions were compared
directly and are content-identical (1,323 × 43, 88 species, same mean `a`/`b`):
`90ede9a` 07-30 and 07-31 (prod code, **rfishbase 5.0.1 pinned**) and `ea7f253`
07-31 (**rfishbase unpinned**). The collapse predates both by ~18 months.

#### Consequence for the migration

`merge-landings → calculate_weights()` can **never** succeed against a dev
bucket until the pinned object exists there, and it gates `validate_landings` →
`merge_trips` → `model-indicators` / `export-trips`. PLAN §5's "push a branch =
integration test" is therefore broken for the entire lower half of the DAG, for
every remaining phase.

**Immediate unblock (user action, recommended):** copy
`rfish-table__20241013011806_a41d5ff__.rds` from `gs://timor` to
`gs://timor-dev`. One object, no code change, prod untouched. It also makes dev
and prod resolve the *same* coefficient table, which is what makes the Phase 4/8
golden diffs meaningful.

**Do not "align" by dropping the pin.** See §C.

### C. Alignment question raised by the user, answered: the target is `taxa-fishbase-enriched`

The premise that the reference packages do not do this is incorrect —
**Mozambique performs the same length→weight conversion.**
`peskas.mozambique.data.pipeline/R/model-taxa.R` has `getLWCoeffs()` →
`rfishbase::load_taxa()` + `get_length_weight_batch()` →
`rfishbase::length_weight()`. The difference is *where the coefficients live*:

| | mechanism | snapshot | pinned |
|---|---|---|---|
| Timor | `ingest_rfish_table()` → `rfish-table__*` in the **country** bucket | yes, versioned | yes, hardcoded |
| Mozambique | `getLWCoeffs()` inline, every run | **no** | no |
| **coasts (harmonized)** | `enrich_taxa()` → `taxa-fishbase-enriched__*` in the **hub** bucket | yes, versioned | no |

`coasts::enrich_taxa()` is keyed on
`conf$metadata$fishbase$taxa_enriched$file_prefix` — the config key Phase 1
deliberately withheld pending Phase 4b — and the hub already holds 118 prod / 22
dev `taxa-fishbase-enriched__*` objects, current to 2026-07-31. **That is the
alignment target**, and PLAN Phase 4b already names it.

Two things must survive the alignment:

1. **Timor needs more than Moz does.** Timor's forms record length
   *frequencies* (hence the `length_frequency` list-column), so
   `get_morphometric_tables()` builds both a length-weight table (`a`, `b`) and a
   length-**length** table (`aL`, `bL`, `Length1`, `Length2`) — plus locally
   curated morphometric rows merged in via `manual_table`. Moz's ADNAP form
   records weights/buckets directly (`catch_bucket_kg`), so it needs less
   machinery. This is a data-model difference, not gratuitous divergence.
2. **Snapshot-and-version semantics.** Moz's inline refetch means its weights
   change silently whenever FishBase does — a reproducibility hole, and exactly
   the "reference repo has bugs, copy patterns not files" case of PLAN §7. Do not
   import it.

**New hard prerequisite for Phase 4b:** whatever replaces `ingest_rfish_table()`
must be validated against the **5,926-row / 693-species / mean `a` 0.02716**
pinned baseline, not merely against "the job ran". On current evidence any
regeneration path — Timor's own or `coasts::enrich_taxa()` — has to be proven to
recover the 693 species before the pin is removed. Removing the pin without that
proof would cut the species coverage by 87% and change every catch weight the
portal publishes.

Recorded, not fixed: all of §B and §C is Phase 4b scope. Phase 2 changed no
modelling behaviour.

---

## Addendum 2 — can Timor's weight/length path delegate to `coasts`? — 2026-07-31

Question raised by the user: use the new packages' way of getting weight and
length instead of Timor's own functions, for consistency and to remove
redundancy. Investigated against the frame base, `coasts/R/fishbase.R` and
`peskas.mozambique.data.pipeline/R/model-taxa.R`.

**Answer: partly — and more redundancy is removable than expected — but one
required capability does not exist in either package, and `coasts::enrich_taxa()`
has a hard blocker for Timor.**

### The frame's `taxa` table is the right input, confirmed

`taxa` (`tbliS8LrISaMft5Yg`) fields: `country`, `entity`, `form_version`,
`Form ID`, `survey_label`, `alpha3_code`, `scientific_name`, `english_name`,
`family`, `order`. 57 Timor-Leste rows, all `form_version: PeskAAS 2`,
`entity: MAF`.

Measured against the Google Sheets `catch_types`:

- `alpha3_code` ≡ `interagency_code`. The Airtable set is a **strict superset**:
  all 56 Sheets codes present, plus `LOX` (Lobsters nei / Reptantia). Sheets has
  61 rows / 60 non-NA codes / 56 distinct, i.e. duplicates.
- All 56 codes in `models.all_taxa` are covered.
- **`coasts::enrich_taxa()` already reads exactly this pair** —
  `dplyr::select("alpha3_code", "scientific_name")` off the assets snapshot.

Gaps in the Airtable rows, both non-blocking for Timor:

- `survey_label` is **empty for all 57 rows**, and `Form ID` is unlinked. Not
  blocking *here* because Timor's pipeline already stores `catch_taxon` as the
  interagency/alpha3 code (see `test_validated_landings.R`, which validates
  `catch_taxon` against `catch_types$interagency_code`), so the join key is the
  code, not the raw label. Would matter if Timor ever needed raw-label → standard
  mapping for taxa.
- 7 of 57 have no `family` (higher taxa: Reptantia, Brachyura, Rajiformes,
  Selachimorpha, Osteichthyes, Holothuroidea, Algae) — expected. 1 has no
  `english_name` (`BGX` / *Pomadasys spp*).

### What the new packages cover — genuine redundancy to remove

1. **Taxa input.** Replaces the `catch_types` + `fao_catch` Sheets join in
   `get_catch_types()`.
2. **Taxonomic rank/expansion.** `get_catch_types()` currently calls
   **`taxize::tax_rank(db = "gbif")`** to derive a rank, then
   `get_fish_length()` branches on it to call
   `rfishbase::species(Class=/Order=/Family=/Genus=/Species=)`.
   `coasts::expand_taxonomic_info()` does this off the FishBase/SeaLifeBase
   backbone instead — dropping an undeclared **GBIF** dependency from the middle
   of the fetch. Rank could also be derived structurally from the frame with zero
   external calls (`scientific_name == family` → family, `"X spp"` → genus, two
   words → species, `== order` → order).
3. **Nutrients.** `enrich_taxa()` already emits Calcium, Iron, Omega3, Protein,
   VitaminA, Zinc. Timor has its own `R/calculate-nutrients.R`. **This inverts a
   PLAN assumption:** PLAN §10 lists nutrients as a Timor→coasts *upstream*
   candidate, but coasts already has it. Phase 8 should compare the two and
   delete Timor's if they agree, not plan to upstream it.
4. **Length-weight `a`/`b`.** Moz's `get_length_weight_batch()` is the pattern.

### What is NOT covered — the blocker

- **Length-length conversion is absent from both packages.** `grep -rn
  "length_length"` over `coasts/R/` and `peskas.mozambique.data.pipeline/R/`
  returns **nothing**. Timor's `get_fish_length()` calls
  `rfishbase::length_length()` and emits `aL`, `bL`, `Length1`, `Length2`, which
  `get_morphometric_tables()` turns into the `ll` table (including the reciprocal
  relationship) and `estimate_weight()` uses to convert a recorded length type to
  the type the `a`/`b` are expressed in.
  Moz avoids needing it by filtering to **`Type == "TL"` only**
  (`database == "fishbase" & Type == "TL"`). Timor cannot: `catch_types` carries a
  per-taxon **`length_type`**, and `estimate_weight()` matches on
  `Length2 == length_type`.
- **`length_type` per taxon exists only in the Sheets `catch_types`**, not in the
  Airtable `taxa` table. So adopting the frame for taxa does **not** allow
  dropping `catch_types` outright, unless `length_type` is added to Airtable.
- `LengthMin` / `LengthMax` bounds, and Timor's curated `morphometric_table`
  (`manual_table`), have no equivalent.

### Two blockers inside `coasts::enrich_taxa()`

1. **It hardcodes FAO Area 57** — `dplyr::filter(.data$AreaCode %in% c(NA_integer_, 57))`,
   Western Indian Ocean. **Timor-Leste is FAO Area 71** (Western Central
   Pacific). As written, calling it for Timor would filter out its species. The
   area must become configurable — a `coasts` PR.
2. It writes the enriched table to `conf$storage$google$options` (country
   bucket), while `ingestion-pds.R` reads assets from the hub — the
   inconsistency PLAN's Phase 3 already flags.

### Leading hypothesis for the 693 → 88 collapse (not yet proven)

`get_fish_length()`'s expansion is gated entirely on the rank string returned by
`taxize::tax_rank(db = "gbif")`. If GBIF stopped resolving Timor's higher taxa
(families, tribes, infraorders), no `rfishbase::species(Family = ...)` expansion
happens and each code collapses toward a single exact match — which is what
56 codes → **88** species looks like, against 693 when family/genus expansion
works. It would also explain why the failure is silent rather than an error, and
why the rfishbase version is irrelevant (confirmed: the pinned-vs-unpinned
comparison in Addendum 1 showed no difference). **Testable** by running
`taxize::tax_rank()` over the 57 `scientific_name`s and counting non-NA
higher-rank returns. If it holds, moving to
`coasts::expand_taxonomic_info()` is not merely tidier — it is the fix.

### Sequencing consequence

PLAN has Phase 4b (adopt coasts helpers) *before* Phase 10 (upstream to coasts).
For weights the dependency runs the other way: coasts needs the FAO-area fix and
a length-length capability **before** Timor can delegate. Either land a small
`coasts` PR ahead of Phase 4b, or accept that Timor keeps `get_fish_length()`'s
length-length half while delegating the taxa/expansion/nutrients half.

None of this changes the immediate unblock: the pinned `rfish-table` object still
has to exist in `timor-dev` for any branch push to get past `calculate_weights()`.

---

## Addendum 3 — adopting the Moz/coasts weight path — 2026-08-02

User decision: adopt Mozambique's weight calculation, add a modern length
function, and **remove the `rfish-table` dependency entirely**. Scoped below;
the coasts-side work is now a standing deliverable in
[COASTS-TODO.md](COASTS-TODO.md).

**Measured, and it changes the design in one place**

- `catch_types$length_type` is `NA` for **56 of 61** rows. The only five values
  are invertebrate measures — `CL`, `CW`, `ML` (×2), `ShL`. The real driver is
  `survey_version`: **v1 → `FL`, v2/v3 → `TL`**
  ([calculate-weights.R:343-348](../../R/calculate-weights.R#L343-L348)), with a
  hardcoded override forcing `OCZ`/`SLV`/`IAX`/`MOO` to `TL`.
  → **Length-length is load-bearing for every v1 catch row**, and coasts has no
  length-length. This is the one hard blocker (COASTS-TODO C3).
- Coefficient `Type` mix in the good table: 2,684 `TL`, 1,829 `FL`, 617 `SL`,
  plus `NG`/`OT`/`PC`/`WD`. Moz's `Type == "TL"` filter is therefore **not**
  safe to copy: no taxon loses everything, but `CJX` 10→3, `EMP` 25→12,
  `MOB` 9→4, `YDX` 11→4 species.
- **Coverage gain, not just dedup:** `rfishbase::species()` is FishBase-only, so
  the pinned table has **zero `a`/`b` for 11 of 56 codes** — `COZ`, `CRA`,
  `CUX`, `IAX`, `OCZ`, `PEZ`, `SLV`, `SWX` (invertebrates/algae) plus `FLY`,
  `LGE`, `MZZ`. Moz/coasts query SeaLifeBase too, so delegating should *add*
  coverage. `LOX` is absent from the table entirely (Airtable-only code).

**Verdict**

Adopt Moz's fetch (dual-server, `alpha3_code` → species → `a`/`b`), keep
Timor's length-length half until coasts gains it, and do not copy the TL-only
filter. `rfish-table` can be removed completely once COASTS-TODO C1–C3 land —
until then the pinned object remains the only source of correct weights.

**Timor-side decisions still open** (Phase 4b)

1. `morphometric_table` (the Sheets `manual_table` merged in
   `get_morphometric_tables()`) — Timor-curated; keep locally or upstream?
2. `length_type` for the 5 invertebrate cases lives only in Sheets
   `catch_types`. Either add a `length_type` field to the Airtable `taxa` table
   or keep that one column.
3. `estimate_weight()`'s per-taxon `quantile_coeff` special cases (e.g.
   `FLY ~ 0.75`) are Timor-specific and must survive any rewrite.
4. `LOX` exists in Airtable but not in the Sheets — decide whether to admit it
   as a 57th taxon.
5. Acceptance gate before the pin is removed: 693 species / 5,926 rows /
   ≥45 codes with coefficients / mean `a` 0.02716 / mean `b` 2.9781, then a
   total-catch-weight diff against the Phase 0 golden snapshot.

---

## Addendum 4 — coasts 4.6.0 adopted, frame re-checked — 2026-08-09

**Done on `feat/align-coasts-phase2` (still uncommitted, now ~60 files)**

Coasts pin dropped, aligned to the Mozambique pattern:

- `DESCRIPTION`: `github::WorldFishCenter/peskas.coasts`, no `@tag`.
- `Dockerfile` + `Dockerfile.prod`: `ARG COASTS_REF` with **no default** plus the
  `test -n "$COASTS_REF"` guard, so a build always records which hub release it
  used.
- `.github/workflows/data-pipeline.yaml`: new "Resolve latest peskas.coasts
  release" step (`gh api .../releases/latest --jq .tag_name`) feeding
  `build-args: COASTS_REF=`.
- `docker-compose.yaml`: passes `COASTS_REF` (default `v4.6.0`, env-overridable)
  so the local RStudio flow stays one command.

This closes PLAN's **Phase 11 "unpin coasts"** and the **Phase 2** bullet "add
the Resolve peskas.coasts release step", both early. The Phase 1 rationale for
pinning ("coasts ships a breaking change mid-migration") does not apply when the
same team ships coasts and the release was built backwards-compatible.

Deleted, now provided by coasts 4.6.0:

- `insistent_upload_cloud_file()` / `insistent_download_cloud_file()` — two call
  sites (`ingest-pds-data.R:185`, `preprocess-pds-trips.R:253`) now
  `coasts::insistent_upload_cloud_file()`. **Behaviour change:** Timor's
  wrappers ran `Sys.sleep(delay = 3)` after every upload; coasts' do not. That
  was an unconditional throttle, not retry, and coasts' exponential backoff
  covers the failure it guarded. ~3s saved per newly-uploaded PDS track. Revert
  at the call site if it was guarding an unrecorded rate limit.
- `get_public_files()` / `get_tracks_map()` now use
  `coasts::resolve_storage_opts(pars, "public")` (verified: resolves
  `public-timor-dev`). The other `public_storage` reads in `export.R`,
  `format-public-data.R` and `ingest-pds-data.R` are the export path and were
  left alone.
- `_pkgdown.yml`: dropped `matches("cloud")`, which selected nothing after the
  deletion and would have failed the pkgdown build.

`devtools::check()` unchanged from the Phase 2 baseline: 1 WARNING, 5 NOTEs,
unused-Imports NOTE still `arrow` only.

**PESKAS | FRAME re-measured — two open items closed**

The user populated Timor's rows. Measured 2026-08-09 with the `.env` PAT (the
MCP Airtable connector now 403s on `appMMEJYlJdfSJEjm` and sees only a different
base — use the PAT route, as coasts does):

| table | Timor rows | note |
|---|---|---|
| `taxa` | **60** rows / **56** distinct `alpha3_code` | `survey_label` **60/60** |
| `gears` | **9** (was 7) | `survey_label` 9/9 |
| `vessels` | 2 | `survey_label` 2/2 |
| `landing_sites` | 40 | all 40 carry lat/lon |

- **`survey_label` is now fully populated.** STATE Addendum 2 recorded it empty
  for all 57 taxa rows and flagged it as a Phase 4 risk — Timor could only join
  on `alpha3_code`. That risk is gone; raw v2/v3 form labels can now be mapped.
  60 rows over 56 codes, every row tagged with **both** `peskAAS` and
  `PeskAAS 2`, i.e. the extra rows are alternate raw labels for the same code —
  exactly the shape a label-mapping table should have.
- **`LOX` is gone.** The codes now match `models$all_taxa` **exactly** — zero
  difference in either direction. The Addendum 3 open question "admit LOX as a
  57th taxon?" is moot.
- `landing_sites` link `Country` by record id (`rec7DrrSnRrlzv8BF`), not by
  name — a name grep returns 0 and looks like missing data.

**Next, in order**

1. Review and commit Phase 2 (blocking everything else).
2. Push → dev run. Exercises the coasts-latest resolution and the storage
   delegation together.
3. Copy `rfish-table__20241013011806_a41d5ff__.rds` from `gs://timor` to
   `gs://timor-dev` — still the only thing letting a dev run clear
   `calculate_weights()` before Phase 4b removes the pin.
4. Phase 3 (ingestion + `ingest_assets()`), now better supported than when
   planned: the frame is complete and label-joinable.

---

## Phase 3 — Ingestion — 2026-08-09

Branch: `feat/align-coasts-phase3` (off `feat/align-coasts-phase2` at `7902012`;
**uncommitted**, following the precedent of Phases 0–2 — the user reviews before
it lands)

**Done**

*1. `R/ingestion.R` — new*

- `ingest_landings(versions = c("v2", "v3"))`: `coasts::get_kobo_data()` →
  `flatten_row()` → `coasts::upload_parquet_to_cloud(prefix =
  conf$surveys$landings$<v>$raw$file_prefix)`. One function, one job, replacing
  `ingest_landings_v1v3()` + `ingest_landings_v2()`. v1 is not fetched.
- `ingest_assets()`: delegates to `coasts::ingest_assets(package =
  "peskas.timor.data.pipeline")`.
- `flatten_row()` / `flatten_field()` / `rename_child()` moved here verbatim
  from the deleted `R/retrieve-survey-data.R`. Not exported.
- `get_raw_landings(pars, version)`, internal: the parquet reader, plus the
  compatibility coercion described below.

Deleted: `R/ingest-landings.R`, `R/retrieve-survey-data.R` (Timor's own
`get_kobo_data()`, 233 lines, now `coasts::get_kobo_data()`),
`preprocess_legacy_landings()` and `clean_legacy_landings()` (both dead once v1
is frozen).

*2. Raw storage is parquet, and it is provably the same table*

The three preprocessing entry points now call `get_raw_landings()` instead of
`cloud_object_name()` + `download_cloud_file()` + `read_csv()`.

Parquet keeps the JSON types; preprocessing was written against
`read_csv(col_types = cols(.default = col_character()))` and every downstream
comparison assumes character. So `get_raw_landings()` coerces to character —
and, less obviously, has to reproduce two things the CSV round-trip was doing
silently: **trimming surrounding spaces and tabs**, and mapping **`""` and
`"NA"` to `NA`**. Without them 13 of v2's 311 columns differ.

Measured, old CSV read vs new parquet read, on every submission of both live
forms (v2 64,997 × 311, v3 22,214 × 275, all ids common):

| | differing columns | differing cells |
|---|---|---|
| naive `as.character()` | 13 | thousands |
| + readr's trim and NA rules | **4–5** | **13** |

The 13 residual cells are all the parquet being *closer* to the source JSON:
9 `_geolocation1/2` values rendered at shortest round-trip precision instead of
readr's 17 digits (`-8.1564565` vs `-8.1564565000000009`), and 4 free-text
cells where a space sitting immediately before a trailing newline is trimmed.
`trimws()`'s whitespace class is pinned to `[ \t]` **deliberately**: its default
would also strip the stray leading/trailing newlines ~60 free-text answers
carry. That is an improvement, but making it here would put a text change
inside a format phase and muddy Phase 4's golden diffs. Left for Phase 4.

*3. v1 frozen, in total length*

`data-raw/freeze-landings-v1.R` (one-off, `.Rbuildignore`d) reads the last
`timor-landings-v1_preprocessed__*` and writes
`timor-landings-v1-frozen__*`, which `merge_landings()` now reads.

The substantive part is the length conversion. **v1 records fork length**,
v2/v3 record total length, and `join_weights()` was carrying
`summarise_ll_coeffs()` + `normalise_length_to_tl()` — plus a FishBase
length-length round-trip — every single run, to convert 10,117 rows of a source
that has had no submissions since 2020-08-28. Converted once, at the freeze.
`join_weights()`'s `length_type` `case_when` collapses to "TL unless
`catch_types` says otherwise"; the two functions are now dead and are deleted
in Phase 4 as planned.

Two design points worth keeping:

- The conversion is applied **in place**, `purrr::map()` over `species_group`,
  not through `unnest()`/`nest()`. A round trip turns the empty
  `species_group` tibbles of no-catch submissions into one all-NA row — a
  schema change, not a freeze.
- `species` stays the **raw form number**, not the taxon code. `join_weights()`
  maps number → taxon for every survey version at weight time; a frozen file
  carrying codes would silently fail that join.

The frozen object is `.rds`, not the parquet PLAN specifies. Arrow round-trips
the doubly-nested list-columns correctly (verified: same `unnest()` row counts),
but returns them as `vctrs_list_of`, and `dplyr::bind_rows()` refuses to combine
that with the plain lists coming out of the v2/v3 `.rds` files —
`merge_landings()` errors on `_attachments`. Coercing back is three lines of
fiddly recursion for a file that Phase 4 flattens to long parquet anyway.

*4. `get_taxa_list()` moved to the assets snapshot*

`R/calculate-weights.R` no longer joins the Google Sheets `catch_types` +
`fao_catch`. New `get_assets()` accessor reads the snapshot from the hub;
new `timor_assets()` narrows it to Timor.

**The snapshot has no `country` column** — coasts' `ingest_assets()` does not
select one — and it is cross-country: 1,609 taxa rows over four countries, with
**all 56** of Timor's `alpha3_code`s also used elsewhere, two of them (`MZZ`,
`PWT`) against a different `scientific_name`. Filtering by code alone would mix
another country's taxonomy into the coefficient fetch. The only discriminating
column left in the snapshot is `form_id`, so `metadata.airtable.form_ids`
carries the record ids of Timor's two forms (`recY5MD03ZDwJUBB3` = PeskAAS 2,
`rechg17V73uqnVu2T` = peskAAS). Hardcoded Airtable record ids in config is a
brittle key; filed upstream as COASTS-TODO **C13**.

*5. CI*

- `KOBO_ASSET_ID_V{1,2,3}` added to the `env:` block, fed from the existing
  `KOBO_PESKAS{1,2,3}` secrets (the Phase 1/2 deferral, now closed). Secret
  renames stay in Phase 9.
- `ingest_assets()` added as a third step of `ingest-preprocess-metadata-tables`.
- Landings jobs restructured. One `ingest-landings` job now feeds both
  preprocessing branches, instead of each branch re-fetching from KoBo:

```
build-container
├── ingest-preprocess-metadata-tables  … → ingest_assets
├── ingest-landings                    ingest_landings          [v2 + v3 parquet]
│   ├── preprocess-v3-landings         preprocess_updated_landings
│   └── ingest-preprocess-v2-landings-step1 → step2
```

*6. A live secrets leak, found and fixed*

`read_config()` ended with `logger::log_debug("Running with parameters
{pars}")`. Every workflow function defaults to `log_threshold = logger::DEBUG`,
so **every CI job has been printing the full GCP service-account private key,
the Airtable PAT, the Dataverse token and the blastula Gmail credentials into
its log**. GitHub Actions masks only byte-exact matches of a registered secret,
and the re-serialised JSON (escaped quotes, `\\n` newlines) is not one.
Pre-existing, not introduced by the migration; found because Phase 3 exercises
`read_config()` on a fresh path. Now logs key names only.

**Rotation is a user decision, not done here.** The exposed values are in the
logs of every past run of `data-pipeline.yaml`. Recommend rotating the
service-account key, the Airtable PAT and the Dataverse token, and deleting old
workflow run logs.

**Verified**

*Assets snapshot* — every target from the brief, measured off the object
written to `peskas-coasts-dev`:

| check | target | measured |
|---|---|---|
| taxa rows / distinct `alpha3_code` | 60 / 56 | **60 / 56** ✅ |
| gears | 9 | **9** ✅ |
| vessels | 2 | **2** ✅ |
| landing sites | 40 | **40** ✅ |
| `survey_label` populated | all | 60/60, 9/9, 2/2 ✅ |
| codes vs `models$all_taxa` | zero diff either way | **zero, both ways** ✅ |
| `get_taxa_list()` | 56 | **56 rows / 56 codes** ✅ |

Snapshot totals (cross-country): geo 92, taxa 1,609, gear 96, vessels 49, sites
736, forms 14, devices 910, frame 407.

*Raw parquet*

| | rows | cols | baseline |
|---|---|---|---|
| `timor-landings-v2_raw__*.parquet` | **64,997** | 311 | = AUDIT §1's 64,997 exactly |
| `timor-landings-v3_raw__*.parquet` | **22,214** | 275 | ≥ 22,037 (+177 since 2026-07-31) |

Schema: one column per flattened KoBo field, types as JSON delivered them —
mostly character, with integer `_id`/`_status`-family fields, doubles for
`_geolocation*` and the numeric answers, logicals where KoBo sends booleans.
`arrow::write_parquet(compression = "lz4", compression_level = 12)` via
`coasts::upload_parquet_to_cloud()`: 8.6 Mb for v2, 3.8 Mb for v3.

The 22,214 warnings from `flatten_field()` ("Outer names are only allowed for
unnamed scalar atomic inputs", one per submission) are **pre-existing** — the
helper is byte-identical to the deleted one, which raised them inside
`get_kobo_data()`.

*The freeze*

- v1 preprocessed: **10,117 rows** (asserted). Against AUDIT §1's 12,017 KoBo
  submissions, the 1,900-row gap is `clean_legacy_landings()` dropping the
  older Kg-recording form variant. That accounts for the **whole** of the
  1,900-row "99,051 submissions vs 97,151 merged rows" gap Phase 0 deferred —
  it is all v1, and it is deliberate.
- 140,895 catch rows, 88,764 lengths converted (63.6% of measured), mean length
  34.800 → 36.776 cm.
- **Bit-identical to the old per-run conversion.** Replaying the old code path
  (`survey_version == "v1"` → FL → `normalise_length_to_tl()`) against the
  golden merged snapshot and comparing to the frozen lengths, over the 139,596
  catch rows carrying a length: **max absolute difference 0**, weight delta
  **0.000000%**.

*End-to-end against `timor-dev`*

`ingest_landings()` → `preprocess_updated_landings()` → `merge_landings()` →
`calculate_weights()` all ran. Merged **97,328 × 26** — golden 97,151 plus
exactly the 177 new v3 submissions. Weight artefact 97,328 × 61, 1,751,969
catch rows, 5,196,740 kg.

Compared submission-by-submission against the previous dev run (`7902012`,
which already carried the Phase 4b weight rewrite):

| survey_version | submissions | kg before | kg after | delta |
|---|---|---|---|---|
| v1 | 10,117 | 64,089.1 | 64,089.1 | 0 |
| v2 | 64,997 | 4,819,710.8 | 4,819,710.8 | 0 |
| v3 | 22,214 | 312,940.2 | 312,940.2 | 0 |
| **all** | **97,328** | **5,196,740** | **5,196,740** | **0.000000%** |

Submissions differing by more than 1e-6 kg: **0**. Phase 3 is numerically inert,
which is the right result for a plumbing phase.

*tinytest, against `timor-dev`* — identical to the Phase 2 baseline:

| suite | result |
|---|---|
| `test_merged_trips.R` | all ok, 2/2 |
| `test_validated_pds_trips.R` | all ok, 7/7 |
| `test_public_data.R` | all ok, 1/1 |
| `test_validated_landings.R` | 2 fails / 8 passes — the same two pre-existing assertions (`<33--36>` landing dates prior to 2017, `<49--53>` `catch_purpose` is NULL) |

No assertion was touched.

*`devtools::check()`* — **0 errors, 0 WARNINGs, 4 NOTEs**, against a baseline of
1 WARNING / 5 NOTEs. Both reductions are real and explained:

- The WARNING was "undocumented `get_kobo_data()` arguments". That function is
  deleted.
- The unused-Imports NOTE is **gone**. It would have named `arrow` **and**
  `httr2` after this phase — parquet I/O and KoBo retrieval both live in
  `coasts` now, and no `R/` file references either namespace. Both were dropped
  from `Imports`; the Dockerfiles still install them, and `coasts` needs them.
  **The brief expected `arrow` to clear "when raw ingestion writes parquet" —
  it does not work that way.** Parquet *is* being written; the NOTE is about
  Timor's own namespace usage, and Timor never touches `arrow::` because
  `coasts::upload_parquet_to_cloud()` does. Note this also retires the
  "canary": if the coasts delegation were undone, `coasts` would reappear in a
  restored NOTE.
- testthat unchanged: `FAIL 1 | WARN 9 | SKIP 0 | PASS 9`, the pre-existing
  `test-pre-process-landings.R:16`.

**Deferred, with reasons**

- **The production freeze has not been run.** `timor-landings-v1-frozen` exists
  in `timor-dev` only. `R_CONFIG_ACTIVE=production Rscript
  data-raw/freeze-landings-v1.R` must run before `main` ever executes the new
  `merge_landings()` — i.e. before the Phase 11 cutover. Deliberately not run
  now: it is a write to the live bucket for a branch that is not merged.
- **v2 preprocessing was not run locally.** `preprocess_landings_step_1/2` are
  split across two containers precisely because 65k rows do not fit in one, and
  the reader they use was proven byte-equivalent on all 64,997 v2 submissions,
  so a local re-run would re-prove nothing. It is exercised by the first branch
  push.
- `validate_landings()` was **not** run: it writes to the live
  `VALID_SHEET_ID` Google Sheet, which has no dev twin. That is the Phase 5
  sink decision; running it from a phase branch would mutate live validation
  state.
- COASTS-TODO **C11** worked around rather than fixed upstream, as the brief
  allows. `coasts::ingest_assets()` uploads to the country bucket while every
  reader resolves the hub; Timor's wrapper re-uploads the file coasts leaves on
  disk. **Why not upstream:** the fix lives in a different repository, and
  Timor's container resolves `peskas.coasts` from the *latest release*, so an
  unreleased commit would not reach CI. Cost of the workaround: one extra
  583 Kb upload per run.
- `metadata.google_sheets.tables` still lists all 15 tables. Only the taxa join
  moved this phase; the rest move in Phase 4, and `catch_types` has to stay
  regardless until the frame gains `length_type` (COASTS-TODO C14).
- Legacy config keys (`surveys.landings_{1,2,3}`, `metadata.rfishtable`, the
  `version: preprocess:` fields) untouched — Phase 11 owns them.
  `surveys.landings.v1.{raw,preprocessed}` are kept as the provenance of the
  frozen snapshot; nothing writes them.
- `ANTHROPIC_API_KEY` rotation still open (user action, AUDIT §10).

**Findings that change later phases**

1. **COASTS-TODO gained C13 and C14** (no `country` column on the assets
   snapshot; no `length_type` on frame `taxa`). C13 is the more urgent: every
   country-specific read of the snapshot in Phases 4 and 6 needs the same
   brittle `form_ids` filter until it lands. The snapshot also drops
   `landing_sites` lat/lon, which Phase 4 site harmonization would want.
2. `_pkgdown.yml` still listed `retrieve_lengths` (deleted by the weight
   rewrite in `a2c2881`) and `alert_outlier` (never exported). Both would fail
   the pkgdown build; removed. Same failure mode as the Phase 1 `matches("air")`
   and Phase 2 `matches("google")` entries — **check `_pkgdown.yml` after every
   deletion.**
3. The stray leading/trailing newlines in ~60 free-text answers are real data,
   preserved by the old CSV path and still preserved now. Phase 4 should strip
   them deliberately, in the reshaping code, not as a side effect of a reader.
4. `devtools::load_all()` **breaks `coasts::read_config(package = "...")`**:
   pkgload registers the package at its source directory, so coasts' own
   `system.file("config.yml", package = "peskas.timor.data.pipeline")` resolves
   to the repo root instead of `inst/` and returns "". Any local test of a
   delegated coasts workflow function must use an installed package
   (`devtools::install()` + `library()`), not `load_all()`. Timor's own
   `read_config()` is unaffected — pkgload shims `system.file` inside the
   package's own namespace.

**Open questions for the next session**

1. None blocking. Phase 4 can start from the raw parquet.
2. Decide in Phase 4 whether `timor_assets()` stays or the `form_ids` config
   key is replaced by a `country` filter, depending on whether COASTS-TODO C13
   has shipped.
3. The secrets rotation above is a user decision and is not a Phase 4 task.

**Files added / removed / renamed**

- added: `R/ingestion.R`, `data-raw/freeze-landings-v1.R`,
  `man/ingest_landings.Rd`, `man/ingest_assets.Rd`, `man/get_assets.Rd`,
  `man/timor_assets.Rd`
- removed: `R/ingest-landings.R`, `R/retrieve-survey-data.R`,
  `preprocess_legacy_landings()` and `clean_legacy_landings()` (from
  `R/preprocess-landings.R` and `R/clean-raw-data.R`);
  `man/{get_kobo_data,ingest_landings_v1v3,ingest_landings_v2,preprocess_legacy_landings,clean_legacy_landings}.Rd`;
  5 NAMESPACE exports, 4 added
- modified: `R/preprocess-landings.R`, `R/merge-landings.R`,
  `R/calculate-weights.R`, `R/get-cloud-files.R`, `R/clean-raw-data.R`,
  `R/utils.R`, `NAMESPACE`, `DESCRIPTION` (−`arrow`, −`httr2`),
  `inst/config.yml` (`surveys.landings.v1.frozen`,
  `metadata.airtable.form_ids`), `.github/workflows/data-pipeline.yaml`,
  `_pkgdown.yml`, `.Rbuildignore` (`^data-raw$`), `CLAUDE.md`,
  `.claude/migration/COASTS-TODO.md`, `.claude/migration/STATE.md`
- **unchanged: `inst/tinytest/`, `tests/`, the export path, every
  `portal-*.json` object name, and every serialization format downstream of
  preprocessing.**

**Cloud objects written (all `-dev`)**

- `peskas-coasts-dev/assets__20260809190738__.rds` (583 Kb)
- `timor-dev/assets__20260809190738__.rds` (the coasts-side copy, C11)
- `timor-dev/timor-landings-v2_raw__20260809191552__.parquet` (8.6 Mb)
- `timor-dev/timor-landings-v3_raw__20260809191650__.parquet` (3.8 Mb)
- `timor-dev/timor-landings-v1-frozen__20260809191031_7902012__.rds` (1 Mb)
- `timor-dev/timor-landings-v3_preprocessed__20260809192511__.rds`
- `timor-dev/timor-landings-merged__20260809192619__.rds`
- `timor-dev/timor-landings-merged_weight__20260809193101__.rds`

### Phase 3 follow-up — same session, after review — 2026-08-10

Three points raised on the Phase 3 work. All three are settled; the pipeline's
numbers are still unchanged.

**1. `conf`, not `pars`.** The other country pipelines name the resolved
configuration `conf` (Mozambique: 188 `conf` vs 4 `pars`); Timor used `pars`
everywhere. Renamed: **461 occurrences** across all 21 `R/` files, the four
`inst/tinytest/` suites, `data-raw/`, and the `conf$...` references in
`inst/config.yml`, `inst/config_template.yml` and `CLAUDE.md`. It reaches the
argument names of the exported accessors (`get_models(conf)`,
`get_preprocessed_sheets(conf)`, …); every call site in the repo passes
positionally except five internal `pars = pars` pairs, all updated. Verified: no
`\bpars\b` left in `R/`, `inst/`, `man/` or `data-raw/`; `load_all()` clean;
`check()` and all four tinytest suites unchanged.

**2. `length_type` is not a survey field — measured.** The claim that it comes
from the survey does not hold, and checking it turned up something better.

- Neither live form has such a field. v2 has 311 columns, v3 has 275; the only
  length data an enumerator records is a **count per length bin**
  (`no_fish_by_length_group/no_individuals_10_15` …, plus a free
  `fish_length_over60`). `pt_nest_species()` turns the bin into `mean_length`
  by taking its midpoint. There is no per-record measurement type and never was.
- `length_type` comes from the Google Sheets `catch_types` table, **per taxon**,
  non-`NA` for 5 of 61 rows: `SLV` CL, `OCZ` ML, `IAX` ML, `CRA` CW, `COZ` ShL.
- It affects **nothing**, and cannot: `get_length_length_coeffs()` returns
  `Length1`/`Length2` in `TL`/`FL` only (142 rows), so a CL/CW/ML/ShL row joins
  nothing in `normalise_length_to_tl()` and falls through unchanged — and
  `join_weights()` separately overrode `OCZ`/`SLV`/`IAX`/`MOO` to `"TL"`,
  discarding three of the five values outright. Post-freeze, with no FL left
  anywhere, `length_tl == mean_length` for **every** row.

Where it *could* bite is one step earlier, in coefficient selection.
`summarise_lw_coeffs()` collapses coefficients per taxon by geometric mean of
`a` across every `Type`, mixing measurement axes that are not commensurable:

| taxon | axis mix | g @ 20 cm now | axis-filtered | ratio |
|---|---|---|---|---|
| `SLV` lobster | 34 CL + 19 TL + 2 BL | 1,643 | 4,292 | ×2.61 |
| `OCZ` octopus | 27 ML + 4 TL | 2,393 | 3,394 | ×1.42 |
| `IAX` cuttlefish | 49 ML + 2 TL | 689 | 687 | ×1.00 |
| `COZ` cockle | 26 ShL + 6 ShH | — | no TL rows | — |
| `PEZ` shrimp | 229 TL + 123 CL + 22 BL | — | not declared | — |

Those taxa are 1.45% of catch weight (`OCZ` alone 1.26%), so filtering each
declaring taxon to its own axis would move **national catch weight +0.63%**.

**Decision (user, 2026-08-10): the enumerators measure octopus, lobster and
cuttlefish on total length.** The `catch_types` column describes an intent the
field does not follow, and the hardcoded `→ TL` overrides were right all along.
So no coefficient filter, and the dead machinery goes:

- **Deleted** `summarise_ll_coeffs()` and `normalise_length_to_tl()` — planned
  for Phase 4, brought forward because they are now provably unreachable.
- `estimate_weight()` reads `mean_length` directly; `length_tl` is gone.
- `get_morphometric_tables()` returns `list(expanded, length_weight)`; it no
  longer summarises the length-length table.
- `length_type` is **kept** in the nested `species_group` output, unchanged in
  value, and documented as descriptive-only. Dropping it would change the
  weight artefact's schema for no gain.
- The length-length logic moved **into `data-raw/freeze-landings-v1.R`**, which
  is now its only consumer — it still needs it to produce the production
  snapshot. Re-ran the script standalone: 40 convertible taxa, 88,764
  conversions, mean 34.800 → 36.776 cm, identical to the first run.

Note this is a **deletion, not a simplification of intent**: if field practice
ever changes, the fix is a `Type` filter in `summarise_lw_coeffs()`, not a
resurrection of the length-length path. COASTS-TODO **C14 is withdrawn** —
adding `length_type` to the frame would propagate a wrong assertion to every
country.

**3. The v1 fork-length contradiction, resolved.** `calculate-weights.R`'s
roxygen said "total length (TL) in survey version 1 and fork length (FL) in
survey version 2"; the code said the exact opposite (`survey_version == "v1" ~
"FL"`), and the freeze baked in the code's version. The docs also claimed an
`SRX` → disk-width rule that was never implemented. **Decision (user): the code
is right, v1 is fork length.** The freeze stands as run; the prose is corrected
and the discrepancy recorded in a `@section Length types:` block so it cannot
be re-introduced.

**Verified after all of the above**

- `data-raw/freeze-landings-v1.R` standalone: identical output to the first run.
- `merge_landings()` + `calculate_weights()` re-run end to end against
  `timor-dev`: merged 97,328 × 26, weight 97,328 × 61, 1,751,969 catch rows,
  5,196,740 kg.
- Diffed against the pre-deletion artefact submission by submission:
  **0.000000% total delta, 0 submissions differing by more than 1e-6 kg**, per
  survey version as well. Deleting the length-length path is a proven no-op.
- `devtools::check()`: **0 errors, 0 WARNINGs, 4 NOTEs**; testthat
  `FAIL 1 | WARN 9 | SKIP 0 | PASS 9` (the pre-existing one).
- tinytest unchanged: `test_merged_trips` 2/2, `test_validated_pds_trips` 7/7,
  `test_public_data` 1/1, `test_validated_landings` 2 fails / 8 passes.

**Files touched by the follow-up**

- modified: all 21 `R/*.R` (rename), `inst/tinytest/*.R` (4, rename),
  `data-raw/freeze-landings-v1.R` (rename + the length-length logic moved in),
  `R/calculate-weights.R` (two functions deleted, `estimate_weight()` and
  `join_weights()` simplified, roxygen corrected), `inst/config.yml`,
  `inst/config_template.yml`, `CLAUDE.md`, `.claude/migration/COASTS-TODO.md`
  (C14 withdrawn), `man/*.Rd` (regenerated)
---

## Phase 4 — Preprocessing — 2026-08-10

Branch: `feat/align-coasts-phase4` (off `feat/align-coasts-phase3` at `0e8ab28`)

**Done**

*1. `R/survey-reshaping.R` + `R/preprocessing-surveys.R` — new*

- `reshape_species_groups()` — the `species_group.<n>.species_group/<field>`
  blocks to one row per (submission, catch), empty catch slots dropped,
  `n_catch` 1-based as in the other pipelines.
- `expand_length_frequency()` — one row per 5 cm length bin, with the over-60
  bin taking the recorded `fish_length_over60` measurement in place of the
  60 cm midpoint. `bin_midpoint()` keeps the old regex verbatim so no midpoint
  can move; it is vectorised over the distinct bin names instead of per row.
- `trim_free_text()` — the deliberate newline strip Phase 3 deferred.
- `preprocess_landings(versions = c("v2", "v3"))` — **one** workflow function
  replacing `preprocess_updated_landings()` + `preprocess_landings_step_1/2()`,
  writing `timor-landings-v{2,3}_preprocessed__*.parquet`.
- `merge_landings()` moved here and reduced to a bind: per-version column
  reconciliation now happens in `harmonise_v2()` / `harmonise_v3()`, so
  `merge_versions()` is gone.
- `survey_labels()` / `resolve_catch_taxa()` / `resolve_survey_labels()` — the
  label joins, off the assets snapshot.

Deleted: `R/clean-raw-data.R`, `R/preprocess-landings.R`, `R/pt_nest_species.R`,
`R/pt_nest_attachments.R`, `R/merge-landings.R`, `get_raw_landings()`.
`R/calculate-weights.R` → `R/model-taxa.R` (the 4b rename).

*2. The interchange format is flat long parquet through the merge*

One row per (submission, catch, length bin). The table is a **superset**: the
standard columns sit beside every raw KoBo column, reconciled per form version.
The raw ones are what the 19 validators read and they go in Phase 5.

`join_weights()` is now the whole bridge to the old format: it renames the catch
columns back, re-nests `species_group` / `length_individuals`, and **drops the
18 standard submission columns again**. That last part is not cosmetic — carried
through, they add 18 grouping columns to the three `unnest()`/`nest()` cycles
inside validation for no benefit, since Phase 5 will read them off the merged
table instead. The weight artefact is therefore the old artefact minus
`_attachments`.

*3. `_attachments` dropped*

`pt_nest_attachments()` built a list-column whose only consumer,
`inst/report/unanswered_summary.Rmd`, immediately `select(-\`_attachments\`)`s it.
Nothing else in `R/`, `inst/` or the portal path reads it. Dropped, and the
report now uses `any_of()`.

*4. Labels move to the PESKAS | FRAME assets snapshot*

`survey_labels()` reads taxa, gear, vessels, landing sites and geo from the
snapshot (always through `timor_assets()`) and habitat plus the per-taxon
`length_type` from the Sheets, which have no frame equivalent. The taxon
resolution — including the `MZZ` / `0` rules and the
`OCZ`/`SLV`/`IAX`/`MOO` → `TL` overrides — moved out of `join_weights()` into
`resolve_catch_taxa()`, so the weight path no longer touches `catch_types` for
taxa at all.

Measured against the Sheets tables it replaces: the frame's 60 `survey_label` →
`alpha3_code` rows are **identical** to `catch_types` for all 60 shared labels
(the only difference is the `0` "no catch" sentinel, which the frame correctly
does not carry); all 9 gear codes, both vessel codes and all 40 station codes
used by the live forms are covered.

*5. Metadata tables trimmed by one, and annotated*

`fao_catch` dropped from `metadata.google_sheets.tables` and from
`preprocess_metadata_tables()` — Phase 3 orphaned it when `get_taxa_list()`
moved to the snapshot and it has had no reader since. `pt_validate_fao_catch()`
went with it.

**The brief asked for a trim to six tables and that is not yet possible.** The
seven it would remove (`devices`, `vessel_types`, `gear_types`, `stations`,
`reporting_units`, `boats`, `vms_installs`) are read by the validators and by
`data_report.Rmd`, not by preprocessing — preprocessing never joined a Sheets
table. The frame equivalents are now resolved in `preprocess_landings()`, so the
validators can move onto them in Phase 5 and the tables go then. Every entry in
the config list is annotated with the phase that removes it.

*6. v1's flattening lives in the freeze script*

`data-raw/freeze-landings-v1.R` now also reconciles v1's columns and flattens the
snapshot to the long shape, writing **parquet**. Deliberate: the form is dead and
`preprocess_landings()` should not carry a shape nothing will produce again. The
`.rds`-vs-parquet reason recorded in Phase 3 (arrow returns list-columns as
`vctrs_list_of`) disappears with the list-columns.

*7. CI*

The three preprocessing jobs collapse to one `preprocess-landings`, which now
also needs `ingest-preprocess-metadata-tables` (it reads the assets snapshot and
the Sheets). The v2 split into halves existed because 65k submissions with nested
list-columns did not fit one container; the long table costs less, and both
versions now run in one job in **1.1 minutes**.

**Verified**

*Reshaping, cell by cell against the Phase 3 weight artefact*

Every catch column of both live forms, compared row for row after sorting:

| | catch rows compared | differing cells |
|---|---|---|
| v2 | 1,131,090 | **0** in all of `catch_taxon`, `catch_use`, `length_type`, `length`, `n_individuals`, `photo`, `other_species_name`, `n_catch` |
| v3 | 374,648 | **0**, same columns |

v3 excludes the 627 no-catch submissions, which are a deliberate change — see
the deltas below.

*Submission columns*

Column for column against the Phase 3 merged artefact, the only differences are
the two intended ones: whitespace trimming (19 v2 cells, 35 v3 cells, all
verified pure trims) and 289 + 9 `reason_no_activity` values that were `NA`
before.

*End to end against `timor-dev`*

| artefact | rows | baseline |
|---|---|---|
| `timor-landings-v1-frozen__*.parquet` | 10,117 submissions / 140,895 catch rows | = Phase 3's 140,895 |
| `timor-landings-v2_preprocessed__*.parquet` | 64,997 / 1,131,090 | — |
| `timor-landings-v3_preprocessed__*.parquet` | 22,214 / 375,275 | — |
| `timor-landings-merged__*.parquet` | 97,328 / 1,647,260 | = 97,328 submissions |
| `timor-landings-merged_weight__*.rds` | 97,328 × 60 | was 97,328 × 61 |

| survey_version | kg before | kg after | delta |
|---|---|---|---|
| v1 | 64,089.1 | 64,089.1 | 0 |
| v2 | 4,819,710.8 | 4,819,710.8 | 0 |
| v3 | 312,940.2 | 312,940.2 | 0 |
| **all** | **5,196,740** | **5,196,740** | **0.000000%** |

Submissions differing by more than 1e-6 kg: **0** of 97,328. The freeze
reproduced its Phase 3 numbers exactly (40 convertible taxa, 88,764 conversions,
mean length 34.800 → 36.776 cm).

**Deltas, all explained**

1. **−104,709 catch rows (−6.0%)**, and `catch_taxon == "0"` 178,642 → 73,933.
   `preprocess_updated_landings()` applied `catch_outcome == "0" ~ "0"` across
   *every* `species_group/species` column, including the empty slots, so each of
   v3's 627 no-catch submissions carried **168** phantom catch rows (14 slots ×
   12 bins) instead of one. They hold no individuals and no weight, which is why
   total catch weight is unchanged to six decimal places. Checked before
   accepting: no portal aggregation reads catch `length` — `format-public-data.R`
   only ever sums `catch` and takes `first()` of submission columns — so the only
   affected published object is the `timor_catch.rds` row count, which
   `fetchData.js` does not consume.
2. **`_attachments` gone**: merged/weight lose one column.
3. **`group_conservation_trading/trader` was a literal string on all 22,214 v3
   rows.** `clean_updated_landings()` coalesced two column *names* rather than
   the columns, so the field held
   `"group_conservation_trading/SE_FAAN_ITA_BO_OT_HAKARAK_FAA"` verbatim. Fixed;
   nothing reads the column.
4. **298 `reason_no_activity` values recovered.** `clean_updated_landings()`
   renamed v3's `Tanba_sa_...` but not `Seluk_hakerek_manualmente`, so the
   "other, written in" branch resolved to `NA` for every v3 row. Fixed; nothing
   reads the column.
5. **Length bins are now in ascending order** inside `length_frequency`. The old
   order followed raw column order and was not even consistent between
   submissions (15 distinct orders in 2,000 v2 rows), so this cannot be a
   regression. Catch order within a submission is unchanged — `n` was already
   `0..k-1`, verified over all 30,347 multi-catch submissions, which is what
   `dplyr::first(species)` in `validate_landing_regularity()` depends on.

*Labels, diffed against the Google Sheets tables they replace*

The check PLAN §4 asks for — "a mapping that silently drops a gear will look
like a clean run":

| | codes used by the live forms | resolved from the frame | agreement with the Sheets |
|---|---|---|---|
| gear | 9 | **9** | same code → same gear, Title Case (`MC` = "Gleaning" vs "manual collection") |
| vessel | 2 | **2** | same mapping ("Canoes"/"Motorized Boat" vs "unmotorised"/"motorised") |
| habitat | 7 | **7** | identical — habitat has no frame table and still comes from the Sheets |
| landing sites | 41 | **38** | 27 names identical, 11 spelling variants, 1 genuinely different (`28`) |

The 3 unresolved site codes (`12`, `14`, `17`, 31 submissions) are **absent from
the Sheets `stations` too** — they are what `validate_sites()` already flags as
alert 16, not a frame regression. 40 submissions of 97,328 (0.04%) end with no
`gaul_1_name`. One frame site name carries an embedded newline, so
`survey_labels()` uses `str_squish()` on it: these are labels and they go into
the Phase 6 API export.

*Validation still runs, and its output is where it was*

`validate_landings()` itself was **not** run — it writes to the live
`VALID_SHEET_ID`, which has no dev twin (Phase 5's sink decision). Its body was
replayed locally up to and including the validated-landings upload, skipping the
Sheets sync.

| | golden `90ede9a` | Phase 4 |
|---|---|---|
| validated landings | 97,151 × 19 | **97,328 × 19**, identical column set |
| flagged | 12,805 (13.18%) | 13,388 (13.76%) |
| distinct alert combinations | 37 | **37** |

**35 of the 37 combinations are identical, count for count.** All movement is in
three alerts: `17` 2,849 → 3,420, `11` 2,033 → 2,013, `10` 3,943 → 3,950
(plus `10-17` 133 → 159, `10-11` 93 → 92). Alerts 12–16 and 19–21 — the ones
that would catch a broken label mapping — do not move at all.

**That movement is not Phase 4's.** It is commit `a2c2881`, the weight rewrite
that landed before this phase and has never been through validation. Measured on
the 97,151 submissions the golden and the new artefact share: the weight
artefact went **6,137,008 → 5,194,304 kg, −15.4%**, exactly the figure PLAN §4b
records. Lower weights raise price per kg (median 3.26 → 3.39), and the
submissions outside the configured `price_per_weight` band of `[0.25, 30]` go
**5,018 → 5,702** — which is what alert 17 counts. Phase 4 is weight-neutral to six decimal places,
so it cannot move either alert; and the phantom rows it removes carry no
individuals and no weight, so they are filtered out by `validate_price_weight()`
and `get_bounds_table()` before any threshold is computed.

Validated catch weight is 1,004,984 → 964,584 kg (−4.0%) for the same reason:
more alert-17 submissions means more blanked weights. Recorded, not accepted as
Phase 4's.

*tinytest, against `timor-dev`* — the new validated artefact, and identical to
the Phase 2 and Phase 3 baselines:

| suite | result |
|---|---|
| `test_merged_trips.R` | all ok, 2/2 |
| `test_validated_pds_trips.R` | all ok, 7/7 |
| `test_public_data.R` | all ok, 1/1 |
| `test_validated_landings.R` | 2 fails / 8 passes — the same two pre-existing assertions (`<33--36>` landing dates prior to 2017, `<49--53>` `catch_purpose` is NULL), the same four "Unknown or uninitialised column" warnings |

**No assertion was touched.** The one that matters here passed: `catch_taxon`
validated against `catch_types$interagency_code`, i.e. the frame-derived taxa
are all valid interagency codes.

*`devtools::check()`* — **0 errors, 0 WARNINGs, 4 NOTEs** against a baseline of
0/0/4, and **testthat is green for the first time in the migration**.
`tests/testthat/test-pre-process-landings.R` tested `pt_nest_attachments()` and
`pt_nest_species()`, both deleted; the long-standing
`FAIL 1 | WARN 9 | PASS 9` goes with them. Replaced by
`test-survey-reshaping.R`, 8 assertions over `reshape_species_groups()` and
`expand_length_frequency()` including the over-60 bin and the kept empty bins.
The 4 NOTEs are unchanged (33 Imports, install size, file timestamps,
`sd`/`rnorm`/`if_all`/`Estimated revenue` globals).

**Deferred, with reasons**

- **`metadata.google_sheets.tables` went 15 → 14, not 15 → 6.** See "Done" §5.
  The brief's target is right but it is Phase 5's to reach: the seven tables it
  would remove are read by the validators, which still do their own Sheets
  joins because they also emit alert codes 12–16. Every entry is annotated with
  the phase that removes it.
- **The `join_weights()` bridge stays.** Deleting it means rewriting the 19
  validators against the long table, which is Phase 5, and doing it here would
  have put the 12,805-flag alert-parity gate at risk in a phase whose gate is
  catch weight.
- **The production freeze has still not been run**, and now it must be re-run
  with the *new* script: `R_CONFIG_ACTIVE=production Rscript
  data-raw/freeze-landings-v1.R` writes the long parquet `merge_landings()`
  reads. The `.rds` snapshot in `timor-dev` is superseded.
- `preprocess_metadata_tables()`, `R/preprocess-metadata-tables.R` and
  `R/ingest-metadata-tables.R` were not restructured. PLAN's file map folds
  them into `preprocessing-surveys.R`; they are metadata, not surveys, and
  moving them would have added noise to a diff that already deletes five files.
- The `_geolocation*` columns, `deviceid`, `_submitted_by` and the other KoBo
  bookkeeping fields ride along in the long table. They are legacy passthrough
  and go with the rest in Phase 11.
- `ANTHROPIC_API_KEY` rotation and the Phase 3 secrets rotation are still open
  user actions.

**Findings that change later phases**

1. **Phase 5's first job is the `join_weights()` bridge, and the merged parquet
   already has everything it needs.** `standard_survey_cols()` in
   `R/preprocessing-surveys.R` is the exact list of columns the validators
   should move onto; `join_weights()` drops precisely that list. Delete both
   halves together.
2. **Validation is the slow step and it is not Phase 4's fault.** The local
   replay took ~50 minutes on 1.65 M catch rows, almost all of it in
   `validate_catch_params()`'s per-(gear × taxon) `univOutl::LocScaleB()` calls
   and the `purrr::map` over 97,328 nested tibbles in the final assembly. Both
   disappear with the nesting. Do not add grouping columns to the weight
   artefact — carrying the 18 standard columns through measurably slowed the
   `unnest()`/`nest()` cycles, which is why `join_weights()` drops them.
3. **Nutrients: `coasts::enrich_taxa()` does not replace
   `R/calculate-nutrients.R`.** Measured, not inferred: coasts emits six
   nutrients (Calcium, Iron, Omega3, Protein, VitaminA, Zinc) against Timor's
   seven, has no **Selenium**, does no unit conversion to grams, has no FAO
   food-composition override for the six invertebrate codes FishBase cannot
   estimate (`OCZ`, `IAX`, `COZ`, `PEZ`, `CRA`, `SLV`), and no hardcoded `FLY`
   row. The portal publishes `nutrients_aggregated` off the seven `*_mu`
   columns, and `Selenium_mu` is the left edge of the
   `mean_length:Vitamin_A_mu` range three validators select on. PLAN §4b's
   "delete Timor's if they agree" test **fails**; the file stays and is renamed
   to `R/nutrients.R` in Phase 8 as planned.
4. **`_pkgdown.yml` needed fixing again** — `matches("nest")`, `matches("clean")`
   and `coalist` all selected nothing after the deletions. Fourth phase running,
   fourth dangling entry. **Check `_pkgdown.yml` after every deletion.**
5. 272 v3 submissions record `no_men_fishers` as `"_10"` (an "other" choice
   code), so `as.numeric()` warns and yields `NA`. Pre-existing —
   `validate_n_fishers()` has always coerced the same values the same way — but
   the warning is newly visible in the preprocessing log. Whoever rewrites
   `n_fishers` should decide whether `"_10"` means 10.
6. `preprocess_landings()` runs both live forms in **1.1 minutes** in one
   process. The v2 half-and-half split existed only because 65k submissions with
   nested list-columns did not fit one container.

**Files added / removed / renamed**

- added: `R/preprocessing-surveys.R`, `R/survey-reshaping.R`,
  `tests/testthat/test-survey-reshaping.R`,
  `man/{preprocess_landings,reshape_landings,reshape_species_groups,expand_length_frequency,survey_labels,get_weighted_landings}.Rd`
- renamed: `R/calculate-weights.R` → `R/model-taxa.R`
- removed: `R/clean-raw-data.R`, `R/preprocess-landings.R`,
  `R/pt_nest_species.R`, `R/pt_nest_attachments.R`, `R/merge-landings.R`,
  `tests/testthat/test-pre-process-landings.R`; `get_raw_landings()` from
  `R/ingestion.R`; `pt_validate_fao_catch()` and the `fao_catch` entry from
  `R/preprocess-metadata-tables.R`;
  `man/{clean_catches,clean_updated_landings,coalist,preprocess_landings_step_1,preprocess_landings_step_2,preprocess_updated_landings,pt_nest_species,pt_nest_attachments,pt_validate_fao_catch}.Rd`
- modified: `R/model-taxa.R` (`join_weights()` rewritten against the long table),
  `R/get-cloud-files.R` (`get_merged_landings()` moved here and split from
  `get_weighted_landings()`), `R/ingestion.R`, `R/validate-landings.R`,
  `R/validation-functions.R` (roxygen examples only), `NAMESPACE`, `DESCRIPTION`
  (−`textclean`), `inst/config.yml` (metadata tables), `_pkgdown.yml`,
  `.github/workflows/data-pipeline.yaml` (three preprocessing jobs → one),
  `data-raw/freeze-landings-v1.R` (flatten to long parquet),
  `inst/report/{unanswered_summary,enumerators_summary}.Rmd`, `CLAUDE.md`,
  `.claude/migration/PLAN.md`, `.claude/migration/STATE.md`
- **unchanged: `inst/tinytest/`, the export path, `format-public-data.R`,
  `merge-trips.R`, and every `portal-*.json` object name.**

**Cloud objects written (all `-dev`)**

- `timor-dev/timor-landings-v1-frozen__20260810153814_0e8ab28__.parquet`
- `timor-dev/timor-landings-v2_preprocessed__20260810171248_0e8ab28__.parquet`
- `timor-dev/timor-landings-v3_preprocessed__20260810171324_0e8ab28__.parquet`
- `timor-dev/timor-landings-merged__20260810171351_0e8ab28__.parquet`
- `timor-dev/timor-landings-merged_weight__20260810161646_0e8ab28__.rds`
- `timor-dev/timor-landings-merged_validated__20260810170839_0e8ab28__.rds`

**Open questions for the next session**

1. None blocking. Phase 5 can start from the merged long parquet.
2. ~~The validation flags sink is still the open sub-decision~~ — **resolved
   after this entry was written, 2026-08-10: MongoDB**, shared `validation-*`
   database, one `surveys_flags-<asset_id>` collection per live form. See the
   "Current position" bullet and PLAN §2.6.
3. Whether `"_10"` in v3's `no_men_fishers` should parse as 10 (finding 5).

---

## Phase 5 — Validation — 2026-08-10

Branch: `feat/align-coasts-phase5` (off `feat/align-coasts-phase4` at `2814dff`),
committed as `75985a8`

**Done**

*1. `R/validate-landings.R` → `R/validation.R`*

`validate_landings()` plus the MongoDB sink (`push_validation_flags()`), the
KoBo status reconciliation, and `sync_validation_status()` — the write-back,
exported and documented but **deliberately not wired into the pipeline** (see
Deferred). `R/validation-functions.R` keeps its name; every validator is tagged
`@keywords validation` and the file opens with the alert-code → validator table.

*2. The `join_weights()` bridge is gone*

`join_weights()` now adds `weight` and the seven nutrient columns to the long
table and changes nothing else. `standard_survey_cols()` went with it, and
`timor-landings-merged_weight__*` is **parquet** — 1,647,692 rows × 106 columns,
where the `.rds` was 97,347 × 60. `get_weighted_landings()` is a
`download_parquet_from_cloud()` call.

Sixteen validators (the 19 count included three helpers) read the long table or
the one-row-per-submission view `validation_submissions()` derives from it, all
by standard column name. Nothing in `R/` reads a raw KoBo column any more.

*3. Preprocessing gained the seven columns validation needed*

`harmonise_submissions()` derives `submitted_by`, `has_boat`, `mesh_size` (mm —
the ×25.4 conversion and the `seluk` handling moved out of `validate_mesh()`),
`n_gleaners`, `fuel`, `conservation_code` and `happiness`; `add_missing_cols()`
NAs out the questions a given form never asked. `reshape_landings()` stops
dropping `landing_site_code` / `gear_code` / `vessel_code` / `habitat_code` —
validation needs the code beside the label, because an unresolved code is
exactly what alerts 12, 14, 16 and 19 report. Merged went 87 → 98 columns.

`data-raw/freeze-landings-v1.R` emits the same set. Without it v1's 10,117
submissions arrive with no site code and are all flagged 16 — which is how the
gate caught it.

*4. Flags sink: MongoDB, WIO layout, verified live against `validation-dev`*

No config change was needed; the block declared in Phase 2 was correct.

| collection | documents |
|---|---|
| `surveys_flags-aaztUDtRzb9SpSV7i9iptb` (v2) | 64,997 + 1 metadata |
| `surveys_flags-aEoWV7aprG47Q4uTpaopgD` (v3) | 22,233 + 1 metadata |
| `enumerators_stats-<v2 asset>` | 7,975 (one row per raised alert) |
| `enumerators_stats-<v3 asset>` | 2,920 |

v1 is frozen and gets neither. The document schema was read off the four
non-empty WIO collections rather than inferred, and matches them field for
field: `submission_id, survey_version, submitted_by, submission_date,
alert_flag, validation_status, validated_at, validated_by, fetch_error`.
`alert_flag` is comma-separated and **`NA` when clean**, which is what the shared
UI reads as "nothing to review"; Timor's internal `-`-joined string is unchanged.

`coasts::mdb_collection_push()` replaces a collection wholesale, so
`validate_landings()` also writes `validation_alerts__*.parquet` to the country
bucket — the only history of what was flagged when. `get_validation_sheet()` →
`get_validation_flags()`, reading that.

*5. `send_validation_mail()` reads Mongo*

Flags from both live collections, descriptions from a new
`validation.alerts` block in `inst/config.yml` — the `alerts` tab of the retired
sheet, carried over verbatim plus the four codes it never documented. Nothing in
`R/` reads `VALID_SHEET_ID` any more.

*6. Two deletions worth the space they free*

- **`get_bounds_table()`** — assigned at the old line 68 and never read, with no
  other caller anywhere. `split()` over ~600 (taxon × gear) groups of 1.65M rows
  with a `univOutl::LocScaleB()` each.
- **The Cook's-distance fit in `validate_price_weight()`** — a `stats::lm()` plus
  `broom::augment()` per submission whose `cooksd` has been commented out of the
  threshold for years. `single_catches` went too, same reason. `cook_dist` is
  kept as a parameter because `config.yml` still supplies it.

Together with the nesting, **validation went from ~50 minutes to 8.0**, of which
~5 is the Mongo push and ~1.5 the KoBo status read.

**Verified**

Method: a `git worktree` of `2814dff` replayed the **old** validation path on the
**same** dev weight artefact, skipping only the Sheets sync and
`get_bounds_table()` — both provably irrelevant to the alerts. Same-data
baseline, so every delta below is this phase's.

*Flags — the gate*

| | baseline (`2814dff` replay) | Phase 5 |
|---|---|---|
| submissions | 97,347 | 97,347 |
| flagged | 13,388 (13.75%) | **13,387** |
| distinct alert combinations | 37 | **37** |
| non-zero codes produced | 1, 3, 4, 5, 6, 10, 11, 16, 17, 20, 21, 22 | **identical** |

**Exactly one submission of 97,347 has a different flag string**, and it is the
deliberate data correction below. Before that correction the diff was **0 rows** —
byte-identical flag strings across the whole table, every code count equal.

*Validated artefact*

97,347 × 19, identical column set, `landing_id` identical. Sixteen of the
eighteen submission-level columns are byte-identical; catch rows 1,647,692 both,
with `catch_taxon`, `catch_use`, `length_type`, `length`, `number_of_fish` and
`catch` all differing in **0** cells. **Validated catch weight 964,937.9 kg in
both.** Total catch weight 5,197,093.9 kg, unchanged.

The two columns that do differ are the intended frame relabels (PLAN §2.5), a
clean 1:1 remap losing no rows:

| | Sheets (was) | frame (now) | submissions |
|---|---|---|---|
| `gear` | `gill net`, `hand line`, `long line`, `spear gun`, `seine net`, `beach seine`, `cast net`, `trap`, `manual collection` | `Gill Net`, `Hand Line`, `Long Line`, `Spear Gun`, `Seine`, `Beach Seine`, `Cast Net`, `Trap`, **`Gleaning`** | 91,217 |
| `propulsion_gear` | `motorised` / `unmotorised` | `Motorized Boat` / `Canoes` | 89,566 |

**Phase 8 must check the portal front-end for hardcoded gear or propulsion
labels** — `peskas.timor.portal.v2` is outside this repo and was not inspected.
Nothing in `R/` keys off either value: `jsonify_indicators()`'s `habitat_gear`
groups are data-derived, and the modelling code never touches gear.

*tinytest, against `timor-dev`* — **all four suites green, for the first time in
the migration:**

| suite | result |
|---|---|
| `test_validated_landings.R` | **all ok, 10/10** (was 2 fails / 8 passes) |
| `test_validated_pds_trips.R` | all ok, 7/7 |
| `test_merged_trips.R` | all ok, 2/2 |
| `test_public_data.R` | all ok, 1/1 |

Both long-standing failures are closed, and **no assertion was deleted or
weakened**:

- `<49--53>` `catch_purpose` — the suite named four columns the artefact has
  never had (`trip_duration`, `landing_value`, `catch_purpose`, `individuals`), a
  schema that never shipped. Reading a missing column returns NULL, so three of
  them passed vacuously while warning "Unknown or uninitialised column" and the
  fourth failed on an empty compare. Pointed at the real columns
  (`trip_length`, `catch_price`, `catch_use`, `number_of_fish`) — same
  quantities, and the names `format_public_data()` reads. The four warnings are
  gone with them.
- `<33--36>` landing dates prior to 2017 — **one** submission of 97,347:
  `16182387`, v1, submitted 2017-12-14 recording a landing on 2015-07-07, two
  years before the form existed. No catch, no individuals, no revenue.
  **Decision (user, 2026-08-10): null the date in the freeze script**, with the
  reasoning in a comment. Consequence, and the only flag that moves in this
  phase: its alert 10 (submitted >28 days after landing) can no longer fire, so
  10 goes 4,321 → 4,320 and the submission reads `0`. It contributes nothing to
  any aggregate.

The `setwd("../..")` in `test_validated_landings.R` — dead since the `local:`
config era, and the reason `.env` was never found on a local run — is replaced by
a guarded `dotenv::load_dot_env()`. The other three suites still carry it; they
are Phase 9's when the harness is tidied.

*`devtools::check()`* — **0 errors, 0 WARNINGs, 4 NOTEs** against a Phase 4
baseline of 0/0/4, and **testthat OK**. `pkgdown::check_pkgdown()` clean. The
NOTEs are unchanged in kind (34 Imports — `httr2` is back, the KoBo client needs
it; install size; file timestamps; the `sd`/`rnorm`/`if_all`/`Estimated revenue`
globals). Two WARNINGs appeared mid-phase and were fixed rather than accepted:
the undeclared `httr2`, and a dangling `[validation-functions]` Rd link left by
converting that block to a comment.

**Two live bugs fixed, both alert-identical on the current data**

1. **`validate_catch_params()`'s positional assignment.** It assigned
   `validated_length_nested$length_individuals` into a separately-derived frame
   and worked only if both sides were built from the same rows in the same order.
   The rewrite mutates in place, so the question does not arise. Verified no
   alert or value moves.
2. **`isTRUE()` on a vector made alerts 12–15 unreachable.**
   `if_else(isTRUE(<vector>), 12, NA)` collapses to a length-1 `FALSE`, so those
   four codes could never fire. The rewrites are properly vectorised. Measured
   before changing anything: with the bug fixed, **all four still produce zero**
   — every gear and vessel code the live forms use resolves from the frame, and
   no submission declares a boat without a type. So the fix is free and the alert
   distribution is untouched. That is why 12–15 remain in the "never observed"
   set.

**Deviations from the brief, all measured**

*`metadata.google_sheets.tables` went 14 → 12, not 14 → 9.* `vessel_types` and
`gear_types` are gone with their parsers. The other three the brief assigned to
this phase must stay, and the config now records why:

- **`devices`** — the frame's `pds_devices` carries 442 Timor devices against the
  Sheets' 595. Replayed both through `validate_this_imei()`: alert 3 goes
  **824 → 1,475** and 651 submissions lose their resolved `tracker_imei`, which
  is `merge_trips()`'s join key. Reconsider with the PDS switch in Phase 7,
  where the device list is the subject anyway.
- **`stations` / `reporting_units`** — these are the *published* labels, not just
  a validation lookup. `municipality` is Timor's reporting unit, which is not
  `gaul_1_name`: only 22 of 40 sites agree, Atauro is its own reporting unit
  while GAUL puts it inside Dili, and `format_public_data()` hardcodes five
  reporting-unit names. `landing_site` is worse — 11 of 40 site names differ
  between the sources, and `get_summary_data()` hardcodes five Sheets spellings
  to classify the north coast, two of which the frame writes differently
  (`Tutuala` → `Tutuala/Valu/Savirara`, `Comando` → `Comando/Obrato/Behau`).
  Swapping either would silently reclassify a published object. `validate_sites()`
  therefore keeps its Sheets join and gains only the standard code column.
  **This is Phase 8's**, with the portal parity gate.

*The alert-11 blanking scope, and the one number that could have moved.* Alert 11
blanks **every** catch of the affected landing, not only the outlying row — an
artefact of `validate_catch_params()` having returned one nested row per
submission, so un-nesting spread the alert over every row. Writing the row-level
version first recovered **13,857 kg across 845 submissions, +1.44% of validated
catch weight**. Reverted: **decision (user, 2026-08-10) is to keep the
whole-landing scope**, so Phase 5 stays weight-neutral and Phase 8's portal diff
compares like with like. The narrowing is a one-line change and is documented at
the call site.

*The KoBo status read is a bulk query, not a loop.* Mozambique's pattern is one
request per previously-flagged submission through `furrr`. Measured: 7,776 v2
submissions took **over twenty minutes** even across ten workers. The data
endpoint returns `_validation_status` alongside `_id` for 1,000 submissions per
request, so `list_validation_statuses()` pages through it — **65 requests and
~70 seconds** for v2's 64,997, and it covers *every* submission rather than only
those a previous run flagged, so an approval entered by hand on a submission the
pipeline never flagged is seen too. That also removed the `mdb_collection_pull()`
round-trip the loop needed to pick its ids.

*`KOBO_TOKEN` is the wrong credential and is no longer needed.* The token in
`.env` authenticates as user **`peskas`**, which has no data access to either
Timor asset: 200 on `/assets/<id>/`, **404 on `/assets/<id>/data/`**. Every
status read failed silently as `fetch_error = TRUE` until this was traced. The
KoBo client now takes basic auth, which `KOBO_USERNAME` / `KOBO_PASSWORD`
already provide and which are **already GitHub secrets** — so the write-back has
no missing-secret blocker at all, and `KOBO_TOKEN` is optional passthrough. It
is still mapped in the workflow for the `ingestion` block.

Related, and kept deliberately: KoBo answers **404** for a submission that has
never been validated, which is the normal case. `httr2` throws on 4xx by
default, so Mozambique's `!= 200 → "not_validated"` branch is unreachable there
and every unvalidated submission is recorded as a fetch failure. Timor's
`kobo_request()` sets `req_error(is_error = ~ FALSE)` so the branch works:
`not_validated` means no status, `fetch_error` means a real transport failure.

**Deferred, with reasons**

- **`sync_validation_status()` is not wired into any workflow.** It PATCHes
  KoBoToolbox, there is no development KoBo instance, and the two assets are
  live in both environments — so running it from a migration branch would change
  production review state whatever `R_CONFIG_ACTIVE` says. It also skips
  submissions already carrying the target status, so the first real run is
  cheap. Wiring it up is a deliberate operational decision, not a phase task.
- **`MONGODB_CONNECTION_STRING_VALIDATION` is not a GitHub secret.** Verified
  against `gh api .../actions/secrets`. Locally it is set and the sink is fully
  verified against `validation-dev`; in CI `validate_landings()` will log a
  warning and write only the GCS snapshot. **User action.**
- **The alert-code vocabulary may collide in the shared UI.** Timor's codes run
  1–23 with its own meanings; Mozambique uses 4/5/6 for taxa/total-catch/price.
  If the validation app holds one dictionary rather than one per country, Timor's
  flags will render with the wrong descriptions. Not something this repo can fix
  — the `surveys` / `countries` / `districts` collections are shared metadata.
  Raise with whoever owns the app.
- **The production freeze still has not been run**, and now it carries the
  16182387 date correction as well: `R_CONFIG_ACTIVE=production Rscript
  data-raw/freeze-landings-v1.R` before Phase 11 merges to `main`.
- `validation.google_sheets` and `validation.version.preprocess` are left in
  `config.yml` with no reader — Phase 11 owns legacy keys. `googlesheets4` stays
  in Imports; `ingest_metadata_tables()` still uses it.
- The three other tinytest suites keep their dead `setwd("../..")`.
- `ANTHROPIC_API_KEY` rotation and the Phase 3 secrets rotation are still open
  user actions.

**Findings that change later phases**

1. **Phase 6 has its input already.**
   `timor-landings-merged_validated_long__*.parquet` is written beside the nested
   artefact — the same content, one row per (submission, catch, length bin),
   under standard names (`trip_duration`, `vessel_type`, `catch_habitat`,
   `n_individuals`, `catch_kg` in kilos). Nothing reads it yet. Phase 8 drops the
   nested artefact when `format_public_data()` moves onto this one.
2. **`devtools::load_all()` does not reach `furrr`/`future` workers.** A worker
   loads the *installed* namespace, so a new unexported helper is "could not find
   function" inside the parallel map while working perfectly in the parent. Same
   class of trap as the `coasts::read_config()` one in the Phase 3 entry, and the
   same fix: `devtools::install()` + `library()` to test anything parallel. Cost
   half an hour of misdiagnosis here.
3. **The Mongo push is now the slowest step of validation** — ~4.5 minutes for
   64,997 v2 documents through `mongolite`'s row-wise insert, against ~1 minute
   for all the validators together. `coasts::mdb_collection_push()` is the place
   to look if that matters; it also `remove("{}")`s first, so a failed push
   between the clear and the insert would leave the collection empty. The GCS
   snapshot is the mitigation.
4. **Phase 8 owns three label reconciliations**, all documented at their call
   sites: `landing_site` and `municipality` (Sheets vs frame, above), and
   checking the portal front-end for hardcoded gear/propulsion labels.
5. `inst/report/data_report.Rmd` hardcodes the nine **Sheets** gear names as
   factor levels (lines ~1187–1195). It now receives Title Case from the frame,
   so those levels resolve to `NA`. `data-report.yaml` has been disabled since
   2026-06-01 and reports move to `R/reports.R` in Phase 8 — fix it there.

**Files added / removed / renamed**

- renamed: `R/validate-landings.R` → `R/validation.R` (body rewritten)
- added: `man/{list_validation_statuses,get_validation_status,update_validation_status,push_validation_flags,sync_validation_status,validation_submissions,validate_imeis,validate_landing_regularity,validate_vessel_type,validate_gear_type,validate_sites,validate_n_fishers,validate_habitat,validate_mesh,validate_gleaners,validate_fuel,validate_conservation,validate_happiness,get_validation_flags}.Rd`
- removed: `get_bounds_table()`, `get_deployed_imeis()`, the Cook's-distance fit
  and `single_catches` from `validate_price_weight()`, `standard_survey_cols()`,
  `pt_validate_gear_types()`, `pt_validate_vessel_types()`,
  `get_validation_sheet()`; `man/get_validation_sheet.Rd`
- modified: `R/validation-functions.R` (rewritten), `R/model-taxa.R`
  (`join_weights()`, `estimate_weight()`, `calculate_weights()` → parquet),
  `R/preprocessing-surveys.R`, `R/get-cloud-files.R`,
  `R/preprocess-metadata-tables.R`, `R/send-email.R`,
  `data-raw/freeze-landings-v1.R`, `inst/config.yml` (`validation.alerts`, the
  metadata-table list), `inst/tinytest/test_validated_landings.R`, `DESCRIPTION`
  (+`httr2`), `NAMESPACE`, `_pkgdown.yml` (keyword-driven validation section),
  `.github/workflows/data-pipeline.yaml` (two env vars), `CLAUDE.md`,
  `.claude/migration/STATE.md`
- **unchanged: `R/format-public-data.R`, `R/export.R`, `R/merge-trips.R`, the
  other three tinytest suites, and every `portal-*.json` object name.**

**Cloud objects written (all `-dev`)**

- `timor-dev/timor-landings-v1-frozen__20260810231255_2814dff__.parquet`
- `timor-dev/timor-landings-v{2,3}_preprocessed__20260810212*_2814dff__.parquet`
- `timor-dev/timor-landings-merged__20260810231*_2814dff__.parquet`
- `timor-dev/timor-landings-merged_weight__20260810231559_2814dff__.parquet`
- `timor-dev/timor-landings-merged_validated__20260810231*_2814dff__.rds`
- `timor-dev/timor-landings-merged_validated_long__20260810231*_2814dff__.parquet`
- `timor-dev/validation_alerts__20260810231729_2814dff__.parquet`
- `validation-dev`: `surveys_flags-<v2 asset>`, `surveys_flags-<v3 asset>`,
  `enumerators_stats-<v2 asset>`, `enumerators_stats-<v3 asset>`

**Open questions for the next session**

1. None blocking. Phase 6 can start from
   `timor-landings-merged_validated_long__*.parquet`.
2. The `MONGODB_CONNECTION_STRING_VALIDATION` GitHub secret, so a branch push
   exercises the sink end to end. Until it exists, CI writes flags only to the
   GCS snapshot — a warning, not a failure.
3. Whether the shared validation app can hold a per-country alert dictionary.

## Phase 6 — API contract + merge_trips — 2026-08-11

Branch: `feat/align-coasts-phase6` (off `feat/align-coasts-phase5` at `992bc6d`),
committed as `ec0b7e5`

**Done**

*1. `R/api.R` — `export_api_raw()` / `export_api_validated()`*

Both project a long catch table onto the 22-column cross-country schema and
upload versioned parquet to `conf$api$trips$*$cloud_path` in the `options_api`
bucket. They differ only in their input: raw reads
`timor-landings-merged_weight__*.parquet` (the pre-validation table — the first
stage that has a weight at all, so Mozambique's "preprocessed" has no exact
Timor counterpart), validated reads
`timor-landings-merged_validated_long__*.parquet`. Neither is wired into
`data-pipeline.yaml`; PLAN is explicit that this phase proves the contract, not
that it schedules it.

*2. The schema was read off the live objects, not off Mozambique's `R/api.R`*

`peskas-api-prod` holds the same 22 columns in the same order for Kenya,
Mozambique and Zanzibar, raw and validated alike. Timor's emitted parquet was
asserted — not eyeballed — column-name-for-column-name, order and type, against
`mozambique/validated/trips-validated__20260809013209_037cf84__.parquet`.

The two upstream type inconsistencies were resolved toward **Mozambique**, the
reference implementation: `landing_date` **Date** (Kenya and Zanzibar write
POSIXct) and `n_catch` **integer** (Kenya writes numeric). Both come out of
Timor's own tables in those types already, so this cost nothing.

*3. `long_validated_landings()` widened, so the export is a projection*

The long validated artefact gained the eight columns the API needs and the
*nested* artefact never had: `survey_version`, `gaul_1_code`, `gaul_1_name`,
`gaul_2_code`, `gaul_2_name`, `n_fishers`, `catch_outcome`, `scientific_name`.
It is 1,647,692 × 40, was × 32.

- `rename_validated_catch()` carries `catch_outcome` and `scientific_name`.
- `nest_landing_catch()` gained a leading `select()`. This is the load-bearing
  part: `tidyr::nest()` groups on every column it is not nesting, so a new
  column in `validated_catch` would silently re-group the portal's artefact.
  A unit test pins it, using a `catch_outcome` that **varies within** the group
  — the case that would have split a row.
- `n_fishers` is `sum_fishers()` over the *validated* `fisher_number_*` trio,
  reusing preprocessing's helper rather than re-deriving the rule.
- `api_submission_extras()` takes the rest off the weight table by
  `distinct()` — verified to be exactly 97,347 rows, one per submission.

*4. New config key and accessor*

`surveys.landings.validated_long.file_prefix`, replacing the
`paste(..., "long", sep = "_")` composed in `validate_landings()`, plus
`get_validated_landings_long()` beside the other storage accessors.

*5. `merge_trips()` — deliberately untouched*

PLAN offered "rewrite in the standard shape (or adopt
`coasts::merge_survey_trips()`)". Neither, and the export needs neither:

- `coasts::merge_survey_trips()` does a different job (COASTS-TODO C10): it
  assembles a cross-country table from matches each country already made,
  joining `submission_id` ↔ `trip` out of `peskas-api-*`. Timor's
  `merge_trips()` does the matching itself on `(landing_date, tracker_imei)`.
- The 22-column contract has no slot for a PDS trip id, so nothing in Phase 6
  reads `all_trips` at all.

`all_trips` is therefore byte-for-byte the artefact it was — confirmed against
the CI run below, 175,089 × 26 with 84,741 tracker matches, the Phase 5
baseline exactly.

**The three decisions worth arguing about**

*`trip_id` is `TRIP_<submission_id>`, not `tracker_trip_id`.* The brief
mapped it to `all_trips$tracker_trip_id`. The observable contract says
otherwise and so does the consumer:

- All three countries write `TRIP_<id>` derived from the submission —
  `TRIP_715693918` (Moz), `TRIP_645323011` (Zanzibar), `TRIP_8464` (Kenya).
- `coasts::summarize_data()` does `group_by(trip_id) |> slice(1)` on the
  assumption that trip-level columns are constant within a `trip_id`. Only
  84,741 of 175,089 Timor trips carry a `tracker_trip_id`; every unmatched
  landing would carry `NA`, and `group_by()` would collapse ~90k landings into
  a single "trip". That is not a drift risk, it is a wrong answer.

So the PDS trip id is not published. If it should be, the place for it is a
23rd column agreed across all four countries, not a redefinition of `trip_id`.

*The length-bin collapse.* The API grain is (trip, catch); Timor's is
(submission, catch, length bin). Grouping is on
`(submission_id, n_catch, catch_taxon)` — 144,291 groups, 244 more than the
144,047 distinct `(submission, n_catch)` pairs, because a handful of catch
slots carry two taxa. Grouping on the pair alone would have silently dropped
one of each.

- `catch_kg` is the sum over bins; **this is lossless**, and measured so: the
  raw export sums to **5,197,093.9 kg** and the validated to **964,937.9 kg**,
  both equal to their source artefacts to the last published decimal.
- `length_cm` is the bin midpoint weighted by the individuals counted in each
  bin. It is not lossless and cannot be — one number replaces a distribution.
- **Empty bins are kept.** Dropping them first was measured: it loses 13,219
  of 144,291 catch records and 26 kg. Keeping them costs nothing because a bin
  with no count contributes 0 to both sums.
- A catch with no weighable bin is `NA`, not `0`. Under Mozambique's plain
  `sum()` the whole landing's `tot_catch_kg` would go `NA` when any one catch
  is unweighable: 15,419 of 97,347 landings, against 14,179 under `na.rm`.
  `na.rm` is also what makes the totals identity above hold.

*`catch_price` is `NA` and `tot_catch_price` is the landing's revenue.* Timor
prices the landing, not the catch item, so there is nothing to put in
`catch_price` — exactly Mozambique's situation and exactly what Mozambique
publishes (all 1,995 rows `NA`). Kenya, which does collect per-catch prices,
fills it. Also `catch_taxon == "0"` (Timor's no-catch sentinel) is written as
`NA`, which is how the other three represent the same thing.

**Verified**

*The emitted objects*, both in `peskas-api-dev`:

| | rows × cols | trips | Σ `catch_kg` | Σ per-trip `tot_catch_kg` |
|---|---|---|---|---|
| `timor/raw/trips-raw__20260811003451_992bc6d__.parquet` | 144,291 × 22 | 97,347 | 5,197,093.9 kg | 5,197,093.9 kg |
| `timor/validated/trips-validated__20260811003526_992bc6d__.parquet` | 144,291 × 22 | 97,347 | **964,937.9 kg** | 964,937.9 kg |

Both match the Phase 5 baselines exactly. Asserted, not eyeballed:
`identical(names(timor), names(moz))` and `identical(types, types)` for both
stages; `trip_id` unique per landing; **0** trips whose trip-level columns are
not constant, which is the precondition `summarize_data()` relies on.

*The nested artefact did not move.* `all.equal()` over the whole 97,347 × 19
frame is `TRUE`, column names identical, `landing_id` identical, 144,291 catch
rows both. The only inequality `identical()` finds is last-bit floating point
(1e-16 relative) in the weight-derived nutrient columns, inherited from a
different upstream weight run — not from the `select()`, which changes no
values and which the new unit test pins directly.

*`coasts::summarize_data(package = "peskas.timor.data.pipeline")` does **not**
run end to end, and Phase 6 is not what stops it.* Its three inputs:

| input | resolves to | present? |
|---|---|---|
| `api.trips.validated` | `peskas-api-dev` | ✅ read fine, 144,291 × 22 |
| `asfis` | `conf$storage$google$options` → `timor-dev` | ❌ 0 objects |
| `<pds_tracks>-grid_summaries` | `conf$storage$google$options` → `timor-dev` | ❌ 0 objects |

Both missing ones live in the **hub** (`peskas-coasts-dev` holds 204
`pds-tracks-grid_summaries__*`), where they resolve correctly for coasts itself
because coasts' `storage.google.options` *is* the hub. Called with
`package = "<country>"` the same line reads the country bucket. Filed as
**COASTS-TODO C17**, the same class as C4 and C11. `conf$surveys$summaries$file_prefix`
is also absent from Timor's config; it is a PDS-shaped product, so it belongs
with Phase 7.

What could be verified was: the API read itself, the `group_by(trip_id) |>
slice(1)` collapse (97,347 rows, one per trip), the taxon collapse (137,919
rows), the monthly summary (979 rows over 21 GAUL-2 municipalities), and
**`coasts::calculate_fishery_metrics()` run for real on Timor's parquet** —
11,726 metric rows, clean. The contract holds; the function around it is not
ready for Timor.

*Write access to `peskas-api-prod` — AUDIT §6's open question, closed.*
Answered without writing anything, via the bucket `testIamPermissions`
endpoint: `data-ingestion@peskas.iam.gserviceaccount.com` has
`storage.objects.create`, `.delete`, `.get` and `.list` on **both**
`peskas-api-dev` and `peskas-api-prod`. No user action needed before Phase 11.

*The dev pipeline run — the real news.* The branch was pushed at the start of
the session and run **31436031588** went green end to end in 32 minutes. This
is the first time Phases 3, 4 and 5 have been exercised in CI at all:

- every job green, including the four tinytest suites (10 / 7 / 2 / 1);
- `13387 of 97347 submissions flagged` in CI, identical to the local number;
- the MongoDB sink ran in CI for the first time —
  `MONGODB_CONNECTION_STRING_VALIDATION` now exists as a secret, and both
  `surveys_flags-*` collections were pushed (64,997 and 22,233 documents).

Everything measured locally afterwards was measured against the artefacts that
run produced (`*_992bc6d__*`), not against the Phase 5 leftovers.

A second run, **31439673841**, then went green on the Phase 6 code itself —
all twelve jobs, the same `13387 of 97347 submissions flagged`, the same four
tinytest suites (10 / 7 / 2 / 1). The widened artefact it produced is
**1,647,692 × 40** carrying all eight new columns and the same 964,937.9 kg,
and both `surveys_flags-*` collections came out at 64,998 / 22,234. So the
widening survives CI, not only a local run.

*Local gates*: `devtools::load_all()`, `devtools::document()`,
`devtools::check()` — 0 errors, 0 WARNINGs, 4 NOTEs, unchanged in kind from the
Phase 5 baseline; `pkgdown::check_pkgdown()` clean; testthat 27 passing (19 new
in `tests/testthat/test-api.R`); the four tinytest suites re-run locally
against `timor-dev`, all green.

**Deviations from the brief**

- **`trip_id`**, above. The brief's mapping table would have broken
  `summarize_data()`; the three-country contract won.
- **`_pkgdown.yml` was not edited.** Its Workflow section is
  `has_keyword("workflow")` and the Cloud section is `matches("get")`, so the
  two new exports and the new accessor are already indexed;
  `pkgdown::check_pkgdown()` is clean. Adding them by name would have created
  duplicate index entries for no gain.
- **`merge_trips()` was not rewritten**, above.

**Operational note, for honesty**

Two `validate_landings()` runs were started concurrently by mistake during this
session. `coasts::mdb_collection_push()` clears a collection and then inserts,
so the two interleaved and left `surveys_flags-<v2>` with 126,995 documents
instead of 64,998. A single clean re-run restored **64,998 / 22,234 /
7,975 / 2,920**, matching the Phase 5 entry exactly. Worth recording because it
is the failure mode the Phase 5 entry predicted (finding 3): the push has no
locking, and a clear-then-insert that is interrupted or raced leaves the shared
collection wrong. The versioned `validation_alerts__*.parquet` snapshot is the
only thing that makes that recoverable.

**Deferred, with reasons**

- **Nothing was published to `peskas-api-prod`.** `default` resolves
  `peskas-api-dev` and the phase never ran with `R_CONFIG_ACTIVE=production`.
  Live consumers pick up whatever lands in prod; the first prod write should be
  a deliberate act on `main`.
- **Not wired into `data-pipeline.yaml`.** PLAN §Phase 6. The natural place is
  a step after `validate-landings` (validated) and after `merge-landings`
  (raw), in Phase 9 when the workflow is rewritten.
- **`summarize_data()`** — blocked on C17 and on PDS grid summaries, i.e.
  Phase 7.
- The 651 submissions whose `tracker_imei` resolves only through the Google
  Sheets `devices` table are still the `merge_trips()` match key. Phase 7 moves
  the device list to the frame and the 84,741 match count moves with it.
- `sync_validation_status()` still not wired; the production v1 freeze still
  un-run; `ANTHROPIC_API_KEY` and the Phase 3 secrets rotation still open.

**Findings that change later phases**

1. **Phase 8 inherits a long validated table that is now a superset of the
   nested one.** Everything `format_public_data()` reads by name is in it under
   a standard name, plus the eight API columns. The nested artefact is now pure
   legacy shape.
2. **`n_catch` is not a unique catch index.** 244 `(submission, n_catch)` pairs
   carry two taxa. Any future collapse to catch level must include
   `catch_taxon` in the key, as `api_trips()` does. Mozambique has the same
   property (13 rows per trip against `n_catch` max 8).
3. **Timor will be the largest table in the API bucket by an order of
   magnitude** — 144,291 rows against Kenya's 339,588 *including* its legacy
   form, Mozambique's 1,995 and Zanzibar's 16,241. Anything downstream that
   loads all four countries into memory should be told.
4. **COASTS-TODO gained C16 and C17**: `resolve_storage_opts()` has no `"api"`
   type, and `summarize_data()` reads two hub artefacts from the country
   bucket.

**Files added / removed / renamed**

- added: `R/api.R`, `tests/testthat/test-api.R`,
  `man/{export_api_raw,export_api_validated,get_validated_landings_long}.Rd`
- modified: `R/validation.R` (`rename_validated_catch()`,
  `nest_landing_catch()`, `long_validated_landings()`, new
  `api_submission_extras()`), `R/get-cloud-files.R`, `inst/config.yml`
  (`surveys.landings.validated_long`), `NAMESPACE`, `CLAUDE.md`,
  `.claude/migration/COASTS-TODO.md` (C16, C17),
  `.claude/migration/STATE.md`
- **unchanged: `R/merge-trips.R`, `R/format-public-data.R`, `R/export.R`,
  `_pkgdown.yml`, the four tinytest suites, and every `portal-*.json` object
  name.**

**Cloud objects written**

- `peskas-api-dev/timor/raw/trips-raw__20260811003451_992bc6d__.parquet`
- `peskas-api-dev/timor/validated/trips-validated__20260811003526_992bc6d__.parquet`
- `timor-dev/timor-landings-merged_validated_long__2026081100*_992bc6d__.parquet`
  (the widened, 40-column shape) plus the nested `.rds` and
  `validation_alerts__*` of the same runs
- `validation-dev`: both `surveys_flags-*` and both `enumerators_stats-*`
  re-pushed clean

**Open questions for the next session**

1. None blocking. Phase 7 (PDS) can start.
2. Should Phase 9 wire the two exports into `data-pipeline.yaml`, and should
   the first `peskas-api-prod` write happen before the Phase 11 merge or as
   part of it?
3. Whether the shared validation app can hold a per-country alert dictionary
   (carried over from Phase 5).
