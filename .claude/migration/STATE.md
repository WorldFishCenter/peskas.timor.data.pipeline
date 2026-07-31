# Migration state log

Handoff file for the `peskas.timor.data.pipeline` → harmonized-standard migration.
Read `.claude/migration/PLAN.md` then `.claude/migration/STRUCTURAL-DIFF.md` first.
Append one entry per completed phase, newest at the bottom.

## Current position

- **Phase:** 1 **complete** (2026-07-31). Phase 2 not started.
- **Branches:** Phase 0 committed as `494a8d0` on `feat/align-coasts`. Phase 1
  work is on `feat/align-coasts-phase1`, **uncommitted** — the user asked to
  review it before it lands. Commit and merge into `feat/align-coasts` at the
  start of Phase 2, then push to exercise the dev buckets.
- **Environment:** `gs://timor-dev` seeded from prod run `90ede9a` (21
  prefixes, re-verified 2026-07-31); `timor/{raw,validated}` markers exist in
  both `peskas-api-dev` and `peskas-api-prod`.
- **Read before Phase 2:** [AUDIT.md](AUDIT.md) §7, and the
  `purrr::map(pars$storage, ...)` hazard in the Phase 1 entry below — it is the
  first thing Phase 2 must fix.
- **Blocking sub-decision:** validation flags sink — Google Sheets vs MongoDB.
  Raise at Phase 5. Recommendation: **MongoDB**, now stronger — AUDIT §5 shows
  `send_validation_mail()` has not run successfully since ≥2025-09, so there is
  no working Sheets reader to preserve.

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
