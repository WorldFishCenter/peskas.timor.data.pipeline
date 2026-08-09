# Migration state log

Handoff file for the `peskas.timor.data.pipeline` → harmonized-standard migration.
Read `.claude/migration/PLAN.md` then `.claude/migration/STRUCTURAL-DIFF.md` first.
Append one entry per completed phase, newest at the bottom.

## Current position

- **Phase:** 2 **complete** (2026-07-31). Phase 3 not started.
- **Branches:** Phase 0 = `494a8d0`, Phase 1 = `ea7f253`, both on
  `feat/align-coasts` (Phase 1 fast-forward-merged at the start of Phase 2).
  Phase 2 work is on `feat/align-coasts-phase2`, **uncommitted** — the user
  asked to review it before it lands. Commit and merge into `feat/align-coasts`
  at the start of Phase 3, then push to exercise the dev buckets.
- **Environment:** `gs://timor-dev` seeded from prod run `90ede9a` (21
  prefixes, re-verified 2026-07-31); `timor/{raw,validated}` markers exist in
  both `peskas-api-dev` and `peskas-api-prod`. Local `coasts` is now **4.5.0**,
  matching the `DESCRIPTION` / `COASTS_REF` pin.
- **Read before Phase 3:** the Phase 2 entry's "Findings that change later
  phases" — in particular that `assets__*` in `peskas-coasts-dev` is stale to
  2026-06-29 and predates Timor's Airtable rows, so Phase 3 **must** run
  `ingest_assets()` itself rather than reading the existing dev snapshot.
- **Phase 3 prerequisite, still open:** `KOBO_ASSET_ID_V1/2/3` must be added to
  the `env:` block of `data-pipeline.yaml`, fed from the existing
  `KOBO_PESKAS{1,2,3}` secrets. `KOBO_TOKEN` is optional (basic auth works).
  Deliberately **not** done in Phase 2.
- **Blocking sub-decision:** validation flags sink — Google Sheets vs MongoDB.
  Raise at Phase 5. Recommendation: **MongoDB**, now stronger — AUDIT §5 shows
  `send_validation_mail()` has not run successfully since ≥2025-09, so there is
  no working Sheets reader to preserve. `storage.mongodb` is now declared in
  `config.yml` (Phase 2) and inert, so enabling it is a code change only.

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
