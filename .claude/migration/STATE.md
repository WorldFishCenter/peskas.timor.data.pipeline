# Migration state log

Handoff file for the `peskas.timor.data.pipeline` → harmonized-standard migration.
Read `.claude/migration/PLAN.md` then `.claude/migration/STRUCTURAL-DIFF.md` first.
Append one entry per completed phase, newest at the bottom.

## Current position

- **Phase:** 0 **complete** (2026-07-31). Phase 1 not started.
- **Branch:** `feat/align-coasts` (created off `feat-upgrade`). Phase 0 changes
  are **uncommitted** in the working tree — the user asked to review them before
  they land. Commit them at the start of Phase 1.
- **Environment ready for Phase 1:** `gs://timor-dev` seeded from prod run
  `90ede9a`; `timor/{raw,validated}` created in both API buckets.
- **Read before Phase 1:** [AUDIT.md](AUDIT.md) §7 — six measured facts that
  contradict PLAN.md. The plan needs those corrections applied before Phase 3,
  6 and 7 are scoped.
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
