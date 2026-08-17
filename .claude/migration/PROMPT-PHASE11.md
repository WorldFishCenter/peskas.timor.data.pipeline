# Prompt for the next session — Phase 11

Copy everything below the line into a fresh Claude Code session started in the
**`peskas.timor.data.pipeline`** working directory.

---

Phase 11 — cutover

This is the **last** migration phase, and the only one that changes production.
Everything before it was reversible; this one merges to `main` and lets a real
run write to `timor`, `pds-timor`, `public-timor` and — if you remove two `if:`
lines — `peskas-api-prod`.

Read in order before doing anything: `CLAUDE.md`,
`.claude/migration/PLAN.md` (§Phase 11), `.claude/migration/STATE.md` — the
**Phase 10 entry first**, then Phase 8's "Findings that change later phases" and
Phase 9's — and `.claude/migration/COASTS-TODO.md`'s status lines, which record
what coasts 4.7.0 now provides.

Execute Phase 11 only. End the session by appending a Phase 11 entry to
`.claude/migration/STATE.md`.

## Where things stand

Phases 0–10 are complete. Phase 10 upstreamed five items to `peskas.coasts`
(PR #17, merged as `989049c`) and they shipped in **release v4.7.0**
(`597003c`). Timor is **already unpinned** — `.github/workflows/data-pipeline.yaml`
resolves `repos/WorldFishCenter/peskas.coasts/releases/latest` at container build
time — so PLAN's "unpin coasts" item is **already done**; just read the resolved
ref out of the build log rather than re-doing it.

**The dev-run gate is already met.** Run
[31778254836](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/actions/runs/31778254836)
(2026-08-14, on this branch at `0573d4f`) logged `Resolved peskas.coasts ref:
v4.7.0` and went **green on all thirteen jobs**, including the four that carry
the tinytest suites. Timor's existing code works against the upstreamed hub.
What that run does *not* prove is that the hub versions are wired in — Timor's
own definitions still win over the imports — so the deletions below still need
their own run.

**coasts has since shipped v4.8.0**, so your next build resolves that, not
4.7.0. It is immaterial here, verified rather than assumed: v4.7.0..v4.8.0
touches only `summarize_data()` and `export_portal()`, and the only mentions of
either in this repo are comments in `R/api.R`, `R/pds-tracks.R`, `R/pds-maps.R`,
`inst/config.yml` and `data-pipeline.yaml`. All five Phase 10 commits are
ancestors of v4.8.0. One further commit on coasts `main`, `dedf4d6` (removes the
dead `fetch_assets()`), is **unreleased**; harmless either way, since every
`fetch_assets` reference in the country repos is a local definition, not a
`coasts::` call.

Do not read Timor's **`main`** pipeline runs as evidence about the hub: `main`
is pre-migration and has no `COASTS_REF` at all, so its green production runs
say nothing about coasts compatibility.

Branch: `feat/align-coasts-phase9`, which despite the name carries Phases 9 and
10's documentation. Decide early whether Phase 11 continues on it or branches
off it; either is fine, but **do not merge to `main` until the very end.**

## What 4.7.0 makes deletable — and what it does not

Delete only after a green dev run against 4.7.0 proves the hub versions work.

| delete from Timor | replace with | evidence it is equivalent |
|---|---|---|
| `kobo_request()`, `kobo_validation_url()`, `list_validation_statuses()`, `get_validation_status()`, `update_validation_status()` in `R/validation-functions.R` | `coasts::` equivalents | Phase 10 verified live: 22,250 rows in 22.3 s, `all.equal()` TRUE against Timor's implementation |
| the **second** upload in `ingest_assets()` (`R/ingestion.R`) — the hand-rolled hub mirror | nothing; coasts 4.7.0 writes the hub itself (C11) | verified by running it off the branch: the object landed in `peskas-coasts-dev` |
| `timor_assets()`'s `metadata.airtable.form_ids` filter | `country == "Timor-Leste"` (C13) | measured identical: **60 taxa, 9 gears, 2 vessels** either way. The value is trimmed upstream, but assert it |

**Do not delete `add_version()`** — Timor keeps it deliberately; coasts exports a
body-identical copy and delegating would mean editing ~40 call sites plus three
`inst/report/` drivers for no behavioural gain. The reason is in CLAUDE.md.

Timor's local copies winning over the hub's is **not** a bug while both exist —
a package's own definitions take precedence over an import — so a green run
before deletion proves 4.7.0 does not break Timor, not that the hub versions are
wired in. Those are two separate tests; do both.

## The rest of the phase

- **Legacy config keys.** `inst/config.yml` is a superset; every legacy key is
  marked `# [legacy]`. Delete a key only after grepping for its last reader.
  `validation.google_sheets` and `validation.version.preprocess` lost theirs in
  Phase 5; `VALID_SHEET_ID` has had none since Phase 5.
- **Dead code**, listed in the Phase 8 STATE entry and confirmed unreferenced:
  `ingest_pds_map()`, `ingest_kepler_tracks()`, `kepler_mapper()`,
  `ingest_complete_tracks()`, `get_tracks_map()` and `inst/kepler_mapper.py`.
  `model_indicators()` has no caller either — decide deliberately, it is a
  second glmmTMB implementation someone may want. **Keep
  `sync_validation_status()`**: unwired on purpose, not dead.
- **Directories**: `auth/`, `docs/`, `inst/__pycache__`, `cran-comments.md`, and
  the untracked `peskas.mozambique.data.pipeline/` reference copy.
- **Cloud cleanup**, all deferred here: the 103,373 dead
  `pds-track-<id>__*__.csv.gz` in `pds-timor-dev`, and the 45 leaked
  absolute-path objects in `public-timor` dating from January 2026.
- **Re-enable the three `disabled_inactivity` workflows** —
  `data-report.yaml`, `dataverse-upload.yaml`, `validation-email-sender.yaml` —
  **only after** the merge. A cron fires from the default branch, so enabling
  them earlier runs `main`'s pre-migration code against production.
- **The gate**: a full green run on the phase branch (dev buckets), all four
  tinytest suites, then `Rscript data-raw/compare-portal-json.R` against the
  Phase 0 golden. Then merge, then watch one production run end to end.

## Load-bearing points

- **`data-raw/convert-pds-tracks.R` against production is mandatory before the
  merge.** `pds-timor` (prod) holds **zero** `pds-tracks_*.parquet`; without the
  conversion the first production run re-fetches ~98k tracks from the PDS API.
  Phase 10's C18 guard does **not** catch this: after the prefix filter the
  listing is empty, which takes the early `character(0)` return and is
  indistinguishable from a fresh bucket. `pds-timor-dev` is fully converted
  (101,960 objects, measured 2026-08-13), so dev runs are cheap.
- **`data-raw/freeze-landings-v1.R` against production is also mandatory** —
  `merge_landings()` reads a frozen v1 snapshot that exists in `timor-dev` only.
- **Timor's first `peskas-api-prod` write is a separate decision.** It is the two
  `if: ${{ !endsWith(github.ref, '/main') }}` lines on the API export steps.
  Deleting them is not implied by "merge to main".
- **`log_threshold = logger::INFO` at the `coasts::` call sites.** C21 is fixed
  upstream and the fix is in 4.7.0, so removing the argument is now *safe* — but
  it costs nothing and protects against a future regression. PLAN left the call
  to this phase; make it deliberately either way.
- **The portal contract is seven objects** and `portal-*.json` numbers are
  host-sensitive at the fourth decimal — read a numeric diff with tolerance and
  let the structural assertions carry the weight.
- Harness traps, all cost time before: `devtools::load_all()` reaches neither
  `coasts::read_config(package = )` nor `furrr`/`future` workers — use
  `devtools::install()` + `library()`; the tinytest suites need
  `dotenv::load_dot_env('<repo>/.env')` in the same `Rscript` call; and
  `add_version()` only stamps a git sha when the working directory is inside the
  repo.
- **`R CMD check` does not catch a call that omits a required argument** —
  `codetools` does not report it. Phase 10 broke a caller this way and only
  runtime surfaced it. When you change a signature, grep every call site.
- **Two lessons from Phase 10's fallout in the country repos**, both of which
  cost live pipeline runs and both avoidable:
  1. **Never write an elision inside code someone may paste.** A recommended
     snippet contained `str_detect(.data$form_id, ...)`, where `...` stood for
     "the existing pattern". It was pasted literally into Mozambique and
     Zanzibar; inside a `purrr` `~` lambda `...` is the lambda's own arguments,
     so `str_detect()` received the tibble as its pattern and **every
     preprocessing run aborted** until it was fixed. Write the whole line or
     name a variable.
  2. **`select(-c("x"))` errors when `x` is absent; `-any_of()` does not.**
     Stripping `country` at read time made `map_surveys()`' existing
     `select(-c("form_id", "district_code", "country"))` fail in both repos.
     When you remove a column upstream, grep for every negative select that
     names it.

## Things Phase 10 learned about the hub repo, if you go back into it

- `peskas.coasts/.github/workflows/data-pipeline.yaml` is `on: push:` with **no
  branch filter**, so pushing *any* branch there runs the whole coasts pipeline;
  on `main` it runs against **production**.
- On 2026-08-13 a merge to coasts `main` produced **no workflow runs at all** —
  GitHub created one check suite and concluded it `skipped`, with Actions
  enabled, no `[skip ci]` token and no filter excluding it. Cause unexplained.
  If a coasts release ever fails to appear after merging a NEWS bump,
  `gh release create` by hand is equivalent: every country resolves the version
  via `repos/.../releases/latest`.
- The assets snapshot is **data**, read at `version = "latest"`, so a schema
  change there reaches Mozambique, Kenya and Zanzibar on their next cron
  regardless of any release. All three carry a read-time
  `-any_of(c("country","latitude","longitude"))` drop as of 2026-08-13.

## Carried user actions

Rotate the credentials exposed in past CI logs (four repos' run history);
rotate `ANTHROPIC_API_KEY`; add the 27 missing IMEIs to PESKAS | FRAME (worth
2,791 trips and 59 landing↔trip matches); delete the obsolete `AIRTABLE_KEY`
GitHub secret; and re-push `AIRTABLE_TOKEN` if its value changed locally.
