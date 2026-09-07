# Prompt for the next session — Phase 10

Copy everything below the line into a fresh Claude Code session started in the
**`peskas.timor.data.pipeline`** working directory (it needs Timor's migration
docs in context, even though the work happens in the coasts repo).

---

Phase 10 — upstream to coasts

Repos: the work happens in `peskas.coasts`
(`~/Desktop/work/wf_projects/peskas.coasts`, currently at `58a5fdf`, one commit
past release **v4.6.0**). The migration log lives in
`peskas.timor.data.pipeline`, branch `feat/align-coasts-phase9` (committed at
`fc8bc2c`, pushed, CI green end to end twice). Branch coasts work off its default
branch, one branch per PR; do **not** branch Timor.

Read in order before doing anything: `CLAUDE.md`,
`.claude/migration/PLAN.md` (§Phase 10 was re-scoped 2026-08-12 against the repo
as it actually stands — read that wording, and the table of five candidates),
`.claude/migration/COASTS-TODO.md` **in full** — it is the whole input to this
phase, C1 through C21 — then `.claude/migration/STATE.md` (the Phase 5, 7, 8 and
9 entries, plus the addenda), and `peskas.coasts/CLAUDE.md` on the other side.

Execute Phase 10 only. Do not start Phase 11. PLAN budgets it at 1–2 sessions
and rates it medium risk. The rating is right and the risk is worth naming
precisely: **every country pipeline resolves the latest coasts *release* at
container build time.** A tagged coasts release reaches Timor, Mozambique, Kenya
and Zanzibar on their next build with nobody's approval, and three of those are
in production. A regression here is not one phase lost, it is four pipelines.
Prefer additive APIs and new function names over changing the behaviour of a
function three countries already call.

End the session by appending a Phase 10 entry to Timor's
`.claude/migration/STATE.md` in the template's format, even though the code
landed elsewhere — that file is the migration's only continuous record. Update
COASTS-TODO.md's status lines for whatever you close.

## Where things stand

Phases 0–9 are done. Config, secrets, container, storage, ingestion,
preprocessing, validation, the cross-country API export, PDS, the country
modules and now CI, repo metadata and docs are all on the harmonized standard.
The survey path is parquet end to end, the portal emits seven objects and has a
gate script, and `data-pipeline.yaml` is thirteen jobs on `ubuntu-latest` /
`checkout@v5` / `build-push-action@v6`, green.

What is left after this phase is Phase 11: strip the legacy config keys and dead
code, delete Timor's local copies of whatever this phase upstreams, run the two
one-off production scripts, merge to `main`, watch one production run.

## What you inherit

**Already delivered, do not re-do.** Retry wrappers shipped in coasts 4.6.0 as
`insistent_upload_cloud_file()` / `insistent_download_cloud_file()` (C5) and
Timor's copies are gone. `resolve_storage_opts()` learned `"public"` and
`"api"` (C6, C16). Timor is registered in the hub's `api.trips` block and its
PDS customer list (C7). `read_config()`'s secrets leak was fixed by the user in
all three packages on 2026-08-12 (C21).

**The five candidates**, with the question that decides each — PLAN §Phase 10
carries the same table:

- `get_validation_status()` / `update_validation_status()` (**C15**) — the
  clearest win. Every country writing flags to the shared validation database
  needs them; they exist only in Mozambique's repo; and Moz's copy has two live
  bugs Timor fixed while porting: `httr2` throws on the 404 KoBoToolbox returns
  for a never-validated submission, so its `not_validated` branch is
  unreachable and every such submission is recorded as `fetch_error`; and it
  issues one request per submission where the data endpoint returns 1,000 —
  measured at 65 requests / ~70 s against more than twenty minutes for 7,776
  submissions.
- `validate_pds_trips()`'s consecutive-trip merging and outlier logic — no
  coasts equivalent, and `coasts::merge_survey_trips()` does a different job
  (C10).
- The richer validators — landing regularity, mesh, gleaners, fuel,
  conservation, happiness. Decide which are Timor-specific by *form* rather than
  by intent.
- Nutrients / RDI — `coasts::enrich_taxa()` emits six nutrients against Timor's
  seven, no selenium, no unit conversion, and no FAO food-composition override
  for the six invertebrates FishBase cannot estimate. Upstreaming means changing
  a function three countries already call, so decide whether this is an
  extension or a fork before writing code.
- Dataverse publishing — nobody else does it. "Timor-only by nature" is an
  acceptable answer.

**Still open upstream, and some are better fixed while you are in that repo than
filed again:** C11 (`ingest_assets()` writes to the country bucket while every
reader resolves the hub — Timor mirrors by hand), C13 (the assets snapshot has
no `country` column, so every read must be narrowed by form id), C17
(`summarize_data()` resolves two hub artefacts from the country bucket — the
only thing gating Timor from the coasts portal path), C20
(`preprocess_pds_tracks()`'s first pass reads every track with
`detectCores() - 1` workers, one on a CI runner), C18, C19, C12. C17 and C20
together are why Timor does not run `preprocess_pds_tracks()` at all.

## Load-bearing points

Timor keeps `log_threshold = logger::INFO` at both `coasts::` call sites in
`data-pipeline.yaml` regardless of C21 being fixed upstream, because the
container resolves whatever release is latest at build time. If a release ships
during this phase, note it — but removing that argument is Phase 11's call, not
this one's, and it costs nothing to keep.

Do not delete Timor's local copy of anything you upstream. Timor re-pins and
deletes in Phase 11, after the release exists and after a green run against it.
An upstreamed function that Timor still defines locally is not a bug; a Timor
that calls a hub function which is not in any release yet is.

COASTS-TODO.md has an "Acceptance gate for any replacement" section. It was
written for the taxa/weight path but the shape of it applies to every item here:
a replacement is only a replacement if it reproduces the numbers, and the
numbers Timor publishes are in STATE.

One PR per item, each with its own branch and its own description of what it
changes for the three countries that did not ask for it.

## Carried user actions, none of them this phase's work

Rotate the credentials exposed in past CI logs (and in coasts' logs before
2026-08-12); rotate `ANTHROPIC_API_KEY`; run
`R_CONFIG_ACTIVE=production Rscript data-raw/freeze-landings-v1.R` and
`data-raw/convert-pds-tracks.R`; add the 27 missing IMEIs to PESKAS | FRAME;
delete the obsolete `AIRTABLE_KEY` GitHub secret (verified dead against
`origin/main` in Phase 9); and re-push `AIRTABLE_TOKEN` if its value changed
locally.
