# Prompt for the next session — Phase 11b: prepare the cutover

Copy everything below the line into a fresh Claude Code session started in the
**`peskas.timor.data.pipeline`** working directory.

**This is the session that readies production for the cutover — but does not
perform it.** The user commits, opens the PR and merges by hand; see "The rule
that shapes this whole session" below. Do not send this prompt until
`PROMPT-PHASE11A.md` is done and its dev run is green with zero portal change.

---

Phase 11b — prepare the cutover, and hand it over

Read in order: `CLAUDE.md`; `.claude/migration/STATE.md` — the Phase 11a entry
first, then the 2026-09-04 production dry-run entry, then the alignment-audit
entry; `.claude/migration/PLAN.md` §Phase 11.

End the session by appending a Phase 11b entry to `.claude/migration/STATE.md`.

## The rule that shapes this whole session

> **You do not commit, you do not push, and you do not merge.**
>
> The user commits, opens the PR and merges, by hand, when they choose. Your job
> is to get the branch into a state where that is a one-step decision, and to
> hand over what they need to make it: what changed, what to watch on the first
> production run, and how to undo it.
>
> This is deliberate and was decided 2026-09-04. **The merge is the moment the
> live portal steps to the migrated numbers** — catch and tonnage roughly −22%,
> price/kg +22%, nutrient supply −24% to −38%. That is a bias correction the user
> has approved in principle, but publishing it is theirs to time, because people
> read that dashboard.
>
> Everything downstream of the merge — the production bucket cleanup and
> re-enabling the three disabled workflows — is therefore **not this session's**.
> It is listed at the bottom, gated on the first green production run.

## Entry gate — all four were met on 2026-09-04

**Re-verify cheaply, do not re-derive.** Each was measured in the 2026-09-04
session and recorded in the STATE entry of that date:

| gate | state | evidence |
|---|---|---|
| 1. Phase 11a green, zero portal change | ✅ | run **32152744776**, 13/13 jobs; `compare-portal-json.R` 0 structural failures, **0 of 84** numeric columns moved |
| 2. Both portal corrections in | ✅ | verified in the published dev JSON: North Coast revenue carries Lautem; Manatuto 213 boats, Viqueque 207 |
| 3. `convert-pds-tracks.R` against production | ✅ | `gs://pds-timor` holds **97,830** `pds-tracks_*.parquet`; all 97,830 legacy `.csv.gz` intact |
| 4. `main` a clean fast-forward | ✅ | `git rev-list --left-right --count` → left side 0 (re-check, it is the one that can go stale) |

Only gate 4 can rot. Re-run that one command; take the other three from the
record unless something looks wrong.

<details>
<summary>The original gate text, kept for the reasoning behind gate 3</summary>

1. **Phase 11a is merged into this branch and its run was green** on all thirteen
   jobs, with `compare-portal-json.R` showing **zero** portal change.
2. **Both portal corrections are in** — the coast rule (`export.R`, Lautem into
   North Coast) and `registered_boats` reading the frame's `geo.total_boats`.
3. **`data-raw/convert-pds-tracks.R` has been run against production.** Verify by
   listing: `gs://pds-timor` must hold `pds-tracks_*.parquet`. It held **0** on
   2026-08-18 against 97,827 legacy `.csv.gz`. Without it the first production run
   computes `new_trip_ids` as **all ~95,700 trips** and re-fetches the entire
   history from the shared PDS API with `detectCores()-1` workers on a CI runner.
   Phase 10's C18 guard does not catch this: after the prefix filter the listing
   is empty, which takes the early `character(0)` return and is indistinguishable
   from a fresh bucket.
4. `main` is still a clean fast-forward from the branch (`git rev-list
   --left-right --count main...<branch>` — left side 0).

If gate 3 fails, **stop and tell the user.** It is theirs to run and it takes
hours.

</details>

## 1. The v1 freeze — ✅ DONE 2026-09-04

`gs://timor/timor-landings-v1-frozen__20260904115114_f38f31c__.parquet`, built by
post-11a code, byte-identical in size to the dev snapshot the pre-11a code built.
**Nothing to do here.** The rest of this section is why it had to run when it
did, and the command trap that nearly sent it to the wrong bucket.

```
R_CONFIG_ACTIVE=production Rscript data-raw/freeze-landings-v1.R   # DOES NOT WORK
```

**That documented command silently runs against `-dev`.** `.Renviron` in the repo
root pins `R_CONFIG_ACTIVE=default` and R applies it *after* the inherited
environment, so the variable is overwritten at startup. Verified 2026-09-04. Use
either of these instead, both confirmed to resolve `timor` / `pds-timor` /
`public-timor`:

```
R_ENVIRON_USER=/dev/null R_CONFIG_ACTIVE=production Rscript data-raw/freeze-landings-v1.R
```

or, in an interactive session, `use_prod()` from `.Rprofile` — it calls
`Sys.setenv()` after startup, which beats `.Renviron`. CI is immune: `.Renviron`
is gitignored and never reaches the image.

`merge_landings()` reads `timor-landings-v1-frozen__*.parquet` unconditionally
inside `bind_rows()`. `gs://timor` held **0** on 2026-08-18, so without this the
first production run of migrated code fails at `merge-landings` and every
downstream job is skipped — the portal stops updating rather than publishing
anything wrong.

**It runs here, not earlier, and that ordering is deliberate.** The freeze
produces a *permanent* artefact — made once, read forever — and it is built by
package code: `get_preprocessed_sheets()`, `get_taxa_list()`, and four internals
via `:::` (`sum_fishers()`, `mesh_size_mm()`, `resolve_catch_taxa()`,
`resolve_survey_labels()`). Running it before 11a's deletions would commit a
permanent production object built by code that no longer exists.

Verify all four of those internals still exist after 11a before you run it. Then
check the script's own `stopifnot(nrow(v1) == 10117)` passes, and confirm the
object landed.

This is an **additive** write on a prefix `main`'s pre-migration code does not
read, so it is invisible to the currently-running production pipeline.

## 2. Prepare the merge — then stop and hand over

**You do not commit, push, open the PR, or merge.** Leave the working tree with
your changes unstaged or staged, whichever is tidier, and hand the user a
summary. If the tree is already clean and nothing needs changing, say so — "the
branch is ready to merge as it stands" is a perfectly good outcome for this
session.

What to hand over, in your final message:

1. **What the merge publishes.** The first production run after it writes the
   seven `portal-*.json` objects, and the live dashboard steps to: catch and
   tonnage **≈ −22%**, price/kg **≈ +22%**, nutrient supply **−24% to −38%**,
   North/South Coast revenue redistributed by the Lautem fix. Measured
   2026-09-04 against the live set, not predicted. Labels and categories are
   unchanged, so nothing on the site should break or empty.
2. **A draft PR description** they can paste — what changed across Phases 0-11a,
   the measured portal deltas, and the rollback.
3. **What to watch on the first run**: all thirteen jobs green; the
   `Resolved peskas.coasts ref:` line in the build log, recorded; the seven
   `portal-*.json` written to `gs://public-timor`; and
   `portal-indicators_grid` / `portal-label_groups_list` **stopping** — expected,
   since Phase 8 dropped them and `fetchData.js` excludes both. Their existing
   versions stay in the bucket and the site does not change because of it.
4. **The rollback, in one line.** Nothing is ever overwritten and the portal
   keeps the newest version of each name, so deleting the seven newly written
   objects makes the previous set newest again and the site reverts on its next
   fetch. The user has a copy of the pre-merge live set at
   `~/peskas-portal-live` (9 files, `20260903032053_90ede9a`).
5. **`release.yaml` fires on the push to `main`** and cuts `v4.0.0` from the top
   block of `NEWS.md`. Not a side effect to discover afterwards.

## 2b. What is already done, so you do not redo it

The 2026-09-04 session ran the **entire production pipeline locally except the
publish** — 19 stages, all green, all four tinytest suites green on production
data. So `gs://timor` already holds migrated raw, preprocessed, merged,
weighted, validated, `all_trips`, models and `timor_*` artefacts, and
`validation-prod` holds migrated flags. **The portal was not published**: the
newest `portal-*` in `gs://public-timor` is still `90ede9a`.

That means the first post-merge run re-treads warm ground, and it also means
production is currently in a mixed state — migrated artefacts, `main`'s old code
still on the schedule. `main` rebuilds everything it reads within each run, so
this is stable rather than urgent, but it is a reason not to leave the merge
sitting for weeks.

`export_files()` was rehearsed with `coasts::upload_cloud_file` stubbed in the
coasts namespace, which is a clean way to exercise the publish path without
touching the bucket — reuse it if you need to re-measure. The script is at
`scratchpad/prodrun/export-dryrun.R` in that session's notes; the technique is
three lines and is written up in the STATE entry.

# ==========================================================================
# AFTER THE MERGE — not this session
# ==========================================================================
#
# Both of the following are gated on the user having merged **and** on one green
# production run. Since this session neither commits nor merges, neither can
# happen here. Carry them into a short follow-up session, or hand them to the
# user as steps. Do not do them early: §4 in particular is actively unsafe
# before the merge.

## 3. Production cloud cleanup  *(after the merge + first green run)*

The **45** leaked absolute-path objects in `gs://public-timor`, dating from
2026-01-18 and all of the form
`/tmp/Rtmph2kG06/portal-json/portal-*__20260118223318_0d6af65__.json`. Verified
present 2026-08-18. `export_files()` already passes basenames and normalises
correctly — these are historical residue, not a live bug.

Delete them **after** the first green production run, not before: if anything
goes wrong you want the bucket unchanged apart from the new writes.

## 4. Re-enable the three disabled workflows  *(after the merge)*

`data-report.yaml`, `dataverse-upload.yaml`, `validation-email-sender.yaml`, all
`disabled_inactivity`. **Only after the merge** — a cron fires from the default
branch, so enabling them earlier runs `main`'s pre-migration code against
production.

Their prerequisites are met: `MONGODB_CONNECTION_STRING_VALIDATION` exists as a
GitHub secret (created 2026-08-10) and is wired into both `data-pipeline.yaml:31`
and `validation-email-sender.yaml:28`.

Note `data_report.Rmd` lost its boats section in 11a — re-read it before
enabling, and check the nine hardcoded Sheets gear names at its lines ~1187–1195,
which have received Title Case from the frame since Phase 5 and resolve to `NA`.
That was flagged in the Phase 5 entry and never fixed.

# ==========================================================================
# DECISIONS AND HANDOVER
# ==========================================================================

## 5. Not implied by "merge to main" — leave unless the user says otherwise

**Timor's first `peskas-api-prod` write.** It is the two
`if: ${{ !endsWith(github.ref, '/main') }}` lines on the API export steps.
The service account has object create/delete on that bucket (verified 2026-08-11
via `testIamPermissions`), so the first prod write is a decision, not a
permission. Ask; do not assume.

## 6. Three open questions, none blocking — raise them, let the user rule

Found 2026-09-04 while attributing the portal deltas. All three sit inside the
weight/nutrient change that the −22% comes from:

- **`FLY`'s per-taxon `quantile_coeff` override is gone.** Phase 4b's decision
  list (STATE Addendum 3, item 3) recorded that `estimate_weight()`'s per-taxon
  overrides "must survive any rewrite". `summarise_lw_coeffs()` is now a plain
  geometric mean with no special cases. Probably superseded by the decision to
  use a central estimate — keeping per-taxon percentiles would contradict it —
  but that supersession is written down nowhere.
- **`GZP` moves −71%**, the largest relative change, and commit `a2c2881`'s own
  message called it "worth a look" because it is a common-name rescue whose
  species set depends on what `common_to_sci("Garfish")` returns. No record that
  anyone looked.
- **The per-taxon share of the nutrient drop is unquantified.** Published supply
  falls 24% (calcium) to 38% (protein) against catch's 22.5%. Volume and the
  per-kg profile shift (−13% selenium to +9% zinc, from the 693 → 5,259 species
  expansion) explain calcium exactly; protein and zinc do not close from the
  published aggregates alone. The residual is catch composition — which taxa lost
  weight — and closing it means tracing the per-taxon tables.

## Carried user actions — report, do not perform

- Rotate the credentials exposed in past CI logs (four repos' run history) and
  rotate `ANTHROPIC_API_KEY`. Note it also sits in plaintext in the untracked
  `.Renviron`.
- Delete the obsolete `AIRTABLE_KEY` and `VALID_SHEET_ID` GitHub secrets.
- **The Airtable device gap is two jobs, not one.** Adding the 27 IMEIs to a
  frame customer is worth 2,791 trips and 59 landing↔trip matches on the *PDS*
  side, but recovers **zero** survey-side matches — measured, by matching
  submissions against all 910 `pds_devices` rows and getting results identical to
  the 457-row Timor subset. The survey-side gap is **144** Sheets IMEIs absent
  from `pds_devices` altogether, and that is what gates moving `validate_imeis()`
  off the Sheets. It is Phase 12's.

## After this phase

Phase 12 — static assets and label sources. Scoped in
`.claude/migration/ALIGNMENT-AUDIT.md` §15 and `PLAN.md`. Nothing in it belongs
here.
