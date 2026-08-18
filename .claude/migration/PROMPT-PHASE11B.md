# Prompt for the next session — Phase 11b: the cutover

Copy everything below the line into a fresh Claude Code session started in the
**`peskas.timor.data.pipeline`** working directory.

**This is the session that changes production.** Everything before it was
reversible. Do not send it until `PROMPT-PHASE11A.md` is done and its dev run is
green with zero portal change.

---

Phase 11b — cutover

Read in order: `CLAUDE.md`; `.claude/migration/STATE.md` — the Phase 11a entry
first, then the alignment-audit entry; `.claude/migration/PLAN.md` §Phase 11.

End the session by appending a Phase 11b entry to `.claude/migration/STATE.md`.

## Entry gate — verify all four before touching anything

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

## 1. The v1 freeze — first, and from the final code

```
R_CONFIG_ACTIVE=production Rscript data-raw/freeze-landings-v1.R
```

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

## 2. Merge to `main`

Fast-forward. Then watch **one full production run end to end** — ~1h35m on the
schedule, or dispatch it.

What to check on that run:
- all thirteen jobs green;
- `Resolved peskas.coasts ref:` in the build log — record which release it used;
- the seven `portal-*.json` objects written to `gs://public-timor`;
- `portal-indicators_grid` and `portal-label_groups_list` **stop** being written.
  That is expected — Phase 8 dropped them and the portal's `fetchData.js`
  excludes both. Their existing versions stay in the bucket; the newest simply
  freezes at merge time. Nothing on the site changes.

## 3. Production cloud cleanup

The **45** leaked absolute-path objects in `gs://public-timor`, dating from
2026-01-18 and all of the form
`/tmp/Rtmph2kG06/portal-json/portal-*__20260118223318_0d6af65__.json`. Verified
present 2026-08-18. `export_files()` already passes basenames and normalises
correctly — these are historical residue, not a live bug.

Delete them **after** the first green production run, not before: if anything
goes wrong you want the bucket unchanged apart from the new writes.

## 4. Re-enable the three disabled workflows

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

## 5. Not implied by "merge to main" — leave unless the user says otherwise

**Timor's first `peskas-api-prod` write.** It is the two
`if: ${{ !endsWith(github.ref, '/main') }}` lines on the API export steps.
The service account has object create/delete on that bucket (verified 2026-08-11
via `testIamPermissions`), so the first prod write is a decision, not a
permission. Ask; do not assume.

## Carried user actions — report, do not perform

- Rotate the credentials exposed in past CI logs (four repos' run history) and
  rotate `ANTHROPIC_API_KEY`.
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
