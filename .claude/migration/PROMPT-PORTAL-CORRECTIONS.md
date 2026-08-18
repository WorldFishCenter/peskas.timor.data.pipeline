# Prompt for the next session — portal corrections (pre-cutover)

Copy everything below the line into a fresh Claude Code session started in the
**`peskas.timor.data.pipeline`** working directory.

**Send this BEFORE `PROMPT-PHASE11.md`.** Two published numbers are wrong; both
fixes are small, both are decided, and doing them first lets Phase 11's own dev
run be asserted as *zero portal change*, which is a far stronger gate on a
deletion phase than a diff mixing three unrelated changes.

---

Portal corrections — two isolated fixes, before the Phase 11 cutover

Read first: `CLAUDE.md`, then `.claude/migration/ALIGNMENT-AUDIT.md` §11 and §15.
Everything below is measured there; do not re-derive it, but **do** re-assert
each number before and after your change.

You are making **two commits**. Each gets its **own push, its own dev run and
its own portal diff.** Do not bundle them — the entire point is that each
published delta is attributable to one change. Do not start Phase 11 in this
session, and do not merge to `main`.

Branch: `feat/align-coasts-phase9`. Every workflow sets `R_CONFIG_ACTIVE=production`
only on `main`, so a branch push exercises the whole pipeline against the `-dev`
buckets. That is the test mechanism. A run is ~35 minutes.

## Commit 1 — the coast rule

**The bug.** `portal-summary_data` is built from two different "North Coast"
definitions. `get_summary_data()` in `R/format-public-data.R` classifies by
municipality **plus a site-level rescue list**; `estimated_revenue` in
`R/export.R:377` classifies by municipality only, because by then the model has
already collapsed the data to municipality and the site is gone.

**`format_public_data()` is the correct one.** Coast is a property of the *site*.
Measured over the 40 sites and 76,379 landings in the current `all_trips`,
exactly two municipalities contain sites on both coasts:

| municipality | North | South | % north |
|---|---|---|---|
| Lautem | 4,403 (Com 2,059 · Tutuala 1,514 · Ililai 827 · Sentru/Liarafa 3) | 2 (Lore) | 99.95 % |
| Manatuto | 2,919 (Comando) | 1 (Welaluhu) · Natarbora 0 | 99.97 % |

`export.R` misfiles **Lautem's 14.25 % of national revenue** into South Coast.
Its `"Manatuto"` entry is a crude compensation for not being able to see
Comando; what it is actually missing is Lautem.

**The change: add `"Lautem"` to the vector at `R/export.R:377`.** That is the
whole fix. The two rules then agree on all but **3** of 76,379 landings
(Welaluhu 1, Natarbora 0, Lore 2).

**Do not** try to derive coast from coordinates. The frame carries lat/lon for
all 40 Timor sites, but the two classes overlap — north-coast sites span
−9.310 to −8.358 and south-coast −9.350 to −8.645, because Oecusse is an exclave
whose north-facing sites sit further south than most of the south coast. Verified;
a latitude threshold misclassifies it.

**Expected portal delta**, from the newest published `portal-municipal_aggregated`:

| Area | before | after |
|---|---|---|
| Atauro island | 13,369,136 (7.9 %) | 13,369,136 (7.9 %) |
| North Coast | 109,079,352 (**64.8 %**) | 133,087,662 (**79.0 %**) |
| South Coast | 45,995,276 (**27.3 %**) | 21,986,965 (**13.1 %**) |

`summary_data$estimated_revenue` is the **only** key that may move. Assert that
`n_surveys`, `estimated_tons`, `catch_habitat`, `revenue_habitat`,
`nutrients_per_catch`, `nutrients_habitat`, `conservation` and `region_cpue` are
unchanged, and that the other six portal objects are byte-identical in structure.

The durable version — one site→coast table read by both call sites, replacing
the five hardcoded site names — is **Phase 12, not this session.** Leave a
comment at both call sites pointing at ALIGNMENT-AUDIT §11.

## Commit 2 — `registered_boats` from the frame

**The decision.** The user ruled 2026-08-18 that **Airtable is authoritative**.
The Sheets `registered_boats` says Manatuto 283 and Viqueque 213; the frame's
`geo.total_boats` says 213 and 207. The other ten of twelve are byte-identical.
So the live portal is over-reporting, and this is a correction to publish.

**The change.** `estimate_fishery_indicators()` (`R/model-fishery.R:32`) reads
`get_preprocessed_sheets(conf)$registered_boats`. Replace that with the frame.
`model_indicators()` at line 654 reads the same thing and has no caller — if
Phase 11 deletes it, this line goes with it; if you touch it here, keep the two
consistent.

The replacement needs a **four-case recode, not an accent strip**:
`iconv(x, "UTF-8", "ASCII//TRANSLIT")` yields `Laut'em` and `Liquic'a` on macOS
and matches nothing. Write the accented names literally, from the snapshot:

```r
frame_boats <- timor_assets(get_assets(conf)$geo, conf) |>
  dplyr::filter(!is.na(.data$total_boats)) |>
  dplyr::transmute(
    reporting_region = dplyr::case_when(
      .data$gaul_2_name == "Atauro"  ~ "Atauro",   # Timor's own reporting unit
      .data$gaul_1_name == "<Liquica with its cedilla and acute>" ~ "Liquica",
      .data$gaul_1_name == "<Lautem with its acute>"              ~ "Lautem",
      .data$gaul_1_name == "Oecussi"                              ~ "Oecusse",
      TRUE ~ .data$gaul_1_name
    ),
    n_boats = as.integer(.data$total_boats)
  )
```

The two placeholders are the frame's accented spellings of Liquica and Lautem.
**Read them out of the live snapshot and paste the real strings** — they are
deliberately not written literally here, because an elision or a mangled
character pasted verbatim is exactly how Phase 10 broke two country pipelines
(see PROMPT-PHASE11.md, "Two lessons from Phase 10's fallout").

**Assertions before you push** — all four were verified 2026-08-18 and must
still hold:

1. `identical(sort(frame_boats$reporting_region), sort(sheets$reporting_region))`
   — the 12 keys match exactly.
2. All 12 distinct `municipality` values in `all_trips` resolve against
   `frame_boats$reporting_region`.
3. Only Manatuto (283 → 213) and Viqueque (213 → 207) change value.
4. National `n_boats` 3,872 → 3,796 (−1.96 %).

**Expected portal delta.** `catch = landing_catch * n_landings_per_boat *
n_boats` is strictly linear, so:

| region | share of national catch | factor | delta |
|---|---|---|---|
| Manatuto | 18.54 % | 213/283 | **−24.7 %** |
| Viqueque | 5.55 % | 207/213 | **−2.8 %** |
| **national** | | | **−4.74 %** |

Note the two figures differ for a reason: boats −1.96 %, catch −4.74 %, because
catch is raised per municipality and Manatuto is the second-largest.
`portal-municipal_aggregated`, `portal-aggregated`, `portal-taxa_aggregated`,
`portal-municipal_taxa`, `portal-nutrients_aggregated` and
`summary_data$estimated_tons` / `$estimated_revenue` all move. Assert no *other*
region changes and no column or key set changes.

**Do not** delete the `registered_boats` entry from
`metadata.google_sheets.tables` in this session — that is Phase 11's, after this
change has had a green run.

## Gate for both commits

`Rscript data-raw/compare-portal-json.R` — it asserts object names, keys,
nesting, column sets and column types against the Phase 0 golden and prints
per-column numeric summaries. Row counts and column order are reported, not
failed. Portal numbers are host-sensitive at the fourth decimal; read numeric
diffs with tolerance and let the structural assertions carry the weight.

Harness traps that have cost time before: `devtools::load_all()` reaches neither
`coasts::read_config(package = )` nor `furrr`/`future` workers — use
`devtools::install()` + `library()`; the tinytest suites need
`dotenv::load_dot_env('<repo>/.env')` in the same `Rscript` call.

## Rules

- **Never set `R_CONFIG_ACTIVE=production`.** Two pushes, two dev runs.
- **Do not merge to `main`** and do not run
  `data-raw/freeze-landings-v1.R` or `data-raw/convert-pds-tracks.R` — the freeze
  belongs *after* Phase 11 so it is built by the final code, and the track
  conversion is the user's to launch.
- **Do not weaken a test assertion** to make a diff pass. If a tinytest suite
  fails, that is the finding.
- End the session by appending a STATE.md entry with the two measured deltas.
