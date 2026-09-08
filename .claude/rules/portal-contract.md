---
paths:
  - "R/export.R"
  - "R/format-public-data.R"
  - "R/pds-maps.R"
  - "R/api.R"
  - "R/portal-contract.R"
  - "inst/tinytest/test_portal_contract.R"
---

# Portal contract — do not break

`export_files()` writes **seven** objects as `portal-<name>__*__.json` to
`public-timor`:

```
aggregated  taxa_aggregated  municipal_aggregated  municipal_taxa
nutrients_aggregated  data_last_updated  summary_data
```

`peskas.timor.portal.v2/scripts/fetchData.js` lists `portal-*` in the bucket,
keeps the newest version of each, and **excludes** `indicators_grid.json`,
`label_groups_list.json`, `pars.json`, `taxa_names.json`,
`var_dictionary.json` — so these seven are exactly the live set.

The portal discovers files dynamically, meaning a **renamed or dropped object
silently disappears from the site**. `check_portal_contract()` is the guard, and
`inst/tinytest/test_portal_contract.R` runs it in `export-trips` on every
pipeline run, so there is no manual step to remember:

```r
check_portal_contract(
  baseline = "gs://public-timor@20260731031516_90ede9a",
  candidate = "latest",
  allow_dropped = c("portal-indicators_grid", "portal-label_groups_list")
)
```

It asserts object names, then keys, nesting, column sets and column types, and
returns the failures. Row counts and column order are reported, not failed —
the portal reads an array of objects by key; `numbers = TRUE` adds per-column
numeric summaries, which never fail, because accepting a moved number needs a
human. Either side may be a local directory, a bucket, or `latest`, so no
golden copy has to be kept in step.

That baseline is the frozen shape, production run 90ede9a of 2026-07-31; pin
the **full** version, since a bare sha is shared by every run of that commit.
Those nine objects are still in `gs://public-timor`, nothing deletes from that
bucket and no lifecycle policy is set. `latest` is the newest set in whichever
public bucket the active config resolves, so the same line works on dev and
production.

**`data-pipeline.yaml` runs exactly this after `export_files()`**, so a
structural break fails the run instead of waiting to be noticed on the site.

One limitation, which is the portal's too: a bucket set is the newest version of
each name, so if `export_files()` stopped writing an object the previous version
stays newest and the name check cannot fire. That is why dropping
`indicators_grid` was harmless. Comparing two local directories does catch it. They predate three deliberate corrections, so
they *should* differ numerically — compare against the newest
`public-timor-dev` set when you want like-for-like numbers.

`landing_site` appears in **none** of the seven; it reaches the cross-country
API only. `municipality` **is** published, as `region` in
`municipal_aggregated` / `municipal_taxa`.

## Timezone

Fixed in commit `15f6b18` — re-verify after any rewrite here. It lives in
`summarise_estimations()`, and the point is that `today` must be built in the
**data's** timezone, not the session's, or
`floor_date(today, "month") == date_bin_start` silently matches nothing and the
current month is published unscaled.

## The cross-country API

`export_api_raw()` / `export_api_validated()` write a **22-column** trips table
to `peskas-api-{dev,prod}/timor/{raw,validated}`. That schema is the contract
Kenya, Mozambique and Zanzibar already publish: **do not add, drop or reorder a
column without agreeing it across all four.**

## What this path reads

`format_public_data()` reads exactly two things, `get_merged_trips()` and
`get_models()`. The nested `landing_catch` / `length_frequency` shape it works
in is produced on *read* by `get_validated_landings()`, not stored.
