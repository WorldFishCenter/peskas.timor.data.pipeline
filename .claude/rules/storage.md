---
paths:
  - "R/get-cloud-files.R"
  - "R/pds-tracks.R"
---

# Cloud storage

GCS project `peskas`, service account
`data-ingestion@peskas.iam.gserviceaccount.com` — it can read/write objects but
**cannot** `storage.buckets.list`.

Versioned naming, from `add_version()`:
`<prefix>__<YYYYMMDDHHMMSS>_<git-sha7>__.<ext>`. The sha comes from
`git2r::sha(git2r::last_commit())`, falling back to `$GITHUB_SHA` in
containers.

**The layer is `coasts::*`.** Every call site is namespaced
`coasts::{cloud_storage_authenticate, upload_cloud_file, download_cloud_file,
cloud_object_name, insistent_upload_cloud_file}`. **Always write the prefix** —
Timor does not export these names, so an unqualified call resolves off the
search path or not at all.

**`add_version()` is Timor's, deliberately.** A naming helper, not a storage
function. `coasts` exports a body-identical copy, but delegating means editing
~40 call sites plus the report drivers to remove an exported name for no
behavioural gain. Dedupe when it is upstreamed, not before.

**`coasts::cloud_object_name()` returns one name** (`selected_rows$name[1]`).
Use **`coasts::cloud_object_names()`** (plural, with `latest_only`) to
enumerate a bucket — `get_tracks_descriptors()` and
the one-off track converter did. Never use the singular for that: it
silently returns one name, and on the PDS path that reads as "no tracks stored"
and re-fetches the entire history from the API. Listing that bucket is not
cheap — ~100k objects at 1,000 per request, three to thirty minutes. Do it once
per function and pass the vector down, as `describe_pds_tracks()` does.

Prefer `coasts::resolve_storage_opts(conf, type)` over reaching into
`conf$storage$google$options_*` by hand: it knows `"coasts"` (hub, falling back
to `options`), `"country"`, `"pds"` and `"public"`.

| bucket | contents |
|---|---|
| `timor` / `timor-dev` | surveys and derived tables, all parquet: `timor-landings-v{2,3}_{raw,preprocessed}`, `timor-landings-merged`, `timor-landings-v1-frozen`, the weight artefact, `timor-landings-merged_validated_long`, plus `asfis__*` and `tracker-imeis__*` |
| `pds-timor` / `pds-timor-dev` | one parquet per GPS trip, `pds-tracks_<trip_id>.parquet`, from `coasts::ingest_pds_tracks()`. **Not versioned** — a re-ingest overwrites |
| `public-timor` / `public-timor-dev` | `portal-*.json`, the live portal contract |
| `peskas-coasts` / `peskas-coasts-dev` | the shared cross-country hub (`options_coasts`), read **and written** by coasts. Both are live — `default` must stay on `-dev` |
| `peskas-api-prod` / `peskas-api-dev` | cross-country API parquet (`options_api`) |

**No lifecycle policy is set.** Every run appends and nothing is deleted;
`gs://timor` holds ~33k objects, `gs://pds-timor` ~98k. `gs://public-timor`
also carries 45 objects named with a full local path (`//tmp/Rtmp…`), residue
of a January 2026 upload bug, invisible to the portal because their names do
not begin with `portal-`.

## Interchange format

**Flat long parquet** from raw through validated: one row per (submission,
catch, length bin). No `.rds` remains on the survey path.
`get_validated_landings()` re-nests on read, so `format_public_data()`,
`estimate_fishery_indicators()` and `merge_trips()` did not have to change. The
two shapes were proven interchangeable first — 97,360 submissions, 1,648,016
catch rows, `all.equal` TRUE, the only difference 1,599 catch weights moving by
≤2.9e-11 g on the grams → kg → grams round trip.

Two artefacts stay `.rds`, both deliberately:

- **`all_trips__*.rds`** has no cross-country counterpart, so flattening it
  would buy no harmonization while requiring `format-public-data.R`,
  `model-fishery.R` and `pds-maps.R` to be rewritten against a live portal.
- **`pds-trips_validated__*.rds`** carries `tracker_trip_start` / `_end` as
  `Asia/Dili` POSIXct and `merge_trips()` derives `landing_date` from
  `tracker_trip_end`, so a parquet round trip is a timezone risk on the exact
  code path commit `15f6b18` exists to protect — for no gain, since only
  `merge_trips()` and `test_validated_pds_trips.R` read it.
