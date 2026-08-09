# coasts v4.6.0 — what changed, and what it means for Timor

Written 2026-08-04. Closes out [COASTS-TODO.md](COASTS-TODO.md) (items C1–C8) and
records three fixes beyond it, plus two new items (C9/C10) that surfaced during
the work. Landed on `feat-scaling` in `WorldFishCenter/peskas.coasts`.

Every number here is measured, not inferred.

---

## Task 0: the GBIF hypothesis was wrong — the real cause is worse

COASTS-TODO's leading hypothesis was that GBIF stopped resolving higher taxa.
**Measured and rejected.** `taxize::tax_rank(db = "gbif")` over the 57 Timor
`scientific_name` values resolves **56 of 57**, of which **48 return a usable
higher rank** (23 family, 22 genus, 1 class, 1 order, 1 unranked). The only `NA`
is `LOX`/Reptantia, which is not in the Sheets `catch_types` anyway.

The actual cause of the 693 → 88 species collapse:

**`rfishbase::species()` has no `Family` / `Genus` / `Order` / `Class` /
`Species` arguments.** They fall into `...` and are silently ignored. Verified on
rfishbase 5.0.1:

```r
identical(dim(rfishbase::species(Family = "Lethrinidae")),
          dim(rfishbase::species()))
#> TRUE      -- both 36132 x 101, i.e. the entire species table
```

So every rank-based branch of `get_fish_length()` returns the full species table
as a *data frame*, which `rfishbase::country()` then resolves to **0 rows**. Only
the three hardcoded `comm_name` branches survive — Tuna/Shark/Garfish via
`common_to_sci()`, which returns a proper character vector. Reproduced:
**87 distinct species / 465 length_weight rows**, against the reported 88 / 1,323.

Why [STATE.md](STATE.md) Addendum 1 missed it: that test compared rfishbase 5.0.1
**pinned vs unpinned** — both post-break — so it could never have detected the
change. The conclusion "rfishbase version is irrelevant" was correct but
misleading.

**Consequence:** `coasts::expand_taxonomic_info()` is still the fix, but for a
better reason than tidiness. It joins the FishBase/SeaLifeBase `species` +
`families` tables directly via `get_taxa_backbone()`, so it never touches those
filter arguments and never calls `country()`.

---

## Blocking items — all landed

| item | what to use |
|---|---|
| **C1** FAO area | `enrich_taxa(fao_areas = 71)`, or `metadata.fishbase.fao_areas: [71]` in Timor's `conf.yml`. Default is `c(51, 57)`; an absent key falls back to the same. |
| **C2** length-weight | `get_length_weight_coeffs(expanded)` → `a`, `b`, `aTL`, `Type`, `EsQ`, `LengthMin`, `LengthMax`, plus study metadata. Both servers. **No `Type`/`EsQ` filtering is applied** — filter at the call site. |
| **C3** length-length | `get_length_length_coeffs(expanded, length_types = c("TL", "FL"))` → `aL`, `bL`, `Length1`, `Length2`. |
| both from one call | `get_taxa_morphometrics(data, fao_areas =, filter_by_area =, length_types =, strip_parentheticals =)` → `list(expanded, length_weight, length_length)` |
| **C4** snapshot home | `enrich_taxa()` now reads *and* writes through `resolve_storage_opts(conf, "coasts")` — the hub, agreeing with the readers. No-op inside coasts; redirects downstream packages. |

Note on C1: FAO Area **57 is the Eastern Indian Ocean**. Releases ≤4.5.0
hardcoded 57 annotated "Western Indian Ocean", which was simply wrong — the
Western Indian Ocean is **51**. 4.6.0 defaults to `c(51, 57)` rather than 51
alone because FishBase's area data is incomplete at family level: 51 by itself
drops 19 taxon codes entirely, including `CLP` (Clupeidae, "Herrings, sardines
nei"), whose 15 backbone species carry 57 but none carry 51. Timor should set
`71` explicitly regardless.

---

## Verified against Timor's measured baseline

Run with `filter_by_area = FALSE, strip_parentheticals = TRUE` (see below — both
matter):

| check | baseline | coasts 4.6.0 | |
|---|---|---|---|
| distinct species | 693 | **1,347** | ✓ |
| rows, in Timor's lw×ll joined shape | 5,926 | **10,347** | ✓ |
| taxon codes with ≥1 `a`/`b` | 45 of 56 | **50 of 56** | ✓ |
| mean `b` | 2.9781 | 2.969 | ✓ |
| length-length codes covered | 40 of 56 | **40 of 56** | ✓ |

### Two traps when comparing numbers

**1. Row counts are not like-for-like.** Timor's 5,926 rows are a
length-weight × length-length **join product**, not distinct coefficient
records. coasts returns two normalised tables: 5,520 lw + 2,081 ll rows.
Reconstitute Timor's shape (`lw |> left_join(ll, by = species)`) before
comparing, or the result looks 2× too low.

**2. Arithmetic mean `a` looks wrong but isn't.** Whole-table mean `a` is
0.0867 vs the baseline's 0.02716. Restricted to FishBase — the same universe the
baseline drew from — it is **0.02362**, with mean `b` 2.9904. The whole-table
figure is inflated by SeaLifeBase invertebrate records measured on different
axes: carapace length (mean `a` 1.03), mantle length (1.01), shell length (0.51).
That is the coverage Timor never had, not drift.

### Coverage gained

7 of the 11 codes that had **no** coefficients now have them: `COZ`, `CUX`,
`FLY`, `IAX`, `OCZ`, `PEZ`, `SLV`. Still absent: `CRA`, `SWX`, `LGE`, `MZZ`.

---

## Two opt-in flags — Timor must set both

Both default to `FALSE` so existing WIO output stays byte-identical. **Without
them the run does not reach the baseline.**

- **`strip_parentheticals = TRUE`** — recovers `GRX` "Haemulidae
  (=Pomadasyidae)" (138 species, and the 40th length-length code). Off by default
  because it also gives `PWT` "Labridae (ex Scaridae)" 569 species, which would
  change output the WIO pipelines already publish.
- **`filter_by_area = FALSE`** — FAO-area filtering costs ~40% of the species
  that have usable coefficients (764 under area 71 vs 1,283 unrestricted).
  Coefficients describe body form and do not stop applying at an FAO boundary,
  and FishBase's area assignments are incomplete. Filter for traits, not for
  coefficients.

---

## What `expand_taxonomic_info()` still cannot resolve

Matching is against the FishBase/SeaLifeBase backbone only, so these return
nothing and need a synonym or common-name route:

| kind | codes |
|---|---|
| tribe | `TUN` Thunnini |
| infraorder | `LOX` Reptantia, `CRA` Brachyura |
| informal group | `MZZ` Osteichthyes, `SWX` Algae, `SKH` Selachimorpha |
| superseded binomial | `LGE` *Leiognathus equulus* — FishBase now lists *L. equula* |

`TUN`, `SKH` and `GZP` were covered by Timor's hardcoded common-name overrides.
**Keep them.**

---

## Batched-in items

- **C5 retry.** `with_storage_retry()`, `insistent_upload_cloud_file()`,
  `insistent_download_cloud_file()` — the same `purrr::insistently()` +
  `purrr::rate_backoff(pause_cap = 300, max_times = 10)` policy Timor maintains.
  **Timor can delete its local copies.** Separately, `get_trips()` and
  `get_trip_points()` gained `req_retry(retry_on_failure = TRUE)` with
  `max_tries = 5`, which covers the `Recv failure: Connection reset by peer` that
  failed run 30637659244.
- **C6 public bucket.** `resolve_storage_opts(conf, "public")` resolves
  `public_storage.google.options` or `storage.google.options_public`, plus an
  `error_if_missing` argument (default `FALSE`, preserving the old return-`NULL`
  behaviour). `get_public_files()` / `get_tracks_map()` can stop reaching into
  config directly.
- **C7 Timor registered.** `timor` block under `api.trips`, and
  `"MAF / WorldFish"` added to `pds.customers` — read off the live PDS API, not
  guessed: 844 devices, timezone `Asia/Dili`, spanning every Timorese
  municipality. `"Traders"` (17 devices) and `"FSSP2: Traders"` (8) were
  deliberately excluded: they are trader rather than vessel devices, and
  `"Traders"` also carries `Asia/Kuala_Lumpur` devices, which would widen the
  shared device filter for every country.
- **C8 `cloud_object_name()` sharp edge.** Documented prominently as "resolves
  one versioned object, never enumerates a bucket", plus a new
  `cloud_object_names()` for enumeration (with `latest_only`). The singular
  function's `[1]` return is unchanged, so Timor's dependence on it is safe.

---

## Three fixes beyond C1–C8

1. **`ingest_pds_tracks(batch_size = n)` NA tail.** Sliced with
   `new_trip_ids[1:n]`, which pads `NA` once `n` exceeds the trips remaining — so
   the final batch of an interactive backfill sent trip id `NA` to the API once
   per overshoot slot. Now `utils::head()`. Unreachable from the pipeline, which
   never passes `batch_size`.
2. **`get_trips()` URL overflow — this one would have broken production.**
   `imeis` is sent as a comma-separated query parameter. Registering Timor took
   the device list from 408 to 850 and the URL from 6,639 to **13,711**
   characters, past the 8192-byte limit nginx/Apache/most CDNs enforce, and the
   server answered an opaque **HTTP 400 Bad Request**. `get_trips()` now splits
   an oversized `imeis` list across as many requests as needed
   (`max_url_chars = 7000`).
3. **`predict_pds_tracks()` filters client-side.** It was the only caller passing
   `imeis`; it now fetches the window once and filters locally, as
   `ingest_pds_trips()` always has. Also faster: the `imeis` parameter makes the
   server re-scan the whole date window once per chunk — measured over 90 days
   with 850 devices, three chunked requests took 11.5s against 2.8s for one
   unfiltered request, for a 2% overfetch and an identical trip set.

---

## Still hardcoded in coasts — new items

### C9. Trip-fetch windows are literals, not config

`ingest_pds_trips()`'s `dateFrom` and `predict_pds_tracks()`'s `date_from` are
both now `"2018-01-01"` string literals. Measured: the WIO fleets have **zero**
PDS trips before 2023 (their deployments started then), so the wider window costs
them nothing but a larger API response. For Timor it is load-bearing — 371 of its
442 devices were last seen before 2023, and its fleet peaked in 2020 (138 devices
last seen that year). Opening the window from 2023 to 2018 took the trips file
from 33,039 to **122,915** rows, all of the increase Timor.

These should become config keys, in the same pattern as
`metadata.fishbase.fao_areas`.

### C10. `merge_survey_trips()` excludes Timor — and Timor does not need it

[match-trips.R:29-59](../../peskas.coasts/R/match-trips.R#L29) pins
`countries <- c("kenya", "mozambique", "zanzibar")` plus three more hardcoded
parallel vectors. Timor also lacks `surveys.timor.*.merged` and
`storage.google.buckets.timor` in config, so adding `"timor"` to that vector
would pass `NULL` prefixes into `download_parquet_from_cloud()`.

More importantly, the two functions do different jobs:

| | coasts `merge_survey_trips()` | Timor `merge_trips()` |
|---|---|---|
| join key | `submission_id` ↔ `trip`, already matched upstream | `(landing_date, tracker_imei)` — does the matching itself |
| inputs | validated API trips (`peskas-api-*` bucket) + per-country merged surveys | validated landings + validated PDS trips, own bucket |
| output | `trips-matched` parquet, hub | `merged_trips__*.rds`, country bucket |

coasts' function assembles a **cross-country** table from matches each country
already made; Timor emits `tracker_trip_id` in RDS to its own bucket and never
writes to `peskas-api-*`. So this is a Phase 6 integration — publish Timor's
match in coasts' shape and feed the API bucket — not a one-line change. Nothing
in Timor's weight-path migration depends on it.

---

## Direction reversal confirmed

Nutrients are **not** a Timor → coasts upstream candidate, contrary to PLAN §10.
`enrich_taxa()` already emits `calcium`, `iron`, `omega3`, `protein`,
`vitamin_a`, `zinc`. Phase 8 should compare Timor's `calculate-nutrients.R`
against coasts' output and **delete Timor's if they agree**.

---

## Suggested next steps, Timor side

1. Pin `DESCRIPTION` `Remotes:` and `ARG COASTS_REF` in **both** Dockerfiles to
   the 4.6.0 tag.
2. Replace `get_catch_types()` + `get_fish_length()` + `retrieve_lengths()` with:

   ```r
   m <- coasts::get_taxa_morphometrics(
     taxa,                        # alpha3_code + scientific_name
     fao_areas = 71,
     filter_by_area = FALSE,
     strip_parentheticals = TRUE
   )
   ```

   This also drops the undeclared **GBIF/taxize** dependency from the middle of
   the fetch.
3. **Keep**, because coasts has no equivalent:
   - `estimate_weight()`'s per-taxon `quantile_coeff` special cases (`FLY ~ 0.75`)
   - the curated `manual_table` morphometric rows
   - the `TUN` / `SKH` / `GZP` common-name overrides
   - `length_type`, which exists only in the Sheets `catch_types` (56 of 61 rows
     are `NA`; the real driver is `survey_version`: v1 → `FL`, v2/v3 → `TL`)
4. Re-run `calculate_weights()` and diff total catch weight against the Phase 0
   golden snapshot **before** removing the `rfish-table` pin. Species coverage
   roughly doubles, so weights *will* move — that is the intent, but it must be a
   measured and explained move, not a surprise.
5. Data-model note: `alpha3_code` in the frame `taxa` table is the FAO 3-alpha
   **taxon** code, not a country code — coasts' roxygen said "country/region
   code" until 4.6.0, which is now fixed. The `assets__*` snapshot's `taxa` table
   has **no `country` column** at all; to get Timor's rows, query Airtable base
   `appMMEJYlJdfSJEjm`, table `tbliS8LrISaMft5Yg`, filtering `country` contains
   "Timor". 57 rows.
