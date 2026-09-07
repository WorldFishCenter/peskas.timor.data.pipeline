# Prompt for the next session — the taxa / weight path: adopt the WIO implementation

Copy everything below the line into a fresh Claude Code session started in the
**`peskas.timor.data.pipeline`** working directory.

**This changes published weights.** It is not the cutover and not a deletion
phase. Read "Ordering" before deciding whether it runs before or after the merge.

---

Taxa and weight path — no Google Sheets, correct FAO areas, no common-name rescue

Read in order: `CLAUDE.md`; `.claude/migration/STATE.md` — the Phase 11a entry and
the 2026-09-04/05 entries; `.claude/migration/ALIGNMENT-AUDIT.md` §7 and §15;
`.claude/migration/COASTS-TODO.md` C23.

End the session by appending a STATE.md entry.

## What the user asked for, 2026-09-05

Three requirements, in their words, plus what measurement says about each.

1. **`calculate_weights()` must not read Google Sheets.** `R/model-taxa.R:33` does
   `metadata <- get_preprocessed_sheets(conf)` and passes
   `metadata$morphometric_table` into `get_morphometric_tables()`. **No WIO
   package has a Sheets metadata source**, and the user asked explicitly that
   those packages be the reference.
2. **Coefficients must be filtered to Timor's FAO areas.** They are not filtered
   at all today: both call sites pass `filter_by_area = FALSE`, and
   `conf$metadata$fishbase$fao_areas` **does not exist** — it resolves to `NULL`.
3. **`rescue_by_common_name()` must go.** No WIO package has it.

## The reference implementation — Mozambique, and it answers all three points

`peskas.mozambique.data.pipeline/R/model-taxa.R:157`, `getLWCoeffs()`:

```
1. load both backbones            rfishbase::load_taxa("fishbase" | "sealifebase")
2. process_species_list(fao_codes, asfis_list)     <- ASFIS, not common names
3. match_species_from_taxa()
4. get_species_areas_batch() |> filter(area_code == 51)   <- area filter, applied
5. get_length_weight_batch()
6. pool: exp(mean(log(a))), mean(b)                <- identical to Timor's today
```

`process_species_list()` (`model-taxa.R:362`) maps each FAO 3-alpha code to a
`scientific_name` and `taxon_group` from ASFIS, derives the rank from the name
itself — `" spp$"` → Genus, `"idae$"` → Family, `"formes$"` → Order, a bare
binomial → Species — and routes invertebrate ISSCAAP groups (57, 45, 43, 42, 56)
to SeaLifeBase. Mozambique also carries per-taxon measurement-axis filters in the
same function (`PEZ` must be `TL`, `OCZ` must be `ML`), which is the WIO answer to
part of what Timor's curated `morphometric_table` does.

**ASFIS is available — the August audit said otherwise and is stale.** Verified
2026-09-05:

```
gs://timor/asfis__20250220202024_a59fabb__.parquet       680,170 B  (since 2026-02-06)
gs://timor-dev/asfis__20250220202024_a59fabb__.parquet   680,170 B  (added 2026-09-05)
```

Same object in both, so `default` and `production` each resolve one. It is **not**
in `peskas-coasts`, so read it from the country bucket exactly as Mozambique does:
`coasts::download_parquet_from_cloud(prefix = "asfis", ...)`. 13,615 rows;
columns `ISSCAAP_Group, Taxonomic_Code, Alpha3_Code, Scientific_Name,
English_name, Family, Order, FishStat_Data`. **Join on `Alpha3_Code`**, not
`Taxonomic_Code`.

## How the WIO packages handle `TUN` and the other unresolvable codes

**This is the part to copy, and it is not a common-name lookup.**

ASFIS gives all six of Timor's unresolved codes a real scientific name, but some
are ranks the taxonomic backbone cannot match:

| code | ASFIS `Scientific_Name` | English | backbone can match? |
|---|---|---|---|
| `TUN` | `Thunnini` | Tunas nei | **no** — a tribe |
| `SKH` | `Selachimorpha (Pleurotremata)` | Various sharks nei | **no** — superorder, parenthetical |
| `MZZ` | `Actinopterygii` | Marine fishes nei | **no** — a class |
| `CRA` | `Brachyura` | Marine crabs nei | **no** — an infraorder |
| `SWX` | `Algae` | Seaweeds nei | **no** |
| `LGE` | `Leiognathus equulus` | Common ponyfish | yes, if FishBase still lists that binomial |
| `GZP` | **`Hyporhamphus quoyi`** | Quoy's garfish | **yes — one named species** |

Mozambique deals with the unmatchable ones through **three explicit mechanisms**:

1. **Recode to a resolvable ASFIS code before the lookup**
   (`preprocessing-surveys.R:248`):

   | from | to | ASFIS name of target | matches as |
   |---|---|---|---|
   | `TUN` | `TUS` | `Thunnus spp` | Genus (`" spp$"`) |
   | `SKH` | `CVX` | `Carcharhiniformes` | Order (`"formes$"`) |
   | `CLP` | `ANX` | `Engraulidae` | Family (`"idae$"`) |

2. **Drop the hopeless** — `get_fao_groups()` ends with
   `filter(!a3_code %in% c("MZZ", "UNKN"))`.

3. **Hardcode where FishBase cannot help** — `FLY` is bound on afterwards as
   `lw_a = 0.00631, lw_b = 3.05` (`preprocessing-surveys.R:261`). This is
   Mozambique's counterpart to Timor's missing `FLY` special case.

**Two of the three transfer directly. One does not:**

- `TUN → TUS` and `SKH → CVX` are taxonomic corrections; they transfer.
- **`CLP → ANX` is a Mozambique fishery choice, not a taxonomic fix** — it swaps
  herrings/sardines for anchovies. Timor's `CLP` already resolves cleanly as the
  Family `Clupeidae` with 170 studies. **Do not copy that recode.**

> ### The one question only the user can answer
>
> **`TUS` is `Thunnus spp` — the genus *Thunnus* alone.** The rescue being
> removed pools a wider set: *Katsuwonus pelamis* (skipjack), *Euthynnus*,
> *Auxis thazard* (frigate tuna), *Sarda*, *Scomberomorus commerson* and
> *Gymnosarda*, alongside the six *Thunnus* species.
>
> **Skipjack is not in *Thunnus*.** If skipjack and frigate tuna are a material
> share of Timor's tuna landings, `TUN → TUS` narrows the pool to the wrong
> species — a fishery question, not a data-engineering one. Compute it both ways
> (pooled `a`/`b`, and implied weight at a reference length) and **put the number
> to the user before choosing.** Today's pooled `TUN` gives 213 g at 25 cm, over
> species ranging 116–319 g.

## Do not simply delete the rescue

> `rescue_by_common_name()` is today the only source of coefficients for `TUN`,
> and **`TUN` is 59% of landed weight**. `coasts::get_taxa_morphometrics()`
> returns **zero** length-weight rows for `SWX, CRA, MZZ, SKH, LGE, TUN`.
>
> Remove it *with* its replacement in the same change, never before. A code
> silently falling to `NA` is the failure mode this session exists to prevent.

`match_species_from_taxa()` switches on rank with a `NULL` default, so **any code
whose rank the name pattern cannot infer is dropped without a warning**. A
straight port therefore fixes `GZP`, loses `TUN`/`MZZ`/`CRA`/`SWX`, and mis-types
`SKH` as a species. Extending the rank derivation instead — tribes (`-ini`),
classes, infraorders, parenthetical names — would be a real improvement on the WIO
implementation and belongs upstream in `coasts`, not only here.

## Measured evidence, 2026-09-05 — do not re-derive

**On the common-name rescue.** `rescue_by_common_name()` does
`rfishbase::common_to_sci()` on the literal strings `"Tuna"`, `"Shark"`,
`"Garfish"`. That is a substring match on common names, so it pulls in whatever
happens to contain the word:

| code | species matched | usable | weight spread at 25 cm | verdict |
|---|---|---|---|---|
| `TUN` | 25 | 20 | 116–319 g (**2.7×**) | sound — all genuine tunas; pooled 213 g, within ~11% of yellowfin |
| `SKH` | 513 | 160 | 18–423 g (**22.9×**) | **contaminated** |
| `GZP` | 53 | 22 | 17–328 g (**18.9×**) | **contaminated** |

`SKH`'s pool contains **`Pangasius sanitwongsei`** (a freshwater Mekong catfish)
and **`Balantiocheilos melanopterus`** (the bala shark, an aquarium fish), plus a
moray eel. `GZP`'s pool is topped by *Cubiceps* driftfishes and *Decapterus*
scads at 185–328 g while the actual garfish — *Belone belone*, *Ablennes hians*,
*Strongylura marina* — sit at 17–21 g. **`GZP` is one of the 13
`models.modelled_taxa`**, so this reaches published estimates, and it is almost
certainly the unexplained `GZP` −71% that commit `a2c2881` flagged as "worth a
look".

So point 3 is right on the evidence, not only on consistency — but see the TUN
warning above.

**On FAO areas.** Timor is in FAO areas **57 and 71**. Two things are wrong:

- `conf$metadata$fishbase$fao_areas` is read twice (`model-taxa.R:150`, `:194`)
  and **does not exist** — a silent `NULL`.
- `coasts::resolve_fao_areas()` falls back to `c(51, 57)` — the **WIO** areas.
  So flipping `filter_by_area = TRUE` without adding the key would filter Timor
  on one wrong area and miss the Pacific one entirely, silently.

**Measured cost of filtering**, coefficients recomputed with `filter_by_area =
TRUE, fao_areas = c(57, 71)` against today's unfiltered set, over the 50 codes
that resolve through the backbone:

- **50 codes keep coefficients either way** — no taxon is lost
- only **9 of 50** move more than 10% in implied weight; the largest are
  `SRX` +130%, `OCZ` +115%, `COZ` +104%, then `DRZ` +28%, `FLY` +19%
- but the evidence base thins sharply: **`CLP` 170 studies → 1**, `SRX` 271 → 4,
  `COZ` 26 → 3

That thinning is the real argument that was never recorded. `R/model-taxa.R:130`
justifies `filter_by_area = FALSE` with *"body form does not stop applying at an
area boundary"*, which answers the wrong question — the filter removes **species
from the pool**, it does not invalidate a coefficient. Whatever this session
decides, **replace that comment with the measured reason.**

Note Mozambique filters to a single area (51). Timor spans two, and `CLP`
dropping to one study suggests a straight port may be too aggressive. A defensible
middle is to filter where the local pool supports it and fall back to unfiltered
where it does not — but decide it deliberately and record the numbers.

## Other things spotted, same path

- **The `FLY` per-taxon override is gone and its removal is unrecorded.** STATE
  Addendum 3 item 3 says `estimate_weight()`'s per-taxon `quantile_coeff`
  overrides "must survive any rewrite". `summarise_lw_coeffs()`
  (`model-taxa.R`) is now a plain geometric mean with no special cases. Probably
  superseded by the move to a central estimate, but nobody wrote that down.
  **Mozambique answers this one**: it hardcodes `FLY` as
  `lw_a = 0.00631, lw_b = 3.05` after the fetch. Adopt that, or record why not.
- **The Sheets reach further than `calculate_weights()`.** If point 1 is about
  removing Sheets as a metadata source, these are the other live readers:
  `catch_types` in `convert_taxa_names()` (`pds-maps.R:47`, on the portal path)
  and in `preprocessing-surveys.R:503`; `devices`, `stations`, `reporting_unit`,
  `conservation` in `validate_landings()` (`validation.R:47`); plus
  `enumerators_summary.Rmd`, `data_report.Rmd`, `test_validated_landings.R` and
  `data-raw/freeze-landings-v1.R`. Each is blocked on Airtable *data*, not code
  — ALIGNMENT-AUDIT §0. **Scope this session to the weight path** unless the
  user says otherwise; the rest is the wider Phase 12.
- **`morphometric_table` is 4.14% of national catch weight**, 559 curated rows
  over 11 mostly-invertebrate codes. It is not a leftover. If it leaves the
  Sheets it needs a home: COASTS-TODO **C23** proposes a curated supplement in
  the hub, and Mozambique's per-taxon `Type` filters cover part of the same
  ground. Do not delete it without replacing what it does.
- **Nutrients ride on the same expansion.** `get_taxa_expansion()` reuses
  `get_taxa_morphometrics()$expanded`, so anything done here moves the published
  nutrient numbers too. Measured 2026-09-04: per-kg nutrient content already
  moved −13% (selenium) to +9% (zinc) from the expansion growing 693 → 5,259
  species. Re-measure nutrients as part of this work, not afterwards.

## Ordering

The branch is ready to merge and has not been merged; production holds every
migrated artefact except the portal JSON. **The user's position, 2026-09-05: the
point of the migration is alignment with the WIO packages — if they use ASFIS,
Timor uses ASFIS.** Do not re-litigate that.

The only thing ordering changes is how many times the live dashboard steps. Say
which you are doing in the STATE entry and move on.

## Verification

- The gate is **total catch weight and the seven portal objects**, not a diff of
  zero. Compare with `data-raw/compare-portal-json.R` against a snapshot taken
  before you start, and state the delta per taxon for anything that moves more
  than a few percent.
- `TUN`, `CLP`, `GZP` and `SKH` each need an explicit before/after number.
- Assert every one of the 56 codes still resolves to a coefficient pair, or list
  the ones that do not and what they are worth in landed weight. **A code
  silently dropping to `NA` is the failure mode this whole session exists to
  prevent.**
- All four tinytest suites must pass. Do not weaken an assertion.

## Rules

- **Never set `R_CONFIG_ACTIVE=production`** and write to no production bucket.
  Note `.Renviron` pins `default` and overrides the command line — use
  `use_prod()` or `R_ENVIRON_USER=/dev/null` only if you deliberately need prod,
  and for reads only.
- `devtools::load_all()` reaches neither `coasts::read_config(package = )` nor
  `furrr` workers — use `devtools::install()` + `library()`.
- The tinytest suites need `dotenv::load_dot_env('<repo>/.env')` in the same
  `Rscript` call.
- Do not commit, push or merge. Hand the work back to the user.
