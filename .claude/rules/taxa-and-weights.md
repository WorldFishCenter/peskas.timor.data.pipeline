---
paths:
  - "R/model-taxa.R"
  - "R/nutrients.R"
  - "inst/extdata/**"
---

# The taxa and weight path

Three sources, none of them a Google Sheet.

| what | where from |
|---|---|
| which taxon *codes* Timor has | the PESKAS \| FRAME assets snapshot |
| each code's *scientific name* | the FAO **ASFIS** list, `gs://timor{,-dev}/asfis__*.parquet`, joined on `Alpha3_Code` — the same object and access path Mozambique uses |
| the curated invertebrate coefficients | `inst/extdata/morphometric-coefficients.csv`, 559 hand-curated rows over 11 codes, ~4% of national catch weight |

**ASFIS was adopted for alignment, not for effect.** 55 of Timor's 56 codes
carry an identical `scientific_name` in the frame and in ASFIS, the exception
being `MZZ` (frame `Osteichthyes`, ASFIS `Actinopterygii`), and both expand to
the same 50 codes over the same species. Join on `Alpha3_Code`, never
`Taxonomic_Code`.

FAO areas **57 and 71**, from `metadata.fishbase.fao_areas`, with
`filter_by_area = TRUE`.

## `taxa_search_aliases()` is the load-bearing part

`expand_taxonomic_info()` matches the FishBase backbone at species / genus /
family / order / class only, and FAO names several taxa at ranks that backbone
has no column for. Aliases are **additive** — extra search names for a code,
never a replacement — so nothing that already resolves can regress and **no
published taxon code changes**. This is deliberately *not* Mozambique's
approach, which recodes `catch_taxon` in the data (`TUN` → `TUS`, `SKH` →
`CVX`, `CLP` → `ANX`) and thereby renames the published taxon.

| code | ASFIS name | why it fails | alias |
|---|---|---|---|
| `TUN` | `Thunnini` | tribe — no such rank | the tribe's 5 genera |
| `SKH` | `Selachimorpha (Pleurotremata)` | superorder | `Carcharhiniformes` |
| `LGE` | `Leiognathus equulus` | binomial FishBase has revised | `Leiognathidae` |
| `CLP` | `Clupeidae` | **stale family**, see below | `Dorosomatidae` too |

`TUN` is **56% of landed weight**, so its pool is a fishery decision: the tribe
*Thunnini* is *Allothunnus, Auxis, Euthynnus, Katsuwonus, Thunnus*, which is
what Timor's "Tunas nei" lands. Mozambique's `TUS` would drop skipjack and
frigate tuna; the ASFIS family `Scombridae` would pull in the mackerels Timor
codes separately as `RAX`. At 25 cm: 249 g for the tribe, 249 g for `TUS`,
172 g for `Scombridae`.

`CLP` is **not** a rank problem. FishBase's 2022 revision moved the tropical
sardines — *Sardinella*, *Amblygaster*, *Herklotsichthys*, *Nematalosa*,
*Tenualosa* — out of `Clupeidae` into `Dorosomatidae`, while FAO still files
them all under `CLUPEIDAE`. FishBase's `Clupeidae` now holds 15 mostly
temperate species, so Timor's second-largest taxon (26% of landed weight) was
priced off **114 records of *Clupea harengus* and 36 of *Sprattus sprattus***.
Searching both families restores 123 area-57/71 records over 25 Indo-Pacific
species. **Do not "fix" this in Airtable** — considered and rejected: `DCX` and
`CLU` are `Clupeoidei`, a *suborder*, which matches nothing; `DAG` is the
freshwater Lake Tanganyika sardine; `SIX` (*Sardinella* spp) resolves but
narrows to 8 species, drops the herrings the label names, and renames a
published taxon key.

## Coefficients are restated on a total-length basis before pooling

A published `W = a * L^b` is fitted on whichever axis the study used, and for
Timor's taxa only 1,824 of 3,702 matched pairs are `TL` — 990 are `FL`, 460
`SL`. Pooling them as published and applying the result to a TL measurement
overestimates weight, because FL and SL are shorter (medians 0.958 TL and
0.827 TL).

- **POPLL fits `Length1 = aL + bL * Length2` — the second column is the
  predictor.** coasts' roxygen states this backwards (COASTS-TODO C28). Getting
  it wrong inverts every ratio.
- Substituting `L_type ~= ratio * TL` gives `W = a * ratio^b * TL^b`, so **`b`
  is unchanged and only `a` is rescaled**.
- **`length_types = NULL` is passed deliberately.** The coasts default keeps
  only `TL`/`FL` pairs, dropping 1,427 of 2,642 usable conversions — all the
  `SL` ones, where the correction is largest.
- **The 285 pairs with no conversion are kept as published, not dropped**, and
  the curated rows are bound on afterwards so they are never restated. Dropping
  the unconvertible takes `MOO` down 92% and `SFA` 78%. Zanzibar converts only
  for taxa that would otherwise have nothing; Timor converts everything it can,
  which is why no `Type` filter is needed.

## Two guards, and they exist because of a measured failure

- `assert_taxa_coverage()` **errors** when a taxon resolves to no coefficient
  pair. `MZZ` (mixed fish — `Actinopterygii`, which FishBase files as
  `Teleostei`) and `SWX` (`Algae`) are the two documented exemptions.
- `get_nutrients_table()` **warns** for taxa with no nutrient values. Currently
  `CUX` and `GZP`, together 0.5% of catch. A warning rather than an error
  because `get_fao_composition()` legitimately does not cover every
  invertebrate.

`expand_taxonomic_info()` warns about names it cannot match, and for Timor it
always names six: `Algae`, `Brachyura`, `Actinopterygii`, `Selachimorpha
(Pleurotremata)`, `Leiognathus equulus`, `Thunnini`. **That is expected, not a
fault.** Four resolve through `taxa_search_aliases()` instead and `CRA` through
the curated table; only `MZZ` and `SWX` genuinely have nothing.

## The FishBase release is pinned, and must stay that way

`metadata.fishbase.db_version: "25.04"`. `"latest"` is not a fixed dataset:
`rfishbase` 5.0.3 moved the parquet host from HuggingFace (which stops at
**25.04**) to Source Cooperative (which serves **26.06**), so a container
rebuild silently moved the pipeline to 26.06 — in which `Caesionidae` and
`Scaridae` survive as family names with **zero species attached**. `CJX` and
`PWT` therefore weighed `NA`, which sums to zero, and `CJX` — 5% of landed
weight — went **missing from `portal-taxa_aggregated`** on two runs with no
error.

- **Pass `conf` to `coasts::get_taxa_morphometrics()`.** Without it coasts
  falls back to its own `read_config()` and resolves `"latest"`, silently
  undoing the pin. Both Timor call sites pass it.
- **Every number in `NEWS.md` was measured on 25.04.** Moving the key
  re-baselines the portal; run `test_portal_contract.R` when you do.
- A trap worth naming: from the artefacts alone, **"varies between runs" and
  "varies between environments" look identical**. The first diagnosis here was
  "non-deterministic partial reads" and it was wrong. Resolve the release before
  concluding either.
