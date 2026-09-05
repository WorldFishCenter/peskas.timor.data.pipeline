# FishBase v26.06 silently changes published catch — pin the release

**For `peskas.coasts` and the Kenya / Mozambique / Zanzibar pipelines.**
Found in Timor 2026-09-05.

## What happened

`rfishbase` **5.0.3** moved the parquet host from HuggingFace to Source
Cooperative. They carry different release sets:

| host | latest FishBase release |
|---|---|
| HuggingFace (rfishbase ≤ 5.0.1) | **25.04** |
| Source Cooperative (rfishbase ≥ 5.0.3) | **26.06** |

`coasts::get_combined_tbl()` calls `rfishbase::fb_tbl()` with **no `version`**,
so `"latest"` became 26.06 the moment a container was rebuilt with a current
rfishbase. Nobody changed a line of code.

**26.06 carries a taxonomic revision:**

```
Caesio, Pterocaesio     Caesionidae -> Lutjanidae
Scarus, Chlorurus, ...  Scaridae    -> Labridae
```

`Caesionidae` and `Scaridae` still exist as family names, with **zero species
in them**. Any taxon whose reference name is one of those families now expands
to nothing, gets no length-weight coefficients, and **weighs `NA` — which sums
to zero**. In Timor that is `CJX` and `PWT`; `CJX` is 5% of landed weight and a
modelled taxon, and it disappeared from the published portal with no error.

FishBase also revised `Clupeidae` in 2022 — *Sardinella*, *Amblygaster*,
*Herklotsichthys* moved to `Dorosomatidae` — so anything mapped to
`Clupeidae` is pooling North Atlantic herring, not tropical sardines.

## Please check, in your own data

1. `grep` your taxa list for reference names that are **families**. Expand each
   against the backbone and assert the species count is **> 0**.
2. Specifically: `Caesionidae`, `Scaridae`, `Clupeidae`.
3. Compare national catch before and after a container rebuild. If it moved and
   no code changed, this is why.

## Asks for `peskas.coasts`

1. **`get_combined_tbl(tbl_name, version = "latest")`**, passing `version` to
   `fb_tbl()`. Thread one resolved version from the top of
   `get_taxa_morphometrics()` — it makes **8** independent reads
   (`species`, `families`, `faoareas`, `poplw`, `popll`, `ecology`, `estimate`,
   `species` again), so today a single run can mix snapshots. Source it from
   `metadata.fishbase.db_version`, defaulting to `"latest"` so nothing changes
   until a country opts in. Validate per server — FishBase has 21.06,
   SeaLifeBase does not.
2. **Warn on an unmatched name.** `expand_taxonomic_info()` inner-joins, so a
   name matching nothing is dropped silently. That is the whole failure mode.
3. **Log the resolved release and row counts**, so an artefact can be traced to
   its inputs afterwards.

Filed as COASTS-TODO C25 / C26 / C27.

## Timor's interim fix

`rfishbase` pinned to 5.0.1 in both Dockerfiles, placed **after**
`install_github()` so it is not upgraded back. That pins the *host*, not the
release, and stops working the day HuggingFace serves 26.06 — replace it with
the config-driven data-version pin as soon as (1) ships.

Timor also added `assert_taxa_coverage()`, which fails the run when any taxon
resolves to no coefficient pair. **That is what caught this**, and it is the
part worth copying first: without it the pipeline stays green and publishes a
hole.
