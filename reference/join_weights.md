# Join length-weights and nutritional parameters info to preprocessed landings

The function integrate nutritional info from the `get_nutrients_table`
function and length-weight info from FIshBase databse to preprocessed
surveys data and convert catch labels according to the FAO nomenclature
(<http://www.fao.org/fishery/statistics/global-production/3/en>).

## Usage

``` r
join_weights(data, rfish_tab, nutrients_table)
```

## Arguments

- data:

  The merged long landings table

- rfish_tab:

  Table with length weight parameters

- nutrients_table:

  Table with nutritional parameters

## Value

The long landings table with `weight` and nutrients added

## Length types

**Every length reaching this function is a total length.** v2 and v3
record TL; v1 recorded fork length and was converted once, at the freeze
(`data-raw/freeze-landings-v1.R`, migration Phase 3).

`length_type` is carried through from `catch_types` and is **descriptive
only** — it selects nothing and converts nothing. It is non-`NA` for
five invertebrate taxa (`SLV` CL, `OCZ` ML, `IAX` ML, `CRA` CW, `COZ`
ShL), but enumerators measure those on total length in the field, which
is what the `OCZ`/`SLV`/`IAX`/`MOO` overrides below encode. Confirmed
2026-08-10.

The prose this replaced claimed the opposite of the code — "total length
(TL) in survey version 1 and fork length (FL) in survey version 2" — and
an `SRX` → disk-width rule that was never implemented. Both were wrong.

## Input and output shape

Both are the **flat long** table — one row per (submission, catch,
length bin) — with the taxon already resolved from the assets snapshot
by
[`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md).
The output adds `weight` (grams) and the seven per-catch nutrient
columns and changes nothing else.

Until migration Phase 5 this function also re-nested the catch columns
into `species_group` / `length_individuals` and dropped the standard
submission columns, purely so the validators could keep reading raw KoBo
names off a legacy-shaped artefact.
[`validate_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landings.md)
reads the long table now, so the bridge is gone and the artefact is
parquet like every stage before it.
