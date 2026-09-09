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
(done once in the freeze).

## Input and output shape

Both are the **flat long** table — one row per (submission, catch,
length bin) — with the taxon already resolved from the assets snapshot
by
[`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md).
The output adds `weight` (grams) and the seven per-catch nutrient
columns and changes nothing else.
