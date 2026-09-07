# Expand each catch into one row per length bin

The form records a count of individuals per 5 cm bin, so the length
itself is the **midpoint of the bin name**, not an answer. Individuals
longer than 60 cm are counted in `no_individuals_over60` and measured in
`fish_length_over60`; that recorded length replaces the 60 cm midpoint
and the measurement column is then dropped, which puts over-60 fish on
the same footing as every other bin.

## Usage

``` r
expand_length_frequency(x)
```

## Arguments

- x:

  Output of
  [`reshape_species_groups()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/reshape_species_groups.md).

## Value

`x` with the `no_fish_by_length_group/*` columns replaced by one row per
bin carrying `length` (cm) and `n_individuals`.

## Details

Bins with no count are **kept**. They carry a length and an `NA` count,
they are what the nested `length_frequency` column has always contained,
and they reach the portal: `timor_catch` is 1.7 M rows of which 93% have
no weight. Dropping them here would change the published table.
