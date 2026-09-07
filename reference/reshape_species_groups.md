# Reshape KoBo species groups into one row per catch

The landing forms record up to 26 catches per submission, each as a
block of `species_group.<n>.species_group/<field>` columns. This turns
that block structure into one row per (submission, catch), dropping the
empty catch slots.

## Usage

``` r
reshape_species_groups(x)
```

## Arguments

- x:

  A raw landings table, one row per submission.

## Value

A tibble keyed on `submission_id` and `n_catch`, with one column per
species-group field (`species`, `food_or_sale`, the
`no_fish_by_length_group/*` counts, ...).

## Details

The catch index `n_catch` is 1-based, matching the other country
pipelines.

## Examples

``` r
dummy <- tidyr::tibble(
  `_id` = "123",
  `species_group.0.species_group/species` = "1",
  `species_group.0.species_group/no_fish_by_length_group/no_individuals_5_10` = "3",
  `species_group.1.species_group/species` = NA_character_
)
reshape_species_groups(dummy)
#> # A tibble: 1 × 4
#>   submission_id n_catch species `no_fish_by_length_group/no_individuals_5_10`
#>   <chr>           <int> <chr>   <chr>                                        
#> 1 123                 1 1       3                                            
```
