# Get FAO Food Composition Data

This function retrieves and processes food composition data from the FAO
database. It specifically focuses on marine food items such as octopus,
squids, cockles, shrimps, crabs, and lobsters. The data is filtered,
categorized for various nutrients including protein, calcium, iron,
zinc, selenium, vitamin A, and omega-3 fatty acids.

## Usage

``` r
get_fao_composition()
```

## Value

A tibble with values for various nutrients for each category of marine
food items. The columns include `Protein_mu`, `Calcium_mu`, `Iron_mu`,
`Zinc_mu`, `Selenium_mu`, `Vitamin_A_mu`, and `Omega_3_mu`.

## Details

The function first reads a CSV file from a given URL using the
`read_csv` function from the `readr` package. It then defines specific
codes for various marine food groups: octopus, squids, cockles, shrimps,
crabs, and lobsters. The FAO composition data is then filtered for these
food items in their raw state. A new `interagency_code` is created for
categorization purposes. The nutrients considered are protein, calcium,
iron, zinc, selenium, vitamin A, and omega-3 fatty acids.

## Examples

``` r
get_fao_composition()
#> Rows: 515 Columns: 13
#> ── Column specification ────────────────────────────────────────────────────────
#> Delimiter: ","
#> chr (4): integragency_code, food_name, habitat, food_state
#> dbl (9): food_id, ISSCAAP, protein(g), calcium(mg), iron(mg), zinc(mg), sele...
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 39 × 8
#>    interagency_code Protein_mu Calcium_mu Iron_mu Zinc_mu Selenium_mu
#>    <chr>                 <dbl>      <dbl>   <dbl>   <dbl>       <dbl>
#>  1 PEZ                    17.7         96     1.9    2.28          31
#>  2 CRA                    17.6         65     0.5    4.43          45
#>  3 CRA                    18.1         46     0.4    4.27          37
#>  4 CRA                    15.5         52     0.6    4.59          53
#>  5 CRA                    18.5         83     2.5    2.8           35
#>  6 CRA                    17.2        153     1.2    3.04          57
#>  7 CRA                    15.9         92     0.7    3.54          37
#>  8 CRA                    19.9        120     0.4    2.45          57
#>  9 CRA                    14.7        190     1.8    3.25          55
#> 10 SLV                    18.1         66     1.2    2.25          63
#> # ℹ 29 more rows
#> # ℹ 2 more variables: Vitamin_A_mu <dbl>, Omega_3_mu <dbl>
```
