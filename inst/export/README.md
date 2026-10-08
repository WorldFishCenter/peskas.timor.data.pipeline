# Peskas Timor-Leste: small-scale fisheries landings

Released <<released>>. Landings from <<start>> to <<end>>: <<trips>> fishing trips in <<rows>> rows.

## What this is

Records of fishing trips landed around Timor-Leste, collected for [Peskas Timor-Leste](https://timor.peskas.org), the country's official national fisheries monitoring system. Enumerators working with the Ministry of Agriculture and Fisheries (MAF) interview fishers at landing sites and record each landing with [KoboToolbox](https://www.kobotoolbox.org). [WorldFish](https://worldfishcenter.org) runs the system that checks and publishes the records.

This is the table the [Peskas Fishery Data API](https://api.peskas.org/docs) serves for Timor-Leste as validated landings. It is released here once a month so that it can be downloaded and cited without an API key.

## Files

| File | Content |
|---|---|
| `timor_landings.csv` | The landings table. Dataverse lists it as `timor_landings.tab`; choose "Original File Format" to download the CSV. |
| `README.md` | This file. |

## How to read the table

- **One row is one catch from one trip.** A catch is one species or species group. A trip that landed three species has three rows with the same `trip_id`, and the trip's details repeat on each row.
- **Trip totals repeat on every row of the trip.** Take `tot_catch_kg` and `tot_catch_price` once per `trip_id`; do not sum them over rows.
- **Empty cells are missing values.** Either the value was not recorded, or it failed a check (see "How the data is produced").
- **A row with an empty `catch_taxon`** is a trip with no catch, or a catch whose species was not recorded. From 2023, `catch_outcome` tells the two apart (0 means no catch).
- **These are surveyed landings, not all landings.** Enumerators record a sample of the trips at the sites they cover. National and municipal totals are estimated from them and shown on [timor.peskas.org](https://timor.peskas.org).

Specific to Timor-Leste:

- `survey_organization` is always `MAF`.
- `survey_id` is the KoboToolbox form. The form changed twice: `aur3fK7mtJem5Cg8Wi2SPd` (SSF Landings, from 2017), `aaztUDtRzb9SpSV7i9iptb` (peskAAS, from 2019) and `aEoWV7aprG47Q4uTpaopgD` (PeskAAS 2, from 2023, the current form).
- `trip_id` is `TRIP_` followed by the KoboToolbox submission number.
- `gaul_1_name` is the municipality and `gaul_2_name` the administrative post.
- `n_catch` is the catch's number within the trip, as entered in the form.
- `length_cm` is the mean length of the fish in the catch. Enumerators count fish by length class, and the mean weighs each class by its count.
- `catch_kg` is estimated, not weighed. It is computed from the counts and lengths with length-weight relationships from [FishBase](https://www.fishbase.org) and [SeaLifeBase](https://www.sealifebase.org).
- `catch_price` is always empty. Timor-Leste records the value of the whole landing, which is `tot_catch_price`, in US dollars.
- `catch_outcome` is recorded only by the current form.

## Columns

<<codebook>>

The descriptions are those of the Peskas Fishery Data API, which serves the same columns for Kenya, Mozambique, Timor-Leste and Zanzibar. Where a description is general, the notes above say what applies to Timor-Leste.

## How the data is produced

1. **Collection.** Enumerators record landings with KoboToolbox.
2. **Processing.** Every two days an automated pipeline downloads the surveys, reconciles the three versions of the form and estimates the weight of each catch.
3. **Checks.** Automated checks look at dates, trip duration, crew size, gear, boat, landing site, habitat, revenue and price per kilogram. A value that fails a check is emptied and the record is kept. Flagged records go to reviewers on the [Peskas Management Platform](https://validation.peskas.org).
4. **Release.** On the first day of each month the latest table is published here.

The pipeline is open source: [code](https://github.com/WorldFishCenter/peskas.timor.data.pipeline) and [documentation](https://worldfishcenter.github.io/peskas.timor.data.pipeline/). This release was produced by version <<version>>.

The approach is described in Tilley A, Dos Reis Lopes J, Wilkinson SP (2020). PeskAAS: A near-real-time, open-source monitoring and analytics system for small-scale fisheries. PLOS ONE 15(11): e0234760. <https://doi.org/10.1371/journal.pone.0234760>

## Versions

- Each monthly release is a new version of this dataset, under the same DOI. Earlier versions stay available from the "Versions" tab of the dataset page.
- Every version holds the whole table from 2017 onwards, not only the new records. Older records can change between versions when a check is improved or a reviewer corrects a record.
- Releases from March 2022 to June 2025 are separate datasets in the [Peskas collection](https://dataverse.harvard.edu/dataverse/peskas), with different files (trips, catches and monthly aggregates).

## Other ways to get the data

- [timor.peskas.org](https://timor.peskas.org): maps, trends and national estimates, in English, Portuguese and Tetum.
- [Peskas Fishery Data API](https://api.peskas.org/docs): the same records, updated every two days, with filters by date, place and species. It also serves the records before checks, and the other Peskas countries. It needs a key: write to <peskas.platform@gmail.com>.
- [Data report](https://storage.googleapis.com/public-timor/data_report.html): a summary of the latest data, updated twice a week.

## Licence and citation

The data is released under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/): you may share and adapt it for non-commercial purposes, with credit, under the same licence.

To cite it, use the citation shown on the dataset page. It includes the version number, so others can find the exact table you used.

## Who is behind it

Peskas is a partnership, since 2016, between [WorldFish](https://worldfishcenter.org) and the Ministry of Agriculture and Fisheries of Timor-Leste. Its development was supported by the Royal Norwegian Embassy in Jakarta, the Minderoo Foundation, the CGIAR Big Data Platform and the Schmidt Foundation. Since 2021 it has been funded by the Government of Timor-Leste, with technical support from WorldFish and Pelagic Data Systems.

Peskas also runs in Kenya, Mozambique and Zanzibar: see [peskas.org](https://peskas.org).

## Contact

Questions and corrections: <peskas.platform@gmail.com>
