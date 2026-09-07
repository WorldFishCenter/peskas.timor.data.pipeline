# peskas.timor.data.pipeline 4.0.0

Alignment of the Timor pipeline to the harmonized Peskas standard shared with
the Mozambique, Kenya and Zanzibar pipelines. Timor was the first Peskas
pipeline and predates the conventions the others share, so most of this release
is structural. The portal *contract* — the seven `portal-*.json` objects, their
keys, nesting, categories and column types — is deliberately unchanged. The
**figures inside it move**, and that is the first section below.

### Published figures change

This release is a bias correction, not a re-skin. The first production run on it
republishes the portal with, against the live production set:

- **catch −18.4%**, **landing weight −15.3%**, **estimated tonnage −18.4%**;
- **price per kg +17.8%**, **revenue −4.5%**;
- **nutrient supply −26.0%**, **nutrient RDI −20.3%**.
- North/South Coast revenue redistributed, Lautem now counted North.

Object names, keys, categories, nesting and column types are unchanged — the
contract gate reports **0 structural failures** — so nothing on the site breaks
or empties.

Three corrections to the length-weight path account for it, and they do not all
push the same way:

1. **A central estimate per taxon.** `summarise_lw_coeffs()` takes a geometric
   mean of FishBase's `a` and an arithmetic mean of `b` over every study not
   flagged questionable, where the previous code selected a per-taxon
   percentile.
2. **The right species behind each taxon.** Coefficients are now restricted to
   FAO areas 57 and 71, `TUN` is the tribe *Thunnini* rather than a common-name
   search, and `CLP` searches `Dorosomatidae` as well as `Clupeidae` — FishBase
   moved the tropical sardines there in 2022, so Timor's second-largest taxon
   had been priced off Atlantic herring. Taken alone this raised catch 12.6%.
3. **One measurement basis.** A published `W = a * L^b` is fitted on whichever
   axis the study used, and more than half of the matched pairs are not total
   length — 990 fork-length and 460 standard-length against 1,824 TL. Pooling
   them as published and applying the result to a TL measurement overestimates
   weight, because FL and SL are shorter than TL. The length-length conversions
   were already being fetched and discarded; 1,363 of 1,648 non-TL pairs are now
   restated on a TL basis before pooling. Taken alone this lowered catch 10.9%.

The largest per-taxon moves against production are `FLY` **+8%**, `CLP`
**+3%**, `CJX` **−8%**, `SNA` **−15%**, `MZZ` **−18%**, `SDX` **−20%**, `CGX`
**−21%**, `TUN` **−25%**, `MOO` and `BEN` **−37%**, `LWX` **−39%** and `GZP`
**−45%**. `GZP`'s fall is a correction: its coefficients came from a
common-name lookup whose pool was topped by driftfishes and scads at 185–328 g
while the actual garfish sit at 17–21 g.

Measured 2026-09-06 against the live production set, on FishBase release 25.04
(see the next section).

### The FishBase release is pinned

`peskas.coasts` read FishBase over the network with no pinned release, so the
reference data moved whenever a container was rebuilt. `rfishbase` 5.0.3 changed
the data host from HuggingFace (latest release **25.04**) to Source Cooperative
(latest **26.06**), and in 26.06 the families `Caesionidae` and `Scaridae`
survive with **zero species attached** — their genera having moved to
`Lutjanidae` and `Labridae`. `CJX` and `PWT` therefore resolved to no
coefficients and weighed `NA`, which sums to zero. `CJX` is 5% of landed weight
and one of the 13 modelled taxa, and it went missing from
`portal-taxa_aggregated` entirely on two runs, with no error.

- **`metadata.fishbase.db_version: "25.04"`** in `inst/config.yml`, read by
  `coasts::resolve_db_version()` and resolved once per call so a run cannot mix
  snapshots. This requires **coasts >= 4.10.0**, which is now a hard floor.
  `conf` is passed to both `get_taxa_morphometrics()` call sites — without it
  coasts resolves `"latest"` from its own configuration and the pin does
  nothing.
- **`assert_taxa_coverage()`** fails the run when any taxon but the two
  documented exemptions resolves to no coefficient pair, so a vanished taxon
  cannot reach the portal silently. This is what caught the above.

Every figure in the section above was measured on 25.04. Moving the key
re-baselines the portal and should be done deliberately, with
`data-raw/compare-portal-json.R` run against the change.

### Breaking changes

- **The taxa and weight path no longer reads Google Sheets, and is filtered to
  Timor's FAO areas.** Taxon *codes* still come from the PESKAS | FRAME
  snapshot; their *scientific names* now come from the FAO ASFIS list in the
  country bucket, joined on `Alpha3_Code`, as Mozambique does — measured
  behaviour-neutral, 55 of 56 names identical. Length-weight coefficients are
  restricted to FAO areas **57 and 71** via the new
  `metadata.fishbase.fao_areas` key, which **must** be set: `coasts` falls back
  to the Indian Ocean pair `c(51, 57)` otherwise. `get_morphometric_tables()`
  loses its `manual_table` argument.

- **Storage is delegated to the shared `peskas.coasts` hub.**
  `R/cloud-storage.R` and `R/google-drive.R` are deleted; every call site is
  `coasts::{cloud_storage_authenticate, upload_cloud_file, download_cloud_file,
  cloud_object_name, cloud_object_names, upload_parquet_to_cloud,
  download_parquet_from_cloud}`. `coasts` ≥ 4.6.0 is a hard floor.
- **Secrets move from an `auth/` directory to `.env`.** The `local:` config
  environment is gone, so a local run and CI resolve the same config branch and
  differ only by `R_CONFIG_ACTIVE`. `.env.example` documents every variable.
- **`inst/conf.yml` → `inst/config.yml`**, derived from the cross-country
  configuration template: `country`, `ingestion`, `surveys.landings.{v1,v2,v3}`,
  `api`, `pds`, `metadata`, `storage.google.options_{coasts,api}`,
  `storage.mongodb`, `validation`. Through the migration the file carried every
  legacy key beside the harmonized one that replaced it, marked `# [legacy]`;
  that half is now deleted, each key after its last reader.
- **Interchange format is flat long parquet**, one row per
  (submission, catch, length bin), from raw through validated. No `.rds`
  artefact is left on the survey path; `get_validated_landings()` re-nests the
  parquet on read for the portal path, which is why that path did not change.
- **KoBo retrieval, PDS ingestion and the Airtable frame are delegated.**
  `ingest-pds-data.R`, `retrieve-pds-data.R`, `retrieve-survey-data.R`,
  `preprocess_pds_trips()` and Timor's own `air_*` client are deleted; the
  pipeline calls `coasts::get_kobo_data()`, `coasts::ingest_pds_trips()`,
  `coasts::ingest_pds_tracks()` and `coasts::ingest_assets()`.
- **Validation flags go to MongoDB**, one `surveys_flags-<asset_id>` collection
  per live form in the shared `validation-{dev,prod}` database, replacing the
  Google Sheets sink. This is what puts Timor into the cross-country validation
  UI.
- **The v1 form is frozen** (last submission 2020-08-28). It is neither
  ingested nor preprocessed; `merge_landings()` reads a snapshot produced once
  per environment by `data-raw/freeze-landings-v1.R`, which also converted v1's
  fork lengths to total length.
- Reference data is now the shared **PESKAS | FRAME** Airtable base wherever it
  overlaps the Google Sheets metadata tables — taxa, gears, vessels, landing
  sites, districts and PDS devices. The Sheets shrink accordingly.
- The resolved configuration is named `conf` throughout, not `pars`.

### New features

- **Timor publishes the cross-country API parquet.** `export_api_raw()` and
  `export_api_validated()` write the 22-column trips table Kenya, Mozambique and
  Zanzibar already publish, to `peskas-api-{dev,prod}/timor/{raw,validated}`.
  The export is gated exactly as those three gate theirs — the shared "Set env
  to production" step and nothing else — so this release is Timor's first write
  to `peskas-api-prod`, and the API gains its fourth country.
- `data-raw/compare-portal-json.R` — a gate over the seven `portal-*.json`
  objects: object names, then keys, nesting, column sets and column types, then
  per-column numeric summaries. Run it before touching the export path.

### Improvements

- `read_config()` no longer logs the resolved configuration. It was printing the
  service-account private key and every token into each CI job log; it now logs
  key names only, and every `coasts::` workflow call passes
  `log_threshold = logger::INFO` for the same reason.
- `DESCRIPTION` declares `ggchicklet` in `Remotes:`. It is a GitHub-only package
  used by `inst/report/data_report.Rmd` and installed in both Dockerfiles, but it
  was declared nowhere `pak` could see it, so any dependency resolution outside
  the container failed to solve.
- Reference sections in `_pkgdown.yml` are keyword-driven rather than
  name-pattern driven, so a rename can no longer silently move a function
  between sections.
- CI is on current templates: `R-CMD-check.yaml` (replacing
  `check-standard.yaml`), `pkgdown.yaml`, `test-coverage.yaml` and
  `pr-commands.yaml` from the r-lib v2 examples, `checkout@v5`,
  `build-push-action@v6`, `ubuntu-latest`, and a `release.yaml` that cuts a
  GitHub release from this file. Four workflows that could only ever build
  through the retired `docker.pkg.github.com` registry were retired or rebuilt.
- The export emits the seven `portal-*.json` objects the portal actually
  consumes. `portal-indicators_grid` and `portal-label_groups_list`, which the
  portal excludes and which were rebuilt on every run from a 2024-07-27
  artefact, are no longer written.

### Bug fixes

- `validate_catch_params()` assigned `length_individuals` positionally into a
  separately-derived frame, and an `isTRUE()` on a vector made alerts 12–15
  unreachable. Both are alert-identical to the intended behaviour on current
  data.
- **The coast rule put Lautem in the South.** `export_files()` classified Lautem
  as a South Coast municipality; it is on the north coast. North and South Coast
  revenue in `portal-summary_data` change accordingly.
- **`registered_boats` came from a stale Google Sheet.** It now reads
  `geo.total_boats` from the PESKAS | FRAME Airtable base, the same source the
  rest of the pipeline uses, which moves Manatuto from 283 boats to 213 and
  Viqueque from 213 to 207. The other ten municipalities are unchanged.
- A duplicate `get_preprocessed_metadata()` shadowed the correct definition.
- The `local:` config environment inherited a non-existent `development`
  environment.

### Removals

None of the following changed a published number; each was verified to have no
caller or no reader before it went.

- **25 unreferenced functions, ~2,900 lines.** The second, uncalled glmmTMB
  estimator `model_indicators()` and its 674-line subgraph; the four PDS map
  products (`ingest_pds_map()`, `ingest_kepler_tracks()`, `kepler_mapper()`,
  `ingest_complete_tracks()`) with `inst/kepler_mapper.py`; four cloud
  accessors; and twelve scattered helpers.
- **Six Google Sheets metadata tables.** Five whose last reader had gone —
  `vms_installs`, `centro_pescas`, `boats`, `fishing_vessel_statistics` and
  `registered_boats` — plus `morphometric_table`, whose 559 curated
  length-weight rows are now package data
  (`inst/extdata/morphometric-coefficients.csv`). That was the weight path's
  last Google Sheet. Six tables remain, each annotated in `inst/config.yml`
  with what blocks moving it.
- **`rescue_by_common_name()`**, which reached three taxa by
  `rfishbase::common_to_sci()` on the literal strings `"Tuna"`, `"Shark"` and
  `"Garfish"`. Being a substring match on common names, `SKH`'s pool contained a
  Mekong catfish and an aquarium bala shark. Replaced in the same change by an
  explicit alias table — it was the only source of coefficients for `TUN`, 56%
  of landed weight, and removing it alone would have halved published catch.
- **The KoBoToolbox validation-status client**, upstreamed to `coasts` 4.7.0 and
  now called from there. A package's own definitions win over its imports, so
  the delegation was only real once the local copy was gone.
- `glmmTMB`, `httr2` and `reticulate` leave `Imports` and `ggplot2` moves to
  `Suggests`. The `glmmTMB` GitHub install leaves both Dockerfiles with it — a
  TMB compile on every image build, for a package nothing imports.


# peskas.timor.data.pipeline 3.3.0

### Breaking changes

- Now kobo surveys are retrieved according the new v2 API. The function `retrieve_survey_data()` has been replaced by `get_kobo_data` updated to retrieve data from the new API. The function `retrieve_survey_metadata()` has been deprecated.


# peskas.timor.data.pipeline 3.2.0

## Improvements
- Align variables to the [Aquatic Food Ontology (AQFO)](https://agroportal.lirmm.fr/ontologies/AQFO)

# peskas.timor.data.pipeline 3.1.0

## Improvements

- Estimations are now weighted based on the number of records in each monthly bin. This improve the accuracy of estimations and reduce outliers effect.

# peskas.timor.data.pipeline 3.0.0

## New features

- Integrated the ingestion of the new survey form "Peskas 2". The new form was
deployed with the aim to be more manageable for enumerators. It also includes some
more questions regarding the fish handling and trading.


# peskas.timor.data.pipeline 2.0.0

## Breaking changes

- Validation step in now implemented using google sheets using the `googlesheets4`
package.


# peskas.timor.data.pipeline 1.4.0

## Improvements

- Improve national and municipal estimates combining packags `Amelia` and
`mice` for missing and outliers data imputation.

- Integrating price per kg into export data

## Bug fixes

- Improved revenue outliers identification based on empirical information


# peskas.timor.data.pipeline 1.3.0

## New features

- Integrated taxa selection and filtering in tracks file deemed to the leaflet map in the portal

# peskas.timor.data.pipeline 1.2.0

## New features

- Added a new feature in the function `ingest_pds_map`. The function process and upload a data frame containing the number of trips, CPE (catch per unit effort) and RPE (revenue per unit effort) splitted by grids to produce leaflet maps in the web portal.

# peskas.timor.data.pipeline 1.1.0

## Improvements

- Improve the estimation of some fish groups' catches by calculating the weight per individual case by case.

## Bug fixes

-Fix error in cleaning of legacy landings: the columns indicating the number of individuals and fish length for catches > 60cm were exchanged

# peskas.timor.data.pipeline 1.0.0

## Improvements

Adding option to produce Timor map filtered by fishing trips

# peskas.timor.data.pipeline 0.20.0

## Improvements

- Improve the validation step by flagging those observation characterized by having positive revenue 
(individuals) despite 0 or null individuals (revenue).

- Use a log model to identify abnormal weight-revenue relationships on Cook's distance estimation.

- Replace NA catch code with catch code "0" and ensure these observations having non positive 
individuals and revenue.

- Use total length for weight calculation of MOO in all the landings (weights calculated with FL seem quite unrealistic)

- Use 95° quantile instead of median to summarise weight parameters for catch types, it seems to return 
more realistic weight estimations by single individuals.

## Bug fixes

- Split landings preprocessing in two jobs to avoid to run out of memory (docker exit code 137)

# peskas.timor.data.pipeline 0.19.0

## New features

- Added a new function `ingest_pds_matched_trips` to ingest matched pds tracks and survey landings in a zip folder on a monthly scale.

# peskas.timor.data.pipeline 0.18.0

## New features

- Added nutritional values for each catch by the function `get_nutrients_table`. The function links to the repository https://github.com/mamacneil/NutrientFishbase and join the estimated nutrients values with the FishBase species data.

# peskas.timor.data.pipeline 0.17.0

## New features

- Added the folder *report* containing the scripts useful to generate a pdf report of Peskas. Such report is downloadable from Peskas portal.
- Added a series of functions (`get_sync_tracks()`, `get_full_tracks()` ,`get_full_trips()`) useful to retrieve the complete file of pds tracks.
- Generate and retrieve the pds track map with `ingest_pds_map()` and `get_tracks_map()`.


# peskas.timor.data.pipeline 0.16.0

## Breaking changes

- The Docker file now run rocker/r-ver 4.1.1 instead of rocker/geospatial:4.0.3 

## Bug fixes

- Fixed the bug when calling `ingest_rfish_table()` in the main pipeline.


# peskas.timor.data.pipeline 0.15.0

## New features

- Added a series of functions (`get_catch_types()`, `get_fish_length()` ,`retrieve_lengths()`) useful to retrieve morphometric conversion factors from catch types names in metadata tables. 
- Added a new metadata table in airtable (morphometric_table) containing length-weight and length-length conversion factors.
- Added `join_weights()` which integrates morphometric data with merged landings.


# peskas.timor.data.pipeline 0.14.0

## New features

- We test validated data to ensure its integrity
- Added `merge_trips()` which integrates data from the landings and tracking together
- Added `format_public_data()` to format and export data to be used for analytics

# peskas.timor.data.pipeline 0.13.0

## New features

- Added functions to process and validate pds trips. Namely, `preprocess_pds_trips()` and `validate_pds_trips()`. These functions make sure the data types are appropriate, check for trip duplicates and perform basic checks for trip duration and distance.

# peskas.timor.data.pipeline 0.11.0

## New features

- Added `retrieve_pds_trips_data()`, `retrieve_pds_trips()`,  `retrieve_pds_tracks_data()` and `retrieve_pds_tracks()` to 
download trips and tracks from Pelagic Data System API. 
- Added function `get_pds_res()` to convert Pelagic Data System API responses to data frames and merge trips and tracks data in a unique file.
- Added `ingest_pds_trips()` and `ingest_pds_tracks()` to upload Pelagic Data System data into cloud.
- Created a new bucket (pds-timor-dev) to store pds data.

## Bug fixes

- `cloud_object_name()` returns an empty vector when the bucket is empty

# peskas.timor.data.pipeline 0.10.1

## Bug fixes

- Fixed the bug in survey retrieve process. Integrating a server response status check in `retrieve_survey_data()`

# peskas.timor.data.pipeline 0.10.0

## New features

- Added `merge_landings()` to merge and upload pre-processed recent and legacy landings data.

# peskas.timor.data.pipeline 0.9.1

## Improvements

- Simplified jobs in the pipeline workflow

## Bug fixes

- Fixed workflow so that runs that are supposed to run in production actually do so

# peskas.timor.data.pipeline 0.9.0

## New features

- Added `clean_catches()`, `coalist()` and `clean_legacy_landings()` to restructure legacy landings to recent landings.
- Added `preprocess_legacy_landings()` to clean and ingest preprocessed legacy data.

# peskas.timor.data.pipeline 0.8.0

## New features

- Added `validate_landings()` and `ingest_validation_tables()` to get validation data, check the ladings and upload flags to Airtable. 
- Added `air_tibble_to_records()` and `air_upload_records()` to create and update records in Airtable
- Added logic to validate IMEIs in the landing data
- Added a script in *inst/airtable/edit-submission-link.js* which can be used in Airtable to generate a link to the editable submission form

### Improvements

- Deactivated check of uniqueness of IMEI per boat in `pt_`

### Breaking changes

- Landings pre-processing now renames a few columns in the data: specifically *submission_id* (used to be *_id*) and *landing_date* (used to be *date*)

# peskas.timor.data.pipeline 0.7.0

### New features

- Added `ingest_legacy_landings` to retrieve data from legacy data (SFF landings)

# peskas.timor.data.pipeline 0.6.0

### New features

- Added `air_get_records()` and `air_records_to_tibble()` to retrieve and process records from Airtable
- Added `pt_validate_boats()`, `pt_validate_devices()`, and `pt_validate_vms_installs()` to perform basic data validation from the metadata tables

### Improvements

- `cloud_object_name()` can now also match files by exact name and not just by prefix

### Breaking changes

- `ingest_metadata_tables()` and `preprocess_metadata_tables()` now use logic to use Airtable instead og Google Sheets

# peskas.timor.data.pipeline 0.5.0

### New features

- Added `ingest_metadata_tables()` to ingest data about boats, species, municipalities, etc. 
- Added `preprocess_metadata_tables()` to preprocess the data from the metadata ingestion. 
- Added `pt_get_devices_table()` and `pt_validate_flags()` as helper functions for the metadata preprocessing. 

### Improvements

- We use a single function to read the config.file and load the environment parameters
- Added an skeleton pipeline to provide guidance to future development. This is now visible in the README.

# peskas.timor.data.pipeline 0.4.0

### Breaking changes

- Renamed `ingest_timor_landings()` to `ingest_landings()` for brevity and because all functions relate to Timor anyways. 

### Improvements

- Renamed the job names in the github workflow as the functions that the job calls. 
- Improved documentation of the package: particularly the readme and the function reference.

### Bug fixes

- We now skip RCurl from windows tests

# peskas.timor.data.pipeline 0.3.0

### New features

- The preprocessing of East Timor landings is implemented in `preprocess_landings()`
- Added `pt_nest_attachments()` to group all attachment columns into a nested column containing data frames.
- Added `pt_nest_species()` to group all attachment columns into a nested column containing data frames.
- Added `cloud_object_name()` as a complement to `add_version()` to return the latest or an specified version of an object in an storage location.
- Added `download_cloud_file()` to download files from cloud storage providers.

### Improvements

- Now using `cloud_storage_authenticate()` to internally authenticate to cloud storage instead of authenticating separately in each cloud functionjj. This simplifies authentication and ensures authentication is not attempted when credentials have been already validated. 

# peskas.timor.data.pipeline 0.2.0

### Breaking changes

- `download_survey_data()`, `download_survey_metadata()`, and `download_survey()` have been renamed to `retrieve_survey_data()`, `retrieve_survey_metadata()`, and `retrieve_survey()`. This is to avoid confusion with planned functions that download data from cloud locations.
- The suffix *raw* and *metadata* that is appended to the prefix when retrieving survey information is now separated using "_" rather than "-". This is to more easily distinguish between information encoded in the file name.

### New features

- The prefix name of surveys is not hard-coded and can be specified in the config file (`file_prefix` field).

# peskas.timor.data.pipeline 0.1.0

Adds infrastructure to download survey data and upload it to cloud storage providers and implements the ingestion of East Timor landings. 

### New features

- The ingestion of East Timor Landings is implemented in `ingest_timor_landings()`. 
- The functions `download_survey_data()` and `download_survey_metadata()` which download data and metadata for an electronic survey hosted by *kobo*, *kobohr*, or *ona*. 
- `download_survey()` can be used as a wrapper to download data and metadata in a single call. 
- `upload_cloud_file()` can be used to upload a set of files to a cloud storage bucket. Currently only Google Cloud Services (GCS) is supported. 
- `add_version()` is an utility function that appends date-time and sha information to a string and is used to version file names. 
- `get_host_url()` is an utility function that gets the host url of an electronic survey provider API. 

### Pipeline

The data pipeline is implemented and run in GitHub Actions on a schedule. 
