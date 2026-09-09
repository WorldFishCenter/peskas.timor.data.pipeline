# Package index

## Workflow

These are arguably the most important functions in the package. Each of
these functions executes a step in the data pipeline, and each is called
from `.github/workflows/data-pipeline.yaml` by name.

- [`calculate_weights()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/calculate_weights.md)
  : Add weight of species to merged landings
- [`describe_pds_tracks()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/describe_pds_tracks.md)
  : Extract per-trip descriptors from PDS tracks
- [`estimate_fishery_indicators()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/estimate_fishery_indicators.md)
  : Estimate fisheries indicators
- [`export_api_raw()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_api_raw.md)
  : Export the raw trips table to the cross-country API bucket
- [`export_api_validated()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_api_validated.md)
  : Export the validated trips table to the cross-country API bucket
- [`export_files()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_files.md)
  : Export portal datasets as versioned JSON files to public cloud
  storage
- [`format_public_data()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/format_public_data.md)
  : Format public data
- [`ingest_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/ingest_landings.md)
  : Ingest landings surveys from KoboToolbox
- [`merge_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_landings.md)
  : Merge the pre-processed landings of every form version
- [`merge_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_trips.md)
  : Merge trips
- [`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md)
  : Pre-process the landing surveys into a flat long catch table
- [`sync_validation_status()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/sync_validation_status.md)
  : Write validation statuses back to KoBoToolbox
- [`upload_dataverse()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/upload_dataverse.md)
  : Upload and publish a dataset on Dataverse
- [`validate_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landings.md)
  : Validate landings
- [`validate_pds_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_pds_trips.md)
  : Validate Pelagic Data System trips

## Cloud storage

Accessors over the versioned artefacts in Google Cloud Storage. The
storage layer itself is `coasts::*`; these resolve the latest version of
each artefact and give it back in the shape its readers expect.

- [`get_assets()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_assets.md)
  : Download the Airtable frame mapping tables for Timor
- [`get_merged_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_merged_landings.md)
  : Download the merged landings
- [`get_merged_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_merged_trips.md)
  : Download Peskas surveys and PDS data
- [`get_models()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_models.md)
  : Download models estimates
- [`get_tracks_descriptors()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_tracks_descriptors.md)
  : Extract summaries from one pds track
- [`get_validated_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_validated_landings.md)
  : Download Peskas validated landings, in the nested portal shape
- [`get_validated_landings_long()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_validated_landings_long.md)
  : Download the validated landings in the flat long shape
- [`get_weighted_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_weighted_landings.md)
  : Download the merged landings with catch weights

## Ingestion

Functions dedicated to the ingestion module.

- [`ingest_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/ingest_landings.md)
  : Ingest landings surveys from KoboToolbox

## Preprocessing

Functions dedicated to the preprocessing module.

- [`expand_length_frequency()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/expand_length_frequency.md)
  : Expand each catch into one row per length bin
- [`join_weights()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/join_weights.md)
  : Join length-weights and nutritional parameters info to preprocessed
  landings
- [`merge_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_landings.md)
  : Merge the pre-processed landings of every form version
- [`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md)
  : Pre-process the landing surveys into a flat long catch table
- [`reshape_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/reshape_landings.md)
  : Reshape one raw landings table into the flat long catch table
- [`reshape_species_groups()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/reshape_species_groups.md)
  : Reshape KoBo species groups into one row per catch
- [`survey_labels()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/survey_labels.md)
  : Label lookup tables for preprocessing

## Validation

Functions dedicated to the validation module. Deeper than any other
country pipeline — the alert codes are a contract, documented in the
header of `R/validation-functions.R` and in the `validation.alerts`
block of `inst/config.yml`.

- [`push_validation_flags()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/push_validation_flags.md)
  : Push the validation flags to the shared validation database
- [`sync_validation_status()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/sync_validation_status.md)
  : Write validation statuses back to KoBoToolbox
- [`validate_catch_params()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_catch_params.md)
  : Validate surveys' catch parameters
- [`validate_catch_price()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_catch_price.md)
  : Validate surveys' total catch values
- [`validate_conservation()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_conservation.md)
  : Resolve the recorded catch-preservation method
- [`validate_fuel()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_fuel.md)
  : Validate the recorded fuel use
- [`validate_gear_type()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_gear_type.md)
  : Validate the recorded gear type
- [`validate_gleaners()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_gleaners.md)
  : Validate the recorded number of gleaners
- [`validate_habitat()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_habitat.md)
  : Validate the recorded habitat
- [`validate_happiness()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_happiness.md)
  : Carry through the recorded happiness rating
- [`validate_imeis()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_imeis.md)
  : Validate the tracker IMEIs recorded on a survey
- [`validate_landing_regularity()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landing_regularity.md)
  : Validate the internal consistency of a landing
- [`validate_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landings.md)
  : Validate landings
- [`validate_mesh()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_mesh.md)
  : Validate the recorded mesh size
- [`validate_n_fishers()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_n_fishers.md)
  : Validate the recorded crew size
- [`validate_pds_data()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_pds_data.md)
  : Validate pds trips duration and distance
- [`validate_pds_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_pds_trips.md)
  : Validate Pelagic Data System trips
- [`validate_price_weight()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_price_weight.md)
  : Outlier identification of the price-weight relation
- [`validate_sites()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_sites.md)
  : Validate the recorded landing site
- [`validate_surveys_time()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_surveys_time.md)
  : Validate surveys' temporal parameters
- [`validate_vessel_type()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_vessel_type.md)
  : Validate the recorded vessel type
- [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md)
  : One row per submission

## Export

Publication of processed and analysed fisheries data — the portal JSON
contract, the cross-country API parquet, Dataverse and the emailed
reports.

- [`check_portal_contract()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/check_portal_contract.md)
  : Check the portal JSON contract
- [`export_api_raw()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_api_raw.md)
  : Export the raw trips table to the cross-country API bucket
- [`export_api_validated()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_api_validated.md)
  : Export the validated trips table to the cross-country API bucket
- [`generate_description()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/generate_description.md)
  : Generate data description
- [`publish_last_dataset()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/publish_last_dataset.md)
  : Publish latest dataset created
- [`send_validation_mail()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/send_validation_mail.md)
  : Send validation summary email
- [`upload_dataverse()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/upload_dataverse.md)
  : Upload and publish a dataset on Dataverse
- [`upload_files()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/upload_files.md)
  : Upload files to Dataverse

## Helper functions

Reshaping, taxonomy, geometry and configuration helpers.

- [`add_version()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/add_version.md)
  : Add timestamp and sha string to a file name
- [`convert_taxa_names()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/convert_taxa_names.md)
  : Assign each taxon code to a broad species group
- [`fill_missing_regions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/fill_missing_regions.md)
  : Fill missing regions
- [`get_airtable_form_id()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_airtable_form_id.md)
  : Resolve a KoBo asset id to its Airtable form record id
- [`get_distance()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_distance.md)
  : Estimate distance between consecutive trips
- [`get_fao_composition()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_fao_composition.md)
  : Get FAO Food Composition Data
- [`get_morphometric_tables()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_morphometric_tables.md)
  : Build per-taxon length-weight coefficients
- [`get_nutrients_table()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_nutrients_table.md)
  : Get taxa nutritional values
- [`get_timor_boundaries()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_timor_boundaries.md)
  : Extract Timor Geographical Boundaries
- [`load_dotenv()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/load_dotenv.md)
  : Load environment variables from a .env file
- [`merge_consecutive_trips()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/merge_consecutive_trips.md)
  : Merge short intervalled trips
- [`read_config()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/read_config.md)
  : Read configuration file
