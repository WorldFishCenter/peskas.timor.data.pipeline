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
