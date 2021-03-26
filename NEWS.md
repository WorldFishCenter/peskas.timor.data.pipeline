# peskas.timor.data.pipeline (development version)

## New features

- Added `pt_nest_attachments()` to group all attachment columns into a nested column containing data frames.

# peskas.timor.data.pipeline 0.2.0

## Breaking changes

- `download_survey_data()`, `download_survey_metadata()`, and `download_survey()` have been renamed to `retrieve_survey_data()`, `retrieve_survey_metadata()`, and `retrieve_survey()`. This is to avoid confusion with planned functions that download data from cloud locations.
- The suffix *raw* and *metadata* that is appended to the prefix when retrieving survey information is now separated using "_" rather than "-". This is to more easily distinguish between information encoded in the file name.

## New features

- The prefix name of surveys is not hard-coded and can be specified in the config file (`file_prefix` field).

# peskas.timor.data.pipeline 0.1.0

Adds infrastructure to download survey data and upload it to cloud storage providers and implements the ingestion of East Timor landings. 

## New features

- The ingestion of East Timor Landings is implemented in `ingest_timor_landings()`. 
- The functions `download_survey_data()` and `download_survey_metadata()` which download data and metadata for an electronic survey hosted by *kobo*, *kobohr*, or *ona*. 
- `download_survey()` can be used as a wrapper to download data and metadata in a single call. 
- `upload_cloud_file()` can be used to upload a set of files to a cloud storage bucket. Currently only Google Cloud Services (GCS) is supported. 
- `add_version()` is an utility function that appends date-time and sha information to a string and is used to version file names. 
- `get_host_url()` is an utility function that gets the host url of an electronic survey provider API. 

## Pipeline

The data pipeline is implemented and run in GitHub Actions on a schedule. 
