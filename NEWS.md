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
