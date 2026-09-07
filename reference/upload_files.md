# Upload files to Dataverse

This function upload a list files in a specific Dataverse repository.

## Usage

``` r
upload_files(file_list = NULL, key = NULL, dataverse = NULL, server = NULL)
```

## Arguments

- file_list:

  Paths indicating the files to be uploaded to Dataverse.

- key:

  API token associated to the Dataverse account.

- dataverse:

  A character string specifying the Dataverse ID.

- server:

  A character string specifying a Dataverse server.

## Examples

``` r
if (FALSE) { # \dontrun{
upload_files(
  file_list = c("file1.tsv", "file2.tsv"),
  key = "my_key",
  dataverse = "my_dataverse",
  server = "dataverse.example.com"
)
} # }
```
