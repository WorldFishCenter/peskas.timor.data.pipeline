# Publish latest dataset created

The function converts from "draft" to "public" the latest dataset
uploaded in a specific Dataverse repository.

## Usage

``` r
publish_last_dataset(key = NULL, dataverse = NULL, server = NULL)
```

## Arguments

- key:

  API token associated to the Dataverse account.

- dataverse:

  A character string specifying the Dataverse ID.

- server:

  A character string specifying a Dataverse server.

## Examples

``` r
if (FALSE) { # \dontrun{
publish_last_dataset(key = "my_key", dataverse = "my_dataverse", server = "dataverse.example.com")
} # }
```
