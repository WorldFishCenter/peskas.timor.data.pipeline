# Add weight of species to merged landings

Downloads merged landings and calculates the weight of the catch.

## Usage

``` r
calculate_weights(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging threshold level

## Value

No outputs. This function is used for it's side effects.

## Details

The file is then uploaded to the cloud. The name is the same as for
merged landings but with "\_weight" at the end. The parameters needed
are:

    surveys:
      merged_landings:
        file_prefix:
        version:
    storage:
      storage_name:
        key:
        options:
          project:
          bucket:
          service_account_key:

Progress through the function is tracked using the package *logger*.
