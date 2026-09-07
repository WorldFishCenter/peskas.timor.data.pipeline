# Estimate fisheries indicators

Uses the trip data to estimate various fisheries statistics.

## Usage

``` r
estimate_fishery_indicators(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging threshold level

## Details

The parameters needed in `conf.yml` are:

    models:
      file_prefix:
    storage:
      storage_name:
        key:
        options:
          project:
          bucket:
          service_account_key:
