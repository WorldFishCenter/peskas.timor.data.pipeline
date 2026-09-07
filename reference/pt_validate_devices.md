# Parse and validate devices table

Convert date and date-time columns and ensures that device_imei is
stored as a character. Currently this table performs no validations.

## Usage

``` r
pt_validate_devices(devices_table)
```

## Arguments

- devices_table:

  a data frame with the devices

## Value

a tibble
