# Send validation summary email

This function takes advantage of the package `blastula` to send an email
containing a summary of the latest submissions with problems.

## Usage

``` r
send_validation_mail(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure. See
  [logger::log_levels](https://daroczig.github.io/logger/reference/log_levels.html)
  for more details

## Value

Nothing, this function is useful for its side effects.

## Details

Reads the flags out of the shared validation database — the sink since
migration Phase 5 — and the alert descriptions out of `config.yml`'s
`validation.alerts` block, which replaced the `alerts` tab of the
retired Google Sheet. Note that the underlying workflow,
`validation-email-sender.yaml`, has been disabled since ≥2025-09 (AUDIT
§5).
