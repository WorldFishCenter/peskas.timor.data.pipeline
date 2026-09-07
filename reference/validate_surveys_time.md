# Validate surveys' temporal parameters

This function takes the submission-level view of the landings and
validates the temporal info associated to each survey.

## Usage

``` r
validate_surveys_time(submissions, hrs = NULL, submission_delay)
```

## Arguments

- submissions:

  Output of
  [`validation_submissions()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validation_submissions.md).

- hrs:

  Limit of trip duration in hours to be considered a valid catch
  session.

- submission_delay:

  Limit for maximum difference (in days) between the survey submission
  date and the recorded landing date.

## Value

A list containing data frames with validated catch dates and catch
duration.

## Examples

``` r
if (FALSE) { # \dontrun{
conf <- read_config()
landings <- get_weighted_landings(conf)
validate_surveys_time(validation_submissions(landings), hrs = 18, 28)
} # }
```
