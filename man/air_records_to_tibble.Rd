% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/airtable.R
\name{air_records_to_tibble}
\alias{air_records_to_tibble}
\title{Format Airtable records to a data frame (tibble)}
\usage{
air_records_to_tibble(records)
}
\arguments{
\item{records}{List of records from an airtable table}
}
\value{
a tibble
}
\description{
Transforms a list of records obtained from the airtable api (for example
using \code{air_get_records}) into a data frame.
}
\details{
Fields that have a only one item are stored as a vector while fields that
accept multiple items are stored as a nested list column
}
\examples{
\dontrun{
# Get all records for the table boats
air_get_records(table = "boats", base_id = "appjEVaN8kBNXAWak") \%>\%
  air_record_to_tibble()
}
}
