% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/airtable.R
\name{air_tibble_to_records}
\alias{air_tibble_to_records}
\title{Format a data frame (tibble) as Airtable records}
\usage{
air_tibble_to_records(
  this_tibble,
  id_fields = NULL,
  link_fields = NULL,
  max_records = 10
)
}
\arguments{
\item{this_tibble}{A data frame with the fields to be uploaded to Airtable}

\item{id_fields}{Character vector with the name of the column storing the id}

\item{link_fields}{Character vector with the name of the columns that are
link fields (fields that contain the id of another row's table)}

\item{max_records}{Records are grouped into batches of max_records. Defaults
to 10 which is the current limit imposed by the Airtable API.}
}
\value{
a list with records
}
\description{
Gets a data frame and converts it into a list that can be passed to
\code{air_upload_records()} to be uploaded to Airtable. This is required because
the Airtable API does not accept tabular data and has certain restrictions
about the format of the body request
}
