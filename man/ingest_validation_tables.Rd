% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ingest-metadata-tables.R
\name{ingest_validation_tables}
\alias{ingest_validation_tables}
\title{Ingest flags tables}
\usage{
ingest_validation_tables(log_threshold = logger::DEBUG)
}
\description{
Flags tables are manually updated in a Google Drive spreadsheet. This
function downloads the table and uploads it to a drive location. These tables
include information about flags, boats, devices, municipalities, etc.
}
\details{
The parameters needed in \code{conf.yml} are:

\if{html}{\out{<div class="sourceCode">}}\preformatted{flags:
  airtable:
    base_id:
    name:
    api_key:
    tables:
      -
      -
storage:
  storage_name:
    key:
    options:
      project:
      bucket:
      service_account_key:
}\if{html}{\out{</div>}}
}
\keyword{workflow}
