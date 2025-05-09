% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/preprocess-metadata-tables.R
\name{preprocess_metadata_tables}
\alias{preprocess_metadata_tables}
\title{Preprocess metadata tables}
\usage{
preprocess_metadata_tables(log_threshold = logger::DEBUG)
}
\description{
Takes ingested metadata tables, validates them, and processes them into
artefacts (devices information, flags) to be used further down in the
pipeline.
}
\details{
Specifically this function:
\itemize{
\item Validates the flags table containing the columns: flag_id, flag_category, flag_message
\item Constructs a devices table containing the columns: imei
}

This function requires no arguments because it retrieves the parameters from
\code{conf.yml}. The fields required are:

\if{html}{\out{<div class="sourceCode">}}\preformatted{metadata:
  spreadsheet:
    name:
  version:
    preprocess:
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
