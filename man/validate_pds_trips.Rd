% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/validate-pds-trips.R
\name{validate_pds_trips}
\alias{validate_pds_trips}
\title{Validate Pelagic Data System trips}
\usage{
validate_pds_trips(log_threshold = logger::DEBUG)
}
\arguments{
\item{log_threshold}{The (standard Apache logj4) log level used as a
threshold for the logging infrastructure. See \link[logger:log_levels]{logger::log_levels} for more
details}
}
\value{
no outputs. This function is used for it's side effects
}
\description{
Downloads the preprocessed version of pds trips and pds tracks disgnostics
from cloud storage services and validates a range of information so that it
can be safely used for analysis.
}
\details{
The parameters needed in the config file are those required for
\code{preprocess_pds_trips()}, as well as parameters needed to identify anomalous
trips.
}
