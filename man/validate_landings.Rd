% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/validate-landings.R
\name{validate_landings}
\alias{validate_landings}
\title{Validate landings}
\usage{
validate_landings(log_threshold = logger::DEBUG)
}
\arguments{
\item{log_threshold}{The (standard Apache logj4) log level used as a
threshold for the logging infrastructure. See \link[logger:log_levels]{logger::log_levels} for more details}
}
\value{
no outputs. This function is used for it's side effects
}
\description{
Downloads the preprocessed version of the data from cloud storage services and
validates a range of information so that it can be safely used for analysis.
By default the function uses the method of the median absolute deviation (MAD)
for outliers identification.
}
\details{
The parameters needed in the config file are those required for
\code{preprocess_landings_step_1()} or \code{preprocess_landings_step_2()},
\code{preprocess_metadata_tables()} \verb{combined, as well as parameters needed to outliers identification that are }hrs\verb{, }method\code{and}k`.
}
\keyword{workflow}
