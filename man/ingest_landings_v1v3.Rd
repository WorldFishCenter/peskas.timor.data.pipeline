% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ingest-landings.R
\name{ingest_landings_v1v3}
\alias{ingest_landings_v1v3}
\title{Ingest Landings Survey data}
\usage{
ingest_landings_v1v3(log_threshold = logger::DEBUG)
}
\arguments{
\item{log_threshold}{The (standard Apache logj4) log level used as a
threshold for the logging infrastructure. See \link[logger:log_levels]{logger::log_levels} for more details}
}
\value{
No output. This function is used for its side effects
}
\description{
Downloads landings information for multiple surveys collected using Kobo Toolbox and
uploads it to cloud storage services.
}
\details{
This function downloads the survey metadata and responses for multiple landing surveys
(landings_1, landings_3). It then uploads this information to cloud
services. File names contain a versioning string that includes the date-time and,
if available, the first 7 digits of the git commit sha, accomplished using \code{\link[=add_version]{add_version()}}.

The parameters needed in \code{conf.yml} are:

\if{html}{\out{<div class="sourceCode">}}\preformatted{surveys:
  kobo_username:
  kobo_password:
  landings_1:
    file_prefix:
    asset_id:
  landings_3:
    file_prefix:
    asset_id:
storage:
  storage_name:
    key:
    options:
      project:
      bucket:
      service_account_key:
}\if{html}{\out{</div>}}

Progress through the function is tracked using the package \emph{logger}.
}
\keyword{workflow}
