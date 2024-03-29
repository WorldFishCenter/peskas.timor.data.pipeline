% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/retrieve-survey-data.R
\name{retrieve_survey_data}
\alias{retrieve_survey_data}
\title{Download kobo data in as json using the v1 API}
\usage{
retrieve_survey_data(
  path,
  id = NULL,
  token = NULL,
  api = "kobohr",
  overwrite = TRUE
)
}
\arguments{
\item{path}{string with path to file where API request should be saved}

\item{id}{survey id. Usually a 6 digit number. See \href{https://support.kobotoolbox.org/pulling_data_into_powerbi.html}{this support page} for
an example on how this can be obtained}

\item{token}{access token for the account e.g. "Token XXXXXXX"}

\item{api}{Either "kobo", "kobohr", "ona", or a custom (full) URL. API URLs
are made available for KoBo Toolbox ("kobo",
\url{https://kc.kobotoolbox.org/api/v1/}), KoBo Humanitarian Response
("kobohr", \url{https://kc.humanitarianresponse.info/api/v1/}), Ona ("ona",
\url{https://api.ona.io/api/v1/}) and Unhcr ("unhcr",
\url{https://kobocat.unhcr.org/api/v1/}) . For your own installation, or
other installations using the same API but accessed at a different URL,
enter the full URL.}

\item{overwrite}{Will only overwrite existing path if TRUE.}
}
\value{
The file path
}
\description{
Download kobo data in as json using the v1 API
}
\examples{
\dontrun{
retrieve_survey_data("test.json", id = 753491, token = "XXX")
file.remove("test.json")
}
}
