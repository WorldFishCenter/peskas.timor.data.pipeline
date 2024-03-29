% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/validation-functions.R
\name{validate_catch_price}
\alias{validate_catch_price}
\title{Validate surveys' total catch values}
\usage{
validate_catch_price(data, method = NULL, k = NULL)
}
\arguments{
\item{data}{A preprocessed data frame}

\item{method}{
		character identifying how to estimate the scale of the distribution. Available choices are:
		
   	   \code{method='IQR'} for using the Inter-Quartile Range, i.e. Q3-Q1; 
 
  	   \code{method='IDR'} for using the Inter-Decile Range; i.e. P90-P10 
		 		   	   
		\code{method='MAD'} for using the Median Absolute Deviation;

		\code{method='Gini'} robust scale estimate based on Gini's Mean Difference (see  \code{\link[Hmisc]{GiniMd}}); 
		
		\code{method='ScaleTau2'} robust tau-estimate of univariate scale, as proposed by Maronna and Zamar (2002) (see also\code{\link[robustbase]{scaleTau2}}); 
		
		\code{method='Qn'} for using the Qn estimator proposed by Rousseeuw and Croux (1993) (see also \code{\link[robustbase]{Qn}}); 
		
		\code{method='Sn'} for using the Sn estimator proposed by Rousseeuw and Croux (1993) (see also \code{\link[robustbase]{Sn}}). 
		
		When \code{method='dQ'} the estimated scale for the left tail is (Q2-Q1)/0.6745, while for the right tail it is considered (Q3-Q2)/0.6745 (Q2 is the median); this double estimate should be able to account for slight skewness.
		
		When \code{method='dD'} the estimated scale for the left tail is (P50-P10)/1.2816, while for the right tail it is considered (P90-P50)/1.2816 (P50 is the median); this double estimate should be able to account for skewness.
		
		 Finally, when \code{method='AdjOut'}, bounds are based on the adjusted outlyingness method as proposed by Hubert and Van der Veeken (2008).
}

\item{k}{
		Nonnegative constant that determines the extension of bounds. Commonly used values are 2, 2.5 and 3 (default).
}
}
\value{
A data frame containing validated catch values.
}
\description{
This function takes a preprocessed landings' matrix and uses univariate
techniques (see \link[univOutl:LocScaleB]{univOutl::LocScaleB}) for the identification of outliers in
the distribution of the total catch values associated to surveys.
}
\examples{
\dontrun{
pars <- read_config()
landings <- get_merged_landings(pars)
validate_catch_value(landings, method = "MAD", k = 13)
}

}
