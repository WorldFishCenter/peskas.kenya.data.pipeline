% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/validation-functions.R
\name{validate_total_catch}
\alias{validate_total_catch}
\title{Validate Total Catch Data}
\usage{
validate_total_catch(data = NULL, k = NULL, flag_value = NULL)
}
\arguments{
\item{data}{A data frame containing columns: \code{submission_id}, \code{landing_site}, \code{gear},
\code{total_catch_kg}.}

\item{k}{A numeric value passed to \code{\link{get_total_catch_bounds}} for outlier detection.}

\item{flag_value}{A numeric value to use as the flag for catches exceeding the upper bound. Default is 4.}
}
\value{
A data frame with columns: \code{submission_id}, \code{total_catch_kg}, and \code{alert_catch}.
}
\description{
Compares the total catch (in \code{total_catch_kg}) to the upper bounds and flags
values that exceed the bound. Values exceeding bounds are set to NA in the total_catch_kg column.
Bounds are calculated based on landing site and gear type combinations.
}
\keyword{validation}
