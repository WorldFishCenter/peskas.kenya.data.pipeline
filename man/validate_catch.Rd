% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/validation.R
\name{validate_catch}
\alias{validate_catch}
\title{Validate Catch Data}
\usage{
validate_catch(data = NULL, k = NULL)
}
\arguments{
\item{data}{A data frame containing catch data with columns: catch_id, gear_new, catch_name, and catch_kg.}

\item{k}{A numeric value used in the get_catch_bounds function for outlier detection.}
}
\value{
A data frame with columns: catch_id, catch_kg, and alert_catch.
}
\description{
This function validates the catch data by comparing it to calculated upper bounds.
}
\examples{
\dontrun{
validated_catch <- validate_catch(your_data, k = 3)
}
}
\keyword{validation}
