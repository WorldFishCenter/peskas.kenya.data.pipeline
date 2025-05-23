% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/validation-functions.R
\name{get_total_catch_bounds_iqr}
\alias{get_total_catch_bounds_iqr}
\title{Get Total Catch Bounds using IQR method}
\usage{
get_total_catch_bounds_iqr(data = NULL, multiplier = 1.5)
}
\arguments{
\item{data}{A data frame containing required columns}

\item{multiplier}{multiplier for IQR range (default is 1.5)}
}
\value{
A data frame with upper bounds for each landing site and gear combination
}
\description{
Get Total Catch Bounds using IQR method
}
\keyword{validation}
