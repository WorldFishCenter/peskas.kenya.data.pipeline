% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/preprocessing.R
\name{get_individual_data}
\alias{get_individual_data}
\title{Get fishers ID and trip cost}
\usage{
get_individual_data(raw_dat)
}
\arguments{
\item{raw_dat}{Raw data from cloud storage}
}
\value{
A data frame with the submission ID, fisher ID, and trip cost
}
\description{
This function extracts the fisher ID and trip cost from the raw data.
}
\keyword{internal}
