% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ingestion.R
\name{flatten_field}
\alias{flatten_field}
\title{Flatten a Single Field of Kobotoolbox Data}
\usage{
flatten_field(x, p)
}
\arguments{
\item{x}{The field to be flattened.}

\item{p}{The name of the parent field.}
}
\value{
A flattened list representing the input field.
}
\description{
This internal function flattens a single field within a row of Kobotoolbox data.
}
\keyword{internal}
