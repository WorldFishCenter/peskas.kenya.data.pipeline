% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/export.R
\name{export_summaries}
\alias{export_summaries}
\title{Export Summarized Fishery Data}
\usage{
export_summaries(log_threshold = logger::DEBUG)
}
\arguments{
\item{log_threshold}{Logging threshold level (default: logger::DEBUG)}
}
\value{
This function does not return a value. It processes the data and uploads
three separate collections to MongoDB:
\itemize{
\item Monthly catch summaries
\item Gear distribution statistics
\item Fish distribution statistics
}
}
\description{
This function processes and exports validated fishery data by calculating various
summary metrics and distributions, then uploads the results to MongoDB for
dashboard usage.
}
\details{
The function performs the following operations:
\enumerate{
\item Pulls validated data from the "legacy-validated" MongoDB collection
\item Creates three distinct summary datasets:
a. Monthly summaries: Aggregates catch, effort, and CPUE metrics by BMU and month
b. Gear distribution: Calculates gear usage percentages by landing site
c. Fish distribution: Calculates catch composition percentages by landing site
\item Uploads all three datasets to their respective MongoDB collections
}

The following metrics are calculated:
\itemize{
\item Effort = Number of fishers / Size of BMU in km
\item CPUE = Total catch in kg / Effort
\item Monthly aggregations include:
\itemize{
\item Total catch (kg)
\item Mean catch per trip
\item Mean effort
\item Mean CPUE
}
\item Gear distribution includes:
\itemize{
\item Count of gear types used
\item Percentage of each gear type by landing site
}
\item Fish distribution includes:
\itemize{
\item Total catch by fish category
\item Percentage of each fish category in total catch
}
}
}
\note{
This function requires:
\itemize{
\item A configuration file readable by the 'read_config' function with MongoDB connection details
\item Access to a 'bmu_size' dataset through the 'get_metadata()' function
\item The following R packages: dplyr, tidyr, lubridate, purrr, logger
}
}
\examples{
\dontrun{
export_summaries()
}

}
\keyword{data-processing}
\keyword{export}
\keyword{workflow}
