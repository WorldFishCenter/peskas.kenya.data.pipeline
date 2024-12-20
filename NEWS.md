# peskas.kenya.data.pipeline 0.6.0
## New features
- Add merge_landings function for combining data sources
- Add validation catch function to improve data quality checks
- Include form consent and total catch fields in merged data
- Calculate total catch in legacy landings

## Enhancements
- Homogenise gear and fish groups names
- Update validation code structure
- Fix mismatch between fish groups sum and total catch
- Fix landing sites names
- Improve validation catch function
- Update export data based on validation
- Fix legacy submission IDs

# peskas.kenya.data.pipeline 0.5.1
## New features
- Add functions to extract effort and CPUE from validated legacy data
- Implement MongoDB pushing functionality
- Add storage and metadata functions

## Enhancements
- Index functions by topic and package functional position
- Update package documentation and website
- Improve MongoDB storage calls
- Add configuration file for MongoDB database and collection references

## Fixes
- Drop stale files
- Update storage calls for better efficiency
- Fix various typos and syntax errors

# peskas.kenya.data.pipeline 0.5.0
## Enhancements
- Improve mongoDB storage functions by adding indexes to improve query performance. Before this fix column order in data was not preserved.

# peskas.kenya.data.pipeline 0.4.0
## New features
- Now `ingest_surveys()` uses Kobotoolbox API directly to download surveys instead of rely on R package

## Fixes
- Fix ingestion functions as it was limited to download max 30,000 submissions. The approach now uses pagination to retrieve large datasets, with a limit of 30,000 records per request

# peskas.kenya.data.pipeline 0.3.0
## New features
- Add ingestion function to ingest ongoing data `ingest_surveys()`
- Add preprocessing function to preprocess ongoing data `preprocess_landings()`

# peskas.kenya.data.pipeline 0.2.0
- Add package website with documentation functions
- Integrate mongodb storage with package functions
- Optimize configuration file 

# peskas.kenya.data.pipeline 0.1.0
* Initial CRAN submission.
