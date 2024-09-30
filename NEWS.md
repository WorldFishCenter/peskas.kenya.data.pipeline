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
