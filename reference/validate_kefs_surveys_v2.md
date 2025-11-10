# Validate KEFS Surveys Data (Version 2)

This function imports and validates preprocessed KEFS (Kenya Fisheries)
survey data from Google Cloud Storage. It performs validation checks on
trip characteristics, catch data, and derived indicators (CPUE, RPUE,
price per kg). The function queries KoboToolbox for manual validation
status and respects human-reviewed approvals while generating automated
validation flags for data quality issues.

## Usage

``` r
validate_kefs_surveys_v2()
```

## Value

No return value. Function processes the data and uploads the validated
results and alert flags as Parquet files to Google Cloud Storage.

## Details

The function performs the following main operations:

1.  Downloads preprocessed KEFS survey data from Google Cloud Storage.

2.  Sets up parallel processing with rate limiting (max 4 workers, 200ms
    delay) to avoid overwhelming the API server.

3.  Queries KoboToolbox API to retrieve existing validation statuses for
    all submissions.

4.  Identifies manually approved submissions (excluding system
    approvals) to preserve human review decisions.

5.  Validates the data across multiple dimensions:

    - Information flags: Missing catch outcome and weight data (flag
      1.1)

    - Trip flags: Horse power, number of fishers, trip duration, and
      revenue anomalies (flags 1-4)

    - Catch flags: Sample weight inconsistencies (flags 5.1-5.2)

    - Indicator flags: CPUE, RPUE, and price per kg outliers (flags
      6.1-6.3)

6.  Combines all validation flags into a comprehensive alert system.

7.  Clears validation flags for manually approved submissions
    (respecting human decisions).

8.  Filters valid data based on presence of alert flags.

9.  Uploads both the validation flags and validated dataset as Parquet
    files to Google Cloud Storage.

## Note

This function requires:

- A configuration file with Google Cloud Storage credentials and
  KoboToolbox API credentials

- The `future` package configured for parallel processing

- The preprocessed KEFS surveys data to be available in Google Cloud
  Storage

## Validation Limits

The function uses the following default limits for trip validation:

- max_hp: 150 (maximum horse power)

- max_n_fishers: 100 (maximum number of fishers)

- max_trip_duration: 96 hours (maximum trip duration)

- max_revenue: 387,600 KSH (approximately 3,000 USD)

And for indicator validation:

- max_cpue: 20 kg/fisher/hour (maximum catch per unit effort)

- max_rpue: 3,876 KSH/fisher/hour (approximately 30 USD/fisher/hour)

- max_price_kg: 3,876 KSH/kg (approximately 30 USD/kg)
