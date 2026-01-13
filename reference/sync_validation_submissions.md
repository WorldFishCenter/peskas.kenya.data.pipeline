# Synchronize Validation Statuses with KoboToolbox

Synchronizes validation statuses between the local system and
KoboToolbox by processing validation flags and updating submission
statuses accordingly. This function respects manual human approvals and
handles both flagged and clean submissions in parallel with rate
limiting.

## Usage

``` r
sync_validation_submissions(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging level threshold for the logger package (e.g., DEBUG,
  INFO). Default is logger::DEBUG.

## Value

None. The function performs status updates and database operations as
side effects.

## Details

The function follows these steps:

1.  Downloads the current validation flags from cloud storage

2.  Sets up parallel processing using the future package (max 4 workers
    with rate limiting)

3.  Fetches current validation status from KoboToolbox to identify
    manual approvals

4.  Identifies manually approved submissions and preserves them
    (excludes system approvals)

5.  Processes flagged submissions (marking as not approved), EXCLUDING
    manually approved ones

6.  Processes clean submissions (marking as approved)

7.  Combines validation statuses from updates and preserved manual
    approvals

8.  Adds KoboToolbox validation status to validation flags

9.  Pushes all validation flags with KoboToolbox status to MongoDB for
    record-keeping

Progress reporting is enabled to track the status of submissions being
processed.

## Note

This function requires proper configuration in the config file,
including:

- MongoDB connection parameters

- KoboToolbox asset ID and token (configured under
  ingestion\$kefs\$koboform)

- Google cloud storage parameters

## Manual Approval Preservation

The function identifies submissions that have been manually approved by
humans (validated_by is not empty and is not the system username) and
preserves these approvals even if automated validation would flag them.
Flagged submissions that were manually approved will NOT be marked as
"not approved" by this function. This ensures human review decisions are
always respected and never overwritten by the automated system.

## Rate Limiting

To avoid overwhelming the KoboToolbox API server (kf.fims.kefs.go.ke),
the function limits parallel workers to 4 and adds a 200ms delay between
requests. This provides approximately 20 requests per second across all
workers while maintaining server stability. With this configuration,
processing 13,000 submissions takes approximately 2-3 hours (including
the initial status fetch to identify manual approvals).

## Examples

``` r
if (FALSE) { # \dontrun{
# Run with default DEBUG logging
sync_validation_submissions()

# Run with INFO level logging
sync_validation_submissions(log_threshold = logger::INFO)
} # }
```
