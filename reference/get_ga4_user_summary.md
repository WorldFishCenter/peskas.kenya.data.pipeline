# Get GA4 User Summary by Role and BMU

Queries GA4 Data API for user activity metrics grouped by date, BMU and
user role (custom dimensions), using a service account configured in
[`read_config()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/read_config.md).

## Usage

``` r
get_ga4_user_summary(
  property_id = "487803003",
  start_date = "500daysAgo",
  end_date = "today"
)
```

## Arguments

- property_id:

  GA4 property ID.

- start_date:

  Start date for the query (default: `"500daysAgo"`).

- end_date:

  End date for the query (default: `"today"`).

## Value

A data frame summarised by `date`, `custom_event_user_bmu`, and
`custom_event_user_roles` with columns:

- date

- custom_event_user_bmu

- custom_event_user_roles

- users

- sessions

## Details

The function tries event-scoped custom dimensions first
(`customEvent:`), and if not available falls back to user-scoped custom
dimensions (`customUser:`).

## Examples

``` r
if (FALSE) { # \dontrun{
get_ga4_user_summary(property_id = "your_property_id")
} # }
```
