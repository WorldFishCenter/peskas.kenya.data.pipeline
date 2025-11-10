# Generate an alert vector based on IQR method

Generate an alert vector based on IQR method

## Usage

``` r
alert_outlier_iqr(
  x,
  no_alert_value = NA_real_,
  alert_if_larger = no_alert_value,
  alert_if_smaller = no_alert_value,
  multiplier = 1.5
)
```

## Arguments

- x:

  numeric vector where outliers will be checked

- no_alert_value:

  value to put in the output when there is no alert

- alert_if_larger:

  alert for when x is above the upper bound

- alert_if_smaller:

  alert for when x is below the lower bound

- multiplier:

  multiplier for IQR range (default is 1.5)

## Value

a vector of the same length as x with alert values
