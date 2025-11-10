# Check for outliers using IQR method

Check for outliers using IQR method

## Usage

``` r
check_outliers_iqr(x, multiplier = 1.5)
```

## Arguments

- x:

  numeric vector where outliers will be checked

- multiplier:

  multiplier for IQR range (default is 1.5)

## Value

a logical vector indicating which values are within bounds (TRUE) or
outliers (FALSE)

## Examples

``` r
if (FALSE) { # \dontrun{
x <- c(1, 2, 3, 100)
check_outliers_iqr(x, multiplier = 1.5)
} # }
```
