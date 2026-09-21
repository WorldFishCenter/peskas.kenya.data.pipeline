# Collapse individual length measurements to the species they belong to

`PrioritySpeciesCatch` records one row per *measured fish*, while
`OverallSampleWeight` records one row per *catch item* (a species in the
weighed sample). The two are nested, and the cross-country API schema
carries a single `length_cm` per catch row, so the individuals have to
be collapsed onto their species before the two can be joined.

## Usage

``` r
summarise_priority_lengths(priority_df = NULL)
```

## Arguments

- priority_df:

  Long priority-species data from
  [`reshape_priority_species()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/reshape_priority_species.md).

## Value

Tibble with one row per `submission_id` x `priority_species`:

- submission_id:

  Unique identifier for each submission

- priority_species:

  Survey label of the measured species

- length_type:

  Length convention used (e.g. `total_length`)

- length_cm:

  Mean length of the measured individuals

- length_min_cm, length_max_cm:

  Range of the measured individuals

- n_measured:

  Number of individuals measured for that species

- measured_weight_kg:

  Summed weight of those individuals

## Details

`length_cm` is the plain mean across individuals, which – because Kenya
records one row per fish rather than per length bin – is the same
individual-weighted mean the Timor pipeline publishes for the shared API
schema. It is a subsample statistic: `measured_weight_kg` is the weight
of the fish actually measured and is generally *less* than the species'
`sample_weight`, which covers the whole weighed sample.

Rows carrying no usable length are dropped, so a species measured only
with missing lengths contributes nothing rather than an `NaN` mean.
