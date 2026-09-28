# Restate fork lengths as total lengths

KEFS records each fish on the length type the enumerator measured:
mostly total length, but fork length for tunas, mackerels, jacks and
some snappers. Everything downstream reads `length_cm` as total length,
and the size views compare it with FishBase lengths at maturity that
[`coasts::enrich_taxa()`](https://rdrr.io/pkg/coasts/man/enrich_taxa.html)
restates as total length. So fork lengths are restated here, per fish
and before
[`summarise_priority_lengths()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/summarise_priority_lengths.md)
averages them, with the same POPLL fits: `TL = intercept + slope * FL`,
from
[`coasts::get_tl_conversions()`](https://rdrr.io/pkg/coasts/man/get_tl_conversions.html).

## Usage

``` r
convert_fork_lengths(
  priority_df = NULL,
  taxa_mapping = NULL,
  version = "latest"
)
```

## Arguments

- priority_df:

  Long priority-species data from
  [`reshape_priority_species()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/reshape_priority_species.md).

- taxa_mapping:

  Airtable taxa mapping for the KEFS form, with `survey_label`,
  `alpha3_code` and `scientific_name`.

- version:

  FishBase / SeaLifeBase release. Keep it the release
  [`coasts::enrich_taxa()`](https://rdrr.io/pkg/coasts/man/enrich_taxa.html)
  reads, so both sides of the size comparison use the same fits.

## Value

`priority_df`, with converted fish carrying
`length_type = "total_length"`.

## Details

A species with no fork-length fit keeps its fork lengths, and the step
logs a warning naming it. Carapace (lobsters, crabs) and mantle
(octopus, squid) lengths are left as measured: they are the standard
measures for those animals, and none of the lobsters, crabs, octopus or
squid KEFS measures has a length at maturity to compare against
(2026-09).
