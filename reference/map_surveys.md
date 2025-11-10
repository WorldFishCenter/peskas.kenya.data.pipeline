# Map Survey Labels to Standardized Taxa, Gear, and Vessel Names

Converts local species, gear, and vessel labels from surveys to
standardized names using Airtable reference tables. Handles two survey
form types: the standard form which uses catch_taxon, and the KEFs v2
form which uses priority_species and sample_species. Replaces local
names with scientific names and alpha3 codes, and standardizes gear,
vessel, and site names.

## Usage

``` r
map_surveys(
  data = NULL,
  taxa_mapping = NULL,
  gear_mapping = NULL,
  vessels_mapping = NULL,
  sites_mapping = NULL,
  kefs_v2 = FALSE
)
```

## Arguments

- data:

  A data frame with preprocessed survey data. For standard surveys, must
  contain catch_taxon, gear, vessel_type, and landing_site columns. For
  KEFs v2 surveys, must contain priority_species, sample_species, gear,
  vessel_type, and landing_site columns.

- taxa_mapping:

  A data frame from Airtable taxa table with survey_label, alpha3_code,
  and scientific_name columns.

- gear_mapping:

  A data frame from Airtable gears table with survey_label and
  standard_name columns.

- vessels_mapping:

  A data frame from Airtable vessels table with survey_label and
  standard_name columns.

- sites_mapping:

  A data frame from Airtable landing_sites table with site_code and site
  columns.

- kefs_v2:

  Logical. If TRUE, processes data from the KEFs v2 survey form which
  uses priority_species and sample_species columns. If FALSE (default),
  processes standard survey forms with catch_taxon column.

## Value

A tibble with survey labels replaced by standardized names:

- For standard surveys: catch_taxon replaced by scientific_name and
  alpha3_code

- For KEFs v2 surveys: priority_species replaced by
  priority_scientific_name and priority_alpha3_code; sample_species
  replaced by sample_scientific_name and sample_alpha3_code

- gear and vessel_type replaced by standardized names

- landing_site replaced by the full site name

Records without matches will have NA values.

## Details

This function is called within `preprocess_landings()` after processing
raw survey data. The mapping tables are retrieved from Airtable frame
base and filtered by form ID before being passed to this function.

The KEFs v2 survey form captures both priority species (target catch)
and sample species (for biological sampling), requiring separate
taxonomic mappings for each.
