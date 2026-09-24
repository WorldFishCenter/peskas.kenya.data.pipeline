# Peskas Kenya data pipeline

[![R-CMD-check](https://github.com/WorldFishCenter/peskas.kenya.data.pipeline/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/WorldFishCenter/peskas.kenya.data.pipeline/actions/workflows/R-CMD-check.yaml)
[![pkgdown](https://github.com/WorldFishCenter/peskas.kenya.data.pipeline/actions/workflows/pkgdown.yaml/badge.svg)](https://github.com/WorldFishCenter/peskas.kenya.data.pipeline/actions/workflows/pkgdown.yaml)

The code that turns fish landing surveys and boat GPS tracks from
Kenya’s coast into checked data for Peskas.

**See the results:** [Peskas
Kenya](https://peskas-dashboard-kenya.vercel.app/en), [Peskas Kenya BMU
dashboard](https://digitalfisheries.kenya.peskas.org), [Peskas
Management Platform](https://validation.peskas.org), [Peskas Fishery
Data API](https://api.peskas.org/docs) and [Peskas
Coasts](https://coasts.peskas.org).

## What it is

This pipeline serves fisheries managers, Beach Management Units, survey
teams and researchers working on Kenya’s small-scale fisheries. It
processes two survey programmes: catch and price surveys by the Wildlife
Conservation Society (WCS), and catch assessment surveys by the Kenya
Fisheries Service (KEFS). It links surveys to trips recorded by GPS
trackers on boats, checks every record for likely errors, and publishes
the results.

## What it produces

- Calculates monthly catch, revenue and price indicators for each Beach
  Management Unit from the WCS surveys, for the Peskas Kenya BMU
  dashboard.
- Prepares monthly summaries by district, species and fishing gear for
  the Peskas Kenya dashboard, and Kenya’s figures for the Peskas Coasts
  regional comparison.
- Flags likely errors in each KEFS survey, such as an impossible number
  of fishers, trip duration or fish length, so survey teams can review
  and correct them in the Peskas Management Platform.
- Publishes landing records from both programmes, before and after
  checks, through the Peskas Fishery Data API.
- Links surveys to GPS-tracked trips, which Peskas Coasts uses to
  estimate catch per hour of fishing.

## Where the data comes from

- **WCS surveys.** Enumerators (trained data collectors) record landings
  and fish prices at landing sites on KoboToolbox, the free mobile
  survey app. A landing is a boat’s return to shore with its catch.
- **KEFS surveys.** KEFS catch assessment surveys are recorded on KEFS’s
  own KoboToolbox server.
- **GPS trackers (Pelagic Data Systems).** Small solar-powered devices
  on boats record where they travel. A trip is one fishing outing, from
  leaving shore to landing.
- **Reference data.** Boat and tracker records kept in Airtable, and
  reference tables kept in Google Sheets.

A BMU (Beach Management Unit) is the community body that manages a
landing site in Kenya. The data is updated every two days.

Known limits:

- WCS and KEFS collect different information and sample the catch
  differently, so their figures are not directly comparable. Every
  record says which organisation collected it.
- Surveys are matched to GPS trips by comparing boat and fisher names
  (for WCS, the boat name only), so some matches are missed.
- Only KEFS surveys are sent to the Peskas Management Platform for
  review. WCS surveys are checked inside the pipeline.
- The first version of the KEFS survey form is no longer processed.

## Who runs it

Peskas Kenya is run by [WorldFish](https://worldfishcenter.org/) with
the [Wildlife Conservation Society](https://www.wcs.org/) (WCS) and the
[Kenya Fisheries Service](https://kefs.go.ke/) (KEFS), as part of the
Asia-Africa BlueTech Superhighway project, funded by the UK Government’s
Foreign, Commonwealth and Development Office (FCDO). For questions,
write to <peskas.platform@gmail.com>.

## Part of Peskas

Peskas is WorldFish’s open-source platform for monitoring small-scale
fisheries (<https://peskas.org>).

- [Peskas Zanzibar](https://zanzibar.peskas.org), [Peskas
  Kenya](https://peskas-dashboard-kenya.vercel.app/en), [Peskas
  Mozambique](https://peskas-dashboard-mozambique.vercel.app): country
  dashboards
- [Peskas Timor-Leste](https://timor.peskas.org): Timor-Leste portal
- [Peskas Coasts](https://coasts.peskas.org): regional comparison across
  countries
- [Peskas Tracks](https://tracks.peskas.org): app for fishers to see
  their trips and log catches
- [Peskas Kenya BMU
  dashboard](https://digitalfisheries.kenya.peskas.org): dashboard for
  Beach Management Units in Kenya
- [Peskas Management Platform](https://validation.peskas.org): data
  review and download for survey teams
- [Peskas Fishery Data API](https://api.peskas.org/docs): programmatic
  access to landing data
- Data pipelines:
  [Zanzibar](https://github.com/WorldFishCenter/peskas.zanzibar.data.pipeline),
  [Mozambique](https://github.com/WorldFishCenter/peskas.mozambique.data.pipeline),
  [Timor-Leste](https://github.com/WorldFishCenter/peskas.timor.data.pipeline),
  [Coasts](https://github.com/WorldFishCenter/peskas.coasts)

## For developers

The code is an R package called `peskas.kenya.data.pipeline`. It relies
on the shared
[`coasts`](https://github.com/WorldFishCenter/peskas.coasts) package for
storage, KoboToolbox, GPS tracks and dashboard data. Function reference:
<https://worldfishcenter.github.io/peskas.kenya.data.pipeline/>.

If you work on the WCS side of the pipeline only, start with the [WCS
collaborator
guide](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/WCS-GUIDE.md).

### Requirements

- R 4.5 with the GDAL, GEOS and PROJ spatial libraries. Production uses
  the `rocker/geospatial:4.5` image.
- The `coasts` package, installed from GitHub by
  `devtools::install_deps()`.

### Setup

``` r

devtools::install_deps()
devtools::load_all()
```

Credentials come from environment variables. Copy
[`.env.example`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/.env.example)
to `.env` (git-ignored) and fill in your values.
[`read_config()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/read_config.md)
loads it and reads
[`inst/config.yml`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/inst/config.yml).
The `default` profile uses the development buckets and databases. The
`production` profile is switched on only by CI on `main`.

### Main commands

``` r

devtools::document()  # after editing roxygen comments; man/ is committed
devtools::check()
```

Each pipeline step is one exported function, for example
[`validate_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_kefs_surveys_v2.md).
The order of the steps is in
[`.github/workflows/data-pipeline.yaml`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/.github/workflows/data-pipeline.yaml).

### How it runs in production

GitHub Actions runs every job inside a Docker image built by
[`build-container.yaml`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/.github/workflows/build-container.yaml),
which installs the latest `coasts` release. Runs on `main` use the
`production` profile; runs on any other branch use `default`.

| Workflow | When it runs | What it does |
|----|----|----|
| [`data-pipeline.yaml`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/.github/workflows/data-pipeline.yaml) | 00:00 UTC on every second day of the month (1st, 3rd, 5th, …), and on every push | The full pipeline: WCS, KEFS and GPS data |
| [`wcs-pipeline.yaml`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/.github/workflows/wcs-pipeline.yaml) | On demand, and on push to the WCS code | The WCS chain alone, writing to development storage only |

Do not add a schedule to `wcs-pipeline.yaml`: the main pipeline already
runs the WCS chain, and two scheduled runs would overwrite each other’s
files.

### Releases

Bump `Version:` in `DESCRIPTION` and add a block at the top of
[`NEWS.md`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/NEWS.md)
headed `# peskas.kenya.data.pipeline X.Y.Z`, written for non-technical
readers. On push to `main`, `release.yaml` turns that block into a
GitHub release if the version is new.

### Tests

There are no automated tests yet. `R-CMD-check.yaml` checks that the
package builds and its documentation is consistent. Check a change by
running the affected function against the `default` profile.

### Contributing

Read
[`.github/CONTRIBUTING.md`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/CONTRIBUTING.md).
Code follows the [tidyverse style guide](https://style.tidyverse.org);
comment `/style` on a pull request to apply it. New to R packages? See
[*R Packages*](https://r-pkgs.org) by Hadley Wickham and Jenny Bryan.
For AI-assisted work, see
[`CLAUDE.md`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/CLAUDE.md).

## Licence

GPL-3 or later. See
[`LICENSE.md`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/LICENSE.md).
