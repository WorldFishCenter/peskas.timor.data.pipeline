
<!-- README.md is generated from README.Rmd. Please edit that file -->

# peskas.timor.data.pipeline

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://www.tidyverse.org/lifecycle/#experimental)
[![CRAN
status](https://www.r-pkg.org/badges/version/peskas.timor.data.pipeline)](https://CRAN.R-project.org/package=peskas.timor.data.pipeline)
[![Codecov test
coverage](https://codecov.io/gh/WorldFishCenter/peskas.timor.data.pipeline/branch/master/graph/badge.svg)](https://codecov.io/gh/WorldFishCenter/peskas.timor.data.pipeline?branch=master)
[![R build
status](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/workflows/R-CMD-check/badge.svg)](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/actions)
<!-- badges: end -->

The goal of peskas.timor.data.pipeline is to implement, deploy, and
execute the data and modelling pipelines that underpin Peskas-East
Timor, the small-scale fisheries analytics in East Timor.

## The data pipeline is an R package

peskas.timor.data.pipeline is structured as an R package because it
makes it easier to write production-grade software. Specifically,
structuring the code as an R package allows us to:

  - better handle system and package dependencies,
  - forces us to split the code into functions,
  - makes it easier to document the code, and
  - makes it easier to test the code

We make heavy use of [tidyverse style
conventions](https://engineering-shiny.org) and the
[usethis](https://usethis.r-lib.org) package to automate tasks during
project setup and deployment.

For more information about the rationale of structuring the pipeline as
a package check
[Chapter 3](https://engineering-shiny.org/structuring-project.html#structuring-your-app_)
in [Engineering Production-Grade Shiny
Apps](https://engineering-shiny.org). The book is focused on Shiny
applications but the rationale also applies to data pipelines and
production-ready code in general. The best place to learn more about
package development is probably the [R package book](https://r-pkgs.org)
by Hadley Wickham and Jenny Brian.
## The pipeline runs on Github Actions

While each step in the pipeline are defined as a function in the
package, these functions are deployed and integrated using [GitHub
Actions](https://docs.github.com/en/actions/learn-github-actions). This
allow us to take advantages of best practices in continous development
and integration (CD/CI) and automatically link the code to execution.

Each job in the pipeline is defined in the workflow file:
[`.github/workflows/data-pipeline.yaml`](.github/workflows/data-pipeline.yaml)
and can be seen in the figure below. Note that additional workflows
exist to test the package in multiple environments and build the
documentation website.

![](man/figures/pipeline.png)

Generally, artifacts produced by each job are stored in a cloud storage
container and retrieved from the cloud storage by the next job in the
pipeline. When storing a job’s artifacts are versioned using the
function `add_version()`, which generally includes a timestamp and the
commit sha. This approach allow us to trace each artifact to a unique
run of the pipeline. When retrieving jobs can call `cloud_object_name()`
to obtain the latest or an specific version of an artifact.


## Installation

You can install the development version from
[GitHub](https://github.com/) with:

``` r
# install.packages("remotes")
remotes::install_github("WorldFishCenter/peskas.timor.data.pipeline")
```
