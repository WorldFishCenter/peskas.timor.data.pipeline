FROM rocker/geospatial:4.5

# Development image: RStudio on :8802 via docker-compose.yaml. Mirrors
# Dockerfile.prod's package set but does NOT install the package itself —
# the working tree is bind-mounted and loaded with devtools::load_all().

# Imports (DESCRIPTION Imports:)
RUN install2.r --error --skipinstalled \
    arrow \
    blastula \
    config \
    data.table \
    dataverse \
    digest \
    dotenv \
    dplyr \
    furrr \
    future \
    geosphere \
    ggplot2 \
    git2r \
    glue \
    googleAuthR \
    googleCloudStorageR \
    googledrive \
    googlesheets4 \
    httr \
    httr2 \
    jsonlite \
    logger \
    lubridate \
    magrittr \
    purrr \
    readr \
    remotes \
    reticulate \
    rlang \
    sf \
    stringr \
    tibble \
    tidyr \
    tidyselect \
    univOutl

# Suggests
RUN install2.r --error --skipinstalled \
    Amelia \
    apexcharter \
    bookdown \
    broom \
    covr \
    cowplot \
    devtools \
    ggforce \
    ggpubr \
    htmltools \
    imputeTS \
    janitor \
    kableExtra \
    leaflet \
    mice \
    mime \
    pkgdown \
    RCurl \
    reactable \
    reactablefmtr \
    readxl \
    rmarkdown \
    roxygen2 \
    sessioninfo \
    taxize \
    testthat \
    textclean \
    tidytext \
    tinytest \
    treemapify \
    zip \
    zoo

# GitHub packages. COASTS_REF matches Dockerfile.prod — keep the two in step so
# a local RStudio session runs against the same coasts release as CI.
# docker-compose passes it; a bare `docker build` needs --build-arg.
ARG COASTS_REF
RUN test -n "$COASTS_REF" && \
    Rscript -e "remotes::install_github('WorldFishCenter/peskas.coasts', ref = '${COASTS_REF}')"
RUN installGithub.r hrbrmstr/ggchicklet

# rfishbase is PINNED to 5.0.1, and the reason is data, not API (2026-09-05).
# It runs LAST on purpose: remotes::install_github() above can upgrade its
# dependencies, so a pin placed before it does not survive.
#
# 5.0.3 moved the parquet host from HuggingFace to Source Cooperative, and the
# two carry different release sets: HuggingFace stops at FishBase **v25.04**,
# Source Cooperative also serves **v26.06**. `coasts::get_combined_tbl()` calls
# `rfishbase::fb_tbl()` with no `version`, so "latest" silently became 26.06 —
# in which Caesionidae was dissolved into Lutjanidae and Scaridae into
# Labridae. Both family names still exist with **zero species in them**, so
# `CJX` and `PWT` resolved to no coefficients and `assert_taxa_coverage()`
# failed the run. Every published figure in NEWS.md was measured on 25.04.
#
# THIS IS A STOPGAP. It pins the *host*, not the release, so it stops working
# the day HuggingFace also serves 26.06. The real pin is a data-version
# argument in coasts (COASTS-TODO C25) driven by `metadata.fishbase.db_version`
# in inst/config.yml. Delete this the moment that ships, then adopt 26.06
# deliberately with its own before/after measurement.
RUN Rscript -e "remotes::install_version('rfishbase', version = '5.0.1', repos = 'https://cloud.r-project.org', upgrade = 'never')"


# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
