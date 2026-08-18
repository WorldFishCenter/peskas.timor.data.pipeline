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
    rfishbase \
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

# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
