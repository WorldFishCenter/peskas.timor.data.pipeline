FROM rocker/geospatial:4.0.3

# Install imports
RUN install2.r --error --skipinstalled \
    config \
    dplyr \
    git2r \
    googleAuthR \
    googleCloudStorageR \
    googledrive \
    httr \
    jsonlite \
    logger \
    magrittr \
    purrr \
    RCurl \
    readr \
    readxl \
    stringr \
    tibble \
    tidyr \
    rlang

# Install suggests
RUN install2.r --error --skipinstalled \
    covr \
    pkgdown \
    remotes \
    sessioninfo \
    testthat \
    roxygen2

# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
