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
    lubridate \
    magrittr \
    purrr \
    RCurl \
    readr \
    readxl \
    stringr \
    tibble \
    tidyr \
    rlang \
    furrr \
    future \
    univOutl \
    taxize \
    gsheet \
    tinytest \
    remotes
RUN R -e "remotes::install_version('rfishbase',version='3.1.9',dependencies = TRUE, upgrade='default')"

# Install suggests
RUN install2.r --error --skipinstalled \
    covr \
    pkgdown \
    sessioninfo \
    testthat \
    roxygen2 \
    tidyselect \
    textclean

# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
