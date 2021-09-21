FROM rocker/r-ver:4.1.1

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
    devtools \
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
    remotes \
    rfishbase

# Install specific versions
RUN R -e "devtools::install_version('RCurl', version = '1.98-1.4', repos = 'http://cran.us.r-project.org', dependencies=TRUE)"

# Install suggests
RUN install2.r --error --skipinstalled \
    covr \
    pkgdown \
    sessioninfo \
    testthat \
    roxygen2 \
    tidyselect \
    textclean \
    geosphere

# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
