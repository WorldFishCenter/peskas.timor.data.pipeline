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
    RUN Rscript -e "remotes::install_github('ropensci/rfishbase',dependencies=TRUE, upgrade='always')"

# Install suggests
RUN install2.r --error --skipinstalled \
    covr \
    pkgdown \
    remotes \
    sessioninfo \
    testthat \
    roxygen2 \
    tidyselect \
    textclean

# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
