FROM rocker/verse:4.1.1

# Install imports
RUN install2.r --error --skipinstalled \
    config \
    dplyr \
    git2r \
    glmmTMB \
    googleAuthR \
    googleCloudStorageR \
    googledrive \
    httr \
    jsonlite \
    logger \
    lubridate \
    magrittr \
    purrr \
    Rcurl \
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
    tinytest \
    remotes \
    rfishbase

# Install suggests
RUN install2.r --error --skipinstalled \
    covr \
    pkgdown \
    sessioninfo \
    testthat \
    roxygen2 \
    tidyselect \
    textclean \
    rmarkdown \
    ggplot2 \
    zoo \
    bookdown \
    kableExtra \
    cowplot \
    geosphere \
    broom \
    ggpubr \
    sf \
    udunits2

# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
