FROM rocker/geospatial:4.1.1

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
    stringr \
    tibble \
    tidyr \
    rlang

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
    cowplot \
    broom \
    ggpubr \
    furrr \
    future \
    univOutl \
    taxize \
    tinytest \
    remotes \
    rfishbase \
    tidytext

# Install GitHub packages
RUN installGithub.r wilkelab/ungeviz

# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
