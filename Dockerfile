<<<<<<< HEAD
FROM rocker/verse:4.1.1
=======
FROM rocker/geospatial:4.1.1
>>>>>>> 6b352bd9b735a6d6c62f7d7b36174e5df758e1c0

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
    dataverse \
    rmarkdown \
    geosphere \
    broom \
    zoo \
    ggpubr \
    cowplot \
    kableExtra \
    bookdown \
    ggplot2 \
    tinytest

# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
