FROM rocker/geospatial:4.2

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
    stringr \
    tibble \
    tidyr \
    rlang \
    googlesheets4

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
    rmarkdown \
    ggplot2 \
    bookdown \
    broom \
    zoo \
    ggpubr \
    cowplot \
    kableExtra \
    furrr \
    future \
    univOutl \
    taxize \
    tinytest \
    remotes \
    tidytext \
    zip \
    leaflet \
    htmltools \
    Amelia \
    mice \
    blastula \
    glue \
    mime \
    janitor \
    reactable \
    kableExtra \
    reticulate

# Install GitHub packages
RUN installGithub.r wilkelab/ungeviz
RUN installGithub.r glmmTMB/glmmTMB/glmmTMB
RUN installGithub.r ropensci/rfishbase

#RUN Rscript -e "devtools::install_version('glmmTMB', version = '1.1.5')"
# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
