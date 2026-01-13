FROM rocker/geospatial:4.5

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
    googlesheets4 \
    httr2

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
    reactablefmtr \
    kableExtra \
    reticulate \
    apexcharter \
    ggforce \
    treemapify \
    sf

# Install specific version of rfishbase
RUN Rscript -e 'remotes::install_version("rfishbase", version = "5.0.1")'

# Install GitHub packages
RUN installGithub.r hrbrmstr/ggchicklet
RUN installGithub.r glmmTMB/glmmTMB/glmmTMB



#RUN Rscript -e "devtools::install_version('glmmTMB', version = '1.1.5')"
# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
