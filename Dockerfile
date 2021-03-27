FROM rocker/geospatial:4.0.3

# Extra R packages
RUN install2.r --error --skipinstalled \
  config git2r httr jsonlite logger magrittr purrr covr googleCloudStorageR RCurl remotes sessioninfo stringr testthat

# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
