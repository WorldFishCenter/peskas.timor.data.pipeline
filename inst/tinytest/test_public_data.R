library(magrittr)

logger::log_threshold(logger::ERROR)
# Adjust the working directory so that when running localy the authentication
# details in the config file load properly
setwd("../..")
pars <- peskas.timor.data.pipeline::read_config()

public_files <- peskas.timor.data.pipeline:::get_public_files(pars)

expect_false(
  any(duplicated(public_files$trips$trip_id)),
  info = "anonimised trip ids are duplicated"
)


