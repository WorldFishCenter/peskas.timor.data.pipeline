library(magrittr)

logger::log_threshold(logger::ERROR)
# Local runs read their credentials from `.env`; CI supplies them as real
# environment variables, where this is a no-op. Not a `setwd("../..")`:
# tinytest sets the working directory to the test file's own directory, so
# that lands inside the R library.
if (file.exists(".env")) dotenv::load_dot_env()
conf <- peskas.timor.data.pipeline::read_config()

public_files <- peskas.timor.data.pipeline:::get_public_files(conf)

expect_false(
  any(duplicated(public_files$trips$trip_id)),
  info = "anonimised trip ids are duplicated"
)


