# Helpers for switching between configuration environments.
#
# Since migration Phase 1 there is no `local:` environment: local runs and CI
# resolve the same configuration branch and differ only by R_CONFIG_ACTIVE.
#
#   default     -> the -dev buckets (timor-dev, pds-timor-dev, public-timor-dev)
#   production  -> the live buckets (timor, pds-timor, public-timor)
#
# Secrets come from `.env` in both cases (see `load_dotenv()`), so use_prod()
# points the *same* credentials at the *live* buckets. Handle with care.

use_prod <- function() {
  Sys.setenv(R_CONFIG_ACTIVE = "production")
  cat(
    "\033[32m✓\033[0m Switched to \033[1mPRODUCTION\033[0m environment",
    "\033[33m(live buckets)\033[0m\n"
  )
  invisible(NULL)
}

use_default <- function() {
  Sys.setenv(R_CONFIG_ACTIVE = "default")
  cat(
    "\033[32m✓\033[0m Switched to \033[1mdefault\033[0m environment",
    "(-dev buckets)\n"
  )
  invisible(NULL)
}

show_env <- function() {
  current <- Sys.getenv("R_CONFIG_ACTIVE", "default")
  cat("Current environment: \033[1m", current, "\033[0m\n", sep = "")
  invisible(current)
}

# Show current environment on startup
if (interactive()) {
  cat("\n\033[36mPeskas Timor-Leste Data Pipeline\033[0m\n")
  cat(
    "Current environment: \033[1m",
    Sys.getenv("R_CONFIG_ACTIVE", "default"),
    "\033[0m\n",
    sep = ""
  )
  if (!file.exists(".env")) {
    cat("\033[33m!\033[0m No .env file found — copy .env.example to .env\n")
  }
  cat("\nQuick commands:\n")
  cat("  • use_prod()    - Switch to production (live buckets)\n")
  cat("  • use_default() - Switch to default (-dev buckets)\n")
  cat("  • show_env()    - Show current environment\n\n")
}
