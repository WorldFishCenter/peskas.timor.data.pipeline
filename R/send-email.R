#' Send sites report email
#'
#' This function takes advantage of the package `blastula` to send an email
#' containing a summary report in each landig site.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#' threshold for the logging infrastructure. See [logger::log_levels] for more
#' details
#'
#' @return Nothing, this function is useful for its side effects.
#' @export
#'
send_sites_report <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  conf <- read_config()

  month <- lubridate::month(Sys.Date(), label = T)
  year <- lubridate::year(Sys.Date())

  logger::log_info("Generate mail")
  email <-
    blastula::compose_email(
      body = blastula::md(
        glue::glue(
          "Hello,

      The attached report provides a summary of the overall performance and activity in each landing site on an annual and monthly scale.
      The report includes information on the number of surveys submitted, the working days, and the estimated hours per day in each landing site.

      We believe that this report will provide valuable insights into the enumerators work and can help to identify any areas for improvement.
      We would appreciate it if you could take the time to review the report and provide us with your feedback.

      Please note that the report is automatically generated.
      If you have any questions or concerns, please do not hesitate to contact us.

      Thank you,

      The peskAAS Team"
        )
      ),
      footer = blastula::md(glue::glue("Email sent on ", as.character(Sys.time())))
    )

  logger::log_info("Attach report to mail")

  file <- list.files(
    system.file("report", package = "peskas.timor.data.pipeline"),
    pattern = c("summary_report.pdf"), full.names = T
  )

  content_type <- mime::guess_type(file)
  filename <- basename(file)

  expanded_path <-
    file %>%
    path.expand() %>%
    normalizePath(mustWork = TRUE)

  attachment_list <- list(
    file_path = expanded_path,
    content_type = content_type,
    disposition = "attachment",
    filename = filename
  )
  email$attachments <- c(email$attachments, list(attachment_list))

  logger::log_info("Generate credentials file")

  file_cred <- file("creds.txt")
  writeLines(conf$peskas_mail$key, file_cred)
  close(file_cred)

  logger::log_info("Send mail")

  email %>%
    blastula::smtp_send(
      from = "peskas.platform@gmail.com",
      to = c("l.longobardi@cgiar.org", "lorenzo.longobardi@gmail.com"),
      subject = paste("Monthly Enumerator Activity Report", paste(month, year)),
      credentials = blastula::creds_file("creds.txt")
    )
  file.remove("creds.txt")
}

#' Send validation summary email
#'
#' This function takes advantage of the package `blastula` to send an email
#' containing a summary of the latest submissions with problems.
#'
#' Reads the flags out of the shared validation database — the sink since
#' migration Phase 5 — and the alert descriptions out of `config.yml`'s
#' `validation.alerts` block, which replaced the `alerts` tab of the retired
#' Google Sheet. Note that the underlying workflow,
#' `validation-email-sender.yaml`, has been disabled since ≥2025-09 (AUDIT §5).
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#' threshold for the logging infrastructure. See [logger::log_levels] for more
#' details
#'
#' @return Nothing, this function is useful for its side effects.
#' @export
#'
send_validation_mail <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  conf <- read_config()
  mdb <- conf$storage$mongodb

  logger::log_info("Filtering validation flags from {Sys.Date() - 7}")

  peskas_alerts <-
    purrr::map_dfr(c("v2", "v3"), function(version) {
      coasts::mdb_collection_pull(
        connection_string = mdb$connection_strings$validation,
        db_name = mdb$databases$validation$database_name,
        collection_name = paste(
          mdb$databases$validation$collections$flags,
          conf$ingestion$landings[[version]]$asset_id,
          sep = "-"
        )
      )
    }) %>%
    dplyr::mutate(submission_date = lubridate::as_date(.data$submission_date)) %>%
    dplyr::filter(.data$submission_date >= Sys.Date() - 7) %>%
    dplyr::select("submission_id", "submission_date", "alert_flag")

  peskas_alerts_week <- dplyr::filter(peskas_alerts, !is.na(.data$alert_flag))

  alert_description <- tibble::tibble(
    alert_flag = names(conf$validation$alerts),
    alert_description = unlist(conf$validation$alerts, use.names = FALSE)
  )

  alerts_week <-
    dplyr::left_join(peskas_alerts_week, alert_description, by = "alert_flag") %>%
    dplyr::mutate(
      alert_description = dplyr::if_else(
        grepl(",", .data$alert_flag), "Multiple alerts", .data$alert_description
      )
    ) %>%
    dplyr::rename(
      "submission id" = "submission_id",
      "submission date" = "submission_date",
      description = "alert_description",
      "alert code" = "alert_flag"
    )

  n_submissions_alert <- nrow(alerts_week)
  n_submission_tot <- nrow(peskas_alerts)

  logger::log_info("Generate mail")
  email <-
    blastula::compose_email(
      body = blastula::md(
        c(
          glue::glue(
            "Hi there,

          In the last week there have been {n_submissions_alert} new landing
          surveys that may have some problems with the data entered on a total
          of {n_submission_tot} submissions. Please open the Peskas validation
          app and check the submissions on KoBoToolBox to make corrections.
          If you don't think the flag is an error please let us know.
          "
          ),
          alerts_week %>%
            kableExtra::kbl(align = "c") %>%
            kableExtra::kable_styling(bootstrap_options = "striped")
        )
      ),
      footer = blastula::md(glue::glue("Email sent on ", as.character(Sys.time())))
    )

  logger::log_info("Generate credentials file")

  file_cred <- file("creds.txt")
  writeLines(conf$peskas_mail$key, file_cred)
  close(file_cred)

  logger::log_info("Send mail")

  email %>%
    blastula::smtp_send(
      from = "peskas.platform@gmail.com",
      to = c("l.longobardi@cgiar.org", "v.sozinho@cgiar.org", "mafpeskaas@gmail.com"),
      subject = paste("Peskas automations:", n_submissions_alert, "new submissions have problems"),
      credentials = blastula::creds_file("creds.txt")
    )
  file.remove("creds.txt")
}
