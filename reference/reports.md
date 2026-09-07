# Emailed reports

Renamed from `R/send-email.R` in migration Phase 8; the drivers and Rmd
templates this renders live in `inst/report/`. One function is left,
[`send_validation_mail()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/send_validation_mail.md),
and it is not in a healthy workflow: `validation-email-sender.yaml` is
disabled for inactivity and has been failing since ≥2025-09. Phase 5
rewrote the mail's source from the retired Google Sheets to the MongoDB
flags sink and Phase 9 rebuilt the workflow; re-enabling the schedule
waits for the Phase 11b merge, because a cron fires from the default
branch. `send_sites_report()`, which never had a caller at all, went in
Phase 11.
