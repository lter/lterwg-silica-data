# ---- annual bess coverage ----

check_bess_coverage <- function(gpp, days, area, year) {
  if (!is.numeric(gpp) || !is.numeric(days) || !is.numeric(area) ||
      length(days) != length(gpp) || length(area) != length(gpp) ||
      !length(year) %in% c(1L, length(gpp))) {
    stop("BESS totals, valid days, watershed coverage, and years must align")
  }
  year_days <- as.integer(as.Date(paste0(year + 1L, "-01-01")) -
    as.Date(paste0(year, "-01-01")))
  valid_days <- is.na(days) | (is.finite(days) & days >= 0 &
    days <= year_days & days == floor(days))
  valid_area <- is.na(area) | (is.finite(area) & area >= 0 & area <= 1)
  valid_total <- is.na(gpp) | (is.finite(gpp) & gpp >= 0 &
    days == year_days & area >= 0.8)
  if (!isTRUE(all(valid_days & valid_area & valid_total))) {
    stop("BESS annual totals require a full calendar year and at least 80% watershed coverage")
  }
}
