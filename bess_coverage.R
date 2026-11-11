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

# ---- mcm vegetation reliability ----

mcm_vegetation_fields <- function(fields) {
  time <- paste(c("[0-9]{4}", tolower(month.abb)), collapse = "|")
  # match measured values while retaining coverage, days, flags and source notes
  pattern <- paste0("^(bess_gpp_(", time, ")_gC_m2_(year|day)|",
    "npp_(", time, ")_(kgC|gC)_m2_year|greenup_cycle[01]_(", time, ")MMDD|",
    "gee_glc_[0-9]{4}_[[:alnum:]_]+|gpp_[0-9]{4}|npp_[0-9]{4}|",
    "greenup_c[01]_[0-9]{4}|lc_[[:alpha:]_]+_[0-9]{4})$")
  fields[grepl(pattern, fields)]
}

withhold_mcm_vegetation <- function(data) {
  if (inherits(data, "data.table")) data <- data.table::copy(data)
  rows <- which(data$LTER == "MCM")
  for (field in mcm_vegetation_fields(names(data))) {
    values <- data[[field]]
    values[rows] <- NA
    data[[field]] <- values
  }
  data
}

mcm_vegetation_note <- paste(
  "GPP, NPP, greenup and land cover are unreliable at McMurdo;",
  "analytical values are withheld; raw source values and quality records are retained"
)
