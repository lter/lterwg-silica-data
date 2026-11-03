# ---- spatial columns ----

# load the shared check from the same folder as this script
script_path <- sys.frame(1)$ofile
if (is.null(script_path)) script_path <- "combine_chemistry_spatial.R"
source(file.path(dirname(script_path), "bess_coverage.R"), local = TRUE)

# match annual predictors to the chemistry year and keep static predictors by site
spatial_column_matches <- function(columns) {
  columns <- setdiff(columns, c("LTER", "Stream_Name", "Site_Flag"))
  columns <- columns[!grepl("^greenup_cycle1_", columns)]
  year_text <- regmatches(columns, regexpr("(?<![0-9])(19|20)[0-9]{2}(?![0-9])", columns, perl = TRUE))
  year <- rep(NA_integer_, length(columns))
  annual <- grepl("(?<![0-9])(19|20)[0-9]{2}(?![0-9])", columns, perl = TRUE)
  year[annual] <- as.integer(year_text)
  combined <- columns
  combined[annual] <- gsub("(19|20)[0-9]{2}", "", columns[annual])
  combined <- gsub("__", "_", combined, fixed = TRUE)
  combined <- sub("_$", "", combined)
  result <- data.frame(Source_Column = columns, Combined_Column = combined, Match_Year = year)
  if (anyDuplicated(result[c("Combined_Column", "Match_Year")])) {
    stop("multiple spatial columns supply the same predictor and year")
  }
  result
}

# check every value outside the updated sites without confusing integers and decimals
check_spatial_update <- function(updated, previous, changed_rows) {
  if (!identical(names(updated), names(previous)) || nrow(updated) != nrow(previous)) {
    stop("the spatial update changed columns or the number of sites")
  }
  rows <- setdiff(seq_len(nrow(previous)), changed_rows)
  for (column in names(previous)) {
    before <- previous[[column]][rows]
    after <- updated[[column]][rows]
    unchanged <- if (is.numeric(before) && is.numeric(after)) {
      identical(as.double(before), as.double(after))
    } else identical(before, after)
    if (!unchanged) stop("spatial values changed outside the updated sites: ", column)
  }
  invisible(TRUE)
}

# ---- combined export ----

# discharge must already contain the checked site assignments and original-source additions
# this function adds flow, applies the walker rule, and checks the written csv
write_combined_chemistry <- function(chemistry_file, discharge, spatial, site_metadata,
    chemistry_metadata_file, walker_samples, additions, data_key, output_dir,
    discharge_functions = "discharge_functions.R", chunk_size = 50000L) {
  source(discharge_functions, local = TRUE)
  spatial <- as.data.frame(spatial)
  # require the replacement before reading the large chemistry table
  gpp_columns <- grep("^bess_gpp_[0-9]{4}_gC_m2_year$", names(spatial), value = TRUE)
  gpp_years <- as.integer(sub("^bess_gpp_([0-9]{4})_gC_m2_year$", "\\1", gpp_columns))
  if (!length(gpp_columns) || any(!gpp_years %in% 1982:2022)) {
    stop("provide checked BESSv2 GPP for 1982-2022 before rebuilding the combined export")
  }
  for (column in gpp_columns) {
    days_column <- sub("gC_m2_year$", "valid_days", column)
    area_column <- sub("gC_m2_year$", "area_fraction", column)
    required <- c(column, days_column, area_column)
    if (!all(required %in% names(spatial)) ||
        !all(vapply(spatial[, required, drop = FALSE], is.numeric, logical(1)))) {
      stop("BESS annual GPP needs numeric values, valid days, and watershed area fractions")
    }
    year <- as.integer(sub("^bess_gpp_([0-9]{4})_gC_m2_year$", "\\1", column))
    check_bess_coverage(spatial[[column]], spatial[[days_column]], spatial[[area_column]], year)
  }
  chemistry <- data.table::fread(chemistry_file, na.strings = "", showProgress = FALSE)
  site_metadata <- as.data.frame(site_metadata)
  site_metadata$Productivity_Source <- "BESSv2 GPP"
  site_metadata$Productivity_Units <- "gC_m2_year"
  site_metadata$Productivity_Note <- paste(
    "monthly mean daily GPP weighted by calendar days, with at least 80% watershed coverage;",
    "valid days and area fractions accompany each year; missing years stay blank"
  )
  site_key <- function(data) paste(data$LTER, data$Stream_Name, sep = "\r")
  clean <- function(value) ifelse(is.na(value), "", as.character(value))
  if (anyNA(chemistry$date) || anyNA(as.Date(chemistry$date))) stop("chemistry contains missing or invalid dates")
  if (anyDuplicated(site_key(spatial)) || anyDuplicated(site_key(site_metadata))) {
    stop("spatial data and metadata must each have one row per site")
  }
  sites <- unique(site_key(chemistry))
  if (!setequal(sites, site_key(spatial)) || !setequal(sites, site_key(site_metadata))) {
    stop("chemistry, spatial data, and metadata have different site lists")
  }
  site_rows <- match(site_key(chemistry), site_key(spatial))
  if (!identical(clean(chemistry$Site_Flag), clean(spatial$Site_Flag[site_rows]))) {
    stop("chemistry and spatial site flags differ")
  }
  if (!identical(clean(spatial$Site_Flag), clean(site_metadata$Site_Flag[
      match(site_key(spatial), site_key(site_metadata))]))) stop("site metadata flags differ")
  columns <- spatial_column_matches(names(spatial))
  if (any(columns$Combined_Column %in% c(names(chemistry), "Qcms", "Q_Flag", "year"))) {
    stop("a spatial predictor would replace a chemistry or discharge column")
  }
  joined <- join_chemistry_discharge(chemistry, discharge, walker_samples, additions, data_key)
  if (nrow(joined) != nrow(chemistry) || !all(vapply(names(chemistry), function(column) {
    identical(joined[[column]], chemistry[[column]])
  }, logical(1)))) stop("the discharge join changed chemistry")
  joined$year <- as.integer(format(as.Date(joined$date), "%Y"))

  # build only the rows needed for each writing or readback chunk
  make_chunk <- function(rows) {
    part <- joined[rows, , drop = FALSE]
    matched <- site_rows[rows]
    for (column in unique(columns$Combined_Column)) {
      rules <- columns[columns$Combined_Column == column, , drop = FALSE]
      values <- spatial[[rules$Source_Column[1]]][rep(NA_integer_, length(rows))]
      for (i in seq_len(nrow(rules))) {
        keep <- if (is.na(rules$Match_Year[i])) seq_along(rows) else
          which(part$year == rules$Match_Year[i])
        values[keep] <- spatial[[rules$Source_Column[i]]][matched[keep]]
      }
      part[[column]] <- values
    }
    part
  }
  if (dir.exists(output_dir)) stop("choose a new output directory")
  dir.create(output_dir, recursive = TRUE)
  tag <- format(Sys.Date(), "%Y%m%d")
  output <- file.path(output_dir, paste0(tag, "_combined_chemistry_discharge_spatial.csv"))
  partial <- paste0(output, ".partial")
  starts <- seq.int(1L, nrow(joined), by = chunk_size)
  for (start in starts) {
    rows <- seq.int(start, min(start + chunk_size - 1L, nrow(joined)))
    data.table::fwrite(make_chunk(rows), partial, append = start > 1L, na = "")
  }

  # read the saved file independently and compare every column with its source match
  checked <- 0L
  callback <- readr::SideEffectChunkCallback$new(function(actual, position) {
    rows <- seq.int(checked + 1L, checked + nrow(actual))
    expected <- make_chunk(rows)
    if (!identical(names(actual), names(expected))) stop("exported columns differ")
    for (column in names(expected)) {
      original <- expected[[column]]
      saved <- actual[[column]]
      if (is.numeric(original)) {
        numeric_saved <- suppressWarnings(as.numeric(saved))
        if (!identical(is.na(original), is.na(numeric_saved))) stop("exported missing values differ: ", column)
        keep <- !is.na(original)
        if (any(!is.finite(numeric_saved[keep])) ||
            any(abs(original[keep] - numeric_saved[keep]) > 1e-12 * pmax(1, abs(original[keep])))) {
          stop("exported numeric values differ: ", column)
        }
      } else if (!identical(clean(original), clean(saved))) stop("exported text differs: ", column)
    }
    checked <<- checked + nrow(actual)
  })
  readr::read_csv_chunked(partial, callback, chunk_size = chunk_size,
    col_types = readr::cols(.default = readr::col_character()), na = "", progress = FALSE)
  if (checked != nrow(chemistry)) stop("exported row count differs")

  # keep the existing site and chemistry notes, plus the source-to-output column matches
  site_metadata$Record_Type <- "site"
  chemistry_notes <- data.table::fread(chemistry_metadata_file, na.strings = "", showProgress = FALSE)
  chemistry_notes$Record_Type <- "chemistry note"
  columns$Record_Type <- "spatial column match"
  columns$Note <- ifelse(is.na(columns$Match_Year), "matched by site", "matched by site and chemistry year; no extrapolation")
  notes <- data.frame(Record_Type = "assembly note", Note = c(
    "chemistry values, flags, row count, and row order are unchanged",
    "Qcms is discharge in cubic metres per second; Q_Flag retains discharge qualifications",
    "Walker Branch combined discharge requires both contributing forks for that solute",
    "missing discharge or spatial data do not remove chemistry rows",
    "bess_gpp_gC_m2_year is annual gross primary production from BESSv2 monthly mean daily GPP weighted by calendar days",
    "BESS valid days and watershed area fractions accompany each year; missing years are not extrapolated",
    "saved MODIS NPP and first-cycle greenup are retained when present",
    "monthly predictor columns are source climatologies, not measurements for the chemistry month"))
  metadata <- data.table::rbindlist(list(site_metadata, chemistry_notes, columns, notes), fill = TRUE)
  metadata_path <- file.path(output_dir, paste0(tag, "_combined_metadata.csv"))
  data.table::fwrite(metadata, metadata_path, na = "")
  saved_notes <- data.table::fread(metadata_path, colClasses = "character", na.strings = "", showProgress = FALSE)
  if (nrow(saved_notes) != nrow(metadata) || !all(vapply(names(metadata), function(column) {
    identical(clean(metadata[[column]]), clean(saved_notes[[column]]))
  }, logical(1)))) stop("exported metadata differs")
  if (!file.rename(partial, output)) stop("could not finish the checked export")
  invisible(list(passed = TRUE, chemistry_rows = checked, sites = length(sites),
    discharge_present = sum(is.finite(joined$Qcms)), discharge_missing = sum(!is.finite(joined$Qcms)),
    combined_columns = ncol(make_chunk(1L)), metadata_rows = nrow(metadata),
    files = c(output, metadata_path), checksums = tools::md5sum(c(output, metadata_path))))
}
