# ---- shared helpers ----

helper_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
if (length(helper_argument) != 1L) {
  stop("the script folder could not be identified")
}
source(file.path(
  dirname(dirname(normalizePath(sub("^--file=", "", helper_argument)))),
  "chemistry_functions.R"
))
rm(helper_argument)

# ---- settings ----

input_dir <- required_path("SISYN_CHEM_INPUT_DIR")
selection_dir <- required_path("SISYN_CHEM_SELECTION_INPUT_DIR")
output_dir <- required_path("SISYN_CHEM_QUALIFIER_OUTPUT_DIR")
key_dir <- required_path("SISYN_CHEM_KEY_DIR")

source_files <- c(
  "Chem_HYBAM.csv",
  "UK_nonSi_solutes.csv",
  "UK_Si.csv",
  "CAMELS_USGS_N_P.csv",
  "Como_Creek_chem.csv",
  "USGS_geogenic_solutes.csv",
  "ElbeRiverChem.csv",
  "WalkerBranch_Chem.csv",
  "UMR_si_new_sites.csv",
  "UMR_si_update_existing_sites.csv",
  "Krycklan_NP.csv",
  "WesternAus_AllSites_Filtered_v2.csv"
)

required_files <- c(
  file.path(input_dir, source_files),
  file.path(key_dir, "SiSyn_Data_Key.csv"),
  file.path(key_dir, "SiSyn_Chem_Units_Key.csv")
)

check_required_files(required_files)

read_dirs <- c(input_dir, selection_dir, key_dir)
check_work_folder(output_dir, read_dirs, "qualifier preparation")
dir.create(output_dir, recursive = TRUE)

# ---- qualifier helpers ----

read_source <- function(filename, directory = input_dir, clean_names = FALSE) {
  preferred_dir <- if (identical(directory, input_dir)) selection_dir else NULL
  read_source_csv(filename, directory, clean_names, preferred_dir)
}

assert_vector <- function(observed, expected, label) {
  same_names <- identical(names(observed), names(expected))
  same_values <- identical(as.integer(observed), as.integer(expected))

  if (!same_names || !same_values) {
    stop(label, " changed")
  }
}

assert_unchanged_columns <- function(before, after, changed_columns, filename) {
  if (!identical(dim(before), dim(after)) || !identical(names(before), names(after))) {
    stop("row or column structure changed in '", filename, "'")
  }

  unchanged_columns <- setdiff(names(before), changed_columns)
  for (column in unchanged_columns) {
    if (!identical(before[[column]], after[[column]])) {
      stop("an unapproved column changed in '", filename, "': ", column)
    }
  }
}

count_changed_cells <- function(before, after, columns) {
  changed <- 0L
  for (column in columns) {
    old_values <- before[[column]]
    new_values <- after[[column]]
    both_missing <- is.na(old_values) & is.na(new_values)
    both_present <- !is.na(old_values) & !is.na(new_values)
    same_value <- both_present & old_values == new_values
    unchanged <- both_missing | same_value
    changed <- changed + sum(!unchanged)
  }
  changed
}

prefix_inequality <- function(value, qualifier) {
  value <- as.character(value)
  qualifier <- trimws(as.character(qualifier))
  has_existing <- !is.na(value) & grepl("^\\s*[<>]", value)
  existing <- rep(NA_character_, length(value))
  existing[has_existing] <- sub(
    "^\\s*([<>]).*$",
    "\\1",
    value[has_existing]
  )
  conflict <- qualifier %in% c("<", ">") &
    !is.na(existing) & qualifier != existing

  if (any(conflict, na.rm = TRUE)) {
    stop("a separate qualifier conflicts with a value qualifier")
  }

  add_qualifier <- qualifier %in% c("<", ">") &
    !is_blank(value) & !has_existing
  value[add_qualifier] <- paste0(qualifier[add_qualifier], value[add_qualifier])
  value
}

normalize_mdy_fixed50 <- function(x, label) {
  x <- as.character(x)
  nonblank <- !is_blank(x)
  valid_shape <- grepl("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$", x)

  if (any(nonblank & !valid_shape)) {
    stop(label, " contains a non-mdy date")
  }

  parts <- strsplit(x[nonblank], "/", fixed = TRUE)
  month <- integer(length(parts))
  day <- integer(length(parts))
  year_short <- integer(length(parts))
  for (row in seq_along(parts)) {
    month[row] <- as.integer(parts[[row]][1L])
    day[row] <- as.integer(parts[[row]][2L])
    year_short[row] <- as.integer(parts[[row]][3L])
  }
  # keep the reviewed cutoff: 50-99 means 1950-1999, 00-49 means 2000-2049
  year <- ifelse(year_short >= 50L, 1900L + year_short, 2000L + year_short)
  iso <- sprintf("%04d-%02d-%02d", year, month, day)
  parsed <- as.Date(iso, format = "%Y-%m-%d")

  if (any(is.na(parsed)) || any(format(parsed, "%Y-%m-%d") != iso)) {
    stop(label, " contains an invalid calendar date")
  }

  output <- x
  output[nonblank] <- iso
  output
}

assert_iso_dates <- function(x, expected_count, expected_start, expected_end, label) {
  nonblank <- !is_blank(x)
  values <- x[nonblank]

  assert_count(sum(nonblank), expected_count, paste0(label, " nonblank dates"))
  assert_count(
    sum(!grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", values)),
    0L,
    paste0(label, " non-ISO dates")
  )
  assert_count(
    sum(grepl("/[0-9]{2}$", values)),
    0L,
    paste0(label, " two-digit-year dates")
  )

  if (!identical(min(values), expected_start) ||
      !identical(max(values), expected_end)) {
    stop(label, " date range changed")
  }
}

write_prepared <- function(filename, before, after, changed_columns, changed_count) {
  assert_unchanged_columns(before, after, changed_columns, filename)
  assert_count(
    count_changed_cells(before, after, changed_columns),
    changed_count,
    paste0(filename, " changed-cell count")
  )

  write_prepared_csv(filename, after, output_dir)
}

# ---- preserve reported limit signs ----

prepare_hybam <- function() {
  filename <- "Chem_HYBAM.csv"
  before <- read_source(filename)
  before$source_method_flag <- NA_character_
  after <- before
  suspect_chloride <-
    before[[1L]] == "83980" &
    before$id_station == "17730000" &
    before$nom == "Itaituba" &
    before$date == "3/10/14" &
    before$valeur == "6061" &
    before$code == "NA" &
    before$solute == "Cl"
  source_code <- trimws(after$code)
  blank_value <- is_blank(after$valeur)
  numeric_limit <- grepl(
    "^<\\s*(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)$",
    source_code
  )
  numeric_rows <- which(blank_value & numeric_limit)
  special_rows <- which(
    blank_value & source_code == "<50ppb" & after$solute == "NO3"
  )
  bare_rows <- which(
    blank_value & source_code == "<" &
      after$id_station == "14420000" &
      after$nom == "Serrinha" &
      after$date == "7/5/96" &
      after$solute == "HCO3"
  )

  assert_count(length(numeric_rows), 3895L, "HYBAM numeric limits")
  assert_count(length(special_rows), 1L, "HYBAM nitrate ppb limit")
  assert_count(length(bare_rows), 1L, "HYBAM unresolved bare limit")
  assert_count(sum(blank_value & source_code == "<", na.rm = TRUE), 1L,
               "HYBAM bare-limit total")
  assert_count(sum(grepl("^\\s*[<>]", before$valeur), na.rm = TRUE), 0L,
               "HYBAM existing value qualifiers")
  assert_count(sum(after$solute == "Si", na.rm = TRUE), 2364L,
               "HYBAM silica labels")
  assert_count(sum(after$solute == "DSi", na.rm = TRUE), 0L,
               "HYBAM existing DSi labels")
  assert_count(sum(suspect_chloride, na.rm = TRUE), 1L,
               "HYBAM suspect chloride row")

  # keep the reported value for later outlier review
  if (!identical(after$valeur[suspect_chloride], "6061")) {
    stop("HYBAM reviewed chloride value changed")
  }
  after$valeur[numeric_rows] <- source_code[numeric_rows]
  after$valeur[special_rows] <- "<0.05"
  # exclude the bare qualifier from the working input because no threshold is given
  after$valeur[bare_rows] <- NA_character_
  after$solute[after$solute == "Si"] <- "DSi"
  # hybam filters geochemistry samples before major-element analysis
  # the source reports si measured by colorimetry or icp-oes
  after$source_method_flag[after$solute == "DSi"] <-
    "dissolved Si, 0.22 um filtered"
  after$date <- normalize_mdy_fixed50(after$date, "HYBAM")

  assert_count(sum(grepl("^\\s*<", after$valeur), na.rm = TRUE), 3896L,
               "HYBAM prepared limits")
  assert_count(sum(after$solute == "DSi", na.rm = TRUE), 2364L,
               "HYBAM prepared DSi labels")
  assert_count(
    sum(after$source_method_flag == "dissolved Si, 0.22 um filtered", na.rm = TRUE),
    2364L,
    "HYBAM dissolved-silicon method flags"
  )
  assert_iso_dates(
    after$date,
    101711L,
    "1994-04-01",
    "2020-02-10",
    "HYBAM"
  )
  write_prepared(
    filename,
    before,
    after,
    c("date", "valeur", "solute", "source_method_flag"),
    110336L
  )
}

prepare_uk_nonsi <- function() {
  filename <- "UK_nonSi_solutes.csv"
  before <- read_source(filename)
  after <- before
  assert_count(sum(trimws(before$Qualifier) == "<", na.rm = TRUE), 8569L,
               "UK non-silica less-than flags")
  assert_count(sum(trimws(before$Qualifier) == ">", na.rm = TRUE), 15L,
               "UK non-silica greater-than flags")
  assert_count(sum(grepl("^\\s*[<>]", before$concentration), na.rm = TRUE), 0L,
               "UK non-silica existing value qualifiers")

  after$concentration <- prefix_inequality(
    after$concentration,
    after$Qualifier
  )

  sulfate <- after$solute == "SO4" & after$units == "mg/L"
  assert_count(sum(sulfate, na.rm = TRUE), 65169L,
               "UK non-silica sulfate unit rows")
  after$units[sulfate] <- "mg SO4/L"

  assert_count(sum(grepl("^\\s*<", after$concentration), na.rm = TRUE), 8569L,
               "UK non-silica prepared less-than values")
  assert_count(sum(grepl("^\\s*>", after$concentration), na.rm = TRUE), 15L,
               "UK non-silica prepared greater-than values")

  # choose which sites to include after the full chemistry table is built
  write_prepared(
    filename,
    before,
    after,
    c("concentration", "units"),
    73753L
  )
}

prepare_uk_si <- function() {
  filename <- "UK_Si.csv"
  value_column <- "Silica..mg.SiO2.l."
  before <- read_source(filename)
  after <- before

  assert_count(sum(trimws(before$Qualifier) == "<", na.rm = TRUE), 880L,
               "UK silica less-than flags")
  assert_count(sum(trimws(before$Qualifier) == ">", na.rm = TRUE), 0L,
               "UK silica greater-than flags")
  assert_count(sum(grepl("^\\s*[<>]", before[[value_column]]), na.rm = TRUE), 0L,
               "UK silica existing value qualifiers")

  after[[value_column]] <- prefix_inequality(
    after[[value_column]],
    after$Qualifier
  )

  assert_count(sum(grepl("^\\s*<", after[[value_column]]), na.rm = TRUE), 880L,
               "UK silica prepared limits")
  write_prepared(filename, before, after, value_column, 880L)
}

prepare_camels <- function() {
  filename <- "CAMELS_USGS_N_P.csv"
  before <- read_source(filename)
  after <- before
  result <- suppressWarnings(as.numeric(after$result_va))
  qualifier <- trimws(after$remark_cd)
  unknown <- which(qualifier == "<" & !is.na(result) & result <= 0)
  expected_dates <- c(
    "1982-11-02", "1983-03-24", "1983-05-17",
    "1983-08-30", "1983-11-01", "1984-09-04"
  )

  assert_count(sum(qualifier == "<", na.rm = TRUE), 3963L,
               "CAMELS less-than flags")
  assert_count(length(unknown), 6L, "CAMELS unresolved limits")

  if (
    !all(after$stream[unknown] == "Wild River") ||
      !all(after$variable[unknown] == "SRP") ||
      !identical(sort(after$sample_dt[unknown]), sort(expected_dates))
  ) {
    stop("the reviewed CAMELS unresolved-limit rows changed")
  }

  # exclude qualifiers without positive thresholds from the working input
  after$result_va[unknown] <- NA_character_
  after$result_va <- prefix_inequality(after$result_va, after$remark_cd)

  orthophosphate <- after$parm_cd == "00671" & after$variable == "SRP"
  assert_count(sum(orthophosphate, na.rm = TRUE), 4895L,
               "CAMELS orthophosphate rows")
  after$variable[orthophosphate] <- "PO4"

  assert_count(sum(grepl("^\\s*<", after$result_va), na.rm = TRUE), 3957L,
               "CAMELS prepared limits")
  write_prepared(
    filename,
    before,
    after,
    c("result_va", "variable"),
    8858L
  )
}

prepare_como <- function() {
  filename <- "Como_Creek_chem.csv"
  before <- read_source(filename)
  after <- before
  below_detection <- trimws(after$belowDetectionQF) == "BDL"
  unit_map <- c(
    "Ca" = "mg/L",
    "Cl" = "mg/L",
    "DIC" = "mg C/L",
    "DOC" = "mg C/L",
    "K" = "mg/L",
    "Mg" = "mg/L",
    "Na" = "mg/L",
    "NH4 - N" = "mg NH4-N/L",
    "NO3+NO2 - N" = "mg NO3-N/L",
    "Ortho - P" = "mg PO4-P/L",
    "Si" = "mg Si/L",
    "SO4" = "mg SO4/L",
    "specificConductance" = "uS/cm",
    "TDS" = "mg/L",
    "TN" = "mg N/L",
    "TOC" = "mg C/L",
    "TP" = "mg P/L"
  )
  expected_rows <- c(
    "Ca" = 202L,
    "Cl" = 205L,
    "DIC" = 209L,
    "DOC" = 209L,
    "K" = 211L,
    "Mg" = 180L,
    "Na" = 202L,
    "NH4 - N" = 215L,
    "NO3+NO2 - N" = 207L,
    "Ortho - P" = 218L,
    "Si" = 210L,
    "SO4" = 211L,
    "specificConductance" = 107L,
    "TDS" = 184L,
    "TN" = 206L,
    "TOC" = 209L,
    "TP" = 227L
  )
  expected_blank_units <- c(
    "Ca" = 0L,
    "Cl" = 32L,
    "DIC" = 0L,
    "DOC" = 0L,
    "K" = 0L,
    "Mg" = 0L,
    "Na" = 0L,
    "NH4 - N" = 0L,
    "NO3+NO2 - N" = 0L,
    "Ortho - P" = 0L,
    "Si" = 0L,
    "SO4" = 32L,
    "specificConductance" = 0L,
    "TDS" = 3L,
    "TN" = 0L,
    "TOC" = 0L,
    "TP" = 0L
  )
  source_unit_map <- stats::setNames(
    rep("milligramsPerLiter", length(unit_map)),
    names(unit_map)
  )
  source_unit_map[["specificConductance"]] <-
    "microsiemensPerCentimeter"
  expected_fill_rows <- c("Cl" = 32L, "TDS" = 3L)
  expected_generic_fill_rows <- c("Cl" = 173L, "TDS" = 181L)
  reviewed <- after$analyte %in% names(unit_map)
  fill_units <- after$analyte %in% names(expected_fill_rows) &
    is_blank(after$analyteUnits)
  observed_rows <- vapply(
    names(unit_map),
    function(analyte) sum(after$analyte == analyte, na.rm = TRUE),
    integer(1)
  )
  observed_blank_units <- vapply(
    names(unit_map),
    function(analyte) {
      sum(after$analyte == analyte & is_blank(after$analyteUnits), na.rm = TRUE)
    },
    integer(1)
  )
  observed_generic_units <- vapply(
    names(unit_map),
    function(analyte) {
      sum(
        after$analyte == analyte &
          after$analyteUnits == source_unit_map[[analyte]],
        na.rm = TRUE
      )
    },
    integer(1)
  )

  assert_count(sum(below_detection, na.rm = TRUE), 201L,
               "Como Creek below-detection flags")
  assert_count(sum(below_detection & is_blank(after$analyteConcentration)), 0L,
               "Como Creek blank limits")
  assert_vector(observed_rows, expected_rows,
                "Como Creek reviewed analyte rows")
  assert_vector(observed_blank_units, expected_blank_units,
                "Como Creek blank reviewed units")
  assert_vector(
    observed_generic_units,
    expected_rows - expected_blank_units,
    "Como Creek generic reviewed units"
  )
  observed_fill_rows <- vapply(
    names(expected_fill_rows),
    function(analyte) {
      sum(after$analyte == analyte & is_blank(after$analyteUnits), na.rm = TRUE)
    },
    integer(1)
  )
  observed_generic_fill_rows <- vapply(
    names(expected_fill_rows),
    function(analyte) {
      sum(
        after$analyte == analyte &
          after$analyteUnits == "milligramsPerLiter",
        na.rm = TRUE
      )
    },
    integer(1)
  )
  assert_vector(observed_fill_rows, expected_fill_rows,
                "Como Creek blank chloride and TDS units")
  assert_vector(
    observed_generic_fill_rows,
    expected_generic_fill_rows,
    "Como Creek generic chloride and TDS units"
  )

  after$analyteConcentration <- prefix_inequality(
    after$analyteConcentration,
    ifelse(below_detection, "<", NA_character_)
  )
  after$analyteUnits[reviewed] <- unname(unit_map[after$analyte[reviewed]])
  values_before_unit_fill <- after$analyteConcentration
  after$analyteUnits[fill_units] <- "mg/L"

  if (!identical(after$analyteConcentration, values_before_unit_fill)) {
    stop("Como Creek unit filling changed measurement values")
  }

  assert_count(
    sum(grepl("^\\s*<", after$analyteConcentration), na.rm = TRUE),
    201L,
    "Como Creek prepared limits"
  )
  prepared_unit_rows <- vapply(
    names(unit_map),
    function(analyte) {
      sum(
        after$analyte == analyte &
          after$analyteUnits == unit_map[[analyte]],
        na.rm = TRUE
      )
    },
    integer(1)
  )
  assert_vector(prepared_unit_rows, expected_rows,
                "Como Creek prepared reviewed units")
  prepared_fill_rows <- vapply(
    names(expected_fill_rows),
    function(analyte) {
      sum(
        after$analyte == analyte & after$analyteUnits == "mg/L",
        na.rm = TRUE
      )
    },
    integer(1)
  )
  assert_vector(
    prepared_fill_rows,
    expected_rows[names(expected_fill_rows)],
                "Como Creek filled chloride and TDS units")
  assert_count(
    sum(
      after$analyte %in% names(expected_fill_rows) &
        is_blank(after$analyteUnits),
      na.rm = TRUE
    ),
    0L,
    "Como Creek remaining blank chloride and TDS units"
  )
  write_prepared(
    filename,
    before,
    after,
    c("analyteConcentration", "analyteUnits"),
    3613L
  )
}

prepare_usgs_geogenic <- function() {
  filename <- "USGS_geogenic_solutes.csv"
  before <- read_source(filename)
  after <- before
  qualifier <- trimws(after$remark)
  unresolved_r <- which(qualifier == "R")

  assert_count(sum(qualifier == "<", na.rm = TRUE), 532L,
               "USGS geogenic less-than flags")
  assert_count(sum(qualifier == ">", na.rm = TRUE), 4L,
               "USGS geogenic greater-than flags")
  assert_count(length(unresolved_r), 1L,
               "USGS geogenic unresolved R flag")
  assert_count(sum(grepl("^\\s*[<>]", before$value), na.rm = TRUE), 0L,
               "USGS geogenic existing value qualifiers")

  reviewed_r <- before[unresolved_r, , drop = FALSE]
  expected_r <- c(
    USGSGageNumber = "7144100",
    parameter_code = "00935",
    date = "2011-09-22",
    remark = "R",
    value = "9",
    solutes = "K"
  )
  observed_r <- unlist(reviewed_r[names(expected_r)], use.names = TRUE)

  if (!identical(observed_r, expected_r)) {
    stop("the reviewed USGS geogenic R-coded potassium row changed")
  }

  # the usgs samples record identifies this r-coded result as below its limit
  # preserve the reported 9 mg/l threshold with a less-than qualifier
  # source: https://api.waterdata.usgs.gov/samples-data/results/narrow
  after$value[unresolved_r] <- paste0("<", trimws(after$value[unresolved_r]))
  after$value <- prefix_inequality(after$value, after$remark)

  assert_count(sum(grepl("^\\s*<", after$value), na.rm = TRUE), 533L,
               "USGS geogenic prepared less-than values")
  assert_count(sum(grepl("^\\s*>", after$value), na.rm = TRUE), 4L,
               "USGS geogenic prepared greater-than values")
  write_prepared(filename, before, after, "value", 537L)
}

prepare_elbe <- function() {
  filename <- "ElbeRiverChem.csv"
  before <- read_source(filename)
  after <- before
  measurement_columns <- c(
    "SiO2", "TN", "NO3", "NO2_NO3", "NO2", "DNH4", "TP", "DIP",
    "TOC", "DOC", "SO4", "Cl", "K", "Na", "Mg", "Ca", "Alkalinity",
    "SPM", "SpecCond25C", "DO_mgL", "pH", "Temp_water", "DOSAT"
  )
  expected_less_than <- c(
    SiO2 = 1L, TN = 0L, NO3 = 0L, NO2_NO3 = 0L, NO2 = 33L,
    DNH4 = 71L, TP = 0L, DIP = 11L, TOC = 0L, DOC = 0L, SO4 = 0L,
    Cl = 0L, K = 0L, Na = 0L, Mg = 0L, Ca = 0L, Alkalinity = 0L,
    SPM = 1L, SpecCond25C = 0L, DO_mgL = 0L, pH = 0L,
    Temp_water = 0L, DOSAT = 0L
  )
  observed_less_than <- vapply(
    measurement_columns,
    function(column) {
      sum(trimws(after[[paste0(column, "_vrc")]]) == "<", na.rm = TRUE)
    },
    integer(1)
  )

  assert_vector(observed_less_than, expected_less_than,
                "Elbe qualifier fields")
  assert_count(sum(observed_less_than), 117L, "Elbe qualifier total")
  # glorich defines ton as total organic nitrogen rather than total nitrogen
  # keep it unmapped unless the project adds a separate total organic n field
  assert_count(sum(!is_blank(after$TON)), 13L, "Elbe excluded TON values")

  # glorich stores half the detection limit when the qualifier is less than
  # restore the full threshold before carrying the qualifier into the master
  # source: https://doi.org/10.1594/pangaea.902360
  for (column in measurement_columns) {
    qualifier <- trimws(after[[paste0(column, "_vrc")]])
    censored <- !is.na(qualifier) & qualifier == "<"
    censored_value <- suppressWarnings(as.numeric(after[[column]][censored]))

    if (any(is.na(censored_value))) {
      stop("Elbe less-than values must be numeric")
    }

    after[[column]][censored] <- format(
      censored_value * 2,
      scientific = FALSE,
      trim = TRUE
    )
    after[[column]] <- prefix_inequality(
      after[[column]],
      after[[paste0(column, "_vrc")]]
    )
  }

  expected_thresholds <- c(
    SiO2 = 3.204,
    NO2 = 0.214,
    DNH4 = 1.428,
    DIP = 0.322,
    SPM = 1
  )
  observed_minimums <- vapply(
    names(expected_thresholds),
    function(column) {
      qualifier <- trimws(after[[paste0(column, "_vrc")]])
      values <- after[[column]][!is.na(qualifier) & qualifier == "<"]
      min(as.numeric(sub("^<", "", values)))
    },
    numeric(1)
  )
  if (!isTRUE(all.equal(observed_minimums, expected_thresholds))) {
    stop("Elbe restored detection thresholds changed")
  }

  prepared_count <- sum(vapply(
    measurement_columns,
    function(column) sum(grepl("^\\s*<", after[[column]]), na.rm = TRUE),
    integer(1)
  ))
  assert_count(prepared_count, 117L, "Elbe prepared limits")
  write_prepared(filename, before, after, measurement_columns, 117L)
}

prepare_walker <- function() {
  filename <- "WalkerBranch_Chem.csv"
  before <- read_source(filename)
  after <- before
  flag_fields <- c(
    AL_CONC = "AL_CONC_FL",
    FE_CONC = "FE_CONC_FL",
    K_CONC = "K_CONC_FL",
    MN_CONC = "MN_CONC_FL",
    SR_CONC = "SR_CONC_FL",
    ZN_CONC = "ZN_CONC_FL"
  )
  expected_numeric <- c(
    AL_CONC = 3161L, ALK = 3386L, FE_CONC = 3181L,
    K_CONC = 3132L, MN_CONC = 3207L, PH = 3164L, SP_COND = 3479L,
    SR_CONC = 2719L, TEMP = 3477L, ZN_CONC = 2872L
  )
  expected_unknown <- c(
    AL_CONC = 328L, FE_CONC = 84L, K_CONC = 2L, MN_CONC = 21L,
    SR_CONC = 0L, ZN_CONC = 386L
  )
  expected_v1_positive <- c(
    AL_CONC = 206L, FE_CONC = 12L, K_CONC = 608L, MN_CONC = 11L,
    SR_CONC = 34L, ZN_CONC = 74L
  )
  expected_v7_positive <- c(
    AL_CONC = 1447L, FE_CONC = 1865L, K_CONC = 993L, MN_CONC = 1239L,
    SR_CONC = 2L, ZN_CONC = 1367L
  )
  expected_m1 <- c(
    AL_CONC = 282L, FE_CONC = 262L, K_CONC = 304L, MN_CONC = 236L,
    SR_CONC = 540L, ZN_CONC = 438L
  )
  observed_numeric <- vapply(
    names(expected_numeric),
    function(column) sum(!is.na(suppressWarnings(as.numeric(after[[column]])))),
    integer(1)
  )
  observed_unknown <- observed_v1_positive <- observed_v7_positive <-
    observed_m1 <- observed_m1_numeric <- integer(length(flag_fields))
  names(observed_unknown) <- names(observed_v1_positive) <-
    names(observed_v7_positive) <- names(observed_m1) <-
    names(observed_m1_numeric) <- names(flag_fields)

  for (column in names(flag_fields)) {
    measurement <- suppressWarnings(as.numeric(after[[column]]))
    flag <- trimws(after[[flag_fields[[column]]]])
    observed_unknown[column] <- sum(flag == "V1" & measurement <= 0,
                                    na.rm = TRUE)
    observed_v1_positive[column] <- sum(flag == "V1" & measurement > 0,
                                        na.rm = TRUE)
    observed_v7_positive[column] <- sum(flag == "V7" & measurement > 0,
                                        na.rm = TRUE)
    observed_m1[column] <- sum(flag == "M1", na.rm = TRUE)
    observed_m1_numeric[column] <- sum(flag == "M1" & !is.na(measurement),
                                       na.rm = TRUE)
  }

  assert_vector(observed_numeric, expected_numeric, "Walker numeric values")
  assert_vector(observed_unknown, expected_unknown, "Walker unresolved limits")
  assert_vector(observed_v1_positive, expected_v1_positive,
                "Walker positive V1 values")
  assert_vector(observed_v7_positive, expected_v7_positive,
                "Walker V7 limits")
  assert_vector(observed_m1, expected_m1, "Walker M1 flags")
  assert_vector(
    observed_m1_numeric,
    setNames(integer(length(flag_fields)), names(flag_fields)),
                "Walker numeric M1 values")

  k_measurement <- suppressWarnings(as.numeric(after$K_CONC))
  k_flag <- trimws(after$K_CONC_FL)
  k_date <- as.Date(after$DATE)
  k_v7_one <- k_flag == "V7" & k_measurement == 1
  k_v7_two <- k_flag == "V7" & k_measurement == 2
  k_v7_one_allowed <-
    (k_date >= as.Date("1997-01-07") & k_date <= as.Date("2009-12-29")) |
    (after$site_name == "East Fork" &
       k_date >= as.Date("2010-01-12") &
       k_date <= as.Date("2010-08-17")) |
    k_date >= as.Date("2010-08-24")
  k_v7_two_allowed <-
    (after$site_name == "West Fork" &
       k_date >= as.Date("2010-01-05") &
       k_date <= as.Date("2010-08-17")) |
    (after$site_name == "East Fork" & k_date == as.Date("2010-01-05"))

  assert_count(sum(k_v7_one, na.rm = TRUE), 960L,
               "Walker potassium one mg/L limits")
  assert_count(sum(k_v7_two, na.rm = TRUE), 33L,
               "Walker potassium two mg/L limits")
  assert_count(sum(k_flag == "V7" & !(k_v7_one | k_v7_two), na.rm = TRUE),
               0L, "Walker unexpected potassium limits")
  assert_count(sum(k_v7_one & !k_v7_one_allowed, na.rm = TRUE), 0L,
               "Walker potassium one mg/L limit dates")
  assert_count(sum(k_v7_two & !k_v7_two_allowed, na.rm = TRUE), 0L,
               "Walker potassium two mg/L limit dates")

  k_unresolved <- which(k_flag == "V1" & !is.na(k_measurement) &
                          k_measurement <= 0)
  k_unresolved_keys <- paste(after$site_name[k_unresolved],
                             after$DATE[k_unresolved], sep = "|")
  if (!identical(
    sort(k_unresolved_keys),
    c("East Fork|1996-06-18", "East Fork|1996-08-20")
  )) {
    stop("Walker unresolved potassium records changed")
  }

  walker_rows <- which(before$site_name == "WALK")
  east_source_rows <- which(before$site_name == "East Fork")
  west_source_rows <- which(before$site_name == "West Fork")

  if (anyDuplicated(before$DATE[east_source_rows]) ||
      anyDuplicated(before$DATE[west_source_rows])) {
    stop("Walker branch dates are no longer unique")
  }

  east_rows <- east_source_rows[
    match(before$DATE[walker_rows], before$DATE[east_source_rows])
  ]
  west_rows <- west_source_rows[
    match(before$DATE[walker_rows], before$DATE[west_source_rows])
  ]
  east_discharge <- suppressWarnings(as.numeric(before$EF_DISCHARGE[east_rows]))
  west_discharge <- suppressWarnings(as.numeric(before$WF_DISCHARGE[west_rows]))
  expected_walker_numeric <- c(
    AL_CONC = 1165L, FE_CONC = 1165L, K_CONC = 1158L,
    MN_CONC = 1165L, SR_CONC = 981L, ZN_CONC = 1032L
  )
  expected_walker_limits <- c(
    AL_CONC = 766L, FE_CONC = 1062L, K_CONC = 587L,
    MN_CONC = 904L, SR_CONC = 2L, ZN_CONC = 712L
  )
  expected_walker_unresolved <- c(
    AL_CONC = 253L, FE_CONC = 84L, K_CONC = 2L,
    MN_CONC = 21L, SR_CONC = 0L, ZN_CONC = 265L
  )
  expected_walker_v1 <- c(
    AL_CONC = 138L, FE_CONC = 9L, K_CONC = 446L,
    MN_CONC = 5L, SR_CONC = 33L, ZN_CONC = 54L
  )

  for (column in names(flag_fields)) {
    source_measurement <- suppressWarnings(as.numeric(before[[column]]))
    walker_measurement <- source_measurement[walker_rows]
    east_measurement <- source_measurement[east_rows]
    west_measurement <- source_measurement[west_rows]
    east_flag <- trimws(before[[flag_fields[[column]]]][east_rows])
    west_flag <- trimws(before[[flag_fields[[column]]]][west_rows])
    east_used <- !is.na(east_measurement) & !is.na(east_discharge) &
      east_discharge > 0
    west_used <- !is.na(west_measurement) & !is.na(west_discharge) &
      west_discharge > 0
    both_used <- east_used & west_used
    east_only <- east_used & !west_used
    west_only <- !east_used & west_used
    derived_measurement <- rep(NA_real_, length(walker_rows))

    derived_measurement[both_used] <-
      (east_measurement[both_used] * east_discharge[both_used] +
         west_measurement[both_used] * west_discharge[both_used]) /
      (east_discharge[both_used] + west_discharge[both_used])
    derived_measurement[east_only] <- east_measurement[east_only]
    derived_measurement[west_only] <- west_measurement[west_only]

    walker_numeric <- !is.na(walker_measurement)
    assert_count(sum(walker_numeric), expected_walker_numeric[[column]],
                 paste0("Walker derived ", column, " values"))
    assert_count(sum(walker_numeric & is.na(derived_measurement)), 0L,
                 paste0("Walker unmatched ", column, " values"))

    mismatch <- walker_numeric &
      abs(walker_measurement - derived_measurement) > 1e-10
    assert_count(sum(mismatch, na.rm = TRUE), 0L,
                 paste0("Walker recalculated ", column, " values"))

    any_v7 <-
      (east_used & east_flag == "V7") |
      (west_used & west_flag == "V7")
    unresolved_v1 <-
      (east_used & east_flag == "V1" & east_measurement <= 0) |
      (west_used & west_flag == "V1" & west_measurement <= 0)
    positive_v1 <-
      (east_used & east_flag == "V1" & east_measurement > 0) |
      (west_used & west_flag == "V1" & west_measurement > 0)
    walker_limit <- walker_numeric & any_v7 & !unresolved_v1
    walker_unresolved <- walker_numeric & unresolved_v1
    walker_v1 <- walker_numeric & positive_v1 & !walker_limit & !walker_unresolved

    assert_count(sum(walker_limit, na.rm = TRUE),
                 expected_walker_limits[[column]],
                 paste0("Walker derived ", column, " limits"))
    assert_count(sum(walker_unresolved, na.rm = TRUE),
                 expected_walker_unresolved[[column]],
                 paste0("Walker unresolved derived ", column, " values"))
    assert_count(sum(walker_v1, na.rm = TRUE),
                 expected_walker_v1[[column]],
                 paste0("Walker derived V1 ", column, " values"))

    # exclude v1 rows without positive thresholds from the working input
    after[[column]][walker_rows[walker_unresolved]] <- NA_character_
    after[[flag_fields[[column]]]][walker_rows[walker_v1]] <- "V1"
    after[[column]][walker_rows] <- prefix_inequality(
      after[[column]][walker_rows],
      ifelse(walker_limit, "<", NA_character_)
    )
  }

  for (column in names(flag_fields)) {
    flag <- trimws(after[[flag_fields[[column]]]])
    measurement <- suppressWarnings(as.numeric(after[[column]]))
    unknown <- flag == "V1" & !is.na(measurement) & measurement <= 0
    # leave unresolved and missing values blank rather than inventing thresholds
    after[[column]][unknown | flag == "M1"] <- NA_character_
    after[[column]] <- prefix_inequality(
      after[[column]],
      ifelse(flag == "V7", "<", NA_character_)
    )
  }

  prepared_count <- sum(vapply(
    names(flag_fields),
    function(column) sum(grepl("^\\s*<", after[[column]]), na.rm = TRUE),
    integer(1)
  ))
  assert_count(prepared_count, 10946L, "Walker prepared limits")
  write_prepared(
    filename,
    before,
    after,
    c(names(flag_fields), unname(flag_fields)),
    15139L
  )
}

prepare_umr <- function(filename, expected_invalid, expected_limits,
                        expected_changed) {
  before <- read_source(filename)
  after <- before
  # official umesc qa metadata encodes below-mdl chemistry as a negative mdl
  # https://www.umesc.usgs.gov/data_library/water_quality/wtr_qf_labflags.html
  # limit conversion stays restricted to documented chemistry fields
  # the absolute value is the positive threshold retained with a less-than sign
  measurement_columns <- c(
    "SS", "VSS", "SI", "TN", "NOX", "NHX", "TP", "SRP",
    "CHLcal", "NA.", "K", "CA", "MG", "SO4", "CL"
  )
  chl <- suppressWarnings(as.numeric(after$CHLcal))
  invalid_chl <- !is.na(chl) &
    after$LOCATCD == "AL02.3M" &
    after$DATE == "08/10/2022" &
    after$TIME == "12:41" &
    chl == -0.68747

  assert_count(sum(invalid_chl, na.rm = TRUE), expected_invalid,
               paste0(filename, " invalid chlorophyll values"))
  # keep this unqualified calibration result for later outlier review
  limit_count <- 0L

  for (column in measurement_columns) {
    measurement <- suppressWarnings(as.numeric(after[[column]]))
    below_detection <- !is.na(measurement) & measurement < 0
    if (column == "CHLcal") {
      below_detection <- below_detection & !invalid_chl
    }
    limit_count <- limit_count + sum(below_detection)
    after[[column]][below_detection] <- paste0(
      "<",
      abs(measurement[below_detection])
    )
  }

  assert_count(limit_count, expected_limits,
               paste0(filename, " documented limits"))

  unresolved <- which(
    filename == "UMR_si_new_sites.csv" &
      after$LOCATCD == "BM00.7S" &
      after$DATE == "12/16/1998" &
      suppressWarnings(as.numeric(after$SO4)) == 0
  )
  expected_unresolved <- if (filename == "UMR_si_new_sites.csv") 1L else 0L
  assert_count(length(unresolved), expected_unresolved,
               paste0(filename, " unresolved sulfate values"))
  # exclude zero sulfate from the working input because no threshold is given
  after$SO4[unresolved] <- NA_character_

  prepared_count <- sum(vapply(
    measurement_columns,
    function(column) sum(grepl("^\\s*<", after[[column]]), na.rm = TRUE),
    integer(1)
  ))
  assert_count(prepared_count, expected_limits,
               paste0(filename, " prepared limits"))
  if (expected_invalid == 1L &&
      !identical(after$CHLcal[invalid_chl], "-0.68747")) {
    stop("UMR reviewed chlorophyll result changed")
  }
  write_prepared(
    filename,
    before,
    after,
    measurement_columns,
    expected_changed
  )
}

prepare_krycklan <- function() {
  filename <- "Krycklan_NP.csv"
  nox_column <- "NO2-N + NO3-N"
  nh4_column <- "NH4-N"
  before <- read_source(filename)
  after <- before
  normalized_date <- normalize_mdy_fixed50(after$SampleDate, "Krycklan")
  sample_date <- as.Date(normalized_date, format = "%Y-%m-%d")
  nh4_lod <- trimws(after[[nh4_column]]) == "<LOD"
  nox_lod <- trimws(after[[nox_column]]) == "<LOD"
  known_nh4 <- nh4_lod & !is.na(sample_date) & sample_date >= as.Date("2011-01-01")
  known_nox <- nox_lod & !is.na(sample_date) & sample_date >= as.Date("2011-01-01")
  unresolved_nh4 <- nh4_lod & !known_nh4
  unresolved_nox <- nox_lod & !known_nox

  assert_count(sum(nh4_lod, na.rm = TRUE), 79L, "Krycklan NH4 LOD values")
  assert_count(sum(nox_lod, na.rm = TRUE), 7L, "Krycklan NOx LOD values")
  assert_count(sum(known_nh4, na.rm = TRUE), 59L,
               "Krycklan documented NH4 thresholds")
  assert_count(sum(known_nox, na.rm = TRUE), 4L,
               "Krycklan documented NOx thresholds")
  assert_count(
    sum(unresolved_nh4, na.rm = TRUE) + sum(unresolved_nox, na.rm = TRUE),
    23L,
    "Krycklan unresolved LOD values"
  )

  after[[nh4_column]][known_nh4] <- "<0.3"
  after[[nox_column]][known_nox] <- "<0.4"
  # exclude older lod tokens until their historical thresholds are documented
  after[[nh4_column]][unresolved_nh4] <- NA_character_
  after[[nox_column]][unresolved_nox] <- NA_character_
  after$SampleDate <- normalized_date

  assert_count(
    sum(grepl("^\\s*<", after[[nh4_column]]), na.rm = TRUE) +
      sum(grepl("^\\s*<", after[[nox_column]]), na.rm = TRUE),
    63L,
    "Krycklan prepared limits"
  )
  assert_count(
    sum(after[[nh4_column]] == "<LOD", na.rm = TRUE) +
      sum(after[[nox_column]] == "<LOD", na.rm = TRUE),
    0L,
    "Krycklan remaining LOD tokens"
  )
  assert_iso_dates(
    after$SampleDate,
    5925L,
    "1992-10-22",
    "2022-11-21",
    "Krycklan"
  )
  write_prepared(
    filename,
    before,
    after,
    c("SampleDate", nox_column, nh4_column),
    6011L
  )
}

prepare_western_australia <- function() {
  filename <- "WesternAus_AllSites_Filtered_v2.csv"
  before <- read_source(filename)
  before$source_method_flag <- NA_character_
  after <- before
  numeric_value <- suppressWarnings(as.numeric(after$Value.Num))
  signed_rows <- after$Reading.Sign %in% c("<", ">")
  bad_do <- which(
    after$Site.Name == "Blackwood River - Darradup" &
      after$Collect.Date == "2008-01-25" &
      after$Determinand == "O2-{DO conc} (mg/L)" &
      suppressWarnings(as.numeric(after$Value.Num)) == 163.7
  )
  salinity_conflict <- which(
    after$Site.Name == "Collie River - Central Collie" &
      after$Collect.Date == "2000-07-05" &
      tolower(trimws(after$chemical)) == "salinity"
  )
  invalid_ph <- which(
    tolower(trimws(after$chemical)) == "ph" &
      !is.na(numeric_value) &
      (numeric_value < 1 | numeric_value > 12)
  )
  high_do <- which(
    after$Site.Name == "Donnelly River - Strickland" &
      after$Collect.Date == "2005-09-20" &
      after$Determinand == "O2-{DO conc} (mg/L)" &
      numeric_value == 27.5
  )
  high_conductivity <- which(
    after$Site.Name == "Frankland River - Mount Frankland" &
      after$Collect.Date == "2007-12-04" &
      after$Determinand == "Cond @ 25 deg C (uS/cm)" &
      numeric_value == 86848
  )
  zero_conductivity <- which(
    after$Site.Name == "Collie River - South Branch" &
      after$Collect.Date == "2009-09-01" &
      after$Determinand == "Cond @ 25 deg C (uS/cm)" &
      numeric_value == 0
  )
  soluble_reactive_si <- grepl(
    "^SiO2(-Si)? \\(sol react\\)",
    after$Determinand
  )

  assert_count(sum(after$Reading.Sign == "<", na.rm = TRUE), 2074L,
               "Western Australia less-than flags")
  assert_count(sum(after$Reading.Sign == ">", na.rm = TRUE), 49L,
               "Western Australia greater-than flags")
  assert_count(length(bad_do), 1L, "Western Australia invalid oxygen value")
  assert_count(length(salinity_conflict), 2L,
               "Western Australia salinity conflict")
  assert_count(length(invalid_ph), 8L,
               "Western Australia reviewed pH values")
  assert_count(length(high_do), 1L,
               "Western Australia high oxygen value")
  assert_count(length(high_conductivity), 1L,
               "Western Australia high conductivity value")
  assert_count(length(zero_conductivity), 1L,
               "Western Australia zero conductivity value")
  assert_count(sum(soluble_reactive_si, na.rm = TRUE), 1056L,
               "Western Australia soluble-reactive silicon rows")

  expected_ph <- c(
    "Collie River - South Branch\r2002-08-06\rpH (no units)\r0.79",
    "Kent River - Styx Junction\r2008-07-01\rpH (no units)\r12.74",
    "Murray River - Baden Powell\r1987-06-15\rpH (no units)\r13",
    "Murray River - Baden Powell\r1987-06-18\rpH (no units)\r12.5",
    "Murray River - Baden Powell\r1987-06-20\rpH (no units)\r12.5",
    "Murray River - Baden Powell\r1987-06-25\rpH (no units)\r13",
    "Murray River - Baden Powell\r1987-06-28\rpH (no units)\r13",
    "Murray River - Baden Powell\r1987-06-30\rpH (no units)\r13"
  )
  observed_ph <- do.call(
    paste,
    c(
      after[invalid_ph, c(
        "Site.Name", "Collect.Date", "Determinand", "Value.Num"
      )],
      sep = "\r"
    )
  )

  if (!identical(sort(observed_ph), sort(expected_ph))) {
    stop("the reviewed Western Australia pH rows changed")
  }

  expected_single_rows <- c(
    "Donnelly River - Strickland\r2005-09-20\rO2-{DO conc} (mg/L)\r27.5",
    "Frankland River - Mount Frankland\r2007-12-04\rCond @ 25 deg C (uS/cm)\r86848",
    "Collie River - South Branch\r2009-09-01\rCond @ 25 deg C (uS/cm)\r0"
  )
  single_rows <- c(high_do, high_conductivity, zero_conductivity)
  observed_single_rows <- do.call(
    paste,
    c(
      after[single_rows, c(
        "Site.Name", "Collect.Date", "Determinand", "Value.Num"
      )],
      sep = "\r"
    )
  )

  if (!identical(observed_single_rows, expected_single_rows)) {
    stop("the reviewed Western Australia oxygen or conductivity rows changed")
  }

  conflict_values <- sort(suppressWarnings(
    as.numeric(after$Value.Num[salinity_conflict])
  ))
  if (!identical(conflict_values, c(0.5, 15.1))) {
    stop("the Western Australia salinity conflict values changed")
  }

  after$Value.Num[signed_rows] <- after$Reading.Value[signed_rows]
  after$source_method_flag[soluble_reactive_si] <- "soluble reactive Si"
  # keep reported values for later outlier review
  reviewed_invalid <- c(
    invalid_ph,
    high_do,
    high_conductivity,
    zero_conductivity
  )
  if (!identical(
    after$Value.Num[c(bad_do, reviewed_invalid)],
    after$Reading.Value[c(bad_do, reviewed_invalid)]
  )) {
    stop("Western Australia reviewed source values changed")
  }

  assert_count(sum(grepl("^\\s*<", after$Value.Num), na.rm = TRUE), 2074L,
               "Western Australia prepared less-than values")
  assert_count(sum(grepl("^\\s*>", after$Value.Num), na.rm = TRUE), 49L,
               "Western Australia prepared greater-than values")
  assert_count(sum(!is_blank(after$Value.Num[salinity_conflict])), 2L,
               "Western Australia retained salinity values")
  assert_count(sum(!is_blank(after$Value.Num[reviewed_invalid])), 11L,
               "Western Australia retained reviewed source values")

  # choose which sites to include after the full chemistry table is built
  write_prepared(
    filename,
    before,
    after,
    c("Value.Num", "source_method_flag"),
    3179L
  )
}

# ---- generic parser check ----

normalize_unit_label <- function(x) {
  x <- trimws(as.character(x))
  x <- gsub("µ|μ", "u", x)
  lower <- tolower(x)
  result <- x
  result[is.na(x) | x == ""] <- x[is.na(x) | x == ""]
  result[lower == "na"] <- NA_character_
  result[x %in% c("-", "/")] <- ""
  result[lower %in% c("mg/l", "milligrams/litre", "milligramsperliter")] <-
    "mg/L"
  result[lower == "mg(s)/l"] <- "mg S/L"
  result[lower == "mg(so4)/l"] <- "mg SO4/L"
  result[lower %in% c("ug/l", "micrograms/litre")] <- "ug/L"
  result[lower %in% c(
    "microsiemens/cm", "microsiemens cm", "microsiemenspercentimeter"
  )] <- "uS/cm"
  result[lower %in% c("deg c", "oc", "degrees celsius")] <- "C"
  result[x == "%"] <- "percent"
  result[lower == "pcu"] <- "PCU"
  result[lower == "fnu"] <- "FNU"
  result[lower == "ntu"] <- "NTU"
  result[lower == "ueq/l"] <- "ueq/L"
  result
}

mean_measurement <- function(x, missing_tokens) {
  x <- as.character(x)
  less_than <- grepl("^\\s*<\\s*(?:[0-9]|\\.[0-9])", x)
  greater_than <- grepl("^\\s*>\\s*(?:[0-9]|\\.[0-9])", x)
  number <- suppressWarnings(as.numeric(sub("^\\s*[<>]\\s*", "", x)))
  unparsed <- !is.na(x) & !trimws(x) %in% missing_tokens & is.na(number)

  if (any(unparsed)) {
    stop("unparsed repeated chemistry values remain")
  }

  detected <- number[!is.na(number) & !less_than & !greater_than]
  limited <- number[!is.na(number) & less_than]
  lower_limited <- number[!is.na(number) & greater_than]

  if (length(detected) > 0) {
    return(as.character(mean(detected)))
  }
  if (length(limited) > 0) {
    if (length(lower_limited) > 0) {
      stop("mixed less-than and greater-than repeated measurements remain")
    }
    return(paste0("<", mean(limited)))
  }
  if (length(lower_limited) > 0) {
    return(paste0(">", mean(lower_limited)))
  }
  NA_character_
}

parse_for_qualifier_count <- function(filename, data_key, units_key) {
  raw <- read_source(filename, directory = output_dir, clean_names = TRUE)
  key_rows <- data_key[
    data_key$Raw_Filename == filename &
      !is.na(data_key$Standardized_Column_Name) &
      trimws(data_key$Standardized_Column_Name) != "",
  ]
  missing_columns <- setdiff(key_rows$Raw_Column_Name, names(raw))

  if (length(missing_columns) > 0) {
    stop("the lean data key expects missing prepared columns in '", filename, "'")
  }

  units <- normalize_unit_label(key_rows$Units)
  units[!is.na(units) & nchar(units) == 0] <- NA_character_
  unit_suffix <- gsub("/| |\\-", "_", units)
  actual_names <- ifelse(
    is.na(unit_suffix),
    key_rows$Standardized_Column_Name,
    paste0(key_rows$Standardized_Column_Name, "_", unit_suffix)
  )
  mapped <- raw[key_rows$Raw_Column_Name]
  names(mapped) <- actual_names
  mapped <- data.frame(
    Dataset = key_rows$Dataset[[1]],
    Raw_Filename = filename,
    mapped,
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  mapped <- unique(mapped)

  if (unique(key_rows$Dataset_orientation) == "wide") {
    values <- unlist(mapped, use.names = FALSE)
    return(c(
      less_than = sum(grepl("^\\s*<", values), na.rm = TRUE),
      greater_than = sum(grepl("^\\s*>", values), na.rm = TRUE)
    ))
  }

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("the data.table package is required for the parser check")
  }
  source_has_row_units <- "solute_units" %in% names(mapped)
  if (!source_has_row_units) {
    mapped$solute_units <- NA_character_
  }

  mapped$solute <- tolower(gsub("/| |\\-", "_", mapped$solute))
  mapped$solute_units <- normalize_unit_label(mapped$solute_units)
  mapped$solute_units[mapped$solute_units == ""] <- NA_character_
  mapped$solute_units <- gsub("/| |\\-", "_", mapped$solute_units)

  missing_tokens <- unique(c(
    data_key$NA_indicator,
    "", "NA", "NaN", "N/A", "n/a", "<LOD", "wrong filtering",
    "no filter", "nd"
  ))
  mapped_table <- data.table::as.data.table(mapped)
  group_columns <- setdiff(names(mapped_table), "amount")
  grouped <- mapped_table[
    ,
    .(amount = mean_measurement(amount, missing_tokens)),
    by = group_columns
  ]

  unit_rows <- units_key[units_key$Raw_Filename == filename, ]
  unit_rows$solute <- tolower(gsub("/| |\\-", "_", unit_rows$Variable))
  unit_rows$key_unit <- normalize_unit_label(unit_rows$Units)
  unit_rows$key_unit <- gsub("/| |\\-", "_", unit_rows$key_unit)
  if (filename == "Como_Creek_chem.csv") {
    unit_rows <- unit_rows[unit_rows$solute != "tss___dry_mass", ]
  }

  if (anyDuplicated(unit_rows$solute) > 0) {
    stop("the parser check needs one units-key choice per target solute")
  }

  key_match <- match(grouped$solute, unit_rows$solute)
  grouped$key_unit <- unit_rows$key_unit[key_match]
  grouped <- grouped[!is.na(key_match)]
  keep_unit <- if (source_has_row_units) {
    (is.na(grouped$solute_units) & is.na(grouped$key_unit)) |
      (!is.na(grouped$solute_units) & grouped$solute_units == grouped$key_unit)
  } else {
    rep(TRUE, nrow(grouped))
  }

  if (any(!keep_unit)) {
    stop("row-level units did not resolve in '", filename, "'")
  }

  grouped <- grouped[keep_unit]
  grouped$solute_actual <- ifelse(
    is.na(grouped$key_unit) | grouped$key_unit == "",
    grouped$solute,
    paste0(grouped$solute, "_", grouped$key_unit)
  )
  final_groups <- setdiff(
    names(grouped),
    c("amount", "solute", "solute_units", "key_unit")
  )
  parsed <- grouped[
    ,
    .(amount = mean_measurement(amount, missing_tokens)),
    by = final_groups
  ]

  c(
    less_than = sum(grepl("^\\s*<", parsed$amount), na.rm = TRUE),
    greater_than = sum(grepl("^\\s*>", parsed$amount), na.rm = TRUE)
  )
}

# ---- prepare the files ----

prepare_hybam()
prepare_uk_nonsi()
prepare_uk_si()
prepare_camels()
prepare_como()
prepare_usgs_geogenic()
prepare_elbe()
prepare_walker()
prepare_umr("UMR_si_update_existing_sites.csv", 0L, 27L, 27L)
prepare_umr("UMR_si_new_sites.csv", 1L, 72L, 73L)
prepare_krycklan()
prepare_western_australia()

# ---- validate parser results ----

data_key <- read.csv(
  file.path(key_dir, "SiSyn_Data_Key.csv"),
  stringsAsFactors = FALSE,
  na.strings = ""
)
data_key <- data_key[data_key$Data_type == "chemistry", ]
data_key$Raw_Column_Name <- gsub(
  " |\\+|\\(|\\)|/|\\-",
  ".",
  data_key$Raw_Column_Name
)
units_key <- read.csv(
  file.path(key_dir, "SiSyn_Chem_Units_Key.csv"),
  stringsAsFactors = FALSE,
  na.strings = ""
)
units_key <- units_key[units_key$Data_type == "chemistry", ]

expected_less_than <- c(
  "Chem_HYBAM.csv" = 1751L,
  "UK_nonSi_solutes.csv" = 8490L,
  "UK_Si.csv" = 857L,
  "CAMELS_USGS_N_P.csv" = 3689L,
  "Como_Creek_chem.csv" = 188L,
  "USGS_geogenic_solutes.csv" = 462L,
  "ElbeRiverChem.csv" = 117L,
  "WalkerBranch_Chem.csv" = 10946L,
  "UMR_si_new_sites.csv" = 72L,
  "UMR_si_update_existing_sites.csv" = 27L,
  "Krycklan_NP.csv" = 63L,
  "WesternAus_AllSites_Filtered_v2.csv" = 1996L
)
expected_greater_than <- c(
  "Chem_HYBAM.csv" = 0L,
  "UK_nonSi_solutes.csv" = 15L,
  "UK_Si.csv" = 0L,
  "CAMELS_USGS_N_P.csv" = 0L,
  "Como_Creek_chem.csv" = 0L,
  "USGS_geogenic_solutes.csv" = 2L,
  "ElbeRiverChem.csv" = 0L,
  "WalkerBranch_Chem.csv" = 0L,
  "UMR_si_new_sites.csv" = 0L,
  "UMR_si_update_existing_sites.csv" = 0L,
  "Krycklan_NP.csv" = 0L,
  "WesternAus_AllSites_Filtered_v2.csv" = 25L
)
observed_counts <- vapply(
  names(expected_less_than),
  parse_for_qualifier_count,
  numeric(2),
  data_key = data_key,
  units_key = units_key
)

assert_vector(
  observed_counts["less_than", ],
  expected_less_than,
  "generic parser less-than counts"
)
assert_vector(
  observed_counts["greater_than", ],
  expected_greater_than,
  "generic parser greater-than counts"
)
