# prepare the other chemistry sources
# read the chemistry files and original source downloads without changing them
# prepare the files used to build the 77 master-chemistry inputs
# each source has its own named section and preparation function below
# the final section runs those functions in the reviewed order

# ---- shared helpers ----

helper_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
if (length(helper_argument) != 1L) {
  stop("the script folder could not be identified")
}
preparation_dir <- dirname(normalizePath(sub("^--file=", "", helper_argument)))
source(file.path(dirname(preparation_dir), "chemistry_functions.R"))
rm(helper_argument)

# ---- settings ----

input_dir <- required_path("SISYN_CHEM_INPUT_DIR")
selection_dir <- required_path("SISYN_CHEM_SELECTION_INPUT_DIR")
output_dir <- required_path("SISYN_CHEM_DIRECT_OUTPUT_DIR")
qualifier_dir <- required_path("SISYN_CHEM_QUALIFIER_OUTPUT_DIR")
key_dir <- required_path("SISYN_CHEM_KEY_DIR")
canada_provider_dir <- required_path("SISYN_CHEM_CANADA_PROVIDER_DIR")
cameroon_provider_zip <- required_path("SISYN_CAMEROON_PROVIDER_ZIP")
check_file_versions(cameroon_provider_zip, "8b9238192be6fca67e70bbb3cc697168",
  "Cameroon provider ZIP")

canada_provider_files <- sort(list.files(
  canada_provider_dir,
  pattern = "2000-present[.]csv$",
  full.names = TRUE
))
canada_variable_file <- file.path(
  canada_provider_dir,
  "Water-Qual-Eau-VariableInfo.csv"
)

usgs_provider_dir <- required_path("SISYN_CHEM_USGS_PROVIDER_DIR")
usgs_provider_files <- sort(list.files(
  usgs_provider_dir,
  pattern = "^samples_sites.*_pcodes(06|16).*\\.csv$",
  full.names = TRUE
))
usgs_clinton_file <- required_path("SISYN_CHEM_USGS_CLINTON_FILE")
usgs_exclusion_file <- required_path("SISYN_CHEM_USGS_EXCLUSION_FILE")
susquehanna_filename <-
  "USGS__USGS__SUSQUEHANNA_RIVER__4db2531edf53.csv"
reviewed_susquehanna_file <- required_path(
  "SISYN_CHEM_REVIEWED_SUSQUEHANNA_FILE"
)
carey_file <- required_path("SISYN_CHEM_CAREY_FILE")
nt_nsw_provider_file <- required_path("SISYN_CHEM_NT_NSW_PROVIDER_FILE")
japan_provider_file <- required_path("SISYN_CHEM_JAPAN_PROVIDER_FILE")
netherlands_provider_file <- required_path(
  "SISYN_CHEM_NETHERLANDS_PROVIDER_FILE"
)
arc_provider_dir <- file.path(dirname(canada_provider_dir), "usa_arc")
arc_provider_files <- file.path(
  arc_provider_dir,
  c(
    "1988-2010_Kling_Akchem.csv",
    "legacy_audit_ARC_2002-2024_Kling_AON_Imnavait_Chemistry.csv",
    "legacy_audit_ARC_2012-2020_Kling_Akchem.02.csv"
  )
)
arc_provider_metadata <- file.path(
  arc_provider_dir,
  "1988-2010_Kling_Akchem_eml.xml"
)
check_file_versions(nt_nsw_provider_file, "5832d9fb5276ed53e75a84c6e0f4bf89",
  "NT/NSW silica provider file")
check_file_versions(japan_provider_file, "69c4167a601ebd77cc890a79e3a13251",
  "Japan provider file")
check_file_versions(netherlands_provider_file, "dd92c6ab6756479e569bb11312185ac4",
  "Netherlands provider file")
expected_arc_provider_md5 <- c(
  "a3db20ce1bb2a460a7ee9544a7d87980",
  "9fcb805438e69755aa436d52d6a48d30",
  "0d3f435c4154ac18d333732bd97f30dd",
  "774af538a9b09bcb4ec50900584dd6d3"
)
check_file_versions(c(arc_provider_files, arc_provider_metadata),
  expected_arc_provider_md5, "ARC provider files")

canada_site_files <- c(
  paste0(
    "Canada__Canada__ALSEK_RIVER_ABOVE_BATES_RIVER_IN_",
    "KLUANE_NATIONAL_PARK__b577690976a4.csv"
  ),
  paste0(
    "Canada__Canada__BEAVER_RIVER_ABOVE_HIGHWAY_1_IN_GLACIER_",
    "NATIONAL_PARK__8db5e916b53d.csv"
  ),
  paste0(
    "Canada__Canada__KICKING_HORSE_RIVER_AT_FIELD_IN_YOHO_",
    "NATIONAL_PARK__78eed20cf9d6.csv"
  ),
  paste0(
    "Canada__Canada__KOOTENAY_RIVER_ABOVE_HIGHWAY_93_IN_",
    "KOOTENAY_NATIONAL_PARK__7a1cbddf7523.csv"
  ),
  "Canada__Canada__LIARD_RIVER_AT_UPPER_CROSSING__47916feca4a8.csv",
  "Canada__Canada__SKEENA_RIVER_AT_USK__6692e2a39888.csv"
)

if (length(canada_provider_files) != 17L) {
  stop(
    "the reviewed Canada provider file count changed: expected 17, found ",
    length(canada_provider_files)
  )
}

if (length(usgs_provider_files) != 40L) {
  stop(
    "the reviewed USGS provider file count changed: expected 40, found ",
    length(usgs_provider_files)
  )
}

guadeloupe_files <- c(
  "guadeloupe_GRCBDMDF_chem.csv",
  "guadeloupe_GRCBDPBD_chem.csv",
  "guadeloupe_GRCBDQCK_chem.csv",
  "guadeloupe_GRCCEDIG_chem.csv",
  "guadeloupe_GRCVHBAR_chem.csv",
  "guadeloupe_GRCVHSAV_chem.csv"
)

source_files <- c(
  "Alpine_Clean.csv",
  "Switzerland_CAMELS_CH_AllSites_chemistry.csv",
  "EastRiverSFA_chem.csv",
  "Krycklan_Cation_Clean.csv",
  "Deduplicated_Sites_AllSites_chemistry.csv",
  "Canada_WQ_dat.csv",
  canada_site_files,
  "CatalinaJemez_chemistry_2009-2019_V2.csv",
  "Seine_baseGeo_Naides.csv",
  "Cleaned_Seine_Data.csv",
  "NT_NSW_Chem_Cleaned.csv",
  "MCM_Chem_clean.csv",
  "MurrayDarlingChem_Merged.csv",
  "MD__MD__Merbein__9df18bcf9b24.csv",
  "Chem_Cameroon.csv",
  guadeloupe_files,
  "CAMREX_filled_template.csv",
  "Finnish_riverine_data_07032025.csv",
  "Japan_MLIT_AllSites_chemistry.csv",
  "Netherlands_RWS_AllSites_chemistry.csv",
  "NIVA_geogenic_clean.csv",
  "NIVA_Water_chemistry.csv",
  "NigerRiver.csv",
  "yzeron_301502401_chem.csv",
  "yzeron_v301502402_chem.csv",
  "yzeron_v3015810_chem.csv",
  "20221030_masterdata_chem_V2.csv",
  "USGS__USGS__Choptank_River__da9275d27028.csv",
  "USGS__USGS__Frio_River__4e8764089639.csv",
  "USGS__USGS__Mississippi_River_above_Vicksburg__d2360d259841.csv",
  susquehanna_filename
)

required_files <- c(
  file.path(input_dir, source_files),
  canada_provider_files,
  canada_variable_file,
  cameroon_provider_zip,
  usgs_provider_files,
  usgs_clinton_file,
  usgs_exclusion_file,
  reviewed_susquehanna_file,
  carey_file,
  nt_nsw_provider_file,
  japan_provider_file,
  netherlands_provider_file,
  arc_provider_files,
  arc_provider_metadata,
  file.path(qualifier_dir, "ElbeRiverChem.csv"),
  file.path(key_dir, "SiSyn_Data_Key.csv"),
  file.path(key_dir, "SiSyn_Chem_Units_Key.csv"),
  file.path(key_dir, "Site_Reference_Table.csv")
)

check_required_files(required_files)

read_dirs <- c(
  input_dir,
  selection_dir,
  qualifier_dir,
  key_dir,
  canada_provider_dir,
  dirname(cameroon_provider_zip),
  usgs_provider_dir,
  dirname(usgs_clinton_file),
  dirname(usgs_exclusion_file),
  dirname(reviewed_susquehanna_file),
  dirname(carey_file),
  dirname(nt_nsw_provider_file),
  dirname(japan_provider_file),
  dirname(netherlands_provider_file),
  arc_provider_dir
)
check_work_folder(output_dir, read_dirs, "source preparation")
dir.create(output_dir, recursive = TRUE)

# ---- source preparation helpers ----

read_source <- function(filename, directory = input_dir) {
  preferred_dir <- if (identical(directory, input_dir)) selection_dir else NULL
  read_source_csv(filename, directory, preferred_dir = preferred_dir)
}

as_number <- function(x) {
  suppressWarnings(as.numeric(x))
}

assert_values <- function(observed, expected, label, tolerance = 1e-10) {
  observed <- as.numeric(observed)
  expected <- as.numeric(expected)

  if (
    length(observed) != length(expected) ||
      any(is.na(observed) != is.na(expected)) ||
      any(abs(observed - expected) > tolerance, na.rm = TRUE)
  ) {
    stop(label, " changed")
  }
}

same_character <- function(x, y) {
  identical(as.character(x), as.character(y)) &&
    identical(is.na(x), is.na(y))
}

normalize_site_name <- function(x) {
  tolower(gsub("[[:space:]]+", " ", trimws(as.character(x))))
}

find_tds_labeled_tn <- function(data, tolerance = 1e-9) {
  # exact back-conversion identifies tn entries copied from paired tds values
  amount <- as_number(data$amount)
  sample_key <- paste(data$LTER, data$Stream_Name, data$date, sep = "\r")
  tds <- data$solute == "tot dissolved solids" &
    data$solute_units == "mg/L" &
    !is.na(amount)
  tn <- data$solute == "TN" &
    data$solute_units == "uM" &
    !is.na(amount)
  tds_by_sample <- split(amount[tds], sample_key[tds])
  matched <- rep(FALSE, nrow(data))

  for (row_number in which(tn)) {
    paired_tds <- tds_by_sample[[sample_key[row_number]]]
    if (length(paired_tds) > 0L) {
      matched[row_number] <- any(
        abs(amount[row_number] * 14.0067 / 1000 - paired_tds) < tolerance
      )
    }
  }

  matched
}

normalize_reviewed_dates <- function(
  x,
  date_order,
  expected_nonblank,
  expected_two_digit,
  expected_four_digit,
  expected_unique,
  expected_range,
  label
) {
  date_text <- trimws(as.character(x))
  blank <- is_blank(date_text)
  nonblank <- !blank
  source_pattern <- paste0(
    "^[0-9]{1,2}/[0-9]{1,2}/([0-9]{2}|[0-9]{4})",
    "( [0-9]{1,2}:[0-9]{2})?$"
  )

  assert_count(sum(nonblank), expected_nonblank,
               paste0(label, " nonblank dates"))
  assert_count(sum(nonblank & !grepl(source_pattern, date_text)), 0L,
               paste0(label, " unrecognized dates"))

  date_only <- sub("[[:space:]].*$", "", date_text[nonblank])
  time_suffix <- sub("^[^[:space:]]+", "", date_text[nonblank])
  parts <- do.call(rbind, strsplit(date_only, "/", fixed = TRUE))
  year_width <- nchar(parts[, 3])
  assert_count(sum(year_width == 2L), expected_two_digit,
               paste0(label, " two-digit source years"))
  assert_count(sum(year_width == 4L), expected_four_digit,
               paste0(label, " four-digit source years"))

  first_part <- as.integer(parts[, 1])
  second_part <- as.integer(parts[, 2])
  year <- as.integer(parts[, 3])
  # this source rule puts 00-50 in 2000-2050 and 51-99 in 1951-1999
  # keep this reviewed cutoff separate from the hybam date rule
  recent_two_digit <- year_width == 2L & year <= 50L
  older_two_digit <- year_width == 2L & year > 50L
  year[recent_two_digit] <- year[recent_two_digit] + 2000L
  year[older_two_digit] <- year[older_two_digit] + 1900L

  if (date_order == "mdy") {
    month <- first_part
    day <- second_part
  } else if (date_order == "dmy") {
    day <- first_part
    month <- second_part
  } else {
    stop("unsupported reviewed date order for ", label)
  }

  parsed <- suppressWarnings(as.Date(sprintf(
    "%04d-%02d-%02d",
    year,
    month,
    day
  )))
  if (any(is.na(parsed))) {
    stop(label, " dates did not parse")
  }
  if (!identical(range(parsed), as.Date(expected_range))) {
    stop(label, " date range changed")
  }
  assert_count(length(unique(parsed)), expected_unique,
               paste0(label, " unique dates"))

  normalized <- as.character(x)
  normalized[nonblank] <- paste0(format(parsed, "%Y-%m-%d"), time_suffix)
  assert_count(
    sum(nonblank & !grepl(
      "^[0-9]{4}-[0-9]{2}-[0-9]{2}( [0-9]{1,2}:[0-9]{2})?$",
      normalized
    )),
    0L,
    paste0(label, " non-ISO outputs")
  )
  if (!identical(
    sub("^[^[:space:]]+", "", normalized[nonblank]),
    time_suffix
  )) {
    stop(label, " sampling times changed")
  }
  assert_count(
    sum(nonblank & grepl(
      "^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}( |$)",
      normalized
    )),
    0L,
    paste0(label, " two-digit-year outputs")
  )
  normalized
}

write_prepared <- function(filename, data) {
  write_prepared_csv(filename, data, output_dir)
}

# ---- alpine field and laboratory methods ----

prepare_alpine <- function() {
  filename <- "Alpine_Clean.csv"
  before <- read_source(filename)
  after <- before
  # keep ammonium on its confirmed nh4 basis for every year
  ammonium <- before$parameter == "nh4_mgl"
  assert_count(sum(ammonium & !is_blank(before$value)), 3037L,
               "Alpine retained ammonium source results")
  value_number <- as_number(before$value)
  source_key <- paste(before$site, before$date, sep = "\r")
  field_rows <- before$parameter == "fldph" & !is.na(value_number)
  lab_rows <- before$parameter == "labph" & !is.na(value_number)
  field_keys <- unique(source_key[field_rows])
  lab_keys <- unique(source_key[lab_rows])
  overlapping_keys <- intersect(field_keys, lab_keys)
  overlapping_lab <- lab_rows & source_key %in% overlapping_keys
  field_conductivity <- before$parameter == "fldcond_uscm" &
    !is.na(value_number)
  lab_conductivity <- before$parameter == "labcond_uscm" &
    !is.na(value_number)
  field_conductivity_keys <- unique(source_key[field_conductivity])
  lab_conductivity_keys <- unique(source_key[lab_conductivity])
  overlapping_conductivity_keys <- intersect(
    field_conductivity_keys,
    lab_conductivity_keys
  )
  overlapping_lab_conductivity <- lab_conductivity &
    source_key %in% overlapping_conductivity_keys
  low_temperature <- before$parameter == "temp_c" & value_number < -0.5
  cold_temperature_ids <- c(
    "29939", "36372", "36413", "36434", "36455", "36476", "36497",
    "36518", "36539", "36560", "36581", "37406", "37447", "37469",
    "37491", "37512", "37533", "37554", "37576", "37597", "37618",
    "37640", "42743", "42763", "43792", "43812", "43832", "43852"
  )
  cold_temperature_dates <- c(
    "1994-11-29", "2002-01-08", "2002-01-22", "2002-01-29",
    "2002-02-05", "2002-02-12", "2002-02-19", "2002-02-26",
    "2002-03-05", "2002-03-12", "2002-03-19", "2002-12-24",
    "2003-01-07", "2003-01-14", "2003-01-21", "2003-01-28",
    "2003-02-04", "2003-02-11", "2003-02-18", "2003-02-25",
    "2003-03-04", "2003-03-11", "2008-01-01", "2008-01-08",
    "2009-01-06", "2009-01-13", "2009-01-20", "2009-01-27"
  )
  cold_temperature_values <- c(
    -2, rep(-1, 19L), -2, -1, -0.7, -0.6, -0.7, -0.7, -0.8, -0.8
  )
  cold_temperature_rows <- match(cold_temperature_ids, before[[1L]])
  if (any(is.na(cold_temperature_rows))) {
    stop("reviewed Alpine cold-temperature rows are missing")
  }
  if (
    !all(before$site[cold_temperature_rows] == "loch") ||
      !all(before$date[cold_temperature_rows] == cold_temperature_dates) ||
      !all(before$parameter[cold_temperature_rows] == "temp_c") ||
      !all(before$flag[cold_temperature_rows] == "0")
  ) {
    stop("reviewed Alpine cold-temperature row identity changed")
  }
  assert_values(
    value_number[cold_temperature_rows],
    cold_temperature_values,
    "reviewed Alpine cold-temperature values"
  )
  if (!identical(which(low_temperature), sort(cold_temperature_rows))) {
    stop("Alpine cold-temperature scope changed")
  }
  suspect_fluoride <-
    before[[1L]] == "8363" &
    before$site == "andrewscreek" &
    before$date == "1986-09-09" &
    before$parameter == "f_mgl" &
    before$value == "6" &
    before$flag == "1"

  assert_count(sum(field_rows), 2075L, "Alpine field pH rows")
  assert_count(length(field_keys), 1782L, "Alpine field pH keys")
  assert_count(sum(lab_rows), 2899L, "Alpine lab pH rows")
  assert_count(length(lab_keys), 2519L, "Alpine lab pH keys")
  assert_count(length(overlapping_keys), 1589L, "Alpine overlapping pH keys")
  assert_count(sum(overlapping_lab), 1832L, "Alpine overlapping lab pH rows")
  assert_count(length(setdiff(lab_keys, field_keys)), 930L,
               "Alpine lab-only pH keys")
  assert_count(sum(field_conductivity), 2964L,
               "Alpine field conductivity rows")
  assert_count(length(field_conductivity_keys), 2557L,
               "Alpine field conductivity keys")
  assert_count(sum(lab_conductivity), 2961L,
               "Alpine lab conductivity rows")
  assert_count(length(lab_conductivity_keys), 2568L,
               "Alpine lab conductivity keys")
  assert_count(length(overlapping_conductivity_keys), 2421L,
               "Alpine overlapping conductivity keys")
  assert_count(sum(overlapping_lab_conductivity), 2785L,
               "Alpine overlapping lab conductivity rows")
  assert_count(
    length(setdiff(lab_conductivity_keys, field_conductivity_keys)),
    147L,
    "Alpine lab-only conductivity keys"
  )
  assert_count(sum(low_temperature, na.rm = TRUE), 28L,
               "Alpine low-temperature rows")
  assert_count(sum(suspect_fluoride, na.rm = TRUE), 1L,
               "Alpine suspect fluoride row")

  # keep field ph and conductivity when field and lab share a site-day
  after$source_method_flag <- NA_character_
  after$source_method_flag[field_rows] <- "field pH"
  after$source_method_flag[lab_rows & !overlapping_lab] <-
    "laboratory pH fallback"
  after$source_method_flag[field_conductivity] <- "field conductivity"
  after$source_method_flag[
    lab_conductivity & !overlapping_lab_conductivity
  ] <- "laboratory conductivity fallback"
  after$value[overlapping_lab] <- NA_character_
  after$value[overlapping_lab_conductivity] <- NA_character_
  # keep reported fluoride and cold temperatures for later outlier review
  assert_values(
    as_number(after$value[suspect_fluoride]),
    6,
    "prepared Alpine reviewed fluoride value"
  )
  assert_values(
    as_number(after$value[cold_temperature_rows]),
    cold_temperature_values,
    "prepared Alpine reviewed cold-temperature values"
  )

  assert_count(
    sum(after$parameter == "labph" & !is.na(as_number(after$value))),
    1067L,
    "Alpine retained lab pH rows"
  )
  assert_count(sum(after$source_method_flag == "field pH", na.rm = TRUE),
               2075L, "Alpine field pH method flags")
  assert_count(
    sum(after$source_method_flag == "laboratory pH fallback", na.rm = TRUE),
    1067L,
    "Alpine laboratory pH fallback method flags"
  )
  assert_count(
    sum(after$parameter == "labcond_uscm" & !is.na(as_number(after$value))),
    176L,
    "Alpine retained lab conductivity rows"
  )
  assert_count(
    sum(after$source_method_flag == "field conductivity", na.rm = TRUE),
    2964L,
    "Alpine field conductivity method flags"
  )
  assert_count(
    sum(
      after$source_method_flag == "laboratory conductivity fallback",
      na.rm = TRUE
    ),
    176L,
    "Alpine laboratory conductivity fallback method flags"
  )
  assert_count(
    length(unique(source_key[
      after$source_method_flag == "field conductivity" &
        !is.na(after$source_method_flag)
    ])),
    2557L,
    "Alpine prepared field conductivity keys"
  )
  assert_count(
    length(unique(source_key[
      after$source_method_flag == "laboratory conductivity fallback" &
        !is.na(after$source_method_flag)
    ])),
    147L,
    "Alpine prepared lab conductivity keys"
  )
  assert_count(
    sum(!is.na(as_number(after$value[cold_temperature_rows]))),
    28L,
    "prepared Alpine retained cold-temperature rows"
  )

  if (!same_character(before$value[ammonium], after$value[ammonium])) {
    stop("Alpine ammonium source values changed during preparation")
  }

  write_prepared(filename, after)
}

# ---- switzerland methods and units ----

prepare_switzerland <- function() {
  filename <- "Switzerland_CAMELS_CH_AllSites_chemistry.csv"
  before <- read_source(filename)
  after <- before
  amount_number <- as_number(before$amount)
  source_key <- paste(before$LTER, before$Stream_Name, before$date, sep = "\r")
  lab_ph <- before$solute == "labph" & !is.na(amount_number)
  field_conductivity <- before$solute == "fldcond_uscm" &
    !is.na(amount_number)
  lab_conductivity <- before$solute == "labcond_uscm" &
    !is.na(amount_number)
  field_conductivity_keys <- unique(source_key[field_conductivity])
  lab_conductivity_keys <- unique(source_key[lab_conductivity])

  assert_count(nrow(before), 186696L, "Switzerland source rows")
  assert_count(sum(lab_ph), 20085L, "Switzerland lab pH rows")
  assert_count(
    length(unique(source_key[lab_ph])),
    11316L,
    "Switzerland lab pH keys"
  )
  assert_count(
    sum(field_conductivity),
    14400L,
    "Switzerland field conductivity rows"
  )
  assert_count(
    length(field_conductivity_keys),
    7659L,
    "Switzerland field conductivity keys"
  )
  assert_count(
    sum(lab_conductivity),
    5954L,
    "Switzerland lab conductivity rows"
  )
  assert_count(
    length(lab_conductivity_keys),
    3657L,
    "Switzerland lab conductivity keys"
  )
  assert_count(
    length(intersect(field_conductivity_keys, lab_conductivity_keys)),
    0L,
    "Switzerland overlapping conductivity keys"
  )

  after$source_method_flag <- NA_character_
  after$source_method_flag[lab_ph] <- "laboratory pH fallback"
  after$source_method_flag[field_conductivity] <- "field conductivity"
  after$source_method_flag[lab_conductivity] <-
    "laboratory conductivity fallback"

  assert_count(
    length(unique(source_key[
      after$source_method_flag == "laboratory pH fallback" &
        !is.na(after$source_method_flag)
    ])),
    11316L,
    "Switzerland prepared lab pH keys"
  )
  assert_count(
    length(unique(source_key[
      after$source_method_flag == "field conductivity" &
        !is.na(after$source_method_flag)
    ])),
    7659L,
    "Switzerland prepared field conductivity keys"
  )
  assert_count(
    length(unique(source_key[
      after$source_method_flag == "laboratory conductivity fallback" &
        !is.na(after$source_method_flag)
    ])),
    3657L,
    "Switzerland prepared lab conductivity keys"
  )
  assert_count(
    sum(!is.na(after$source_method_flag)),
    40439L,
    "Switzerland prepared method flags"
  )

  write_prepared(filename, after)
}

# ---- east river reporting limits ----

prepare_east_river <- function() {
  filename <- "EastRiverSFA_chem.csv"
  before <- read_source(filename)
  after <- before
  anion_limits <- c(nitrate = 0.1, phosphate = 0.1, sulfate = 0.1)
  expected_anion_nd <- c(nitrate = 310L, phosphate = 3115L, sulfate = 1L)
  anion_nd <- before$solute %in% names(anion_limits) &
    as_number(before$value) == 0
  observed_anion_nd <- table(before$solute[anion_nd])
  npoc <- before$solute == "npoc"
  assert_count(nrow(before), 33142L, "East River rows")
  assert_count(sum(npoc, na.rm = TRUE), 1420L, "East River NPOC rows")
  if (
    !identical(names(observed_anion_nd), names(expected_anion_nd)) ||
      !identical(as.integer(observed_anion_nd), unname(expected_anion_nd))
  ) {
    stop("the reviewed East River not-detectable scope changed")
  }
  # the original source defines zero anion results as not detectable
  # the reported as11 method has a 0.1 micromolar lower concentration limit
  for (solute_name in names(anion_limits)) {
    target <- anion_nd & before$solute == solute_name
    after$value[target] <- paste0("<", anion_limits[[solute_name]])
  }

  # keep the source label in the saved file and map npoc in the working copy
  after$source_method_flag <- NA_character_
  after$solute[npoc] <- "doc"
  after$source_method_flag[npoc] <- "reported as NPOC"

  write_prepared(filename, after)
}

# ---- japan source values ----

read_japan_provider_values <- function(header_label) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("the jsonlite package is required for the Japan provider file")
  }

  tables <- jsonlite::fromJSON(japan_provider_file, simplifyVector = FALSE)
  values <- list()
  value_row <- 0L

  for (table in tables) {
    if (length(table$rows) < 2L || length(table$rows[[1L]]) == 0L) {
      next
    }

    header <- unlist(table$rows[[1L]], use.names = FALSE)
    column <- which(sub("\\n.*$", "", header) == header_label)
    if (length(column) == 0L) {
      next
    }
    if (length(column) != 1L) {
      stop("a Japan provider table has repeated target columns")
    }

    for (row in table$rows[-1L]) {
      row <- unlist(row, use.names = FALSE)
      if (length(row) < column || is_blank(row[1L]) || is_blank(row[column])) {
        next
      }
      value_row <- value_row + 1L
      values[[value_row]] <- data.frame(
        date = sub(
          "日$",
          "",
          sub("月", "-", sub("年", "-", row[1L], fixed = TRUE), fixed = TRUE)
        ),
        amount = as_number(sub("^<", "", trimws(row[column]))),
        qualifier = if (startsWith(trimws(row[column]), "<")) "<" else NA_character_,
        stringsAsFactors = FALSE
      )
    }
  }

  if (length(values) == 0L) {
    stop("the requested Japan provider column was not found")
  }
  do.call(rbind, values)
}

prepare_japan <- function() {
  filename <- "Japan_MLIT_AllSites_chemistry.csv"
  before <- read_source(filename)
  after <- before
  nh4_factor <- (14.0067 + 4 * 1.00784) / 14.0067
  po4_factor <- (30.973762 + 4 * 15.999) / 30.973762
  nh4_provider <- read_japan_provider_values("アンモニウム態窒素")
  po4_provider <- read_japan_provider_values("溶解性オルトリン酸態リン")
  nh4_rows <- before$solute == "nh4_mgl"
  po4_rows <- which(before$solute == "po4_mgl")

  assert_count(nrow(before), 346L, "Japan source rows")
  assert_count(sum(nh4_rows, na.rm = TRUE), 68L, "Japan ammonium rows")
  assert_count(nrow(nh4_provider), 68L, "Japan provider ammonium rows")
  assert_count(sum(nh4_provider$qualifier == "<", na.rm = TRUE), 27L,
               "Japan provider ammonium limits")
  assert_count(length(po4_rows), 74L, "Japan source phosphate rows")
  assert_count(nrow(po4_provider), 15L,
               "Japan provider dissolved phosphate rows")
  assert_count(sum(po4_provider$qualifier == "<", na.rm = TRUE), 1L,
               "Japan provider dissolved phosphate limits")

  nh4_order <- match(before$date[nh4_rows], nh4_provider$date)
  if (anyNA(nh4_order)) {
    stop("Japan ammonium dates no longer match the provider")
  }
  assert_values(
    as_number(before$amount[nh4_rows]),
    nh4_provider$amount[nh4_order] * nh4_factor,
    "Japan provider ammonium values",
    tolerance = 1e-8
  )
  nh4_qualifier <- nh4_provider$qualifier[nh4_order]
  after$amount[nh4_rows] <- format_amount(
    as_number(before$amount[nh4_rows]),
    nh4_qualifier
  )
  after$solute_units[nh4_rows] <- "mg NH4/L"

  kept_po4 <- integer(nrow(po4_provider))
  available_po4 <- po4_rows
  for (provider_row in seq_len(nrow(po4_provider))) {
    expected_amount <- po4_provider$amount[provider_row] * po4_factor
    candidates <- available_po4[
      before$date[available_po4] == po4_provider$date[provider_row] &
        abs(as_number(before$amount[available_po4]) - expected_amount) < 1e-8
    ]
    if (length(candidates) == 0L) {
      stop("a dissolved Japan phosphate result no longer matches the source")
    }
    kept_po4[provider_row] <- candidates[1L]
    available_po4 <- setdiff(available_po4, candidates[1L])
  }

  after$source_method_flag <- NA_character_
  after$source_method_flag[kept_po4] <-
    "dissolved orthophosphate as P, MLIT"
  after$amount[kept_po4] <- format_amount(
    as_number(before$amount[kept_po4]),
    po4_provider$qualifier
  )
  keep <- before$solute != "po4_mgl" | seq_len(nrow(before)) %in% kept_po4
  after <- after[keep, , drop = FALSE]
  rownames(after) <- NULL

  assert_count(nrow(after), 287L, "prepared Japan rows")
  assert_count(sum(after$solute == "po4_mgl"), 15L,
               "prepared Japan dissolved phosphate rows")
  assert_count(sum(grepl("^<", after$amount)), 28L,
               "prepared Japan qualified rows")
  write_prepared(filename, after)
}

# ---- match netherlands source records ----

netherlands_signature <- function(site, date, value) {
  paste(site, date, sprintf("%.12g", as_number(value)), sep = "\r")
}

prepare_netherlands <- function() {
  filename <- "Netherlands_RWS_AllSites_chemistry.csv"
  before <- read_source(filename)
  after <- before
  provider <- readRDS(netherlands_provider_file)
  if (!isTRUE(provider$complete) || nrow(provider$failures) != 0L) {
    stop("the reviewed Netherlands provider download is incomplete")
  }

  station_names <- c(
    "borgharen.boven" = "Meuse at Borgharen Boven",
    "driel.boven" = "Nederrijn at Driel Boven",
    "eijsden" = "Meuse at Eijsden",
    "hagestein" = "Lek at Hagestein",
    "lith.boven" = "Meuse at Lith Boven",
    "lobith.ponton" = "Rhine at Lobith Ponton",
    "maasbracht" = "Meuse at Maasbracht"
  )
  plan <- data.frame(
    solute = c("nh4_mgl", "no3_mgl", "po4_mgl", "sio2_mgl"),
    target = c(
      "ammonium_as_n_dissolved",
      "nitrate_as_n_dissolved",
      "phosphate_as_p_dissolved",
      "dsi_as_si"
    ),
    factor = c(
      (14.0067 + 4 * 1.00784) / 14.0067,
      (14.0067 + 3 * 15.999) / 14.0067,
      (30.973762 + 4 * 15.999) / 30.973762,
      (28.0855 + 2 * 15.999) / 28.0855
    ),
    source_rows = c(155636L, 16754L, 18815L, 4010L),
    matched_rows = c(4302L, 1767L, 4305L, 4010L),
    excluded_rows = c(151334L, 14987L, 14510L, 0L),
    unmatched_only_days = c(7362L, 1022L, 1012L, 0L),
    limit_rows = c(43L, 1L, 11L, 1L),
    stringsAsFactors = FALSE
  )

  assert_count(nrow(before), 385343L, "Netherlands source rows")
  source_value <- as_number(before$amount)
  source_key <- netherlands_signature(
    before$Stream_Name,
    before$date,
    source_value
  )
  source_site_day <- paste(before$Stream_Name, before$date, sep = "\r")
  keep <- rep(TRUE, nrow(before))

  for (plan_row in seq_len(nrow(plan))) {
    rule <- plan[plan_row, , drop = FALSE]
    source_target <- before$solute == rule$solute
    assert_count(sum(source_target, na.rm = TRUE), rule$source_rows,
                 paste0("Netherlands ", rule$solute, " source rows"))

    observations <- provider$observations[
      provider$observations$target_name == rule$target &
        is.finite(provider$observations$value_numeric) &
        abs(provider$observations$value_numeric) < 1e10,
      ,
      drop = FALSE
    ]
    provider_site <- unname(station_names[observations$station_id])
    if (anyNA(provider_site)) {
      stop("a Netherlands provider station is not mapped")
    }
    provider_date <- substr(observations$source_time, 1L, 10L)
    provider_key <- netherlands_signature(
      provider_site,
      provider_date,
      observations$value_numeric
    )
    matched <- source_target & source_key %in% unique(provider_key)
    limit_key <- unique(provider_key[observations$limit_symbol == "<" &
      !is.na(observations$limit_symbol)])
    limited <- source_target & source_key %in% limit_key

    assert_count(sum(matched, na.rm = TRUE), rule$matched_rows,
                 paste0("Netherlands ", rule$solute, " provider matches"))
    assert_count(sum(source_target & !matched, na.rm = TRUE),
                 rule$excluded_rows,
                 paste0("Netherlands ", rule$solute, " excluded rows"))
    assert_count(sum(limited, na.rm = TRUE), rule$limit_rows,
                 paste0("Netherlands ", rule$solute, " limit rows"))
    unmatched_only <- source_target & !matched &
      !source_site_day %in% unique(source_site_day[matched])
    assert_count(length(unique(source_site_day[unmatched_only])),
                 rule$unmatched_only_days,
                 paste0("Netherlands ", rule$solute, " unmatched-only days"))
    if (!identical(
      sort(unique(before$Stream_Name[source_target])),
      sort(unique(before$Stream_Name[matched]))
    )) {
      stop("a Netherlands solute would lose a source site")
    }

    convert <- matched
    qualifier <- rep(NA_character_, sum(convert))
    qualifier[limited[convert]] <- "<"
    after$amount[convert] <- format_amount(
      source_value[convert] * rule$factor,
      qualifier
    )
    keep[source_target & !matched] <- FALSE
  }

  after <- after[keep, , drop = FALSE]
  rownames(after) <- NULL
  after$solute_units[after$solute == "nh4_mgl"] <- "mg NH4/L"
  assert_count(nrow(after), 204512L, "prepared Netherlands rows")
  assert_count(sum(grepl("^<", after$amount)), 56L,
               "prepared Netherlands qualified rows")
  write_prepared(filename, after)
}



# ---- individual canada source records ----

prepare_canada_sites <- function() {
  site_plan <- data.frame(
    filename = canada_site_files,
    site_no = c(
      "YT08AB0009", "BC08NB0002", "BC08NA0033",
      "BC08NF0001", "YT10AA0001", "BC08EF0001"
    ),
    stream_name = c(
      "ALSEK RIVER ABOVE BATES RIVER IN KLUANE NATIONAL PARK",
      "BEAVER RIVER ABOVE HIGHWAY 1 IN GLACIER NATIONAL PARK",
      "KICKING HORSE RIVER AT FIELD IN YOHO NATIONAL PARK",
      "KOOTENAY RIVER ABOVE HIGHWAY 93 IN KOOTENAY NATIONAL PARK",
      "LIARD RIVER AT UPPER CROSSING",
      "SKEENA RIVER AT USK"
    ),
    matched_rows = c(2445L, 7151L, 8646L, 8752L, 5450L, 8560L),
    selected_rows = c(2445L, 7149L, 8638L, 8750L, 5448L, 8560L),
    provider_days = c(2445L, 7147L, 8638L, 8750L, 5448L, 8559L),
    all_below_days = c(409L, 1375L, 1906L, 2240L, 880L, 1283L),
    dissolved_si_days = c(48L, 119L, 111L, 153L, 78L, 91L),
    legacy_rows = c(3L, 0L, 0L, 0L, 5L, 1L),
    final_rows = c(2448L, 7147L, 8638L, 8750L, 5453L, 8560L),
    source_rows = c(3041L, 8966L, 11413L, 10612L, 7201L, 11886L),
    stringsAsFactors = FALSE
  )

  mapping_rows <- function(
    codes,
    variables,
    provider_units,
    solute,
    solute_units,
    factor
  ) {
    row_count <- length(codes)
    data.frame(
      VMV_CODE = as.character(codes),
      Provider_Variable = rep(variables, length.out = row_count),
      Provider_Units = rep(provider_units, length.out = row_count),
      solute = rep(solute, length.out = row_count),
      solute_units = rep(solute_units, length.out = row_count),
      factor = rep(as.numeric(factor), length.out = row_count),
      stringsAsFactors = FALSE
    )
  }

  mg_to_um <- function(mass) 1000 / mass
  ug_to_um <- function(mass) 1 / mass

  mapping <- do.call(rbind, list(
    mapping_rows(c("107941", "6207"), "ALUMINUM DISSOLVED", "µG/L",
                 "Al", "uM", ug_to_um(26.9815385)),
    mapping_rows("13788", "ALUMINUM DISSOLVED", "MG/L",
                 "Al", "uM", mg_to_um(26.9815385)),
    mapping_rows(c("101535", "8264"), "BROMIDE DISSOLVED", "MG/L",
                 "Br", "uM", mg_to_um(79.904)),
    mapping_rows(c("1180", "13087", "1978"), "CALCIUM DISSOLVED", "MG/L",
                 "Ca", "uM", mg_to_um(40.078)),
    mapping_rows(
      c("108311", "17209", "8262", "1958"),
      c(rep("CHLORIDE DISSOLVED", 3L), "CHLORIDE"),
      "MG/L", "Cl", "uM", mg_to_um(35.45)
    ),
    mapping_rows(
      c("108310", "201836", "8261", "9105"),
      "FLUORIDE DISSOLVED", "MG/L", "F", "uM",
      mg_to_um(18.998403163)
    ),
    mapping_rows(c("107953", "6219"), "IRON DISSOLVED", "µG/L",
                 "Fe", "uM", ug_to_um(55.845)),
    mapping_rows("13801", "IRON DISSOLVED", "MG/L",
                 "Fe", "uM", mg_to_um(55.845)),
    mapping_rows(
      c("1181", "13095", "1980"),
      "POTASSIUM DISSOLVED/FILTERED", "MG/L", "K", "uM",
      mg_to_um(39.0983)
    ),
    mapping_rows(c("107956", "6222"), "LITHIUM DISSOLVED", "µG/L",
                 "Li", "uM", ug_to_um(6.94)),
    mapping_rows("13803", "LITHIUM DISSOLVED", "MG/L",
                 "Li", "uM", mg_to_um(6.94)),
    mapping_rows(c("1179", "13093", "1979"), "MAGNESIUM DISSOLVED", "MG/L",
                 "Mg", "uM", mg_to_um(24.305)),
    mapping_rows(c("107957", "6223"), "MANGANESE DISSOLVED", "µG/L",
                 "Mn", "uM", ug_to_um(54.938044)),
    mapping_rows("13805", "MANGANESE DISSOLVED", "MG/L",
                 "Mn", "uM", mg_to_um(54.938044)),
    mapping_rows(
      c("1178", "13097", "1981"),
      "SODIUM DISSOLVED/FILTERED", "MG/L", "Na", "uM",
      mg_to_um(22.98976928)
    ),
    mapping_rows(c("107968", "6229"), "STRONTIUM DISSOLVED", "µG/L",
                 "Sr", "uM", ug_to_um(87.62)),
    mapping_rows("13815", "STRONTIUM DISSOLVED", "MG/L",
                 "Sr", "uM", mg_to_um(87.62)),
    # keep dissolved silicon reported on a si mass basis
    mapping_rows(c("1175", "13812"), "SILICON DISSOLVED", "MG/L",
                 "DSi", "uM", mg_to_um(28.0855)),
    mapping_rows(c("107975", "6235"), "ZINC DISSOLVED", "µG/L",
                 "Zn", "uM", ug_to_um(65.38)),
    mapping_rows("13825", "ZINC DISSOLVED", "MG/L",
                 "Zn", "uM", mg_to_um(65.38)),
    mapping_rows("7557", "AMMONIA DISSOLVED", "MG/L",
                 "NHX", "uM", mg_to_um(14.0067)),
    mapping_rows(
      c("1099", "1970", "444", "8263"),
      c(rep("NITROGEN NITRITE", 3L), "NITROGEN DISSOLVED NITRITE"),
      "MG/L", "NO2", "uM", mg_to_um(14.0067)
    ),
    mapping_rows(c("7315", "8266"), "DISSOLVED NITROGEN NITRATE", "MG/L",
                 "NO3", "uM", mg_to_um(14.0067)),
    mapping_rows(
      c("1110", "2233"),
      c("DISSOLVED NITRITE/NITRATE", "NITRATE AND NITRITE (AS N)"),
      "MG/L", "NOx", "uM", mg_to_um(14.0067)
    ),
    mapping_rows(
      c("1072", "15253"),
      c("ORTHOPHOSPHORUS DISSOLVED", "ORTHOPHOSPHATE DISSOLVED"),
      "MG/L", "PO4", "uM", mg_to_um(30.973761998)
    ),
    mapping_rows(
      c("108312", "16309", "8265", "1972"),
      c(rep("SULPHATE DISSOLVED", 3L), "SULPHATE"),
      "MG/L", "SO4", "uM", mg_to_um(96.060)
    ),
    mapping_rows(c("108768", "1130", "1967", "9326"),
                 "NITROGEN TOTAL", "MG/L", "TN", "uM",
                 mg_to_um(14.0067)),
    mapping_rows(c("109108", "15406", "2013", "25359"),
                 "PHOSPHORUS TOTAL", "MG/L", "TP", "uM",
                 mg_to_um(30.973761998)),
    mapping_rows(c("109107", "1204", "15102", "2010"),
                 "PHOSPHORUS TOTAL DISSOLVED", "MG/L", "tdp", "uM",
                 mg_to_um(30.973761998)),
    mapping_rows(c("108767", "1968", "7657"),
                 "NITROGEN TOTAL DISSOLVED", "MG/L",
                 "tot dissolved N", "uM", mg_to_um(14.0067)),
    mapping_rows(c("10101", "10111", "1131", "9134"),
                 "ALKALINITY TOTAL CACO3", "MG/L",
                 "alkalinity", "ueq/L", 1000 / 50.04345),
    mapping_rows("1963", "COLOUR TRUE", "CTU",
                 "colour PCU", "PCU", 1),
    mapping_rows(c("100300", "101252", "1974", "201144"),
                 "CARBON DISSOLVED INORGANIC", "MG/L",
                 "dissolved inorg C", "uM", mg_to_um(12.011)),
    mapping_rows(c("106008", "201145", "6104"),
                 "CARBON DISSOLVED ORGANIC", "MG/L",
                 "dissolved org C", "uM", mg_to_um(12.011)),
    mapping_rows(c("1064", "110002"), "OXYGEN DISSOLVED", "MG/L",
                 "dissolved oxygen", "uM", mg_to_um(31.998)),
    mapping_rows(c("2235", "6009"), "CARBON TOTAL ORGANIC", "MG/L",
                 "tot org C", "uM", mg_to_um(12.011)),
    mapping_rows(c("10401", "1145", "1961"), "RESIDUE NONFILTERABLE", "MG/L",
                 "tss   dry mass", "mg/L", 1),
    mapping_rows(c("157", "2074"), "TURBIDITY", "NTU",
                 "turbidity", "NTU", 1),
    mapping_rows(c("1125", "15544"), "TEMPERATURE (WATER)", "DEG C",
                 "temp", "C", 1),
    mapping_rows(c("109026", "10301", "1075", "1965"),
                 c("PH (FIELD)", rep("PH", 3L)), "PH",
                 "pH", NA_character_, 1),
    mapping_rows(c("109027", "2", "2041"),
                 c("SPECIFIC CONDUCTANCE (FIELD)",
                   rep("SPECIFIC CONDUCTANCE", 2L)),
                 "µS/CM", "specific conductivity", "uS/cm", 1)
  ))

  expected_solutes <- c(
    "Al", "Br", "Ca", "Cl", "F", "Fe", "K", "Li", "Mg", "Mn",
    "DSi", "NHX", "NO2", "NO3", "NOx", "Na", "PO4", "SO4", "Sr", "TN",
    "TP", "Zn", "alkalinity", "colour PCU", "dissolved inorg C",
    "dissolved org C", "dissolved oxygen", "pH", "specific conductivity",
    "tdp", "temp", "tot dissolved N", "tot org C", "tss   dry mass",
    "turbidity"
  )
  if (
    anyDuplicated(mapping$VMV_CODE) > 0L ||
      !setequal(unique(mapping$solute), expected_solutes) ||
      length(unique(mapping$solute)) != 35L
  ) {
    stop("the reviewed Canada provider mapping changed")
  }

  variable_info <- read.csv(
    canada_variable_file,
    check.names = FALSE,
    stringsAsFactors = FALSE,
    na.strings = "",
    colClasses = "character",
    comment.char = ""
  )
  mapping_key <- paste(
    mapping$VMV_CODE,
    mapping$Provider_Variable,
    mapping$Provider_Units,
    sep = "\r"
  )
  variable_info_key <- paste(
    variable_info$VMV_CODE,
    variable_info$VARIABLE,
    variable_info[["UNIT_UNITÉ"]],
    sep = "\r"
  )
  mapping_info <- match(mapping_key, variable_info_key)
  method_counts <- vapply(mapping_key, function(key) {
    length(unique(variable_info$METHOD_CODE[variable_info_key == key]))
  }, integer(1))
  bad_info <- is.na(mapping_info)
  bad_method <- method_counts != 1L
  bad_variable <- rep(FALSE, nrow(mapping))
  bad_units <- rep(FALSE, nrow(mapping))
  available_info <- !bad_info
  bad_variable[available_info] <-
    mapping$Provider_Variable[available_info] !=
    variable_info$VARIABLE[mapping_info[available_info]]
  bad_units[available_info] <-
    mapping$Provider_Units[available_info] !=
    variable_info[["UNIT_UNITÉ"]][mapping_info[available_info]]
  allowed_missing_info <- mapping$VMV_CODE == "1125"
  if (
    !identical(mapping$VMV_CODE[bad_info], "1125") ||
      any(bad_method & !allowed_missing_info) ||
      any(bad_variable) ||
      any(bad_units)
  ) {
    bad <- (bad_info & !allowed_missing_info) |
      (bad_method & !allowed_missing_info) | bad_variable | bad_units
    stop(
      "the reviewed Canada provider variable information changed for: ",
      paste(mapping$VMV_CODE[bad], collapse = ", ")
    )
  }
  mapping$Provider_Method_Code <- NA_character_
  mapping$Provider_Method_Code[available_info] <-
    variable_info$METHOD_CODE[mapping_info[available_info]]

  required_columns <- c(
    "SITE_NO", "DATE_TIME_HEURE", "FLAG_MARQUEUR", "VALUE_VALEUR",
    "SDL_LDE", "MDL_LDM", "VMV_CODE", "UNIT_UNITÉ", "VARIABLE",
    "STATUS_STATUT", "SAMPLE_ID_ÉCHANTILLON"
  )
  provider_parts <- vector("list", length(canada_provider_files))
  for (file_index in seq_along(canada_provider_files)) {
    provider_file <- canada_provider_files[[file_index]]
    provider_part <- read.csv(
      provider_file,
      check.names = FALSE,
      stringsAsFactors = FALSE,
      na.strings = "",
      colClasses = "character",
      comment.char = ""
    )
    if (!all(required_columns %in% names(provider_part))) {
      stop("the reviewed Canada provider columns changed in ",
           basename(provider_file))
    }
    provider_part <- provider_part[
      provider_part$SITE_NO %in% site_plan$site_no,
      required_columns,
      drop = FALSE
    ]
    provider_part$Provider_File <- rep(basename(provider_file), nrow(provider_part))
    provider_parts[[file_index]] <- provider_part
  }
  provider <- do.call(rbind, provider_parts)
  rownames(provider) <- NULL

  assert_count(nrow(provider), 128469L,
               "six-site Canada provider source rows")
  assert_count(
    sum(provider$VMV_CODE == "2021" &
          provider$VARIABLE == "COLOUR TRUE" &
          provider[["UNIT_UNITÉ"]] == "REL UNIT"),
    1573L,
    "excluded Canada relative-unit colour rows"
  )

  mapping_index <- match(provider$VMV_CODE, mapping$VMV_CODE)
  mapped <- provider[!is.na(mapping_index), , drop = FALSE]
  mapped_mapping <- mapping[mapping_index[!is.na(mapping_index)], , drop = FALSE]
  if (
    !same_character(mapped$VARIABLE, mapped_mapping$Provider_Variable) ||
      !same_character(mapped[["UNIT_UNITÉ"]], mapped_mapping$Provider_Units)
  ) {
    stop("a reviewed Canada provider code changed variable or unit")
  }

  mapped$date <- substr(mapped$DATE_TIME_HEURE, 1L, 10L)
  mapped$solute <- mapped_mapping$solute
  mapped$solute_units <- mapped_mapping$solute_units
  mapped$Provider_Method_Code <- mapped_mapping$Provider_Method_Code
  mapped$numeric_value <- as_number(mapped$VALUE_VALEUR)
  mapped$converted_value <- mapped$numeric_value * mapped_mapping$factor
  mapped$censored <- !is_blank(mapped$FLAG_MARQUEUR) &
    trimws(mapped$FLAG_MARQUEUR) == "<"

  if (
    any(!grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", mapped$date)) ||
      any(is.na(as.Date(mapped$date))) ||
      any(is.na(mapped$numeric_value)) ||
      any(!is.finite(mapped$converted_value)) ||
      any(!is_blank(mapped$FLAG_MARQUEUR) &
            trimws(mapped$FLAG_MARQUEUR) != "<") ||
      any(mapped$censored & mapped$converted_value <= 0)
  ) {
    stop("the reviewed Canada provider values, dates, or flags changed")
  }

  assert_count(nrow(mapped), 41004L,
               "mapped Canada provider rows before method precedence")
  assert_count(sum(mapped$censored), 8095L,
               "mapped Canada provider censored rows")
  assert_count(sum(mapped$solute == "DSi"), 600L,
               "mapped Canada dissolved-silicon rows")
  for (site_row in seq_len(nrow(site_plan))) {
    assert_count(
      sum(mapped$SITE_NO == site_plan$site_no[site_row]),
      site_plan$matched_rows[site_row],
      paste0(site_plan$site_no[site_row], " mapped Canada rows")
    )
    assert_count(
      sum(
        mapped$SITE_NO == site_plan$site_no[site_row] &
          mapped$solute == "DSi"
      ),
      site_plan$dissolved_si_days[site_row],
      paste0(site_plan$site_no[site_row], " dissolved-silicon rows")
    )
  }

  cold_review_plan <- data.frame(
    site_no = c("BC08NA0033", "BC08NA0033", "BC08NF0001", "BC08NF0001"),
    date_time = c(
      "2004-12-29 09:45", "2007-01-02 08:45",
      "2001-02-12 11:45", "2002-12-30 09:30"
    ),
    sample_id = c("04PY002644", "07PY000426", "01PY000684", "03PY000001"),
    value = c(-2.8, -5, -4, -9),
    stringsAsFactors = FALSE
  )
  cold_review <- rep(FALSE, nrow(mapped))
  for (review_row in seq_len(nrow(cold_review_plan))) {
    plan <- cold_review_plan[review_row, , drop = FALSE]
    target <- mapped$SITE_NO == plan$site_no &
      mapped$DATE_TIME_HEURE == plan$date_time &
      mapped[["SAMPLE_ID_ÉCHANTILLON"]] == plan$sample_id &
      mapped$VMV_CODE == "15544" &
      mapped$VARIABLE == "TEMPERATURE (WATER)" &
      mapped[["UNIT_UNITÉ"]] == "DEG C" &
      mapped$Provider_Method_Code == "4456" &
      mapped$Provider_File == "Water-Qual-Eau-Columbia-2000-present.csv" &
      mapped$STATUS_STATUT == "P" &
      is_blank(mapped$FLAG_MARQUEUR) &
      is_blank(mapped$SDL_LDE) &
      is_blank(mapped$MDL_LDM) &
      abs(mapped$numeric_value - plan$value) < 1e-12
    target[is.na(target)] <- FALSE
    assert_count(sum(target), 1L,
                 paste0("reviewed Canada cold value ", plan$sample_id))
    cold_review <- cold_review | target
  }
  assert_count(sum(cold_review), 4L, "reviewed Canada cold rows")
  # keep reported cold values for later outlier review
  assert_values(
    mapped$numeric_value[cold_review],
    cold_review_plan$value,
    "prepared Canada reviewed cold-temperature values"
  )
  assert_count(nrow(mapped), 41004L,
               "mapped Canada provider rows after cold-value review")

  # use field ph and conductance first when lab values occur on the same day
  site_day <- paste(mapped$SITE_NO, mapped$date, sep = "\r")
  field_ph_days <- unique(site_day[mapped$VMV_CODE == "109026"])
  field_conductance_days <- unique(site_day[mapped$VMV_CODE == "109027"])
  drop_lab_ph <- mapped$VMV_CODE %in% c("10301", "1075", "1965") &
    site_day %in% field_ph_days
  drop_lab_conductance <- mapped$VMV_CODE %in% c("2", "2041") &
    site_day %in% field_conductance_days
  precedence_drop <- drop_lab_ph | drop_lab_conductance

  assert_count(sum(drop_lab_ph), 7L,
               "Canada lab pH rows superseded by field pH")
  assert_count(sum(drop_lab_conductance), 7L,
               "Canada lab conductance rows superseded by field conductance")
  assert_count(sum(precedence_drop & mapped$censored), 0L,
               "censored Canada rows removed by method precedence")

  selected <- mapped[!precedence_drop, , drop = FALSE]
  rownames(selected) <- NULL
  assert_count(nrow(selected), 40990L,
               "selected Canada provider rows before daily aggregation")
  assert_count(sum(selected$censored), 8095L,
               "selected Canada provider censored rows")
  for (site_row in seq_len(nrow(site_plan))) {
    assert_count(
      sum(selected$SITE_NO == site_plan$site_no[site_row]),
      site_plan$selected_rows[site_row],
      paste0(site_plan$site_no[site_row], " selected Canada rows")
    )
  }

  unit_key <- ifelse(is.na(selected$solute_units), "<blank>",
                     selected$solute_units)
  daily_key <- paste(
    selected$SITE_NO,
    selected$date,
    selected$solute,
    unit_key,
    sep = "\r"
  )
  daily_groups <- split(seq_len(nrow(selected)), daily_key)
  group_sizes <- lengths(daily_groups)
  assert_count(sum(group_sizes > 1L), 3L,
               "Canada provider repeated daily keys")
  assert_count(sum(group_sizes - 1L), 3L,
               "Canada provider repeated daily rows")

  collapse_provenance <- function(x) {
    values <- as.character(x)
    values[is.na(values)] <- ""
    collapsed <- paste(sort(unique(values)), collapse = ";")
    if (collapsed == "") NA_character_ else collapsed
  }

  method_flag_by_code <- c(
    "1175" = "dissolved Si, ECCC",
    "13812" = "dissolved Si, ECCC",
    "109026" = "field pH",
    "10301" = "laboratory pH fallback",
    "1075" = "laboratory pH fallback",
    "1965" = "laboratory pH fallback",
    "109027" = "field conductivity",
    "2" = "laboratory conductivity fallback",
    "2041" = "laboratory conductivity fallback"
  )

  # include each canada sample using half its threshold when it has a less-than flag
  daily_rows <- vector("list", length(daily_groups))
  names(daily_rows) <- names(daily_groups)
  for (group_number in seq_along(daily_groups)) {
    row_index <- daily_groups[[group_number]]
    detected <- !selected$censored[row_index]

    source_lab_flag <- half_limit_flag(sum(!detected), length(row_index))
    source_method_values <- unique(unname(
      method_flag_by_code[selected$VMV_CODE[row_index]]
    ))
    source_method_values <- source_method_values[!is.na(source_method_values)]
    if (length(source_method_values) > 1L) {
      stop("multiple retained Canada methods remain within one daily key")
    }
    source_method_flag <- if (length(source_method_values) == 1L) {
      source_method_values
    } else {
      NA_character_
    }
    daily_value <- daily_limit_mean(selected$converted_value[row_index],
      ifelse(selected$censored[row_index], "<", ""))
    amount <- sprintf("%.17g", daily_value)

    # keep original source details from every sample included in the average
    daily_rows[[group_number]] <- data.frame(
      LTER = "Canada",
      Stream_Name = site_plan$stream_name[
        match(selected$SITE_NO[row_index[1L]], site_plan$site_no)
      ],
      date = selected$date[row_index[1L]],
      solute = selected$solute[row_index[1L]],
      solute_units = selected$solute_units[row_index[1L]],
      amount = amount,
      Source_Lab_Flag = source_lab_flag,
      Source_Method_Flag = source_method_flag,
      Provider_Record_Source = "ECCC current provider",
      Provider_File = collapse_provenance(selected$Provider_File[row_index]),
      Provider_VMV_Code = collapse_provenance(selected$VMV_CODE[row_index]),
      Provider_Method_Code = collapse_provenance(
        selected$Provider_Method_Code[row_index]
      ),
      Provider_Sample_ID = collapse_provenance(
        selected[["SAMPLE_ID_ÉCHANTILLON"]][row_index]
      ),
      Provider_Date_Time = collapse_provenance(
        selected$DATE_TIME_HEURE[row_index]
      ),
      Provider_Variable = collapse_provenance(selected$VARIABLE[row_index]),
      Provider_Units = collapse_provenance(
        selected[["UNIT_UNITÉ"]][row_index]
      ),
      Provider_Value = collapse_provenance(selected$VALUE_VALEUR[row_index]),
      Provider_Flag = collapse_provenance(selected$FLAG_MARQUEUR[row_index]),
      Provider_SDL = collapse_provenance(selected$SDL_LDE[row_index]),
      Provider_MDL = collapse_provenance(selected$MDL_LDM[row_index]),
      Provider_Status = collapse_provenance(selected$STATUS_STATUT[row_index]),
      .site_no = selected$SITE_NO[row_index[1L]],
      stringsAsFactors = FALSE
    )
  }
  daily <- do.call(rbind, daily_rows)
  rownames(daily) <- NULL

  # count original below-limit samples separately from the calculated daily flags
  daily_below_count <- vapply(daily_groups, function(rows) {
    sum(selected$censored[rows])
  }, integer(1))
  daily_all_below <- daily_below_count == lengths(daily_groups)
  daily_mixed <- daily_below_count > 0L & !daily_all_below
  assert_count(sum(daily_all_below), 8093L, "Canada all-below-limit days")
  assert_count(sum(daily_mixed), 1L, "Canada mixed detected and below-limit days")
  assert_count(sum(daily_below_count), 8095L, "Canada below-limit source samples")
  if (!identical(unname(half_limit_flag(daily_below_count, lengths(daily_groups))),
      daily$Source_Lab_Flag)) {
    stop("Canada daily flags lost the original below-limit sample counts")
  }

  assert_count(nrow(daily), 40987L, "official Canada provider daily keys")
  assert_count(sum(is_half_limit_flag(daily$Source_Lab_Flag)), 8094L,
               "official Canada daily half-limit flags")
  if (any(grepl("^<", daily$amount))) stop("an unconverted Canada limit remains")
  assert_count(sum(daily$solute == "NO3"), 2139L,
               "official Canada provider nitrate days")
  assert_count(sum(daily$solute == "DSi"), 600L,
               "official Canada dissolved-silicon days")
  assert_count(
    sum(daily$Source_Method_Flag == "dissolved Si, ECCC", na.rm = TRUE),
    600L,
    "official Canada dissolved-silicon method flags"
  )
  method_variables <- daily$solute %in% c(
    "DSi", "pH", "specific conductivity"
  )
  if (
    any(is.na(daily$Source_Method_Flag[method_variables])) ||
      any(!method_variables & !is.na(daily$Source_Method_Flag))
  ) {
    stop("prepared Canada method flags changed")
  }
  for (site_row in seq_len(nrow(site_plan))) {
    site_daily <- daily$.site_no == site_plan$site_no[site_row]
    assert_count(
      sum(site_daily),
      site_plan$provider_days[site_row],
      paste0(site_plan$site_no[site_row], " Canada provider daily keys")
    )
    assert_count(
      sum(site_daily & daily_all_below),
      site_plan$all_below_days[site_row],
      paste0(site_plan$site_no[site_row], " all-below-limit Canada days")
    )
    assert_count(
      sum(site_daily & is_half_limit_flag(daily$Source_Lab_Flag)),
      site_plan$all_below_days[site_row] + sum(site_daily & daily_mixed),
      paste0(site_plan$site_no[site_row], " half-limit Canada daily flags")
    )
  }

  expand_provenance <- function(x) {
    x <- x[!is.na(x) & x != ""]
    values <- unlist(strsplit(x, ";", fixed = TRUE))
    sort(unique(values[values != ""]))
  }
  provenance_pairs <- list(
    VMV_CODE = c("VMV_CODE", "Provider_VMV_Code"),
    Provider_Method_Code = c(
      "Provider_Method_Code", "Provider_Method_Code"
    ),
    Sample_ID = c("SAMPLE_ID_ÉCHANTILLON", "Provider_Sample_ID"),
    SDL = c("SDL_LDE", "Provider_SDL"),
    MDL = c("MDL_LDM", "Provider_MDL")
  )
  for (pair_name in names(provenance_pairs)) {
    source_column <- provenance_pairs[[pair_name]][1L]
    output_column <- provenance_pairs[[pair_name]][2L]
    source_values <- expand_provenance(selected[[source_column]])
    output_values <- expand_provenance(daily[[output_column]])
    if (
      length(source_values) != length(output_values) ||
        !setequal(source_values, output_values)
    ) {
      stop(
        "Canada provider ", pair_name, " provenance changed; missing: ",
        paste(setdiff(source_values, output_values), collapse = ","),
        "; added: ",
        paste(setdiff(output_values, source_values), collapse = ",")
      )
    }
  }

  legacy_plan <- data.frame(
    site_no = c(
      "BC08EF0001", "YT08AB0009", "YT08AB0009", "YT08AB0009",
      rep("YT10AA0001", 5L)
    ),
    date = c(
      "2005-06-05", "2017-02-09", "2023-01-27", "2023-01-27",
      rep("2025-07-29", 5L)
    ),
    solute = c(
      "TP", "dissolved org C", "TN", "tot dissolved N",
      "Al", "Fe", "Mn", "Sr", "Zn"
    ),
    solute_units = "uM",
    expected_amount = c(
      3.29310982631041, 99.9084172841562, 6.42549636959455,
      8.56732849279273, 4.61426615832155, 4.57516339869281,
      0.240270658343788, 1.38096325039945, 0.0252370755582747
    ),
    stringsAsFactors = FALSE
  )
  legacy_rows <- vector("list", nrow(legacy_plan))
  source_by_file <- setNames(
    lapply(site_plan$filename, read_source),
    site_plan$filename
  )
  for (site_row in seq_len(nrow(site_plan))) {
    plan <- site_plan[site_row, , drop = FALSE]
    assert_count(
      nrow(source_by_file[[plan$filename]]),
      plan$source_rows,
      paste0(plan$filename, " source rows")
    )
  }

  for (legacy_index in seq_len(nrow(legacy_plan))) {
    plan <- legacy_plan[legacy_index, , drop = FALSE]
    filename <- site_plan$filename[match(plan$site_no, site_plan$site_no)]
    source_data <- source_by_file[[filename]]
    keep <- source_data$date == plan$date &
      source_data$solute == plan$solute &
      source_data$solute_units == plan$solute_units
    keep[is.na(keep)] <- FALSE
    assert_count(sum(keep), 1L,
                 paste0("legacy-only Canada row ", legacy_index))
    source_row <- source_data[keep, , drop = FALSE]
    assert_values(as_number(source_row$amount), plan$expected_amount,
                  paste0("legacy-only Canada amount ", legacy_index))
    if (grepl("^[<>]", source_row$amount)) {
      stop("a legacy-only Canada row unexpectedly has a qualifier")
    }

    legacy_rows[[legacy_index]] <- data.frame(
      source_row[, c(
        "LTER", "Stream_Name", "date", "solute", "solute_units", "amount"
      )],
      Source_Lab_Flag = NA_character_,
      Source_Method_Flag = NA_character_,
      Provider_Record_Source = "legacy-only whitelist",
      Provider_File = NA_character_,
      Provider_VMV_Code = NA_character_,
      Provider_Method_Code = NA_character_,
      Provider_Sample_ID = NA_character_,
      Provider_Date_Time = NA_character_,
      Provider_Variable = NA_character_,
      Provider_Units = NA_character_,
      Provider_Value = NA_character_,
      Provider_Flag = NA_character_,
      Provider_SDL = NA_character_,
      Provider_MDL = NA_character_,
      Provider_Status = NA_character_,
      .site_no = plan$site_no,
      stringsAsFactors = FALSE
    )
  }
  legacy <- do.call(rbind, legacy_rows)
  rownames(legacy) <- NULL

  excluded_source <- source_by_file[[site_plan$filename[
    site_plan$site_no == "BC08NF0001"
  ]]]
  excluded_nitrate <- excluded_source$date == "2022-12-05" &
    excluded_source$solute == "NO3" &
    excluded_source$solute_units == "uM"
  excluded_nitrate[is.na(excluded_nitrate)] <- FALSE
  assert_count(sum(excluded_nitrate), 1L,
               "withdrawn legacy Kootenay nitrate row")
  assert_values(
    as_number(excluded_source$amount[excluded_nitrate]),
    2.54823502468401,
    "withdrawn legacy Kootenay nitrate value"
  )

  provider_key <- paste(
    daily$.site_no, daily$date, daily$solute,
    ifelse(is.na(daily$solute_units), "<blank>", daily$solute_units),
    sep = "\r"
  )
  legacy_key <- paste(
    legacy$.site_no, legacy$date, legacy$solute,
    ifelse(is.na(legacy$solute_units), "<blank>", legacy$solute_units),
    sep = "\r"
  )
  if (anyDuplicated(legacy_key) > 0L || any(legacy_key %in% provider_key)) {
    stop("the reviewed legacy-only Canada keys changed")
  }

  combined <- rbind(daily, legacy)
  assert_count(nrow(combined), 40996L,
               "provider and legacy Canada output rows")
  assert_count(sum(is_half_limit_flag(combined$Source_Lab_Flag)), 8094L,
               "provider and legacy Canada daily half-limit flags")
  assert_count(length(unique(combined$solute)), 35L,
               "prepared Canada solutes")
  assert_count(sum(combined$solute == "NH4"), 0L,
               "prepared Canada NH4 rows")

  for (site_row in seq_len(nrow(site_plan))) {
    plan <- site_plan[site_row, , drop = FALSE]
    filename <- plan$filename
    before <- source_by_file[[filename]]
    after <- combined[combined$.site_no == plan$site_no, , drop = FALSE]
    after$.site_no <- NULL
    after <- after[
      order(after$date, after$solute, after$solute_units, after$amount),
      ,
      drop = FALSE
    ]
    rownames(after) <- NULL

    assert_count(nrow(after), plan$final_rows,
                 paste0(filename, " prepared rows"))
    assert_count(
      sum(after$Provider_Record_Source == "legacy-only whitelist"),
      plan$legacy_rows,
      paste0(filename, " legacy-only rows")
    )
    assert_count(sum(after$solute == "NH4"), 0L,
                 paste0(filename, " prepared NH4 rows"))
    assert_count(
      sum(after$solute == "DSi"),
      plan$dissolved_si_days,
      paste0(filename, " prepared dissolved-silicon rows")
    )
    assert_count(
      sum(
        after$solute == "DSi" &
          after$Source_Method_Flag == "dissolved Si, ECCC",
        na.rm = TRUE
      ),
      plan$dissolved_si_days,
      paste0(filename, " dissolved-silicon method flags")
    )
    output_key <- paste(
      after$date, after$solute,
      ifelse(is.na(after$solute_units), "<blank>", after$solute_units),
      sep = "\r"
    )
    if (anyDuplicated(output_key) > 0L) {
      stop("duplicate prepared Canada daily keys remain in ", filename)
    }

    write_prepared(filename, after)
  }
}

# ---- seine reporting units ----

prepare_seine_base <- function() {
  filename <- "Seine_baseGeo_Naides.csv"
  before <- read_source(filename)
  after <- before
  sediment_calcium <- after$chemical == "Calcium" &
    after$units == "mg(Ca)/kg"
  isotope <- after$units == "‰ vs SMOW"
  sulfate_counts <- table(after$units[after$chemical == "Sulfates"])
  expected_sulfate_counts <- c(
    "mg(S)/L" = 8930L,
    "mg(SO4)/L" = 3380L,
    "mg/L" = 405L,
    "‰ vs SMOW" = 1L
  )

  assert_count(sum(sediment_calcium, na.rm = TRUE), 69L,
               "Seine sediment-calcium rows")
  assert_count(sum(isotope, na.rm = TRUE), 1L, "Seine isotope rows")
  if (!identical(as.integer(sulfate_counts[names(expected_sulfate_counts)]),
                 as.integer(expected_sulfate_counts))) {
    stop("Seine sulfate-unit counts changed")
  }

  plain_sulfate <- after$chemical == "Sulfates" & after$units == "mg/L"
  assert_count(sum(!is.na(as_number(after$value[plain_sulfate]))), 357L,
               "Seine numeric plain-unit sulfate rows")
  plain_keys <- unique(paste(after$Stream_Name[plain_sulfate &
    !is.na(as_number(after$value))], after$date[plain_sulfate &
    !is.na(as_number(after$value))], sep = "\r"))
  assert_count(length(plain_keys), 347L,
               "Seine distinct plain-unit sulfate keys")

  change_masks <- list(
    calcium = after$chemical == "Calcium" & after$units == "mg(Ca)/L",
    magnesium = after$chemical == "Magnesium" & after$units == "mg(Mg)/L",
    potassium = after$chemical == "Potassium" & after$units == "mg(K)/L",
    sulfate_s = after$chemical == "Sulfates" & after$units == "mg(S)/L",
    sulfate_so4 = after$chemical == "Sulfates" &
      after$units %in% c("mg(SO4)/L", "mg/L")
  )
  expected_changes <- c(
    calcium = 9555L,
    magnesium = 9548L,
    potassium = 9349L,
    sulfate_s = 8930L,
    sulfate_so4 = 3785L
  )
  observed_changes <- vapply(change_masks, sum, integer(1), na.rm = TRUE)
  if (!identical(observed_changes, expected_changes)) {
    stop("Seine reviewed unit-change counts changed")
  }

  # retain the source molecular basis so sulfur and sulfate are not mixed
  after$units[change_masks$calcium] <- "mg/L"
  after$units[change_masks$magnesium] <- "mg/L"
  after$units[change_masks$potassium] <- "mg/L"
  after$units[change_masks$sulfate_s] <- "mg S/L"
  after$units[change_masks$sulfate_so4] <- "mg SO4/L"
  # exclude sediment and isotope rows from the water-concentration master
  after <- after[!(sediment_calcium | isotope), , drop = FALSE]
  rownames(after) <- NULL
  sulfate_labels <- after$chemical == "Sulfates"
  assert_count(sum(sulfate_labels, na.rm = TRUE), 12715L,
               "Seine sulfate labels")
  after$chemical[sulfate_labels] <- "SO4"

  assert_count(nrow(after), 43639L, "prepared Seine base rows")
  assert_count(sum(after$units %in% c(
    "mg(Ca)/kg", "‰ vs SMOW", "mg(Ca)/L", "mg(Mg)/L", "mg(K)/L",
    "mg(S)/L", "mg(SO4)/L"
  )), 0L, "prepared Seine old unit labels")
  assert_count(sum(after$chemical == "Sulfates", na.rm = TRUE), 0L,
               "prepared Seine old sulfate labels")
  assert_count(sum(after$chemical == "SO4" & after$units == "mg S/L"),
               8930L, "prepared Seine sulfate-as-sulfur rows")
  assert_count(sum(after$chemical == "SO4" & after$units == "mg SO4/L"),
               3785L, "prepared Seine sulfate-as-sulfate rows")

  write_prepared(filename, after)
}

# ---- seine source dates ----

prepare_cleaned_seine <- function() {
  filename <- "Cleaned_Seine_Data.csv"
  before <- read_source(filename)
  after <- before
  invalid_do <- after$station_name == "IVRY-SUR-SEINE" &
    after$date == "2/10/04" & as_number(after$DO_mgl) == 95
  invalid_nh3 <- after$station_name == "HEURTEAUVILLE 1" &
    after$date == "7/11/05" & as_number(after$NH3..mg.l.) == -0.002

  assert_count(sum(invalid_do, na.rm = TRUE), 1L, "Seine invalid oxygen row")
  assert_count(sum(invalid_nh3, na.rm = TRUE), 1L, "Seine invalid ammonia row")
  # keep the reported oxygen and ammonia values for later outlier review
  assert_values(as_number(after$DO_mgl[invalid_do]), 95,
                "prepared Seine reviewed oxygen value")
  assert_values(as_number(after$NH3..mg.l.[invalid_nh3]), -0.002,
                "prepared Seine reviewed ammonia value")
  after$date <- normalize_reviewed_dates(
    after$date,
    date_order = "mdy",
    expected_nonblank = 18411L,
    expected_two_digit = 18411L,
    expected_four_digit = 0L,
    expected_unique = 3155L,
    expected_range = c("1955-12-05", "2013-12-20"),
    label = "Seine"
  )

  write_prepared(filename, after)
}


# ---- murray-darling units and methods ----

prepare_murray <- function() {
  filename <- "MurrayDarlingChem_Merged.csv"
  before <- read_source(filename)
  after <- before
  suspect_si <- c(
    "436125" = 110,
    "436221" = 520,
    "436222" = 350.1,
    "436227" = 90.1,
    "436228" = 860,
    "436229" = 70,
    "436230" = 760,
    "436234" = 430
  )
  suspect_rows <- match(names(suspect_si), after[[1L]])

  if (any(is.na(suspect_rows))) {
    stop("reviewed Murray-Darling suspect silica rows are missing")
  }
  assert_count(
    sum(after[[1L]] %in% names(suspect_si)),
    8L,
    "Murray-Darling suspect silica rows"
  )
  if (
    !all(after$Station[suspect_rows] == "414206") ||
      !all(after$Site[suspect_rows] == "Merbein") ||
      !all(after$shortname[suspect_rows] == "Si")
  ) {
    stop("reviewed Murray-Darling suspect silica identity changed")
  }
  assert_values(
    as_number(after$Value[suspect_rows]),
    unname(suspect_si),
    "Murray-Darling suspect silica values"
  )
  # keep reported silica values for later review
  value <- as_number(after$Value)
  source_key <- paste(after$Site, after$Datetime, sep = "\r")
  primary_alkalinity <- after$shortname == "Alk_CaCO3" & !is.na(value)
  fallback_alkalinity <- after$shortname == "BiCarbAlk_CaCO3" &
    !is.na(value)
  primary_keys <- unique(source_key[primary_alkalinity])
  fallback_keys <- unique(source_key[fallback_alkalinity])
  overlap_keys <- intersect(primary_keys, fallback_keys)
  fallback_overlap <- fallback_alkalinity & source_key %in% overlap_keys

  assert_count(sum(primary_alkalinity), 2063L,
               "Murray-Darling primary alkalinity rows")
  assert_count(length(primary_keys), 2063L,
               "Murray-Darling primary alkalinity keys")
  assert_count(sum(fallback_alkalinity), 121L,
               "Murray-Darling fallback alkalinity rows")
  assert_count(length(fallback_keys), 121L,
               "Murray-Darling fallback alkalinity keys")
  assert_count(length(overlap_keys), 94L,
               "Murray-Darling alkalinity overlap keys")
  assert_count(sum(fallback_overlap), 94L,
               "Murray-Darling superseded fallback alkalinity rows")
  assert_count(length(setdiff(fallback_keys, primary_keys)), 27L,
               "Murray-Darling fallback-only alkalinity keys")

  primary_values <- stats::setNames(
    value[primary_alkalinity],
    source_key[primary_alkalinity]
  )
  fallback_values <- stats::setNames(
    value[fallback_alkalinity],
    source_key[fallback_alkalinity]
  )
  disagreement_keys <- overlap_keys[
    primary_values[overlap_keys] != fallback_values[overlap_keys]
  ]
  expected_disagreement <- paste("Swan Hill", "2008-08-26", sep = "\r")
  if (
    !identical(disagreement_keys, expected_disagreement) ||
      primary_values[expected_disagreement] != 20 ||
      fallback_values[expected_disagreement] != 19
  ) {
    stop("the reviewed Murray-Darling alkalinity disagreement changed")
  }

  # prefer total alkalinity and use bicarbonate alkalinity as a fallback
  after$source_method_flag <- NA_character_
  after$source_method_flag[primary_alkalinity] <- "total alkalinity"
  after$source_method_flag[fallback_alkalinity & !fallback_overlap] <-
    "bicarbonate alkalinity fallback"
  after$Value[fallback_overlap] <- NA_character_
  alkalinity_labels <- after$shortname %in%
    c("Alk_CaCO3", "BiCarbAlk_CaCO3")
  water_temperature_labels <- after$shortname == "Water_Temp"
  other_temperature_labels <- after$shortname == "Temp"
  assert_count(sum(alkalinity_labels), 2184L,
               "Murray-Darling alkalinity labels")
  assert_count(sum(water_temperature_labels), 15141L,
               "Murray-Darling water-temperature labels")
  assert_count(sum(other_temperature_labels), 18882L,
               "Murray-Darling unmapped temperature labels")
  after$shortname[alkalinity_labels] <- "alkalinity"

  assert_count(sum(after$shortname %in%
    c("Alk_CaCO3", "BiCarbAlk_CaCO3")), 0L,
    "prepared Murray-Darling old alkalinity labels")
  assert_count(sum(after$shortname == "alkalinity"), 2184L,
               "prepared Murray-Darling alkalinity labels")
  assert_count(sum(after$shortname == "alkalinity" &
    !is.na(as_number(after$Value))), 2090L,
    "prepared Murray-Darling retained alkalinity rows")
  assert_count(
    sum(after$source_method_flag == "total alkalinity", na.rm = TRUE),
    2063L,
    "Murray-Darling total alkalinity method flags"
  )
  assert_count(
    sum(
      after$source_method_flag == "bicarbonate alkalinity fallback",
      na.rm = TRUE
    ),
    27L,
    "Murray-Darling bicarbonate fallback method flags"
  )
  assert_count(sum(after$shortname == "Water_Temp"), 15141L,
               "prepared Murray-Darling water-temperature labels")
  assert_count(sum(after$shortname == "Temp"), 18882L,
               "prepared Murray-Darling unmapped temperature labels")
  temperature_rows <- before$shortname %in% c("Temp", "Water_Temp")
  if (
    !same_character(
      before$shortname[temperature_rows],
      after$shortname[temperature_rows]
    ) ||
      !same_character(before$Value[temperature_rows], after$Value[temperature_rows]) ||
      !same_character(before$Varnam[temperature_rows], after$Varnam[temperature_rows])
  ) {
    stop("Murray-Darling temperature labels, values, or descriptions changed")
  }

  site_map <- c(
    "Euston" = "Euston Weir",
    "Mitta Mitta at Talandoon" = "Mitta Mitta at Tallandoon",
    "Murrimbidgee River @ Balranald" = "Murrumbidgee River at Balranald",
    "Rufus River Junct" = "Rufus River Junction"
  )
  expected_names <- c(
    "Euston" = 16434L,
    "Mitta Mitta at Talandoon" = 27781L,
    "Murrimbidgee River @ Balranald" = 15892L,
    "Rufus River Junct" = 12493L
  )
  observed_names <- vapply(
    names(site_map),
    function(name) sum(after$Site == name, na.rm = TRUE),
    integer(1)
  )
  if (!identical(observed_names, expected_names)) {
    stop("Murray-Darling reviewed source-name counts changed")
  }
  for (old_name in names(site_map)) {
    after$Site[after$Site == old_name] <- unname(site_map[[old_name]])
  }

  value <- as_number(after$Value)
  zero_conductivity <- after$shortname == "Conduct" & value == 0
  zero_field_ec <- after$shortname == "Field_EC" & value == 0
  assert_count(sum(zero_conductivity, na.rm = TRUE), 30L,
               "Murray-Darling zero-conductivity rows")
  assert_count(sum(zero_field_ec, na.rm = TRUE), 38L,
               "Murray-Darling zero field-EC rows")
  # keep reported zero conductivity for later outlier review

  if (!same_character(before$Value[zero_field_ec], after$Value[zero_field_ec])) {
    stop("Murray-Darling unkeyed field EC changed")
  }
  assert_count(sum(after$Site %in% names(site_map)), 0L,
               "prepared Murray-Darling old names")
  write_prepared(filename, after)
}

# ---- merbein source checks ----

prepare_md_merbein <- function() {
  filename <- "MD__MD__Merbein__9df18bcf9b24.csv"
  before <- read_source(filename)
  after <- before
  suspect_values <- c(
    "1987-09-08\rDSi" = 1830.78549019281,
    "1989-08-22\rDSi" = 1499.57975151248,
    "1989-09-05\rDSi" = 1165.04531194088,
    "1989-12-05\rZn" = 1000.30590394616,
    "1991-01-29\rZn" = 1300.09177118385
  )
  source_key <- paste(after$date, after$solute, sep = "\r")
  suspect_rows <- match(names(suspect_values), source_key)

  if (any(is.na(suspect_rows))) {
    stop("reviewed Merbein suspect rows are missing")
  }
  assert_count(
    sum(source_key %in% names(suspect_values)),
    5L,
    "Merbein suspect rows"
  )
  if (
    !all(after$LTER[suspect_rows] == "MD") ||
      !all(after$Stream_Name[suspect_rows] == "Merbein") ||
      !all(after$solute_units[suspect_rows] == "uM")
  ) {
    stop("reviewed Merbein suspect row identity changed")
  }
  assert_values(
    as_number(after$amount[suspect_rows]),
    unname(suspect_values),
    "Merbein suspect row values"
  )
  # keep these five reported values for later outlier review
  assert_count(
    sum(!is.na(as_number(after$amount[suspect_rows]))),
    5L,
    "prepared Merbein retained reviewed rows"
  )

  write_prepared(filename, after)
}

# ---- cameroon source structure and dates ----

prepare_cameroon <- function() {
  filename <- "Chem_Cameroon.csv"
  before <- read_source(filename)
  after <- before
  # the data key now records alkalinity as ueq/l instead of the old um label
  sample_date <- as.Date(after$Date, format = "%m/%d/%y")

  assert_count(nrow(after), 3088L, "Cameroon rows")
  # these are the first, legitimate june 16 records in the 2021 sequences
  assert_count(sum(after$Date == "06/16/21", na.rm = TRUE), 6L,
               "Cameroon retained June 2021 rows")
  assert_count(sum(after$Date == "06/16/22", na.rm = TRUE), 0L,
               "Cameroon absent June 2022 block")
  if (!identical(range(sample_date), as.Date(c("1994-08-11", "2022-11-04")))) {
    stop("Cameroon date range changed")
  }

  if (!requireNamespace("readxl", quietly = TRUE)) {
    stop("the Cameroon date repair requires the readxl package")
  }

  provider_plan <- data.frame(
    site = c(
      "Mbalmayo", "Messam", "Nsimi_outlet", "Nsimi_spring", "Olama",
      "Pont_So'o"
    ),
    workbook = c(
      "Mbalmayo_hydrochemistry.xlsx",
      "Messam_hydrochemistry.xlsx",
      "Nsimi_outlet_hydrochemistry.xlsx",
      "Nsimi_spring_hydrochemistry.xlsx",
      "Olama_hydrochemistry.xlsx",
      "Pont_So'o_hydrochemistry.xlsx"
    ),
    first_excel_row = c(494L, 475L, 495L, 527L, 491L, 520L),
    second_excel_row = c(514L, 495L, 515L, 547L, 511L, 540L),
    stringsAsFactors = FALSE
  )
  provider_columns <- c(
    "Date", "WT", "pH", "EC", "Na+", "K+", "Mg2+", "Ca2+", "F-",
    "Cl-", "NO3-", "SO42-", "Alk", "HCO3-", "Si", "DOC", "SPM"
  )
  provider_measurements <- c(
    "pH", "EC", "Na+", "K+", "Mg2+", "Ca2+", "F-", "Cl-", "NO3-",
    "SO42-", "Alk", "HCO3-", "Si", "DOC", "SPM"
  )
  prepared_measurements <- c(
    "pH", "EC", "Na", "K", "Mg", "Ca", "F", "Cl", "NO3", "SO4",
    "Alk", "HCO3", "Si", "DOC", "SPM"
  )

  extract_dir <- tempfile("cameroon-provider-")
  dir.create(extract_dir)
  on.exit(unlink(extract_dir, recursive = TRUE, force = TRUE), add = TRUE)
  utils::unzip(cameroon_provider_zip, exdir = extract_dir)
  provider_files <- list.files(
    extract_dir,
    pattern = "[.]xlsx$",
    recursive = TRUE,
    full.names = TRUE
  )

  parse_provider_dates <- function(x) {
    if (inherits(x, "POSIXt") || inherits(x, "Date")) {
      return(as.Date(x))
    }
    text <- trimws(as.character(x))
    number <- as_number(text)
    parsed <- rep(as.Date(NA), length(text))
    numeric_rows <- !is.na(number)
    parsed[numeric_rows] <- as.Date(
      number[numeric_rows],
      origin = "1899-12-30"
    )
    iso_rows <- is.na(parsed) & grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}", text)
    parsed[iso_rows] <- as.Date(
      substr(text[iso_rows], 1L, 10L),
      format = "%Y-%m-%d"
    )
    parsed
  }

  restored_rows <- vector("list", nrow(provider_plan))
  for (site_index in seq_len(nrow(provider_plan))) {
    plan <- provider_plan[site_index, , drop = FALSE]
    provider_file <- provider_files[
      basename(provider_files) == plan$workbook
    ]
    assert_count(length(provider_file), 1L,
                 paste0(plan$site, " Cameroon provider workbook"))
    provider <- readxl::read_excel(provider_file)
    expected_provider_columns <- if (plan$site == "Nsimi_outlet") {
      c(provider_columns, "POC")
    } else {
      provider_columns
    }
    if (!identical(names(provider), expected_provider_columns)) {
      stop("the reviewed Cameroon provider columns changed for ", plan$site)
    }

    provider_date <- parse_provider_dates(provider$Date)
    june_rows <- which(provider_date == as.Date("2021-06-16")) + 1L
    expected_june_rows <- c(plan$first_excel_row, plan$second_excel_row)
    if (!identical(june_rows, expected_june_rows)) {
      stop("the reviewed Cameroon June rows changed for ", plan$site)
    }
    second_index <- plan$second_excel_row - 1L
    if (
      provider_date[second_index - 1L] != as.Date("2022-05-31") ||
        provider_date[second_index + 1L] != as.Date("2022-07-06")
    ) {
      stop("the reviewed Cameroon 2022 sequence changed for ", plan$site)
    }

    existing <- before$Site == plan$site & before$Date == "06/16/21"
    assert_count(sum(existing, na.rm = TRUE), 1L,
                 paste0(plan$site, " retained Cameroon 2021 row"))
    first_index <- plan$first_excel_row - 1L
    for (measurement_index in seq_along(prepared_measurements)) {
      prepared_name <- prepared_measurements[measurement_index]
      provider_name <- provider_measurements[measurement_index]
      prepared_value <- as_number(before[[prepared_name]][existing])
      provider_value <- as_number(provider[[provider_name]][first_index])
      if (
        xor(is.na(prepared_value), is.na(provider_value)) ||
          (!is.na(prepared_value) &&
             abs(prepared_value - provider_value) > 1e-10)
      ) {
        stop(
          "the retained Cameroon 2021 value changed for ", plan$site,
          " and ", prepared_name
        )
      }
    }

    restored <- as.data.frame(
      stats::setNames(
        replicate(ncol(after), NA_character_, simplify = FALSE),
        names(after)
      ),
      stringsAsFactors = FALSE
    )
    for (measurement_index in seq_along(prepared_measurements)) {
      prepared_name <- prepared_measurements[measurement_index]
      provider_name <- provider_measurements[measurement_index]
      provider_value <- provider[[provider_name]][second_index]
      restored[[prepared_name]] <- if (is.na(provider_value)) {
        NA_character_
      } else {
        as.character(provider_value)
      }
    }
    restored$Date <- "06/16/22"
    restored$Site <- plan$site
    assert_count(
      sum(!is.na(as_number(unlist(
        restored[prepared_measurements],
        use.names = FALSE
      )))),
      11L,
      paste0(plan$site, " restored Cameroon measurements")
    )
    restored_rows[[site_index]] <- restored
  }

  after <- rbind(after, do.call(rbind, restored_rows))
  rownames(after) <- NULL
  assert_count(nrow(after), 3094L, "prepared Cameroon rows")
  assert_count(sum(after$Date == "06/16/21", na.rm = TRUE), 6L,
               "prepared Cameroon June 2021 rows")
  assert_count(sum(after$Date == "06/16/22", na.rm = TRUE), 6L,
               "restored Cameroon June 2022 rows")

  unresolved_sulfate <- after$Site == "Olama" &
    after$Date == "12/15/03" & tolower(trimws(after$SO4)) == "nd"
  assert_count(sum(unresolved_sulfate, na.rm = TRUE), 1L,
               "Cameroon unresolved sulfate row")
  assert_count(sum(tolower(trimws(after$SO4)) == "nd", na.rm = TRUE), 1L,
               "Cameroon unresolved sulfate total")
  # exclude nd sulfate from the working input because no threshold is given
  after$SO4[unresolved_sulfate] <- NA_character_
  after$Date <- normalize_reviewed_dates(
    after$Date,
    date_order = "mdy",
    expected_nonblank = 3094L,
    expected_two_digit = 3094L,
    expected_four_digit = 0L,
    expected_unique = 768L,
    expected_range = c("1994-08-11", "2022-11-04"),
    label = "Cameroon"
  )

  write_prepared(filename, after)
}

# ---- guadeloupe source checks ----

validate_guadeloupe_sources <- function() {
  expected_rows <- c(
    guadeloupe_GRCBDMDF_chem.csv = 4736L,
    guadeloupe_GRCBDPBD_chem.csv = 1856L,
    guadeloupe_GRCBDQCK_chem.csv = 3184L,
    guadeloupe_GRCCEDIG_chem.csv = 26624L,
    guadeloupe_GRCVHBAR_chem.csv = 464L,
    guadeloupe_GRCVHSAV_chem.csv = 304L
  )
  expected_dates <- c(
    guadeloupe_GRCBDMDF_chem.csv = 289L,
    guadeloupe_GRCBDPBD_chem.csv = 115L,
    guadeloupe_GRCBDQCK_chem.csv = 196L,
    guadeloupe_GRCCEDIG_chem.csv = 516L,
    guadeloupe_GRCVHBAR_chem.csv = 29L,
    guadeloupe_GRCVHSAV_chem.csv = 19L
  )
  expected_columns <- c(
    "Date", "Hour", "Code Site", "Site", "Level", "Type",
    "Flask", "Comment", "Validate", "Site Name", "parameter", "value"
  )

  for (filename in guadeloupe_files) {
    data <- read_source(filename)
    if (!identical(names(data), expected_columns)) {
      stop(filename, " no longer has the original source columns")
    }
    assert_count(nrow(data), expected_rows[[filename]],
                 paste0(filename, " source rows"))

    source_date <- trimws(data$Date)
    assert_count(sum(is_blank(source_date)), 0L,
                 paste0(filename, " blank source dates"))
    parsed_date <- as.Date(source_date, format = "%Y-%m-%d")
    if (anyNA(parsed_date) || any(format(parsed_date, "%Y-%m-%d") != source_date)) {
      stop(filename, " has an invalid source date")
    }
    assert_count(length(unique(parsed_date)), expected_dates[[filename]],
                 paste0(filename, " source dates"))

    source_hour <- trimws(data$Hour)
    invalid_hour <- !is_blank(source_hour) &
      !grepl("^[0-9]{2}:[0-9]{2}$", source_hour)
    assert_count(sum(invalid_hour), 0L,
                 paste0(filename, " invalid source times"))
  }

  invisible(TRUE)
}

# ---- camrex source dates ----

prepare_camrex <- function() {
  filename <- "CAMREX_filled_template.csv"
  before <- read_source(filename)
  undated <- is_blank(before$Sampling.Date)

  assert_count(nrow(before), 191L, "CAMREX rows")
  assert_count(sum(undated), 21L, "CAMREX undated rows")
  # keep undated rows out of the master rather than assigning unsupported dates
  after <- before[!undated, , drop = FALSE]
  rownames(after) <- NULL

  sample_date <- as.Date(after$Sampling.Date, format = "%m/%d/%Y")
  if (
    any(is.na(sample_date)) ||
      !identical(range(sample_date), as.Date(c("1982-05-02", "1991-09-03")))
  ) {
    stop("CAMREX source dates changed")
  }
  # the original guide defines julian day 1 as january 1, 1982
  # the earlier conversion added one day by using that date as a zero-day origin
  after$Sampling.Date <- format(sample_date - 1, "%Y-%m-%d")
  if (!identical(
    range(as.Date(after$Sampling.Date)),
    as.Date(c("1982-05-01", "1991-09-02"))
  )) {
    stop("CAMREX corrected date range changed")
  }

  site_map <- c(
    "Santo Antonio do Ica" = "Amazon River at Santo Antonio do Ica",
    "Itapeua" = "Amazon River at Itapeua",
    "Vargem Grande" = "Amazon River at Vargem Grande"
  )
  expected_names <- c(
    "Santo Antonio do Ica" = 9L,
    "Itapeua" = 12L,
    "Vargem Grande" = 11L
  )
  observed_names <- vapply(
    names(site_map),
    function(name) sum(after$Site.Stream.Name == name, na.rm = TRUE),
    integer(1)
  )
  if (!identical(observed_names, expected_names)) {
    stop("CAMREX reviewed source-name counts changed")
  }
  for (old_name in names(site_map)) {
    after$Site.Stream.Name[after$Site.Stream.Name == old_name] <-
      unname(site_map[[old_name]])
  }

  assert_count(nrow(after), 170L, "prepared CAMREX rows")
  assert_count(sum(after$Site.Stream.Name %in% names(site_map)), 0L,
               "prepared CAMREX old names")
  write_prepared(filename, after)
}


# ---- niva source dates and labels ----

prepare_niva <- function() {
  filename <- "NIVA_Water_chemistry.csv"
  before <- read_source(filename)
  after <- before
  osteglo <- before$station_code == "ØSTEGLO"
  suspect_conductivity <-
    before$station_id == "29782" &
    before$station_code == "NOREVEF" &
    (
      (before$sample_date == "1/6/04 0:00" & before$`COND_mS/m` == "506") |
      (before$sample_date == "2/5/04 0:00" & before$`COND_mS/m` == "800") |
      (before$sample_date == "3/8/04 0:00" & before$`COND_mS/m` == "86.7")
    )

  assert_count(nrow(before), 3677L, "NIVA rows")
  assert_count(sum(osteglo, na.rm = TRUE), 538L, "NIVA ØSTEGLO rows")
  assert_count(sum(suspect_conductivity, na.rm = TRUE), 3L,
               "NIVA suspect conductivity rows")
  # keep reported conductivity values for later outlier review
  assert_values(
    as_number(after$`COND_mS/m`[suspect_conductivity]),
    c(506, 800, 86.7),
    "prepared NIVA reviewed conductivity values"
  )
  after$station_code[osteglo] <- "OSTEGLO"
  after$sample_date <- normalize_reviewed_dates(
    after$sample_date,
    date_order = "mdy",
    expected_nonblank = 3677L,
    expected_two_digit = 3677L,
    expected_four_digit = 0L,
    expected_unique = 2118L,
    expected_range = c("1990-01-09", "2020-12-07"),
    label = "NIVA"
  )

  write_prepared(filename, after)
}

# ---- niva geogenic site names ----

prepare_niva_geogenic <- function() {
  filename <- "NIVA_geogenic_clean.csv"
  before <- read_source(filename)
  after <- before
  osteglo <- before$Stream_Name == "ØSTEGLO"
  sulfate <- before$Parameter_id == "SO4" & before$unit == "mg/l"

  assert_count(nrow(before), 6434L, "NIVA geogenic rows")
  assert_count(sum(osteglo, na.rm = TRUE), 496L,
               "NIVA geogenic ØSTEGLO rows")
  assert_count(sum(sulfate, na.rm = TRUE), 1160L,
               "NIVA geogenic sulfate rows")

  after$Stream_Name[osteglo] <- "OSTEGLO"
  after$unit[sulfate] <- "mg SO4/L"

  write_prepared(filename, after)
}

# ---- niger source checks ----

prepare_niger <- function() {
  filename <- "NigerRiver.csv"
  before <- read_source(filename)

  assert_count(nrow(before), 85L, "Niger rows")
  assert_count(sum(before$Stream_Name == "Niger", na.rm = TRUE), 85L,
               "Niger normalized source names")
  assert_count(sum(is_blank(before$Stream_Name)), 0L, "Niger blank names")

  write_prepared(filename, before)
}

# ---- yzeron source labels ----

prepare_yzeron <- function(filename, station_id, expected_rows) {
  before <- read_source(filename)
  after <- before
  expected_columns <- c("date", "parameter", "value", "flag")

  assert_count(nrow(before), expected_rows, paste0(filename, " rows"))
  if (!identical(names(before), expected_columns)) {
    stop(filename, " source columns changed")
  }

  # site fields come from the file identity and belong only in the working copy
  after$site <- "Yzeron"
  after$`station id` <- station_id

  write_prepared(filename, after)
}


# ---- elbe source name ----

validate_elbe_name <- function() {
  filename <- "ElbeRiverChem.csv"
  data <- read_source(filename, directory = qualifier_dir)
  assert_count(nrow(data), 233L, "Elbe rows")
  assert_count(sum(data$Stream_Name == "Elbe", na.rm = TRUE), 233L,
               "Elbe normalized source names")
  assert_count(sum(is_blank(data$Stream_Name)), 0L, "Elbe blank source names")
}

# ---- usgs and older master repairs ----
# load these repairs here so their setup and later calls keep the same order
source(file.path(preparation_dir, "prepare_usgs_legacy.R"), local = TRUE)

# ---- krycklan source checks ----

prepare_krycklan_cations <- function() {
  filename <- "Krycklan_Cation_Clean.csv"
  before <- read_source(filename)
  suspect <- before[[1L]] == "5673" &
    before$Date == "4/30/2008" &
    before$SiteID == "15" &
    before$Stream_Name == "Site 15" &
    before$K.ug.l == "52438"

  assert_count(nrow(before), 7930L, "Krycklan cation rows")
  assert_count(sum(suspect, na.rm = TRUE), 1L,
               "Krycklan reviewed potassium row")
  write_prepared(filename, before)
}

# ---- legacy canada reporting units ----

prepare_canada <- function() {
  filename <- "Canada_WQ_dat.csv"
  before <- read_source(filename)
  after <- before
  withdrawn_nitrate <-
    before$SITE_NO == "BC08NF0001" &
    before$DATE_TIME_HEURE == "2022-12-05 09:48:00" &
    before$VARIABLE == "DISSOLVED NITROGEN NITRATE" &
    before$VALUE_VALEUR == "0.158" &
    before$UNIT_UNITE == "MG/L"
  withdrawn_nitrate[is.na(withdrawn_nitrate)] <- FALSE
  assert_count(sum(withdrawn_nitrate), 1L,
               "withdrawn legacy Kootenay nitrate row")
  # the current original source download no longer contains this older result
  after <- after[!withdrawn_nitrate, , drop = FALSE]
  rownames(after) <- NULL
  extractable_silicon <- before$VARIABLE == "SILICON EXTRACTABLE"
  unit_map <- c(
    "CARBON DISSOLVED ORGANIC" = "mg C/L",
    "DISSOLVED NITROGEN NITRATE" = "mg NO3-N/L",
    "NITROGEN TOTAL" = "mg N/L",
    "NITROGEN TOTAL DISSOLVED" = "mg N/L",
    "PHOSPHORUS TOTAL" = "mg P/L",
    "PHOSPHORUS TOTAL DISSOLVED" = "mg P/L",
    "AMMONIA DISSOLVED" = "mg N/L",
    "SILICON EXTRACTABLE" = "mg Si/L"
  )
  expected <- c(
    "CARBON DISSOLVED ORGANIC" = 1988L,
    "DISSOLVED NITROGEN NITRATE" = 1973L,
    "NITROGEN TOTAL" = 1097L,
    "NITROGEN TOTAL DISSOLVED" = 1966L,
    "PHOSPHORUS TOTAL" = 1988L,
    "PHOSPHORUS TOTAL DISSOLVED" = 997L,
    "AMMONIA DISSOLVED" = 189L,
    "SILICON EXTRACTABLE" = 1549L
  )

  assert_count(nrow(before), 11748L, "legacy Canada rows")
  original_values <- after$VALUE_VALEUR
  for (variable in names(unit_map)) {
    target <- after$VARIABLE == variable & after$UNIT_UNITE == "MG/L"
    assert_count(sum(target, na.rm = TRUE), expected[[variable]],
                 paste0("legacy Canada ", variable, " rows"))
    after$UNIT_UNITE[target] <- unit_map[[variable]]
  }
  if (!same_character(original_values, after$VALUE_VALEUR)) {
    stop("legacy Canada values changed while labeling reporting bases")
  }

  # keep extractable silicon in the prepared input for source transparency
  # the master workflow excludes it because method f055 confirms no filtration
  assert_count(sum(extractable_silicon, na.rm = TRUE), 1549L,
               "legacy Canada extractable silicon rows")
  assert_count(nrow(after), 11747L, "prepared legacy Canada rows")

  write_prepared(filename, after)
}

# ---- catalina-jemez units and source checks ----

prepare_catalina <- function() {
  filename <- "CatalinaJemez_chemistry_2009-2019_V2.csv"
  before <- read_source(filename)
  after <- before
  inferred <- after$SiteCode == "MG_WEIR" &
    after$SampleCode == "AZ_S1793" & is_blank(after$DateTime)

  assert_count(sum(inferred, na.rm = TRUE), 1L,
               "Catalina inferred-date sample")
  after$DateTime[inferred] <- "2018-04-19"

  date_text <- trimws(after$DateTime)
  mdy <- grepl(
    "^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/(09|1[0-9])$",
    date_text
  )
  iso <- grepl(
    "^20(09|1[0-9])-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$",
    date_text
  )
  assert_count(sum(mdy), 2234L, "Catalina month-day-year dates")
  assert_count(sum(iso), 1L, "Catalina inferred ISO date")
  assert_count(sum(!mdy & !iso), 0L, "Catalina unrecognized dates")

  parsed <- as.Date(rep(NA_character_, nrow(after)))
  parsed[mdy] <- as.Date(date_text[mdy], format = "%m/%d/%y")
  parsed[iso] <- as.Date(date_text[iso], format = "%Y-%m-%d")
  if (anyNA(parsed)) {
    stop("Catalina dates did not parse")
  }
  after$DateTime <- format(parsed, "%Y-%m-%d")

  source_names <- c(MG_WEIR = "MGWEIR", OR_low = "ORlow")
  for (source_name in names(source_names)) {
    after$SiteCode[after$SiteCode == source_name] <- source_names[[source_name]]
  }

  # do not alter the reported ph, oxygen, or temperature values here
  if (!same_character(before$pH, after$pH) ||
      !same_character(before$DO, after$DO) ||
      !same_character(before$Temp, after$Temp)) {
    stop("Catalina measurement values changed during date preparation")
  }

  write_prepared(filename, after)
}

# ---- nt and nsw source silica and reporting units ----

prepare_nt_nsw <- function() {
  filename <- "NT_NSW_Chem_Cleaned.csv"
  before <- read_source(filename)
  after <- before
  row_id <- as.character(after[[1L]])
  portal_duplicate <- row_id %in% c("195038", "195039")
  expected_signature <- data.frame(
    id = c("195038", "195039"),
    Site_Code = c("425007", "425007"),
    Parameter = c("DO", "DO_pct"),
    Date = c("12/1/09 17:41", "12/1/09 17:41"),
    Value = c("69.2", "5.53"),
    Unit = c("mg/L", "%"),
    Name = c(
      "DARLING RIVER AT BURTUNDY",
      "DARLING RIVER AT BURTUNDY"
    ),
    stringsAsFactors = FALSE
  )
  observed_signature <- data.frame(
    id = row_id[portal_duplicate],
    after[portal_duplicate, c(
      "Site_Code", "Parameter", "Date", "Value", "Unit", "Name"
    )],
    stringsAsFactors = FALSE
  )
  rownames(observed_signature) <- NULL
  if (!identical(observed_signature, expected_signature)) {
    stop("the two provider-confirmed NT/NSW duplicate rows changed")
  }

  # the official portal contains only the correctly labeled paired rows
  # remove the two extra swapped duplicates from the prepared input
  after <- after[!portal_duplicate, , drop = FALSE]
  rownames(after) <- NULL

  zinc_source <- as.character(after[[1L]]) == "244833" &
    after$Site_Code == "G8140159" &
    after$Parameter == "Zn" &
    after$Date == "27/09/1995" &
    after$Value == "4.6" &
    after$Unit == "micrograms/litre"
  assert_count(sum(zinc_source, na.rm = TRUE), 1L,
               "NT/NSW source zinc row")
  # the portal reports this result in micrograms per litre
  # convert it only in the working copy used by the chemistry workflow
  after$Value[zinc_source] <- "0.0046"
  after$Unit[zinc_source] <- "mg/L"

  unit_pairs <- data.frame(
    parameter = c(
      "DOC", "NH4", "NO2", "NO2", "NO3", "NO3", "NO3", "NOx", "NOx",
      "SO4", "SO4", "SRP", "SRP", "TKN",
      "TN", "TN", "TP", "TP"
    ),
    source_unit = c(
      "mg/L", "mg/L", "mg/L", "Milligrams/Litre", "mg/L", "milligrams/litre",
      "Milligrams/Litre", "mg/L", "milligrams/litre", "mg/L",
      "milligrams/litre", "mg/L", "milligrams/litre", "mg/L",
      "mg/L", "Milligrams/Litre", "mg/L", "Milligrams/Litre"
    ),
    target_unit = c(
      "mg C/L", "mg NH4-N/L", "mg NO2-N/L", "mg NO2-N/L", "mg NO3-N/L",
      "mg NO3-N/L", "mg NO3-N/L", "mg NO3-N/L", "mg NO3-N/L",
      "mg SO4/L",
      "mg SO4/L", "mg P/L", "mg P/L", "mg N/L", "mg N/L",
      "mg N/L", "mg P/L", "mg P/L"
    ),
    expected_rows = c(
      2172L, 1659L, 72L, 44L, 3894L, 140L, 47L, 5441L, 9L,
      5566L, 138L, 4361L, 95L, 2761L, 9580L, 134L,
      15559L, 137L
    ),
    stringsAsFactors = FALSE
  )
  source_values <- after$Value
  for (pair_row in seq_len(nrow(unit_pairs))) {
    target <- after$Parameter == unit_pairs$parameter[pair_row] &
      after$Unit == unit_pairs$source_unit[pair_row]
    assert_count(sum(target, na.rm = TRUE), unit_pairs$expected_rows[pair_row],
                 "NT/NSW molecular-basis unit rows")
    after$Unit[target] <- unit_pairs$target_unit[pair_row]
  }
  ph_unit <- after$Parameter == "pH" & after$Unit == "/"
  assert_count(sum(ph_unit, na.rm = TRUE), 27308L, "NT/NSW pH unit rows")
  after$Unit[ph_unit] <- NA_character_
  if (!same_character(source_values, after$Value)) {
    stop("NT/NSW measurement values changed during working preparation")
  }

  after$Date <- normalize_reviewed_dates(
    after$Date,
    date_order = "dmy",
    expected_nonblank = 244962L,
    expected_two_digit = 103972L,
    expected_four_digit = 140990L,
    expected_unique = 9338L,
    expected_range = c("1962-01-11", "2019-08-14"),
    label = "NT/NSW"
  )

  # the combined file does not identify the si fraction
  legacy_si <- after$Parameter == "Si"
  assert_count(sum(legacy_si, na.rm = TRUE), 7371L, "NT/NSW generic Si rows")
  assert_count(
    length(unique(after$Site_Code[legacy_si])),
    31L,
    "NT/NSW generic Si sites"
  )

  provider <- read_source_csv(
    basename(nt_nsw_provider_file),
    dirname(nt_nsw_provider_file)
  )
  provider_columns <- c(
    "Station No.", "Station Name", "Parameter", "Date & Time", "Value", "Unit"
  )
  if (!all(provider_columns %in% names(provider))) {
    stop("the NT/NSW silica provider columns changed")
  }

  provider$provider_date <- substr(provider[["Date & Time"]], 1L, 10L)
  valid_provider_date <- grepl(
    "^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    provider$provider_date
  )
  provider_station <- provider[["Station No."]] %in% c("409025", "425007")
  provider_soluble <- provider$Parameter == "Silica - Soluble"
  provider_unit <- tolower(trimws(provider$Unit)) == "mg/l"
  reviewed_provider <- provider[
    provider_station & provider_soluble & provider_unit & valid_provider_date,
    ,
    drop = FALSE
  ]

  legacy_target <- legacy_si & after$Site_Code %in% c("409025", "425007")
  legacy_keys <- unique(paste(
    after$Site_Code[legacy_target],
    substr(after$Date[legacy_target], 1L, 10L),
    sep = "\r"
  ))
  provider_keys <- paste(
    reviewed_provider[["Station No."]],
    reviewed_provider$provider_date,
    sep = "\r"
  )
  reviewed_provider <- reviewed_provider[
    provider_keys %in% legacy_keys,
    ,
    drop = FALSE
  ]
  provider_keys <- provider_keys[provider_keys %in% legacy_keys]

  assert_count(length(unique(legacy_keys)), 3165L,
               "NT/NSW project silica site-days")
  assert_count(nrow(reviewed_provider), 1752L,
               "NT/NSW provider-confirmed soluble silica rows")
  assert_count(length(unique(provider_keys)), 1752L,
               "NT/NSW provider-confirmed soluble silica site-days")
  assert_count(
    sum(reviewed_provider[["Station No."]] == "409025"),
    878L,
    "NT/NSW Yarrawonga soluble silica rows"
  )
  assert_count(
    sum(reviewed_provider[["Station No."]] == "425007"),
    874L,
    "NT/NSW Burtundy soluble silica rows"
  )
  if (any(is_blank(reviewed_provider$Value))) {
    stop("the NT/NSW provider-confirmed silica contains blank values")
  }

  si_rows <- after[rep(1L, nrow(reviewed_provider)), , drop = FALSE]
  si_rows[[1L]] <- paste0(
    "MDBA-",
    reviewed_provider[["Station No."]],
    "-",
    gsub("-", "", reviewed_provider$provider_date)
  )
  si_rows$Site_Code <- reviewed_provider[["Station No."]]
  si_rows$Parameter <- "Si"
  si_rows$Date <- reviewed_provider$provider_date
  si_rows$Value <- reviewed_provider$Value
  si_rows$Unit <- "mg SiO2/L"
  si_rows$Name <- reviewed_provider[["Station Name"]]

  after <- rbind(after[!legacy_si, , drop = FALSE], si_rows)
  rownames(after) <- NULL

  write_prepared(filename, after)
}

# ---- mcmurdo alkalinity units ----

prepare_mcm <- function() {
  filename <- "MCM_Chem_clean.csv"
  before <- read_source(filename)
  after <- before
  andersen_h1 <- after$location == "Andersen Creek at F5"
  aiken_f5 <- after$location == "Aiken Creek at f5"

  assert_count(sum(andersen_h1, na.rm = TRUE), 11L,
               "McMurdo Andersen H1 rows")
  if (!identical(unique(after$strmgageid[andersen_h1]), "andrsn_h1")) {
    stop("McMurdo Andersen gage identity changed")
  }
  assert_count(sum(aiken_f5, na.rm = TRUE), 13L, "McMurdo Aiken F5 rows")
  assert_count(sum(!is.na(as_number(after$alkalinity_meql))), 1949L,
               "McMurdo raw alkalinity rows")

  after$location[andersen_h1] <- "Andersen Creek at H1"
  after$location[aiken_f5] <- "Aiken Creek at F5"
  if (!same_character(before$alkalinity_meql, after$alkalinity_meql)) {
    stop("McMurdo raw alkalinity values changed during source preparation")
  }

  write_prepared(filename, after)
}

# ---- finnish source dates and labels ----

prepare_finnish <- function() {
  filename <- "Finnish_riverine_data_07032025.csv"
  before <- read_source(filename)
  after <- before
  name_map <- c(
    "Koskenkylänjoki 6030" = "Koskenkylanjoki 6030",
    "Närpiönjoki mts 6761" = "Narpionjoki mts 6761",
    "Pyhäjoki Hourunk 11400" = "Pyhajoki Hourunk 11400",
    "SIMOJOKI AS. 13500      " = "SIMOJOKI AS. 13500",
    "TORNIONJ KUKKOLA 14310  " = "TORNIONJ KUKKOLA 14310"
  )
  for (source_name in names(name_map)) {
    after$`Station name`[after$`Station name` == source_name] <-
      name_map[[source_name]]
  }
  sodium <- trimws(after$Parameter) == "NA"
  assert_count(sum(sodium, na.rm = TRUE), 9638L, "Finnish sodium rows")
  after$Parameter[sodium] <- "Na"
  sulfate <- trimws(after$Parameter) == "SO4" & after$Unit == "mg/l"
  assert_count(sum(sulfate, na.rm = TRUE), 9863L, "Finnish sulfate rows")
  after$Unit[sulfate] <- "mg SO4/L"
  source_values <- after$Value
  after$Date <- normalize_reviewed_dates(
    after$Date,
    date_order = "mdy",
    expected_nonblank = 58307L,
    expected_two_digit = 58307L,
    expected_four_digit = 0L,
    expected_unique = 2523L,
    expected_range = c("2000-01-03", "2022-12-15"),
    label = "Finnish"
  )
  if (!same_character(source_values, after$Value)) {
    stop("Finnish measurement values changed during working preparation")
  }
  write_prepared(filename, after)
}

# ---- combined source checks ----

prepare_deduplicated_sites <- function() {
  filename <- "Deduplicated_Sites_AllSites_chemistry.csv"
  before <- read_source(filename)
  assert_count(nrow(before), 39129L, "deduplicated chemistry rows")
  assert_count(sum(before$solute %in% c("TN", "TDS", "tot dissolved solids"),
                   na.rm = TRUE), 0L,
               "deduplicated uppercase TN or TDS rows")
  write_prepared(filename, before)
}

# ---- units-key coverage ----

normalize_solute <- function(x) {
  tolower(gsub("/| |\\-", "_", as.character(x)))
}

normalize_unit <- function(x) {
  value <- trimws(as.character(x))
  value <- gsub("µ|μ", "u", value)
  lower_value <- tolower(value)
  value[is.na(x) | value == "" | lower_value == "na"] <- NA_character_
  value[lower_value %in% c("-", "/")] <- NA_character_
  value[lower_value %in% c(
    "mg/l", "milligrams/litre", "milligramsperliter"
  )] <- "mg/L"
  value[lower_value == "mg(s)/l"] <- "mg S/L"
  value[lower_value == "mg(so4)/l"] <- "mg SO4/L"
  value[lower_value %in% c("ug/l", "micrograms/litre")] <- "ug/L"
  value[lower_value %in% c(
    "microsiemens/cm", "microsiemens cm", "microsiemenspercentimeter"
  )] <- "uS/cm"
  value[lower_value %in% c("deg c", "oc", "degrees celsius")] <- "C"
  value[value == "%"] <- "percent"
  value[lower_value == "pcu"] <- "PCU"
  value[lower_value == "fnu"] <- "FNU"
  value[lower_value == "ntu"] <- "NTU"
  value[lower_value == "ueq/l"] <- "ueq/L"
  gsub("/| |\\-", "_", value)
}

validate_long_units_key <- function() {
  data_key <- read_source("SiSyn_Data_Key.csv", directory = key_dir)
  units_key <- read_source("SiSyn_Chem_Units_Key.csv", directory = key_dir)
  # these reporting bases are confirmed, not assumptions needing a method flag
  confirmed_units <- list(
    c("Alpine_Clean.csv", "nh4_mgl", "mg NH4/L"),
    c("MurrayDarlingChem_Merged.csv", "Si", "mg SiO2/L"),
    c("NT_NSW_Chem_Cleaned.csv", "Si", "mg SiO2/L")
  )
  for (entry in confirmed_units) {
    selected <- units_key$Raw_Filename == entry[1] & units_key$Variable == entry[2]
    selected[is.na(selected)] <- FALSE
    assert_count(sum(selected), 1L, paste(entry[1], entry[2], "units-key rows"))
    if (units_key$Units[selected] != entry[3]) stop("check the confirmed reporting unit for ", entry[1])
  }
  canada_alkalinity <- units_key$Raw_Filename %in% canada_site_files &
    units_key$Variable == "alkalinity" & units_key$Units == "uM"
  canada_alkalinity_fixed <- units_key$Raw_Filename %in% canada_site_files &
    units_key$Variable == "alkalinity" & units_key$Units == "ueq/L"
  canada_nh4 <- units_key$Raw_Filename %in% canada_site_files &
    units_key$Variable == "NH4" & units_key$Units == "uM"

  assert_count(sum(canada_alkalinity), 0L,
               "old six-site Canada alkalinity key rows")
  assert_count(sum(canada_alkalinity_fixed), 6L,
               "fixed six-site Canada alkalinity key rows")
  assert_count(sum(canada_nh4), 0L,
               "old six-site Canada NH4 key rows")
  for (filename in canada_site_files[2:4]) {
    assert_count(
      sum(
        units_key$Raw_Filename == filename &
          units_key$Variable == "NHX" & units_key$Units == "uM"
      ),
      1L,
      paste0(filename, " NHX key rows")
    )
  }
  for (filename in canada_site_files[4:5]) {
    assert_count(
      sum(
        units_key$Raw_Filename == filename &
          units_key$Variable == "PO4" & units_key$Units == "uM"
      ),
      1L,
      paste0(filename, " PO4 key rows")
    )
  }
  murray_alkalinity <- units_key$Raw_Filename ==
    "MurrayDarlingChem_Merged.csv" & units_key$Variable == "alkalinity" &
    units_key$Units == "mg CaCO3/L"
  murray_old_alkalinity <- units_key$Raw_Filename ==
    "MurrayDarlingChem_Merged.csv" & units_key$Variable %in%
    c("Alk_CaCO3", "BiCarbAlk_CaCO3")
  murray_temperature <- units_key$Raw_Filename ==
    "MurrayDarlingChem_Merged.csv" & units_key$Variable == "Water_Temp" &
    units_key$Units == "C"
  murray_unmapped_temperature <- units_key$Raw_Filename ==
    "MurrayDarlingChem_Merged.csv" & units_key$Variable == "Temp"
  seine_sulfate <- units_key$Raw_Filename == "Seine_baseGeo_Naides.csv" &
    units_key$Variable == "SO4" &
    units_key$Units %in% c("mg S/L", "mg SO4/L")
  seine_old_sulfate <- units_key$Raw_Filename ==
    "Seine_baseGeo_Naides.csv" & units_key$Variable == "Sulfates"

  assert_count(sum(murray_alkalinity), 1L,
               "Murray-Darling alkalinity key rows")
  assert_count(sum(murray_old_alkalinity), 0L,
               "Murray-Darling old alkalinity key rows")
  assert_count(sum(murray_temperature), 1L,
               "Murray-Darling temperature key rows")
  assert_count(sum(murray_unmapped_temperature), 0L,
               "Murray-Darling unmapped temperature key rows")
  assert_count(sum(seine_sulfate), 2L, "Seine sulfate key rows")
  assert_count(sum(seine_old_sulfate), 0L, "Seine old sulfate key rows")

  # check units-key coverage against the working files written above
  prepared_files <- list.files(output_dir, pattern = "[.]csv$")
  long_files <- unique(data_key$Raw_Filename[
    data_key$Dataset_orientation == "long" &
      data_key$Raw_Filename %in% prepared_files
  ])
  workflow_aliases <- list(
    "Deduplicated_Sites_AllSites_chemistry.csv" = c(
      "ca_mgl", "cl_mgl", "sio2_mgl", "k_mgl", "mg_mgl",
      "nh4_mgl", "na_mgl", "po4_mgl", "so4_mgl", "tp_ugl",
      "doc_mgl", "no3_mgl", "al_ugl", "f_mgl", "fe_ugl",
      "mn_ugl", "anc_ueql", "temp_c", "tdn_mgl"
    )
  )
  approved_unkeyed <- list(
    "Como_Creek_chem.csv" = "tss___dry_mass",
    "MurrayDarlingChem_Merged.csv" = c(
      "alk_ph", "depth", "field_ec", "flow", "sample_temp", "temp"
    ),
    "NT_NSW_Chem_Cleaned.csv" = c("cu", "q"),
    "guadeloupe_GRCCEDIG_chem.csv" = "nicb_(%)",
    "guadeloupe_GRCBDPBD_chem.csv" = "nicb_(%)",
    "guadeloupe_GRCVHSAV_chem.csv" = "nicb_(%)"
  )
  missing_rows <- data.frame(
    Raw_Filename = character(),
    Variable = character(),
    Units = character(),
    stringsAsFactors = FALSE
  )

  for (filename in long_files) {
    data <- read_source(filename, directory = output_dir)
    key_rows <- data_key[data_key$Raw_Filename == filename, , drop = FALSE]
    solute_column <- key_rows$Raw_Column_Name[
      !is.na(key_rows$Standardized_Column_Name) &
        key_rows$Standardized_Column_Name == "solute"
    ]
    amount_column <- key_rows$Raw_Column_Name[
      !is.na(key_rows$Standardized_Column_Name) &
        key_rows$Standardized_Column_Name == "amount"
    ]
    unit_column <- key_rows$Raw_Column_Name[
      !is.na(key_rows$Standardized_Column_Name) &
        key_rows$Standardized_Column_Name == "solute_units"
    ]

    if (length(solute_column) != 1L || length(amount_column) != 1L) {
      stop("long-file key fields changed for '", filename, "'")
    }

    observed_solute <- normalize_solute(data[[solute_column]])
    observed_unit <- if (length(unit_column) == 1L) {
      normalize_unit(data[[unit_column]])
    } else {
      rep(NA_character_, nrow(data))
    }
    observed_pairs <- unique(data.frame(
      solute = observed_solute,
      source_units = observed_unit,
      stringsAsFactors = FALSE
    ))
    observed_pairs <- observed_pairs[!is_blank(observed_pairs$solute), , drop = FALSE]
    key_rows_for_file <- units_key[
      units_key$Raw_Filename == filename,
      c("Variable", "Units"),
      drop = FALSE
    ]
    key_pairs <- unique(data.frame(
      solute = normalize_solute(key_rows_for_file$Variable),
      key_units = normalize_unit(key_rows_for_file$Units),
      stringsAsFactors = FALSE
    ))
    keyed <- unique(key_pairs$solute)
    observed <- unique(observed_pairs$solute)
    missing <- setdiff(
      observed,
      c(
        keyed,
        workflow_aliases[[filename]],
        approved_unkeyed[[filename]]
      )
    )

    if (length(missing) > 0L) {
      for (solute in missing) {
        source_units <- unique(observed_pairs$source_units[
          observed_pairs$solute == solute & !is.na(observed_pairs$source_units)
        ])
        if (length(source_units) != 1L) {
          stop("missing units-key row has unclear units in '", filename, "'")
        }
        missing_rows <- rbind(
          missing_rows,
          data.frame(
            Raw_Filename = filename,
            Variable = toupper(solute),
            Units = source_units,
            stringsAsFactors = FALSE
          )
        )
      }
    }

    covered_pairs <- observed_pairs[
      observed_pairs$solute %in% keyed,
      ,
      drop = FALSE
    ]
    for (pair_row in seq_len(nrow(covered_pairs))) {
      solute <- covered_pairs$solute[pair_row]
      source_unit <- covered_pairs$source_units[pair_row]
      choices <- key_pairs[key_pairs$solute == solute, , drop = FALSE]
      exact_match <- if (is.na(source_unit)) {
        nrow(choices) == 1L
      } else {
        sum(!is.na(choices$key_units) & choices$key_units == source_unit) == 1L
      }
      if (!exact_match) {
        stop(
          "a prepared variable/unit pair does not resolve in '",
          filename,
          "': ", solute, " / ", source_unit
        )
      }
    }
  }

  missing_rows <- missing_rows[
    order(missing_rows$Raw_Filename, missing_rows$Variable, missing_rows$Units),
    ,
    drop = FALSE
  ]
  rownames(missing_rows) <- NULL
  if (nrow(missing_rows) > 0L) {
    observed_text <- apply(missing_rows, 1L, paste, collapse = " | ")
    stop(
      "prepared long-file rows are missing from the Units Key: ",
      paste(observed_text, collapse = "; ")
    )
  }
  invisible(TRUE)
}

# ---- run ----

prepare_alpine()
prepare_switzerland()
prepare_east_river()
prepare_krycklan_cations()
prepare_canada()
prepare_canada_sites()
prepare_catalina()
prepare_seine_base()
prepare_cleaned_seine()
prepare_nt_nsw()
prepare_mcm()
prepare_murray()
prepare_md_merbein()
prepare_cameroon()
validate_guadeloupe_sources()
prepare_camrex()
prepare_finnish()
prepare_japan()
prepare_netherlands()
prepare_niva()
prepare_niva_geogenic()
prepare_niger()
prepare_yzeron("yzeron_301502401_chem.csv", "V301502401", 258L)
prepare_yzeron("yzeron_v301502402_chem.csv", "V301502402", 145L)
prepare_yzeron("yzeron_v3015810_chem.csv", "V3015810", 301L)
prepare_deduplicated_sites()
validate_elbe_name()
prepare_legacy_usgs()
prepare_current_usgs()
validate_long_units_key()
