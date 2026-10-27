# ---- preparation overview ----
# called automatically by the full chemistry runner
# the function below runs preparation in the reviewed source order
# amazon, source selection, and file assembly are defined later in this file
# canada, gems, danube, qualifiers, and other source rules stay in their named files

# ---- shared functions ----

script_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
if (length(script_argument) != 1L) {
  stop("the script folder could not be identified")
}
preparation_dir <- dirname(normalizePath(sub("^--file=", "", script_argument)))
source(file.path(dirname(preparation_dir), "chemistry_functions.R"))
source(file.path(dirname(preparation_dir), "chemistry_rules.R"))

# ---- preparation order ----

prepare_chemistry <- function(args) {
  if (length(args) != 5L) {
    stop(
      paste(
        "supply the read-only source snapshot, provider-download folder,",
        "key folder, evidence folder, and a new output folder"
      )
    )
  }

  raw_dir <- normalizePath(args[[1]], winslash = "/", mustWork = TRUE)
  provider_dir <- normalizePath(args[[2]], winslash = "/", mustWork = TRUE)
  key_dir <- normalizePath(args[[3]], winslash = "/", mustWork = TRUE)
  evidence_dir <- normalizePath(args[[4]], winslash = "/", mustWork = TRUE)

  output_parent <- normalizePath(
    dirname(path.expand(args[[5]])),
    winslash = "/",
    mustWork = TRUE
  )
  output_dir <- file.path(output_parent, basename(path.expand(args[[5]])))

  if (file.exists(output_dir)) {
    stop("the output folder must not already exist")
  }

  runner_argument <- grep("^--file=", commandArgs(), value = TRUE)
  if (length(runner_argument) != 1L) {
    stop("the script folder could not be identified")
  }
  script_dir <- dirname(normalizePath(
    sub("^--file=", "", runner_argument),
    winslash = "/",
    mustWork = TRUE
  ))
  input_dirs <- c(raw_dir, provider_dir, key_dir, evidence_dir, script_dir)
  if (any(vapply(
    input_dirs,
    function(input_dir) paths_overlap(output_dir, input_dir),
    logical(1)
  ))) {
    stop("the output folder must not overlap an input folder")
  }

  # ---- required files ----

  provider_script <- file.path(script_dir, "prepare_eccc_gems_danube.R")
  qualifier_script <- file.path(script_dir, "prepare_qualifier_inputs.R")
  source_script <- file.path(script_dir, "prepare_other_sources.R")
  legacy_script <- file.path(script_dir, "prepare_usgs_legacy.R")
  data_key <- file.path(key_dir, "SiSyn_Data_Key.csv")
  units_key <- file.path(key_dir, "SiSyn_Chem_Units_Key.csv")
  site_key <- file.path(key_dir, "Site_Reference_Table.csv")

  clinton_file <- file.path(
    evidence_dir,
    "usgs_05420500_tn_tds_20261009.csv"
  )
  exclusion_file <- file.path(
    evidence_dir,
    "usgs_tds_as_tn_exclusions.rds"
  )
  carey_file <- file.path(evidence_dir, "SiSyn_DataTemplate_Carey.xlsx")
  japan_provider_file <- file.path(
    evidence_dir,
    "yanase_chemistry_tables.json"
  )
  netherlands_provider_file <- file.path(
    evidence_dir,
    "netherlands_rws_target_chemistry.rds"
  )
  nt_nsw_provider_file <- file.path(
    provider_dir,
    "australia_mdba",
    "MDBA_RMWQMP_physico-chemical-data_20260914.csv"
  )

  required_files <- c(
    provider_script,
    qualifier_script,
    source_script,
    legacy_script,
    data_key,
    units_key,
    site_key,
    clinton_file,
    exclusion_file,
    carey_file,
    japan_provider_file,
    netherlands_provider_file,
    nt_nsw_provider_file
  )

  check_required_files(required_files, "required files")

  provider_paths <- c(
    file.path(provider_dir, "canada_eccc"),
    file.path(provider_dir, "gems_gemstat"),
    file.path(provider_dir, "danube_icpdr"),
    file.path(provider_dir, "amazon_hybam"),
    file.path(provider_dir, "cameroon_mtropics"),
    file.path(provider_dir, "australia_mdba"),
    file.path(provider_dir, "usa_usgs_samples", "responses")
  )

  if (any(!dir.exists(provider_paths))) {
    stop(
      "provider folders are missing: ",
      paste(basename(provider_paths[!dir.exists(provider_paths)]), collapse = ", ")
    )
  }

  cameroon_zip <- file.path(
    provider_dir,
    "cameroon_mtropics",
    "Cameroon_MTROPICS_HYDROCHEMICAL_DATA_NYONG_20260915.zip"
  )

  if (!file.exists(cameroon_zip)) {
    stop("the reviewed cameroon provider file is missing")
  }

  # ---- input folder checks ----

  read_only_dirs <- c(
    source = raw_dir,
    provider = provider_dir,
    keys = key_dir,
    evidence = evidence_dir
  )
  input_states <- record_input_states(read_only_dirs)
  verify_inputs <- function() {
    check_input_files(
      input_states,
      read_only_dirs,
      context = "chemistry preparation"
    )
  }
  on.exit(verify_inputs(), add = TRUE)

  # ---- temporary preparation folders ----

  work_dir <- tempfile(pattern = "chemistry-input-preparation-")
  dir.create(work_dir)
  on.exit(unlink(work_dir, recursive = TRUE, force = TRUE), add = TRUE)

  provider_output <- file.path(work_dir, "provider")
  amazon_output <- file.path(work_dir, "amazon")
  selection_output <- file.path(work_dir, "selected-sources")
  qualifier_output <- file.path(work_dir, "qualifiers")
  source_output <- file.path(work_dir, "source")

  # ---- prepare inputs ----

  # recover original source details for canada, gems, and danube
  run_r_script(
    provider_script,
    c(raw_dir, provider_dir, data_key, provider_output),
    log_prefix = "provider-inputs-"
  )

  # combine the amazon sources without losing their separate date ranges
  prepare_amazon(
    source_dir = raw_dir,
    provider_dir = file.path(provider_dir, "amazon_hybam"),
    output_dir = amazon_output
  )

  # remove confirmed overlap and tds-as-tn records from the prepared inputs
  select_source_files(
    input_dir = raw_dir,
    output_dir = selection_output,
    usgs_exclusion_file = exclusion_file
  )

  # restore reported qualifiers and documented numeric detection limits
  run_r_script(
    qualifier_script,
    settings = c(
      SISYN_CHEM_INPUT_DIR = raw_dir,
      SISYN_CHEM_SELECTION_INPUT_DIR = selection_output,
      SISYN_CHEM_QUALIFIER_OUTPUT_DIR = qualifier_output,
      SISYN_CHEM_KEY_DIR = key_dir
    ),
    log_prefix = "qualifier-inputs-"
  )

  # apply the remaining unit, date, name, and method choices for each source
  run_r_script(
    source_script,
    settings = c(
      SISYN_CHEM_INPUT_DIR = raw_dir,
      SISYN_CHEM_SELECTION_INPUT_DIR = selection_output,
      SISYN_CHEM_DIRECT_OUTPUT_DIR = source_output,
      SISYN_CHEM_QUALIFIER_OUTPUT_DIR = qualifier_output,
      SISYN_CHEM_KEY_DIR = key_dir,
      SISYN_CHEM_CANADA_PROVIDER_DIR = file.path(provider_dir, "canada_eccc"),
      SISYN_CAMEROON_PROVIDER_ZIP = cameroon_zip,
      SISYN_CHEM_USGS_PROVIDER_DIR = file.path(
        provider_dir,
        "usa_usgs_samples",
        "responses"
      ),
      SISYN_CHEM_USGS_CLINTON_FILE = clinton_file,
      SISYN_CHEM_USGS_EXCLUSION_FILE = exclusion_file,
      SISYN_CHEM_NT_NSW_PROVIDER_FILE = nt_nsw_provider_file,
      SISYN_CHEM_JAPAN_PROVIDER_FILE = japan_provider_file,
      SISYN_CHEM_NETHERLANDS_PROVIDER_FILE = netherlands_provider_file,
      SISYN_CHEM_REVIEWED_SUSQUEHANNA_FILE = file.path(
        raw_dir,
        "USGS__USGS__SUSQUEHANNA_RIVER__4db2531edf53.csv"
      ),
      SISYN_CHEM_CAREY_FILE = carey_file
    ),
    log_prefix = "source-inputs-"
  )

  # collect the prepared versions into the 77 inputs used by the master script
  assemble_inputs(
    raw_dir = raw_dir,
    selection_dir = selection_output,
    qualifier_dir = qualifier_output,
    direct_dir = source_output,
    provider_dir = file.path(provider_output, "chem_raw_subset"),
    amazon_dir = amazon_output,
    key_file = data_key,
    output_dir = output_dir
  )

  # use gems only where eccc or usgs lacks that variable on that date
  additions <- prepare_gems_chemistry_gaps(output_dir, provider_dir, key_dir)
  gems_path <- file.path(output_dir, "GEMS_AllSites_chemistry.csv")
  gems <- read.csv(gems_path, colClasses = "character", check.names = FALSE)
  if (any(gems$LTER %in% c("Canada", "USGS"))) {
    stop("GEMS chemistry gap filling has already been applied")
  }
  data.table::fwrite(dplyr::bind_rows(gems, additions), gems_path, na = "")

  # ---- final checks ----

  data_key_table <- read.csv(
    data_key,
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  chemistry_rows <- !is.na(data_key_table$Data_type) &
    data_key_table$Data_type == "chemistry" &
    !is.na(data_key_table$Raw_Filename) &
    data_key_table$Raw_Filename != ""
  expected_files <- sort(unique(data_key_table$Raw_Filename[chemistry_rows]))

  actual_files <- sort(list.files(output_dir, pattern = "[.]csv$"))
  if (!identical(actual_files, expected_files)) {
    stop("the prepared chemistry inputs do not match the data key")
  }

  verify_inputs()
}

# ---- amazon sources ----

prepare_amazon <- function(source_dir, provider_dir, output_dir) {
  source_dir <- normalizePath(source_dir, winslash = "/", mustWork = TRUE)
  provider_dir <- normalizePath(provider_dir, winslash = "/", mustWork = TRUE)
  output_parent <- normalizePath(dirname(path.expand(output_dir)), winslash = "/", mustWork = TRUE)
  output_dir <- file.path(output_parent, basename(path.expand(output_dir)))

  check_work_folder(output_dir, c(source_dir, provider_dir), "amazon preparation")

  camrex_path <- file.path(source_dir, "CAMREX_filled_template.csv")
  hybam_path <- file.path(source_dir, "Chem_HYBAM.csv")
  provider_path <- file.path(provider_dir, "Manacapuru.csv")
  required_files <- c(camrex_path, hybam_path, provider_path)
  check_required_files(required_files, "required manacapuru files")

  source_state <- data.frame(
    path = required_files,
    size = unname(file.info(required_files)$size),
    md5 = unname(tools::md5sum(required_files)),
    stringsAsFactors = FALSE
  )
  check_sources <- function() {
    current <- data.frame(
      path = required_files,
      size = unname(file.info(required_files)$size),
      md5 = unname(tools::md5sum(required_files)),
      stringsAsFactors = FALSE
    )
    if (!identical(source_state, current)) {
      stop("a manacapuru source file changed during preparation")
    }
  }
  on.exit(check_sources(), add = TRUE)

  # ---- amazon helpers ----

  parse_mdy_year <- function(value) {
    parts <- strsplit(trimws(value), "/", fixed = TRUE)
    valid <- lengths(parts) == 3L
    if (!all(valid)) {
      stop("a HYBAM date does not have month/day/year format")
    }
    month <- integer(length(parts))
    day <- integer(length(parts))
    year_text <- character(length(parts))
    for (row in seq_along(parts)) {
      month[row] <- as.integer(parts[[row]][1L])
      day[row] <- as.integer(parts[[row]][2L])
      year_text[row] <- parts[[row]][3L]
    }
    year <- as.integer(year_text)
    short_year <- nchar(year_text) == 2L
    # two-digit years use the same 1950 cutoff as the reviewed hybam dates
    year[short_year & year >= 50L] <- year[short_year & year >= 50L] + 1900L
    year[short_year & year < 50L] <- year[short_year & year < 50L] + 2000L
    date <- as.Date(sprintf("%04d-%02d-%02d", year, month, day))
    if (anyNA(date)) {
      stop("a HYBAM date could not be parsed")
    }
    date
  }

  apply_conversion <- function(data, rules) {
    rule_row <- match(data$source_variable, rules$source_variable)
    if (anyNA(rule_row)) {
      stop("a manacapuru variable lacks a conversion rule")
    }
    value <- suppressWarnings(as.numeric(data$source_value))
    if (anyNA(value) || any(!is.finite(value))) {
      stop("a manacapuru chemistry value is not numeric")
    }
    data.frame(
      LTER = "HYBAM",
      Stream_Name = "Manacapuru",
      date = as.character(data$date),
      solute = rules$solute[rule_row],
      solute_units = rules$solute_units[rule_row],
      amount = format_amount(
        value * rules$factor[rule_row],
        data$qualifier
      ),
      stringsAsFactors = FALSE
    )
  }

  # molecular and atomic masses match the chemistry units workflow
  C_mw <- 12.011
  Ca_mw <- 40.078
  Cl_mw <- 35.453
  F_mw <- 18.998403163
  Fe_mw <- 55.845
  H_mw <- 1.00784
  K_mw <- 39.0983
  Mg_mw <- 24.305
  Mn_mw <- 54.938044
  N_mw <- 14.0067
  Na_mw <- 22.989769
  O_mw <- 15.999
  S_mw <- 32.065
  Si_mw <- 28.0855
  Sr_mw <- 87.62
  Zn_mw <- 65.38
  Al_mw <- 26.9815385
  HCO3_mw <- H_mw + C_mw + 3 * O_mw
  NO3_mw <- N_mw + 3 * O_mw
  SO4_mw <- S_mw + 4 * O_mw

  # ---- current source data ----

  provider <- data.table::fread(
    provider_path,
    skip = 12L,
    sep = "\t",
    check.names = FALSE,
    colClasses = "character",
    na.strings = "",
    showProgress = FALSE
  )
  names(provider) <- trimws(names(provider))

  provider_columns <- c(
    Date = 1L,
    F = 3L,
    Zn = 8L,
    SO4 = 10L,
    temp = 11L,
    Al = 14L,
    DOC = 20L,
    Fe = 22L,
    K = 24L,
    pH = 25L,
    Na = 33L,
    Mn = 36L,
    DSi = 38L,
    HCO3 = 39L,
    Cl = 40L,
    NO3 = 43L,
    Sr = 45L,
    Ca = 47L,
    Mg = 55L,
    conductivity = 56L
  )
  expected_provider_headers <- c(
    "Date",
    "Mass concentration (F^{-}) (mg/L)",
    "Mass concentration (Zn) (µg/L)",
    "Mass concentration (SO_{4}^{2-}) (mg/L)",
    "Temperature (Temperature) (°C)",
    "Mass concentration (Al) (µg/L)",
    "Mass concentration (COD) (mg/L)",
    "Mass concentration (Fe) (µg/L)",
    "Mass concentration (K) (mg/L)",
    "Chemistry property (pH)",
    "Mass concentration (Na) (mg/L)",
    "Mass concentration (Mn) (µg/L)",
    "Mass concentration (Si) (mg/L)",
    "Mass concentration (HCO_{3}^{-}) (mg/L)",
    "Mass concentration (Cl^{-}) (mg/L)",
    "Mass concentration (NO_{3}^{-}) (mg/L)",
    "Mass concentration (Sr) (µg/L)",
    "Mass concentration (Ca) (mg/L)",
    "Mass concentration (Mg) (mg/L)",
    "Electronics and electromagnetism property (Conductivity) (µs/cm)"
  )
  if (
    ncol(provider) != 60L ||
      !identical(names(provider)[provider_columns], expected_provider_headers)
  ) {
    stop("the reviewed manacapuru provider columns changed")
  }

  provider_values <- provider[, provider_columns[-1L], with = FALSE]
  provider_long <- data.frame(
    date = rep(provider[[provider_columns[["Date"]]]], times = ncol(provider_values)),
    source_variable = rep(names(provider_columns)[-1L], each = nrow(provider)),
    source_value = unlist(provider_values, use.names = FALSE),
    qualifier = NA_character_,
    stringsAsFactors = FALSE
  )
  provider_long <- provider_long[!is_blank(provider_long$source_value), , drop = FALSE]
  provider_long$date <- as.Date(provider_long$date, format = "%Y/%m/%d")
  if (anyNA(provider_long$date)) {
    stop("a provider value has an invalid date")
  }
  assert_count(nrow(provider_long), 3794L, "mapped provider rows")

  provider_rules <- data.frame(
    source_variable = names(provider_columns)[-1L],
    solute = c(
      "F", "Zn", "SO4", "temp", "Al", "dissolved org C", "Fe", "K",
      "pH", "Na", "Mn", "DSi", "HCO3", "Cl", "NO3", "Sr", "Ca",
      "Mg", "conductivity"
    ),
    solute_units = c(
      "uM", "uM", "uM", "C", "uM", "uM", "uM", "uM", NA_character_,
      "uM", "uM", "uM", "uM", "uM", "uM", "uM", "uM", "uM", "uS/cm"
    ),
    factor = c(
      1000 / F_mw,
      1 / Zn_mw,
      1000 / SO4_mw,
      1,
      1 / Al_mw,
      1000 / C_mw,
      1 / Fe_mw,
      1000 / K_mw,
      1,
      1000 / Na_mw,
      1 / Mn_mw,
      1000 / Si_mw,
      1000 / HCO3_mw,
      1000 / Cl_mw,
      1000 / NO3_mw,
      1 / Sr_mw,
      1000 / Ca_mw,
      1000 / Mg_mw,
      1
    ),
    stringsAsFactors = FALSE
  )
  provider_prepared <- apply_conversion(provider_long, provider_rules)
  provider_key <- provider_prepared[c("date", "solute", "solute_units")]
  if (anyDuplicated(provider_key) > 0L) {
    stop("the provider contains repeated manacapuru date-variable keys")
  }
  if (!identical(
    range(provider_prepared$date),
    c("2003-04-08", "2025-02-21")
  )) {
    stop("the provider manacapuru date range changed")
  }

  # ---- earlier hybam data ----

  hybam <- read.csv(
    hybam_path,
    check.names = FALSE,
    stringsAsFactors = FALSE,
    na.strings = "",
    colClasses = "character",
    comment.char = ""
  )
  assert_count(nrow(hybam), 117075L, "full HYBAM source rows")

  legacy_rules <- data.frame(
    source_variable = c(
      "Al", "Ca", "Cl", "DOC", "Si", "F", "Fe", "HCO3", "K", "Mg",
      "Mn", "Na", "NO3", "SO4", "Sr", "Zn"
    ),
    solute = c(
      "Al", "Ca", "Cl", "dissolved org C", "DSi", "F", "Fe", "HCO3",
      "K", "Mg", "Mn", "Na", "NO3", "SO4", "Sr", "Zn"
    ),
    solute_units = rep("uM", 16L),
    factor = c(
      1000 / Al_mw,
      1000 / Ca_mw,
      1000 / Cl_mw,
      1000 / C_mw,
      1000 / Si_mw,
      1000 / F_mw,
      1000 / Fe_mw,
      1000 / HCO3_mw,
      1000 / K_mw,
      1000 / Mg_mw,
      1 / Mn_mw,
      1000 / Na_mw,
      1000 / NO3_mw,
      1000 / SO4_mw,
      1000 / Sr_mw,
      1 / Zn_mw
    ),
    stringsAsFactors = FALSE
  )

  legacy <- hybam[
    hybam$nom == "Manacapuru" &
      hybam$solute %in% legacy_rules$source_variable,
    ,
    drop = FALSE
  ]
  assert_count(nrow(legacy), 2631L, "mapped earlier HYBAM rows")
  legacy$date <- parse_mdy_year(legacy$date)

  blank_value <- is_blank(legacy$valeur)
  source_code <- trimws(legacy$code)
  numeric_limit <- grepl(
    "^<\\s*(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)$",
    source_code
  )
  special_limit <- blank_value & source_code == "<50ppb" & legacy$solute == "NO3"
  if (any(blank_value & !(numeric_limit | special_limit))) {
    stop("an earlier HYBAM limit lacks a numeric threshold")
  }
  assert_count(sum(blank_value), 34L, "earlier HYBAM limit rows")

  legacy_value <- legacy$valeur
  legacy_value[blank_value & numeric_limit] <- sub("^<\\s*", "", source_code[
    blank_value & numeric_limit
  ])
  legacy_value[special_limit] <- "0.05"
  legacy_long <- data.frame(
    date = legacy$date,
    source_variable = legacy$solute,
    source_value = legacy_value,
    qualifier = ifelse(blank_value, "<", NA_character_),
    stringsAsFactors = FALSE
  )
  legacy_prepared <- apply_conversion(legacy_long, legacy_rules)
  legacy_key <- legacy_prepared[c("date", "solute", "solute_units")]
  if (anyDuplicated(legacy_key) > 0L) {
    stop("the earlier HYBAM data contain repeated manacapuru keys")
  }

  shared_key <- merge(
    transform(legacy_key, legacy_row = seq_len(nrow(legacy_key))),
    transform(provider_key, provider_row = seq_len(nrow(provider_key))),
    by = c("date", "solute", "solute_units"),
    sort = FALSE
  )
  assert_count(nrow(shared_key), 2508L, "provider and earlier HYBAM overlap")
  assert_count(
    sum(grepl("^<", legacy_prepared$amount[shared_key$legacy_row])),
    1L,
    "limited earlier HYBAM overlap"
  )

  legacy_only <- legacy_prepared[-shared_key$legacy_row, , drop = FALSE]
  assert_count(nrow(legacy_only), 123L, "earlier-only HYBAM rows")
  assert_count(sum(grepl("^<", legacy_only$amount)), 33L, "earlier-only HYBAM limits")
  hybam_combined <- rbind(legacy_only, provider_prepared)
  assert_count(nrow(hybam_combined), 3917L, "provider-precedence HYBAM rows")

  # ---- earlier camrex data ----

  camrex <- read.csv(
    camrex_path,
    check.names = FALSE,
    stringsAsFactors = FALSE,
    na.strings = "",
    colClasses = "character",
    comment.char = ""
  )
  assert_count(nrow(camrex), 215L, "full CAMREX source rows")
  camrex <- camrex[camrex$Site.Stream.Name == "Manacapuru", , drop = FALSE]
  assert_count(nrow(camrex), 12L, "CAMREX manacapuru events")

  camrex_date <- as.Date(camrex$Sampling.Date, format = "%m/%d/%Y") - 1L
  if (
    anyNA(camrex_date) ||
      !identical(range(camrex_date), as.Date(c("1982-05-12", "1991-08-30")))
  ) {
    stop("the corrected CAMREX manacapuru dates changed")
  }

  camrex_rules <- data.frame(
    source_variable = c(
      "DSi", "TN", "NOx", "TP", "PO4", "DOC", "TOC", "alkalinity",
      "pH", "Na", "K", "Ca", "Mg", "Cl"
    ),
    solute = c(
      "DSi", "TN", "NOx", "TP", "SRP", "dissolved org C", "tot org C",
      "alkalinity", "pH", "Na", "K", "Ca", "Mg", "Cl"
    ),
    solute_units = c(
      rep("uM", 8L), NA_character_, rep("uM", 5L)
    ),
    factor = c(
      rep(1, 5L), 1000 / C_mw, 1000 / C_mw, rep(1, 7L)
    ),
    stringsAsFactors = FALSE
  )

  camrex_values <- camrex[camrex_rules$source_variable]
  camrex_long <- data.frame(
    date = rep(camrex_date, times = ncol(camrex_values)),
    source_variable = rep(names(camrex_values), each = nrow(camrex)),
    source_value = unlist(camrex_values, use.names = FALSE),
    qualifier = NA_character_,
    stringsAsFactors = FALSE
  )
  camrex_long <- camrex_long[!is_blank(camrex_long$source_value), , drop = FALSE]
  assert_count(nrow(camrex_long), 134L, "mapped CAMREX manacapuru values")
  camrex_prepared <- apply_conversion(camrex_long, camrex_rules)

  # ---- combined working input ----

  combined <- rbind(camrex_prepared, hybam_combined)
  combined <- combined[
    order(combined$date, combined$solute, combined$solute_units),
    ,
    drop = FALSE
  ]
  rownames(combined) <- NULL
  assert_count(nrow(combined), 4051L, "prepared manacapuru rows")
  assert_count(sum(grepl("^<", combined$amount)), 33L, "prepared manacapuru limits")
  assert_count(sum(combined$solute == "DSi"), 246L, "prepared manacapuru Si rows")
  if (anyDuplicated(combined[c("date", "solute", "solute_units")]) > 0L) {
    stop("the prepared manacapuru input contains repeated daily keys")
  }

  dir.create(output_dir, recursive = TRUE)
  output_path <- file.path(output_dir, "Manacapuru_chemistry_combined.csv")
  write.csv(combined, output_path, row.names = FALSE, na = "")

  written <- read.csv(
    output_path,
    check.names = FALSE,
    stringsAsFactors = FALSE,
    na.strings = "",
    colClasses = "character"
  )
  if (!identical(combined, written)) {
    stop("the prepared manacapuru values changed when read again")
  }

  check_sources()
}

# ---- source overlap and usgs identity checks ----

select_source_files <- function(input_dir, output_dir, usgs_exclusion_file) {
  source_files <- c(
    "20221030_masterdata_chem_V2.csv",
    "CAMREX_filled_template.csv",
    "Chem_HYBAM.csv"
  )

  required_files <- c(file.path(input_dir, source_files), usgs_exclusion_file)
  check_required_files(required_files, "required selection files")
  check_work_folder(output_dir, input_dir, "source selection")
  dir.create(output_dir, recursive = TRUE)

  # ---- source selection helpers ----

  read_source <- function(filename, directory = input_dir) {
    read_source_csv(filename, directory)
  }

  id_signature <- function(data) {
    if (anyNA(data[[1L]]) || anyDuplicated(data[[1L]]) > 0L) {
      stop("the source row ids are missing or repeated")
    }

    signature_file <- tempfile()
    on.exit(unlink(signature_file), add = TRUE)
    writeLines(data[[1L]], signature_file, useBytes = TRUE)
    unname(tools::md5sum(signature_file))
  }

  # this file check confirms that the same source rows stay in the same order
  write_selected <- function(filename, data, expected_rows, expected_signature) {
    assert_count(nrow(data), expected_rows, paste0(filename, " selected rows"))
    observed_signature <- id_signature(data)
    if (!identical(observed_signature, expected_signature)) {
      stop(filename, " selected source rows changed")
    }

    write_prepared_csv(filename, data, output_dir)
  }

  select_site_rows <- function(filename, site_column, site_counts,
                               full_rows, selected_rows, selected_signature) {
    before <- read_source(filename)
    assert_count(nrow(before), full_rows, paste0(filename, " full source rows"))

    # remove only the named overlaps from this source file
    remove_rows <- before[[site_column]] %in% names(site_counts)
    observed_counts <- table(factor(
      before[[site_column]][remove_rows],
      levels = names(site_counts)
    ))
    if (!identical(as.integer(observed_counts), unname(site_counts))) {
      stop(filename, " selected site counts changed")
    }

    after <- before[!remove_rows, , drop = FALSE]
    rownames(after) <- NULL
    write_selected(filename, after, selected_rows, selected_signature)
  }

  # ---- amazon source selections ----

  # rebuild manacapuru from the full original downloads
  # obidos is supplied by its reviewed combined file
  select_site_rows(
    filename = "CAMREX_filled_template.csv",
    site_column = "Site.Stream.Name",
    site_counts = c(Manacapuru = 12L, Obidos = 12L),
    full_rows = 215L,
    selected_rows = 191L,
    selected_signature = "f8fd85fde2daf48c250646d2f0bd2794"
  )

  select_site_rows(
    filename = "Chem_HYBAM.csv",
    site_column = "nom",
    site_counts = c(Manacapuru = 7690L, Obidos = 7674L),
    full_rows = 117075L,
    selected_rows = 101711L,
    selected_signature = "0520fc06cff738577470e0d8cf12a224"
  )

  # ---- legacy source selection ----

  filename <- "20221030_masterdata_chem_V2.csv"
  before <- read_source(filename)
  assert_count(nrow(before), 1033994L, "legacy full source rows")

  # use data-frame column selection even after the amazon reader loads data.table
  exclusions <- as.data.frame(readRDS(usgs_exclusion_file))
  # these rows were confirmed as tds using official usgs records
  required_columns <- c(
    "source_file", "source_site", "date", "source_value"
  )
  missing_columns <- setdiff(required_columns, names(exclusions))
  if (length(missing_columns) > 0L) {
    stop(
      "the USGS exclusion review lacks columns: ",
      paste(missing_columns, collapse = ", ")
    )
  }

  exclusions <- exclusions[
    exclusions$source_file == filename,
    required_columns,
    drop = FALSE
  ]
  exclusion_keys <- unique(data.frame(
    source_site = exclusions$source_site,
    date = as.character(exclusions$date),
    source_value = as.numeric(exclusions$source_value),
    stringsAsFactors = FALSE
  ))
  assert_count(nrow(exclusion_keys), 8625L, "legacy TDS-as-TN evidence rows")

  tn_scope <- before$LTER == "USGS" &
    before$variable == "TN" &
    before$units == "uM"
  assert_count(sum(tn_scope), 22122L, "legacy USGS total-nitrogen rows")
  candidates <- data.frame(
    source_row = which(tn_scope),
    source_site = before$Site.Stream.Name[tn_scope],
    date = before$Sampling.Date[tn_scope],
    source_value = suppressWarnings(as.numeric(before$value[tn_scope])),
    stringsAsFactors = FALSE
  )
  reviewed_tds <- merge(
    candidates,
    exclusion_keys,
    by = c("source_site", "date", "source_value"),
    sort = FALSE
  )
  if (anyDuplicated(reviewed_tds$source_row) > 0L) {
    stop("the legacy TDS-as-TN review matched a source row more than once")
  }
  assert_count(nrow(reviewed_tds), 8625L, "legacy TDS-as-TN rows")

  remove_tds <- seq_len(nrow(before)) %in% reviewed_tds$source_row
  # retain complete site records so unique variables remain available
  # duplicate source series are resolved later by the master workflow
  after <- before[!remove_tds, , drop = FALSE]
  rownames(after) <- NULL
  write_selected(
    filename,
    after,
    1025369L,
    "498d756e98403a078b7a9fcdfad68975"
  )
}

# ---- collect prepared inputs ----

assemble_inputs <- function(
  raw_dir, selection_dir, qualifier_dir, direct_dir,
  provider_dir, amazon_dir, key_file, output_dir
) {
  required_paths <- c(
    raw_dir,
    selection_dir,
    qualifier_dir,
    direct_dir,
    provider_dir,
    amazon_dir,
    key_file
  )
  check_required_files(required_paths, "run-input paths")

  input_dirs <- c(
    raw_dir,
    selection_dir,
    qualifier_dir,
    direct_dir,
    provider_dir,
    amazon_dir
  )
  check_work_folder(output_dir, input_dirs, "assembled chemistry")
  dir.create(output_dir, recursive = TRUE)

  # ---- selected files ----

  data_key <- read.csv(
    key_file,
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  expected_files <- sort(unique(data_key$Raw_Filename[
    data_key$Data_type == "chemistry" &
      !is.na(data_key$Raw_Filename) &
      data_key$Raw_Filename != ""
  ]))

  provider_files <- c(
    "Canada_ECCC_AllSites_chemistry.csv",
    "GEMS_AllSites_chemistry.csv",
    "Danube_AllSites_chemistry.csv"
  )

  for (filename in expected_files) {
    candidates <- c(
      file.path(raw_dir, filename),
      file.path(selection_dir, filename),
      file.path(qualifier_dir, filename),
      file.path(direct_dir, filename),
      file.path(amazon_dir, filename)
    )
    if (filename %in% provider_files) {
      candidates <- c(candidates, file.path(provider_dir, filename))
    }
    available <- candidates[file.exists(candidates)]
    if (length(available) == 0L) {
      stop("no run input is available for ", filename)
    }

    # use the latest prepared version and leave the downloaded source unchanged
    selected <- available[[length(available)]]
    copied <- file.copy(
      selected,
      file.path(output_dir, filename),
      overwrite = TRUE,
      copy.mode = FALSE,
      copy.date = TRUE
    )
    if (!copied) {
      stop("could not assemble ", filename)
    }
  }

  # ---- final checks ----

  assembled_files <- sort(list.files(output_dir, pattern = "[.]csv$"))
  if (!identical(assembled_files, expected_files)) {
    stop("the assembled chemistry file set does not match the data key")
  }

  for (filename in provider_files) {
    provider_md5 <- unname(tools::md5sum(file.path(provider_dir, filename)))
    assembled_md5 <- unname(tools::md5sum(file.path(output_dir, filename)))
    if (!identical(provider_md5, assembled_md5)) {
      stop("the exact provider output was not retained for ", filename)
    }
  }

  amazon_file <- "Manacapuru_chemistry_combined.csv"
  amazon_md5 <- unname(tools::md5sum(file.path(amazon_dir, amazon_file)))
  assembled_amazon_md5 <- unname(tools::md5sum(file.path(output_dir, amazon_file)))
  if (!identical(amazon_md5, assembled_amazon_md5)) {
    stop("the generated manacapuru input was not retained")
  }
}

# ---- gems silicon gap filling ----

# remove overlaps before the master reads the combined inputs
select_gems_si_gaps <- function(reactive, station_map, direct_keys, exclusions) {
  daily_key <- c("LTER", "Stream_Name", "date")
  if (anyDuplicated(station_map$gems_station)) {
    stop("a GEMS silicon station matches more than one existing site")
  }
  reactive |>
    dplyr::inner_join(station_map, by = "gems_station") |>
    dplyr::anti_join(exclusions, by = c("gems_station", "date")) |>
    dplyr::anti_join(direct_keys, by = daily_key)
}

prepare_gems_si_gaps <- function(input_dir, provider_dir, key_dir, return_sites = FALSE) {
  archive <- file.path(provider_dir, "gems_gemstat", "GFQA_v3.zip")
  stations <- read.csv(unz(archive, "GEMStat_station_metadata.csv"),
    colClasses = "character", check.names = FALSE, fileEncoding = "latin1")
  reference <- read.csv(file.path(key_dir, "Site_Reference_Table.csv"),
    colClasses = "character", check.names = FALSE)
  canada_sites <- read.csv(file.path(provider_dir, "canada_eccc",
    "Water-Qual-Eau-Sites-National.csv"), colClasses = "character")

  # match canada by the original station number, not the nearest coordinates
  canada_map <- stations |>
    dplyr::transmute(gems_station = `GEMS Station Number`,
      SITE_NO = toupper(`Local Station Number`)) |>
    dplyr::inner_join(canada_sites, by = "SITE_NO") |>
    dplyr::transmute(gems_station, LTER = "Canada", Stream_Name = trimws(SITE_NAME)) |>
    dplyr::semi_join(dplyr::filter(reference,
      LTER == "Canada", Include_in_Master_Chem == "Yes"),
      by = c("LTER", "Stream_Name"))
  reactive <- read.csv(unz(archive, "Silicon.csv"), colClasses = "character",
    check.names = FALSE)
  reactive <- reactive |>
    dplyr::filter(`Parameter Code` == "SiO2-Rea",
      `Analysis Method Code` %in% gems_filtered_reactive_methods, Unit == "mg/l") |>
    dplyr::transmute(gems_station = `GEMS Station Number`, date = `Sample Date`,
      source_value = as.numeric(Value), qualifier = trimws(`Value Flags`))
  canada_map <- dplyr::semi_join(canada_map, reactive, by = "gems_station")
  check_reviewed_count(nrow(canada_map), 49L, "reviewed Canada/GEMS silicon matches")

  # the us station numbers were checked against the gems station descriptions
  usgs_map <- gems_usgs_si_sites |>
    dplyr::inner_join(reference, by = c("usgs_station" = "USGSGageNumber")) |>
    dplyr::filter(LTER == "USGS", Include_in_Master_Chem == "Yes") |>
    dplyr::select(gems_station, LTER, Stream_Name)
  check_reviewed_count(nrow(usgs_map), 5L, "reviewed USGS/GEMS silicon matches")
  station_map <- dplyr::bind_rows(canada_map, usgs_map)
  if (isTRUE(return_sites)) return(station_map)

  # inspect prepared direct inputs, not a previous master output
  canada <- as.data.frame(data.table::fread(
    file.path(input_dir, "Canada_ECCC_AllSites_chemistry.csv"),
    select = c("LTER", "Stream_Name", "date", "solute", "amount"),
    colClasses = "character", showProgress = FALSE))
  legacy <- as.data.frame(data.table::fread(
    file.path(input_dir, "20221030_masterdata_chem_V2.csv"),
    select = c("LTER", "site", "Sampling.Date", "variable", "value"),
    colClasses = "character", showProgress = FALSE))
  combined <- as.data.frame(data.table::fread(
    file.path(input_dir, "Deduplicated_Sites_AllSites_chemistry.csv"),
    select = c("LTER", "Stream_Name", "date", "solute", "amount"),
    colClasses = "character", showProgress = FALSE))
  direct <- dplyr::bind_rows(
    dplyr::transmute(canada, LTER, Stream_Name, date, solute, amount),
    dplyr::transmute(legacy, LTER, Stream_Name = site, date = Sampling.Date,
      solute = variable, amount = value),
    dplyr::transmute(combined, LTER, Stream_Name, date, solute, amount)
  ) |>
    dplyr::filter(tolower(solute) %in% c("dsi", "sio2_mgl")) |>
    dplyr::mutate(numeric_value = suppressWarnings(as.numeric(
      sub("^[<>]\\s*", "", trimws(amount))))) |>
    dplyr::filter(is.finite(numeric_value)) |>
    dplyr::distinct(LTER, Stream_Name, date)

  matched <- dplyr::semi_join(reactive, station_map, by = "gems_station")
  if (any(!is.finite(matched$source_value)) || anyNA(as.Date(matched$date)) ||
      any(!matched$qualifier %in% c("", "<"))) {
    stop("a reviewed GEMS reactive silicon result has an unsupported value, date, or qualifier")
  }
  gaps <- select_gems_si_gaps(matched, station_map, direct, gems_si_exclusions)
  days <- dplyr::distinct(gaps, LTER, Stream_Name, date)
  observed <- vapply(names(gems_si_expected_days), function(network) {
    sum(days$LTER == network)
  }, integer(1))
  check_reviewed_counts(observed, gems_si_expected_days,
    paste(names(gems_si_expected_days), "GEMS silicon gap days"))
  check_reviewed_count(sum(gaps$qualifier == "<"), 1L,
    "GEMS silicon gap records below the reported limit")

  # keep source rows for the usual daily averaging and qualifier rules
  additions <- gaps |>
    dplyr::transmute(LTER, Stream_Name, date, solute = "DSi", solute_units = "uM",
      amount = paste0(qualifier, format(source_value * 1000 / 60.0835,
        digits = 15, scientific = FALSE, trim = TRUE)),
      source_lab_flag = NA_character_, source_method_flag = gems_reactive_si_flag)
  additions
}

# use the same unit and variable rules as the master when checking direct coverage
read_gems_direct_keys <- function(input_dir, key_dir, station_map) {
  data_key <- read.csv(file.path(key_dir, "SiSyn_Data_Key.csv"), na.strings = "") |>
    dplyr::filter(Data_type == "chemistry") |>
    dplyr::mutate(Raw_Column_Name = gsub(" |\\+|\\(|\\)|\\/|\\-", ".", Raw_Column_Name))
  units_key <- read.csv(file.path(key_dir, "SiSyn_Chem_Units_Key.csv"), na.strings = "") |>
    dplyr::filter(Data_type == "chemistry") |>
    dplyr::mutate(Variable = tolower(Variable))
  reference <- read.csv(file.path(key_dir, "Site_Reference_Table.csv"), na.strings = "")
  files <- unique(c(data_key$Raw_Filename[data_key$Dataset %in% c("Canada", "USGS")],
    "20221030_masterdata_chem_V2.csv", deduplicated_filename))
  files <- files[files %in% list.files(input_dir)]
  headers <- read_source_headers(input_dir, files)
  names <- dplyr::bind_rows(
    dplyr::transmute(reference, LTER, Stream_Name, source_name = Stream_Name),
    dplyr::transmute(reference, LTER, Stream_Name, source_name = Alt_Stream_Name) |>
      tidyr::separate_longer_delim(source_name, delim = ";"),
    dplyr::transmute(reference, LTER, Stream_Name, source_name = Original_Stream_Name)
  ) |>
    dplyr::filter(!is.na(source_name), trimws(source_name) != "") |>
    dplyr::mutate(name_key = stringr::str_to_lower(stringr::str_squish(source_name))) |>
    dplyr::distinct(name_key, LTER, Stream_Name)
  if (anyDuplicated(names$name_key)) stop("ambiguous direct-source site names")
  keys <- vector("list", length(files))
  for (row in seq_along(files)) {
    direct <- harmonize_source(files[row], data_key, units_key, input_dir, headers) |>
      standardize_source_data(data_key) |>
      dplyr::filter(is.finite(value)) |>
      dplyr::transmute(name_key = stringr::str_to_lower(stringr::str_squish(Stream_Name)),
        date, variable, units) |>
      dplyr::inner_join(names, by = "name_key") |>
      dplyr::semi_join(station_map, by = c("LTER", "Stream_Name")) |>
      dplyr::mutate(date = as.character(as.Date(date))) |>
      dplyr::distinct(LTER, Stream_Name, date, variable, units)
    if (anyNA(direct$date)) stop("a direct-source gap check has an unreadable date")
    keys[[row]] <- direct
  }
  dplyr::distinct(dplyr::bind_rows(keys))
}

# average each gems day before changing the table shape so the lab notes survive
average_gems_gap_days <- function(data) {
  data <- dplyr::distinct(data)
  parsed <- parse_qualified_numbers(data$amount)
  data$sample_value <- parsed$numbers
  data$sample_qualifier <- ifelse(parsed$is_below, "<", ifelse(parsed$is_above, ">", ""))
  days <- data |>
    dplyr::group_by(LTER, Stream_Name, date, solute, solute_units) |>
    dplyr::summarise(
      detected = sum(sample_qualifier == ""),
      below = sum(sample_qualifier == "<"),
      above = sum(sample_qualifier == ">"),
      value = if (detected > 0L) mean(sample_value[sample_qualifier == ""]) else mean(sample_value),
      qualifier = if (detected > 0L) NA_character_ else dplyr::first(sample_qualifier),
      estimated = any(source_lab_flag == "estimated value", na.rm = TRUE),
      source_lab_flag = reduce_daily_flag(
        source_lab_flag[source_lab_flag != "estimated value"], "GEMS lab"),
      source_method_flag = reduce_daily_flag(source_method_flag, "GEMS method"),
      .groups = "drop"
    )
  calculated_flag <- daily_lab_flag(days$detected, days$below, days$above)
  conflict <- !is.na(calculated_flag) & !is.na(days$source_lab_flag) &
    calculated_flag != days$source_lab_flag
  if (any(conflict)) stop("GEMS source and daily detection-limit flags conflict")
  days$source_lab_flag <- add_estimated_lab_flag(
    dplyr::coalesce(calculated_flag, days$source_lab_flag), days$estimated)
  days |>
    dplyr::mutate(amount = format_amount(value, qualifier)) |>
    dplyr::select(LTER, Stream_Name, date, solute, solute_units, amount,
      source_lab_flag, source_method_flag)
}

# fill only missing variables at the stations already reviewed for gems overlap
prepare_gems_chemistry_gaps <- function(input_dir, provider_dir, key_dir) {
  silicon <- prepare_gems_si_gaps(input_dir, provider_dir, key_dir)
  sites <- prepare_gems_si_gaps(input_dir, provider_dir, key_dir, return_sites = TRUE)
  direct <- read_gems_direct_keys(input_dir, key_dir, sites)
  archive <- file.path(provider_dir, "gems_gemstat", "GFQA_v3.zip")
  parameters <- gems_gap_parameters()
  additions <- vector("list", nrow(parameters))
  for (row in seq_len(nrow(parameters))) {
    rule <- parameters[row, ]
    source <- data.table::fread(cmd = sprintf("unzip -p %s %s",
      shQuote(archive), shQuote(rule$source_entry)), encoding = "Latin-1",
      colClasses = "character", na.strings = "", showProgress = FALSE)
    # the master distinguishes oxygen concentration and saturation by their units
    source <- tibble::as_tibble(source) |>
      dplyr::filter(`GEMS Station Number` %in% sites$gems_station,
        `Parameter Code` == rule$parameter_code, Unit == rule$provider_units) |>
      dplyr::transmute(gems_station = `GEMS Station Number`, date = `Sample Date`,
        variable = if (rule$variable == "do percent") "dissolved oxygen" else rule$variable,
        units = rule$units, source_value = suppressWarnings(as.numeric(Value)),
        qualifier = dplyr::coalesce(trimws(`Value Flags`), ""),
        depth = suppressWarnings(as.numeric(Depth))) |>
      dplyr::filter(is.finite(source_value), source_value != -9999,
        !is.na(date), date != "") |>
      dplyr::inner_join(sites, by = "gems_station") |>
      dplyr::anti_join(direct, by = c("LTER", "Stream_Name", "date", "variable", "units"))
    if (!nrow(source)) next
    if (anyNA(as.Date(source$date))) {
      stop("a GEMS gap result has an unsupported date: ", rule$parameter_code)
    }
    source <- source |>
      dplyr::group_by(LTER, Stream_Name, date, variable, units) |>
      dplyr::mutate(depth_order = dplyr::coalesce(depth, Inf),
        multiple_depths = dplyr::n_distinct(depth[!is.na(depth)]) > 1L) |>
      dplyr::filter(depth_order == min(depth_order)) |>
      dplyr::ungroup()
    # keep estimates and uncertain physical-variable flags separate from lab limits
    flags <- interpret_gems_qualifiers(source$qualifier, rule$limit_is_clear)
    source$qualifier <- flags$qualifier
    source$lab_flag <- flags$lab_flag
    additions[[row]] <- source |>
      dplyr::transmute(LTER, Stream_Name, date, solute = rule$variable,
        solute_units = rule$units,
        amount = paste0(qualifier, format(source_value * rule$conversion,
          digits = 15, scientific = FALSE, trim = TRUE)),
        source_lab_flag = lab_flag,
        source_method_flag = dplyr::if_else(multiple_depths,
          "shallowest sample depth used", NA_character_))
  }
  other <- average_gems_gap_days(dplyr::bind_rows(additions))
  keys <- other |>
    dplyr::transmute(LTER, Stream_Name, date,
      variable = dplyr::if_else(solute == "do percent", "dissolved oxygen", solute),
      units = solute_units) |>
    dplyr::distinct()
  if (nrow(dplyr::inner_join(keys, direct,
      by = c("LTER", "Stream_Name", "date", "variable", "units")))) {
    stop("a GEMS addition overlaps a direct chemistry result")
  }
  dplyr::bind_rows(silicon, other)
}

# ---- run preparation ----

prepare_chemistry(commandArgs(trailingOnly = TRUE))
