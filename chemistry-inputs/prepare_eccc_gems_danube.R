# prepare the combined eccc, gems, and danube files from the original downloads

# ---- packages ----

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
})

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

# ---- inputs ----

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 4L) {
  stop(
    paste(
      "supply the source snapshot, provider folder, data key,",
      "and output folder"
    )
  )
}
raw_dir <- normalizePath(args[[1]], mustWork = TRUE)
provider_dir <- normalizePath(args[[2]], mustWork = TRUE)
data_key_path <- normalizePath(args[[3]], mustWork = TRUE)
site_reference_path <- file.path(
  dirname(data_key_path),
  "Site_Reference_Table.csv"
)
if (!file.exists(site_reference_path)) {
  stop("Site_Reference_Table.csv must be beside the data key")
}
output_dir <- args[[4]]

input_dirs <- c(
  raw_dir,
  provider_dir,
  dirname(data_key_path)
)
check_work_folder(output_dir, input_dirs, "provider preparation")

raw_output_dir <- file.path(output_dir, "chem_raw_subset")
dir.create(raw_output_dir, recursive = TRUE)

canada_file <- "Canada_ECCC_AllSites_chemistry.csv"
gems_file <- "GEMS_AllSites_chemistry.csv"
danube_file <- "Danube_AllSites_chemistry.csv"

gems_pattern <- "^GEMS__GEMS__.*[.]csv$"
danube_pattern <- "^Danube__Danube__.*[.]csv$"

raw_files <- list.files(raw_dir)
gems_files <- grep(gems_pattern, raw_files, value = TRUE)
danube_files <- grep(danube_pattern, raw_files, value = TRUE)

if (
  canada_file %in% raw_files ||
    length(gems_files) != 160L ||
    length(danube_files) != 0L
) {
  stop(
    paste(
      "the source snapshot must omit generated Canada and Danube files",
      "and contain exactly 160 GEMS site files"
    )
  )
}

# ---- shared values ----

Al_mw <- 26.9815385
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
P_mw <- 30.973761998
S_mw <- 32.065
Si_mw <- 28.0855
Zn_mw <- 65.38

NH4_mw <- N_mw + 4 * H_mw
NO3_mw <- N_mw + 3 * O_mw
PO4_mw <- P_mw + 4 * O_mw
SO4_mw <- S_mw + 4 * O_mw
SiO2_mw <- Si_mw + 2 * O_mw

read_long_files <- function(filenames) {
  # read one file at a time, keeping the supplied file order
  source_tables <- vector("list", length(filenames))
  for (file_number in seq_along(filenames)) {
    path <- file.path(raw_dir, filenames[file_number])
    source_tables[[file_number]] <- data.table::fread(
      path,
      colClasses = "character",
      na.strings = "",
      showProgress = FALSE
    )
  }

  # match columns by name and leave missing fields blank
  combined <- data.table::rbindlist(source_tables, use.names = TRUE, fill = TRUE)
  combined <- tibble::as_tibble(combined)
  combined$date <- as.Date(combined$date)
  combined
}

# combine method notes without dropping either source detail
combine_method_flags <- function(primary, additional) {
  primary <- dplyr::na_if(trimws(as.character(primary)), "")
  additional <- dplyr::na_if(trimws(as.character(additional)), "")
  dplyr::case_when(
    is.na(primary) ~ additional,
    is.na(additional) ~ primary,
    TRUE ~ paste(primary, additional, sep = "; ")
  )
}

# test one flag inside a semicolon-separated method field
has_method_flag <- function(values, flag) {
  flag_parts <- strsplit(dplyr::coalesce(as.character(values), ""), "; ", fixed = TRUE)
  found <- logical(length(flag_parts))
  names(found) <- names(flag_parts)
  for (row in seq_along(flag_parts)) {
    found[row] <- flag %in% flag_parts[[row]]
  }
  found
}

# ---- canada ----

canada_provider_dir <- file.path(provider_dir, "canada_eccc")
canada_sites_path <- file.path(
  canada_provider_dir,
  "Water-Qual-Eau-Sites-National.csv"
)
canada_data_paths <- list.files(
  canada_provider_dir,
  pattern = "2000-present[.]csv$",
  full.names = TRUE
)

if (!file.exists(canada_sites_path) || length(canada_data_paths) != 17L) {
  stop("the reviewed Canada provider files are unavailable")
}

canada_separate_sites <- c(
  "ALSEK RIVER ABOVE BATES RIVER IN KLUANE NATIONAL PARK",
  "BEAVER RIVER ABOVE HIGHWAY 1 IN GLACIER NATIONAL PARK",
  "KICKING HORSE RIVER AT FIELD IN YOHO NATIONAL PARK",
  "KOOTENAY RIVER ABOVE HIGHWAY 93 IN KOOTENAY NATIONAL PARK",
  "LIARD RIVER AT UPPER CROSSING",
  "SKEENA RIVER AT USK"
)

canada_site_names <- data.table::fread(
  site_reference_path,
  select = c("LTER", "Stream_Name"),
  colClasses = "character",
  showProgress = FALSE
) %>%
  tibble::as_tibble() %>%
  dplyr::filter(LTER == "Canada") %>%
  dplyr::transmute(Stream_Name = trimws(Stream_Name)) %>%
  dplyr::filter(!Stream_Name %in% canada_separate_sites) %>%
  dplyr::distinct()

if (
  nrow(canada_site_names) != 123L ||
    any(is.na(canada_site_names$Stream_Name)) ||
    any(canada_site_names$Stream_Name == "")
) {
  stop("the reviewed Canada site names changed")
}

canada_site_map <- data.table::fread(
  canada_sites_path,
  select = c("SITE_NO", "SITE_NAME"),
  colClasses = "character",
  showProgress = FALSE
) %>%
  tibble::as_tibble() %>%
  dplyr::mutate(Stream_Name = trimws(SITE_NAME)) %>%
  dplyr::semi_join(canada_site_names, by = "Stream_Name") %>%
  dplyr::distinct(SITE_NO, Stream_Name)

if (
  nrow(canada_site_map) != 123L ||
    dplyr::n_distinct(canada_site_map$SITE_NO) != 123L ||
    dplyr::n_distinct(canada_site_map$Stream_Name) != 123L
) {
  stop("the reviewed Canada site map changed")
}

canada_source_columns <- c(
  "SITE_NO",
  "DATE_TIME_HEURE",
  "FLAG_MARQUEUR",
  "VALUE_VALEUR",
  "SDL_LDE",
  "MDL_LDM",
  "VMV_CODE",
  "UNIT_UNITÉ",
  "VARIABLE",
  "STATUS_STATUT",
  "SAMPLE_ID_ÉCHANTILLON"
)

canada_provider <- data.table::rbindlist(
  lapply(canada_data_paths, function(source_path) {
    source_rows <- data.table::fread(
      source_path,
      select = canada_source_columns,
      colClasses = "character",
      showProgress = FALSE
    )
    source_rows[, source_file := basename(source_path)]
    source_rows[SITE_NO %in% canada_site_map$SITE_NO]
  }),
  use.names = TRUE,
  fill = TRUE
) %>%
  tibble::as_tibble() %>%
  dplyr::left_join(canada_site_map, by = "SITE_NO") %>%
  dplyr::transmute(
    site_no = SITE_NO,
    Stream_Name,
    source_datetime = DATE_TIME_HEURE,
    date = as.Date(substr(DATE_TIME_HEURE, 1, 10)),
    source_variable = trimws(VARIABLE),
    source_units = trimws(UNIT_UNITÉ),
    source_value = suppressWarnings(as.numeric(VALUE_VALEUR)),
    value_qualifier = trimws(FLAG_MARQUEUR),
    vmv_code = trimws(VMV_CODE),
    status = trimws(STATUS_STATUT),
    sample_id = SAMPLE_ID_ÉCHANTILLON,
    source_file
  )

canada_cold_review_source <- tibble::tribble(
  ~site_no, ~Stream_Name, ~source_datetime, ~sample_id,
  ~source_file, ~expected_value,
  "AL05BA0011", "BOW RIVER AT HIGHWAY 1 ABOVE LAKE LOUISE",
  "2015-01-14 14:00", "2014PN140164",
  "Water-Qual-Eau-S-Saskatchewan-2000-present.csv", -1.5,
  "MA05OB0001", "PEMBINA RIVER AT WINDYGATES, MANITOBA",
  "2015-12-15 11:00", "2015PN700147",
  "Water-Qual-Eau-Assiniboine-Red-2000-present.csv", -2.5,
  "MA05OB0001", "PEMBINA RIVER AT WINDYGATES, MANITOBA",
  "2016-01-26 13:30", "2015PN700161",
  "Water-Qual-Eau-Assiniboine-Red-2000-present.csv", -2.87,
  "MA05PF0022", "WINNIPEG RIVER AT POINTE DU BOIS,",
  "2015-12-17 11:00", "2015PN700145",
  "Water-Qual-Eau-Winnipeg-2000-present.csv", -1.3
)

canada_cold_source_review <- canada_provider %>%
  dplyr::filter(
    source_variable == "TEMPERATURE WATER (FIELD)",
    source_units == "DEG C",
    vmv_code == "109028"
  ) %>%
  dplyr::inner_join(
    canada_cold_review_source,
    by = c(
      "site_no",
      "Stream_Name",
      "source_datetime",
      "sample_id",
      "source_file"
    )
  )

if (
  nrow(canada_cold_source_review) != 4L ||
    any(canada_cold_source_review$status != "P") ||
    any(
      !is.na(canada_cold_source_review$value_qualifier) &
        canada_cold_source_review$value_qualifier != ""
    ) ||
    any(!dplyr::near(
      canada_cold_source_review$source_value,
      canada_cold_source_review$expected_value
    ))
) {
  stop("the reviewed Canada gross cold-temperature source rows changed")
}

canada_coppermine_source_review <- canada_provider %>%
  dplyr::filter(
    site_no == "NW10PC0019",
    Stream_Name == "COPPERMINE RIVER ABOVE COPPER CREEK",
    source_datetime == "2018-04-09 12:05",
    source_variable == "TEMPERATURE WATER (FIELD)",
    source_units == "DEG C",
    vmv_code == "109028",
    sample_id == "2018PN010001",
    source_file == "Water-Qual-Eau-Arctic-2000-present.csv"
  )

if (
  nrow(canada_coppermine_source_review) != 1L ||
    !dplyr::near(canada_coppermine_source_review$source_value, -10) ||
    canada_coppermine_source_review$status != "P" ||
    (
      !is.na(canada_coppermine_source_review$value_qualifier) &&
        canada_coppermine_source_review$value_qualifier != ""
    )
) {
  stop("the reviewed Canada Coppermine source row changed")
}

# use the agreed elemental si basis for filtered soluble reactive silicon
canada_map <- tibble::tribble(
  ~source_variable, ~source_units, ~variable, ~units, ~conversion,
  "ALKALINITY GRAN CACO3", "MG/L", "alkalinity", "ueq/L", 20,
  "ALKALINITY TOTAL CACO3", "MG/L", "alkalinity", "ueq/L", 20,
  "ALUMINUM DISSOLVED", "MG/L", "Al", "uM", 1000 / Al_mw,
  "ALUMINUM DISSOLVED", "µG/L", "Al", "uM", 1 / Al_mw,
  "CALCIUM", "MG/L", "Ca", "uM", 1000 / Ca_mw,
  "CALCIUM DISSOLVED", "MG/L", "Ca", "uM", 1000 / Ca_mw,
  "CHLOROPHYLL A", "MG/L", "chlorophyll a", "ug/L", 1000,
  "CHLOROPHYLL A", "µG/L", "chlorophyll a", "ug/L", 1,
  "CHLORIDE", "MG/L", "Cl", "uM", 1000 / Cl_mw,
  "CHLORIDE DISSOLVED", "MG/L", "Cl", "uM", 1000 / Cl_mw,
  "CARBON DISSOLVED ORGANIC", "MG/L", "dissolved org C", "uM", 1000 / C_mw,
  "FLUORIDE", "MG/L", "F", "uM", 1000 / F_mw,
  "FLUORIDE DISSOLVED", "MG/L", "F", "uM", 1000 / F_mw,
  "IRON DISSOLVED", "MG/L", "Fe", "uM", 1000 / Fe_mw,
  "IRON DISSOLVED", "µG/L", "Fe", "uM", 1 / Fe_mw,
  "CONDUCTANCE (FIELD)", "µS/CM", "conductivity", "uS/cm", 1,
  "SPECIFIC CONDUCTANCE (FIELD)", "µS/CM", "specific conductivity", "uS/cm", 1,
  "PH (FIELD)", "PH", "pH", NA_character_, 1,
  "POTASSIUM", "MG/L", "K", "uM", 1000 / K_mw,
  "POTASSIUM DISSOLVED/FILTERED", "MG/L", "K", "uM", 1000 / K_mw,
  "POTASSIUM FILTERED", "MG/L", "K", "uM", 1000 / K_mw,
  "MAGNESIUM", "MG/L", "Mg", "uM", 1000 / Mg_mw,
  "MAGNESIUM DISSOLVED", "MG/L", "Mg", "uM", 1000 / Mg_mw,
  "MANGANESE DISSOLVED", "MG/L", "Mn", "uM", 1000 / Mn_mw,
  "MANGANESE DISSOLVED", "µG/L", "Mn", "uM", 1 / Mn_mw,
  "SODIUM", "MG/L", "Na", "uM", 1000 / Na_mw,
  "SODIUM DISSOLVED/FILTERED", "MG/L", "Na", "uM", 1000 / Na_mw,
  "SODIUM FILTERED", "MG/L", "Na", "uM", 1000 / Na_mw,
  "AMMONIA DISSOLVED", "MG/L", "NHX", "uM", 1000 / N_mw,
  "DISSOLVED NITROGEN NITRATE", "MG/L", "NO3", "uM", 1000 / N_mw,
  "DISSOLVED NITROGEN NITRATE", "MG/L N", "NO3", "uM", 1000 / N_mw,
  "NITRATE AND NITRITE (AS N)", "MG/L", "NOx", "uM", 1000 / N_mw,
  "ORTHOPHOSPHORUS DISSOLVED", "MG/L", "PO4", "uM", 1000 / P_mw,
  "PHOSPHORUS SOLUBLE REACTIVE", "MG/L", "SRP", "uM", 1000 / P_mw,
  "SILICA REACTIVE", "MG/L", "DSi", "uM", 1000 / SiO2_mw,
  "SILICA SOLUBLE REACTIVE", "MG/L", "DSi", "uM", 1000 / Si_mw,
  "SULPHATE", "MG/L", "SO4", "uM", 1000 / SO4_mw,
  "SULPHATE DISSOLVED", "MG/L", "SO4", "uM", 1000 / SO4_mw,
  "NITROGEN TOTAL", "MG/L", "TN", "uM", 1000 / N_mw,
  "NITROGEN TOTAL DISSOLVED", "MG/L", "tot dissolved N", "uM", 1000 / N_mw,
  "TEMPERATURE (WATER)", "DEG C", "temp", "C", 1,
  "TEMPERATURE WATER (FIELD)", "DEG C", "temp", "C", 1,
  "PHOSPHORUS TOTAL", "MG/L", "TP", "uM", 1000 / P_mw,
  "PHOSPHORUS TOTAL DISSOLVED", "MG/L", "TDP", "uM", 1000 / P_mw
)

# pass canada si onward in um so the units key does not convert it again
canada_raw_map <- tibble::tribble(
  ~variable, ~units, ~solute, ~solute_units, ~raw_per_final,
  "alkalinity", "ueq/L", "alkalinity", "ueq/L", 1,
  "Al", "uM", "al_ugl", "ug/L", Al_mw,
  "Ca", "uM", "ca_mgl", "mg/L", Ca_mw / 1000,
  "chlorophyll a", "ug/L", "chla_ugl", "ug/L", 1,
  "Cl", "uM", "cl_mgl", "mg/L", Cl_mw / 1000,
  "dissolved org C", "uM", "doc_mgl", "mg C/L", C_mw / 1000,
  "F", "uM", "f_mgl", "mg/L", F_mw / 1000,
  "Fe", "uM", "fe_ugl", "ug/L", Fe_mw,
  "conductivity", "uS/cm", "conductivity", "uS/cm", 1,
  "specific conductivity", "uS/cm", "specific conductivity", "uS/cm", 1,
  "pH", NA_character_, "fldph", NA_character_, 1,
  "K", "uM", "k_mgl", "mg/L", K_mw / 1000,
  "Mg", "uM", "mg_mgl", "mg/L", Mg_mw / 1000,
  "Mn", "uM", "mn_ugl", "ug/L", Mn_mw,
  "Na", "uM", "na_mgl", "mg/L", Na_mw / 1000,
  "NHX", "uM", "NHX", "mg NH4-N/L", N_mw / 1000,
  "NO3", "uM", "NO3", "mg NO3-N/L", N_mw / 1000,
  "NOx", "uM", "NOx", "mg NO3-N/L", N_mw / 1000,
  "PO4", "uM", "PO4", "mg P/L", P_mw / 1000,
  "SRP", "uM", "SRP", "mg P/L", P_mw / 1000,
  "DSi", "uM", "DSi", "uM", 1,
  "SO4", "uM", "so4_mgl", "mg SO4/L", SO4_mw / 1000,
  "TN", "uM", "TN", "mg N/L", N_mw / 1000,
  "tot dissolved N", "uM", "tdn_mgl", "mg N/L", N_mw / 1000,
  "temp", "C", "temp_c", "C", 1,
  "TP", "uM", "tp_ugl", "ug P/L", P_mw,
  "TDP", "uM", "TDP", "mg P/L", P_mw / 1000
)

if (anyDuplicated(canada_map[c("source_variable", "source_units")])) {
  stop("the Canada provider map contains a duplicate source mapping")
}

canada_source <- canada_provider %>%
  dplyr::inner_join(
    canada_map,
    by = c("source_variable", "source_units")
  ) %>%
  dplyr::filter(is.finite(source_value), !is.na(date)) %>%
  dplyr::mutate(
    censored = !is.na(value_qualifier) & value_qualifier == "<",
    detected = is.na(value_qualifier) | value_qualifier == "",
    value = source_value * conversion,
    source_method_flag = dplyr::case_when(
      source_variable == "PH (FIELD)" ~ "field pH",
      source_variable %in% c(
        "CONDUCTANCE (FIELD)",
        "SPECIFIC CONDUCTANCE (FIELD)"
      ) ~ "field conductivity",
      source_variable == "SILICA SOLUBLE REACTIVE" &
        vmv_code == "109113" ~
          "soluble reactive Si, filtered ICP-OES",
      source_variable == "SILICA REACTIVE" &
        vmv_code %in% c("106020", "61334") ~
          "soluble reactive SiO2, ECCC",
      TRUE ~ NA_character_
    )
  )

if (
  nrow(canada_source) != 258055L ||
    sum(canada_source$censored) != 14077L ||
    any(!canada_source$censored & !canada_source$detected)
) {
  stop("the reviewed Canada provider rows changed")
}

# confirm four reported values kept for later outlier review
canada_extreme_plan <- tibble::tribble(
  ~site_no, ~source_datetime, ~source_variable, ~source_units,
  ~vmv_code, ~sample_id, ~source_file, ~expected_value,
  "NW10UF0003", "2016-08-17 16:46", "PH (FIELD)", "PH",
  "109026", "2016PN010077",
  "Water-Qual-Eau-Arctic-2000-present.csv", 63.50,
  "NW10VK0002", "2015-07-15 19:05", "PH (FIELD)", "PH",
  "109026", "2015PN010094",
  "Water-Qual-Eau-Arctic-2000-present.csv", 59.00,
  "NF02YL0011", "2020-10-23 11:45", "TEMPERATURE WATER (FIELD)",
  "DEG C", "109028", "2020-AT0215-0235",
  "Water-Qual-Eau-Newfoundland-TerreNeuve-Labrador-2000-present.csv", 87.47,
  "NF03OE0035", "2023-07-05 11:45", "PH (FIELD)", "PH",
  "109026", "2023-AT0215-0027",
  "Water-Qual-Eau-Newfoundland-TerreNeuve-Labrador-2000-present.csv", 100.30
)
canada_extreme_keys <- names(canada_extreme_plan)[
  names(canada_extreme_plan) != "expected_value"
]
canada_extreme_review <- canada_source %>%
  dplyr::inner_join(
    canada_extreme_plan,
    by = canada_extreme_keys
  )

if (
  nrow(canada_extreme_review) != 4L ||
    any(canada_extreme_review$status != "P") ||
    any(
      !is.na(canada_extreme_review$value_qualifier) &
        canada_extreme_review$value_qualifier != ""
    ) ||
    any(!dplyr::near(
      canada_extreme_review$source_value,
      canada_extreme_review$expected_value
    ))
) {
  stop("the reviewed Canada extreme source values changed")
}

# keep filtered reactive si and exclude the method with no documented fraction
canada_source_valid <- canada_source %>%
  dplyr::filter(
    is.na(source_method_flag) |
      source_method_flag != "soluble reactive SiO2, ECCC"
  )

if (
  nrow(canada_source_valid) != 258015L ||
    any(canada_source_valid$source_method_flag ==
      "soluble reactive SiO2, ECCC", na.rm = TRUE)
) {
  stop("the reviewed Canada silica method selection changed")
}

canada_alkalinity_source <- canada_source_valid %>%
  dplyr::filter(variable == "alkalinity") %>%
  dplyr::group_by(Stream_Name, date, variable, units) %>%
  dplyr::mutate(
    has_total_detected = any(
      source_variable == "ALKALINITY TOTAL CACO3" & detected
    ),
    has_gran_detected = any(
      source_variable == "ALKALINITY GRAN CACO3" & detected
    ),
    has_total = any(source_variable == "ALKALINITY TOTAL CACO3"),
    selected_method = dplyr::case_when(
      has_total_detected ~ "ALKALINITY TOTAL CACO3",
      has_gran_detected ~ "ALKALINITY GRAN CACO3",
      has_total ~ "ALKALINITY TOTAL CACO3",
      TRUE ~ "ALKALINITY GRAN CACO3"
    )
  ) %>%
  dplyr::filter(source_variable == selected_method) %>%
  dplyr::mutate(
    source_method_flag = dplyr::case_when(
      source_variable == "ALKALINITY TOTAL CACO3" ~
        "total alkalinity, ECCC",
      source_variable == "ALKALINITY GRAN CACO3" ~
        "Gran alkalinity fallback, ECCC",
      TRUE ~ source_method_flag
    )
  ) %>%
  dplyr::ungroup()

canada_alkalinity_overlap <- canada_source_valid %>%
  dplyr::filter(variable == "alkalinity") %>%
  dplyr::distinct(Stream_Name, date, source_variable) %>%
  dplyr::count(Stream_Name, date, name = "methods") %>%
  dplyr::filter(methods == 2L)

canada_alkalinity_method_review <- canada_alkalinity_source %>%
  dplyr::semi_join(
    canada_alkalinity_overlap,
    by = c("Stream_Name", "date")
  ) %>%
  dplyr::distinct(
    Stream_Name,
    date,
    has_total_detected,
    has_gran_detected,
    selected_method
  )

if (
  nrow(canada_alkalinity_overlap) != 2831L ||
    dplyr::n_distinct(
      canada_alkalinity_source$Stream_Name,
      canada_alkalinity_source$date
    ) != 13910L ||
    any(canada_alkalinity_source$source_variable !=
      canada_alkalinity_source$selected_method) ||
    sum(
      canada_alkalinity_method_review$has_total_detected &
        canada_alkalinity_method_review$has_gran_detected
    ) != 2157L ||
    sum(
      canada_alkalinity_method_review$has_total_detected &
        !canada_alkalinity_method_review$has_gran_detected
    ) != 1L ||
    sum(
      !canada_alkalinity_method_review$has_total_detected &
        canada_alkalinity_method_review$has_gran_detected
    ) != 622L ||
    sum(
      !canada_alkalinity_method_review$has_total_detected &
        !canada_alkalinity_method_review$has_gran_detected
    ) != 51L ||
    any(
      !canada_alkalinity_method_review$has_total_detected &
        canada_alkalinity_method_review$has_gran_detected &
        canada_alkalinity_method_review$selected_method !=
          "ALKALINITY GRAN CACO3"
    )
) {
  stop("the reviewed Canada alkalinity method selection changed")
}

canada_selected_source <- canada_source_valid %>%
  dplyr::filter(variable != "alkalinity") %>%
  dplyr::bind_rows(canada_alkalinity_source)

# include each below-limit sample at half its reported threshold
canada_daily <- canada_selected_source %>%
  dplyr::group_by(Stream_Name, date, variable, units) %>%
  dplyr::summarize(
    any_censored = any(censored),
    all_censored = all(censored),
    below_count = sum(censored),
    observation_count = dplyr::n(),
    value = daily_limit_mean(value, ifelse(censored, "<", "")),
    value_qualifier = NA_character_,
    source_method_flag = {
      method_flags <- unique(source_method_flag[
        !is.na(source_method_flag) & source_method_flag != ""
      ])
      if (length(method_flags) > 1L) {
        stop("conflicting Canada method flags remain within a daily key")
      }
      if (length(method_flags) == 0L) NA_character_ else method_flags
    },
    .groups = "drop"
  )

if (sum(canada_daily$below_count) != sum(canada_selected_source$censored) ||
    sum(canada_daily$observation_count) != nrow(canada_selected_source)) {
  stop("the Canada daily substitution counts lost source observations")
}

canada_sensitive_variables <- c(
  "alkalinity",
  "conductivity",
  "specific conductivity",
  "NHX",
  "NO3",
  "NOx",
  "PO4",
  "SRP",
  "DSi",
  "TN",
  "TDP"
)

canada_output <- canada_daily %>%
  dplyr::left_join(canada_raw_map, by = c("variable", "units")) %>%
  dplyr::transmute(
    LTER = "Canada",
    Stream_Name,
    source_lab_flag = half_limit_flag(below_count, observation_count),
    source_method_flag,
    date,
    solute,
    solute_units,
    amount = format_amount(value * raw_per_final, value_qualifier)
  ) %>%
  dplyr::arrange(Stream_Name, date, solute, solute_units)

if (
  anyNA(canada_output$solute) ||
    any(canada_output$solute == "NH4") ||
    nrow(canada_output) != 226083L ||
    dplyr::n_distinct(
      canada_output$Stream_Name,
      canada_output$date,
      canada_output$solute,
      canada_output$solute_units
    ) != 226083L
) {
  stop("the reviewed Canada output rows changed")
}

# check the reviewed original daily groups, not less-than signs on calculated means
canada_sensitive_review <- canada_daily %>%
  dplyr::filter(variable %in% canada_sensitive_variables) %>%
  dplyr::left_join(canada_raw_map, by = c("variable", "units")) %>%
  dplyr::group_by(solute) %>%
  dplyr::summarize(
    rows = dplyr::n(),
    all_below_days = sum(all_censored),
    .groups = "drop"
  )

canada_sensitive_expected <- tibble::tribble(
  ~solute, ~rows, ~all_below_days,
  "alkalinity", 13910L, 52L,
  "conductivity", 232L, 0L,
  "specific conductivity", 11208L, 0L,
  "NHX", 8312L, 1813L,
  "NO3", 4585L, 2250L,
  "NOx", 273L, 72L,
  "PO4", 5320L, 1475L,
  "SRP", 211L, 26L,
  "DSi", 4366L, 0L,
  "TN", 7268L, 24L,
  "TDP", 8105L, 2018L
)

if (
  !setequal(canada_sensitive_review, canada_sensitive_expected)
) {
  stop(
    "the reviewed Canada target corrections changed: ",
    paste(
      paste0(
        canada_sensitive_review$solute,
        "=",
        canada_sensitive_review$rows,
        "/",
        canada_sensitive_review$all_below_days
      ),
      collapse = "; "
    )
  )
}

# keep the reported canada tdn value for later outlier review
canada_lockhart_tdn <- canada_output %>%
  dplyr::filter(
    Stream_Name == "LOCKHART RIVER AT OUTLET OF ARTILLERY LAKE",
    date == as.Date("2000-01-17"),
    solute == "tdn_mgl",
    solute_units == "mg N/L"
  )
canada_lockhart_tdn_um <- suppressWarnings(
  as.numeric(canada_lockhart_tdn$amount) / (N_mw / 1000)
)

if (
  nrow(canada_lockhart_tdn) != 1L ||
    grepl("^\\s*[<>]", canada_lockhart_tdn$amount) ||
    !dplyr::near(
      suppressWarnings(as.numeric(canada_lockhart_tdn$amount)),
      204
    ) ||
    !isTRUE(all.equal(
      canada_lockhart_tdn_um,
      14564.4584377476,
      tolerance = 1e-12
    ))
) {
  stop("the reviewed Canada Lockhart TDN row changed")
}

canada_coppermine_output_review <- canada_output %>%
  dplyr::filter(
    Stream_Name == "COPPERMINE RIVER ABOVE COPPER CREEK",
    date == as.Date("2018-04-09"),
    solute == "temp_c",
    solute_units == "C"
  )
if (
  nrow(canada_coppermine_output_review) != 1L ||
    !dplyr::near(
      suppressWarnings(as.numeric(canada_coppermine_output_review$amount)),
      -10
    )
) {
  stop("the reviewed Canada Coppermine output row changed")
}

canada_cold_review_keys <- canada_cold_review_source %>%
  dplyr::transmute(
    Stream_Name,
    date = as.Date(substr(source_datetime, 1, 10)),
    solute = "temp_c",
    solute_units = "C",
    expected_value
  )
canada_cold_output_review <- canada_output %>%
  dplyr::inner_join(
    canada_cold_review_keys,
    by = c("Stream_Name", "date", "solute", "solute_units")
  )

if (
  nrow(canada_cold_output_review) != 4L ||
    any(grepl("^\\s*[<>]", canada_cold_output_review$amount)) ||
    any(!dplyr::near(
      suppressWarnings(as.numeric(canada_cold_output_review$amount)),
      canada_cold_output_review$expected_value
    ))
) {
  stop("the reviewed Canada gross cold-temperature output rows changed")
}

canada_lockhart_tdn_remaining <- canada_output %>%
  dplyr::filter(
    Stream_Name == "LOCKHART RIVER AT OUTLET OF ARTILLERY LAKE",
    date == as.Date("2000-01-17"),
    solute == "tdn_mgl",
    solute_units == "mg N/L"
  )
canada_low_temperature_rows <- canada_output %>%
  dplyr::filter(solute == "temp_c", solute_units == "C") %>%
  dplyr::mutate(
    temperature = suppressWarnings(as.numeric(
      sub("^\\s*[<>]\\s*", "", amount)
    ))
  ) %>%
  dplyr::filter(temperature < -0.5)

if (
  nrow(canada_lockhart_tdn_remaining) != 1L ||
    nrow(canada_low_temperature_rows) != 57L ||
    !isTRUE(all.equal(
      range(canada_low_temperature_rows$temperature),
      c(-10, -0.51),
      tolerance = 1e-12
    )) ||
    any(canada_output$solute == "NH4") ||
    nrow(canada_output) != 226083L ||
    dplyr::n_distinct(
      canada_output$Stream_Name,
      canada_output$date,
      canada_output$solute,
      canada_output$solute_units
    ) != 226083L ||
    any(grepl("^<", canada_output$amount)) ||
    any(canada_output$source_method_flag ==
      "soluble reactive SiO2, ECCC", na.rm = TRUE)
) {
  stop("the prepared Canada input changed")
}

# keep all canada sites and dates until the final site check
# the final master keeps 11295 total and 2131 gran fallback rows
if (
  nrow(canada_output) != 226083L ||
    sum(canada_daily$all_censored) != 11000L ||
    sum(canada_daily$any_censored & !canada_daily$all_censored) != 517L ||
    sum(is_half_limit_flag(canada_output$source_lab_flag)) != 11517L ||
    sum(canada_output$source_method_flag ==
      "soluble reactive Si, filtered ICP-OES", na.rm = TRUE) != 4366L ||
    any(canada_output$source_method_flag ==
      "soluble reactive SiO2, ECCC", na.rm = TRUE) ||
    sum(canada_output$source_method_flag ==
      "field pH", na.rm = TRUE) != 11574L ||
    sum(canada_output$source_method_flag ==
      "field conductivity", na.rm = TRUE) != 11440L ||
    sum(canada_output$source_method_flag ==
      "total alkalinity, ECCC", na.rm = TRUE) != 11668L ||
    sum(canada_output$source_method_flag ==
      "Gran alkalinity fallback, ECCC", na.rm = TRUE) != 2242L
) {
  stop(
    "the prepared Canada lab or method flags changed: rows=",
    nrow(canada_output),
    ", below=",
    sum(canada_daily$all_censored),
    ", mixed=",
    sum(
      canada_daily$any_censored & !canada_daily$all_censored,
      na.rm = TRUE
    ),
    ", field_ph=",
    sum(canada_output$source_method_flag == "field pH", na.rm = TRUE),
    ", field_conductivity=",
    sum(
      canada_output$source_method_flag == "field conductivity",
      na.rm = TRUE
    ),
    ", total_alkalinity=",
    sum(
      canada_output$source_method_flag == "total alkalinity, ECCC",
      na.rm = TRUE
    ),
    ", gran_alkalinity=",
    sum(
      canada_output$source_method_flag == "Gran alkalinity fallback, ECCC",
      na.rm = TRUE
    )
  )
}

# ---- gems ----

gems_current <- read_long_files(gems_files) %>%
  dplyr::mutate(solute = dplyr::if_else(solute == "NH4", "NHX", solute))
gems_provider_path <- file.path(
  provider_dir,
  "gems_gemstat",
  "GFQA_v3.zip"
)

if (!file.exists(gems_provider_path)) {
  stop("the reviewed GEMS provider archive is unavailable")
}

gems_map <- tibble::tribble(
  ~source_entry, ~parameter_code, ~provider_units, ~variable, ~units,
  ~conversion, ~priority, ~safe,
  "Aluminium.csv", "Al-Dis", "mg/l", "Al", "uM", 1000 / Al_mw, 1L, TRUE,
  "Chloride.csv", "Cl-Dis", "mg/l", "Cl", "uM", 1000 / Cl_mw, 1L, TRUE,
  "Pigment.csv", "Chl-a", "mg/l", "chlorophyll a", "ug/L", 1000, 1L, TRUE,
  "Carbon.csv", "DOC", "mg/l", "dissolved org C", "uM", 1000 / C_mw, 1L, TRUE,
  "Carbon.csv", "TOC", "mg/l", "tot org C", "uM", 1000 / C_mw, 1L, TRUE,
  "Silicon.csv", "Si-Dis", "mg/l", "DSi", "uM", 1000 / Si_mw, 1L, TRUE,
  "Silicon.csv", "SiO2-Dis", "mg/l", "DSi", "uM", 1000 / SiO2_mw, 1L, TRUE,
  "Fluoride.csv", "F-Dis", "mg/l", "F", "uM", 1000 / F_mw, 1L, TRUE,
  "Iron.csv", "Fe-Dis", "mg/l", "Fe", "uM", 1000 / Fe_mw, 1L, TRUE,
  "Potassium.csv", "K-Dis", "mg/l", "K", "uM", 1000 / K_mw, 1L, TRUE,
  "Manganese.csv", "Mn-Dis", "mg/l", "Mn", "uM", 1000 / Mn_mw, 1L, TRUE,
  "Other_Nitrogen.csv", "NH4N", "mg/l", "NHX", "uM", 1000 / N_mw, 1L, TRUE,
  "Other_Nitrogen.csv", "TN", "mg/l", "TN", "uM", 1000 / N_mw, 1L, TRUE,
  "Other_Nitrogen.csv", "TKN", "mg/l", "tot kjeldahl N", "uM", 1000 / N_mw, 1L, TRUE,
  "Oxidized_Nitrogen.csv", "NO2N", "mg/l", "NO2", "uM", 1000 / N_mw, 1L, TRUE,
  "Oxidized_Nitrogen.csv", "NO3N", "mg/l", "NO3", "uM", 1000 / N_mw, 1L, TRUE,
  "Phosphorus.csv", "TP", "mg/l", "TP", "uM", 1000 / P_mw, 1L, TRUE,
  "Phosphorus.csv", "TDP", "mg/l", "tdp", "uM", 1000 / P_mw, 1L, TRUE,
  "Water.csv", "TSS", "mg/l", "susp partic matter", "mg/L", 1, 1L, TRUE,
  "Zinc.csv", "Zn-Dis", "mg/l", "Zn", "uM", 1000 / Zn_mw, 1L, TRUE,
  "Dissolved_Gas.csv", "O2-Dis", "mg/l", "dissolved oxygen", "uM", 1000 / (2 * O_mw), 1L, FALSE,
  "Dissolved_Gas.csv", "O2-Dis-Sat", "%", "do percent", "percent", 1, 1L, FALSE,
  "Temperature.csv", "TEMP", "°C", "temp", "C", 1, 1L, FALSE,
  "Electrical_Conductance.csv", "EC", "µS/cm", "conductivity", "uS/cm", 1, 1L, FALSE,
  "pH.csv", "pH", "---", "pH", NA_character_, 1, 1L, FALSE
)
shallowest_depth_method_flag <- "shallowest sample depth used"

gems_station_ids <- unique(gems_current$Stream_Name)

gems_source <- lapply(unique(gems_map$source_entry), function(entry) {
  one_map <- gems_map %>% dplyr::filter(source_entry == entry)
  source_command <- sprintf(
    "unzip -p %s %s",
    shQuote(gems_provider_path),
    shQuote(entry)
  )
  source_rows <- data.table::fread(
    cmd = source_command,
    select = c(
      "GEMS Station Number",
      "Sample Date",
      "Sample Time",
      "Depth",
      "Parameter Code",
      "Analysis Method Code",
      "Value Flags",
      "Value",
      "Unit"
    ),
    na.strings = "",
    encoding = "Latin-1",
    showProgress = FALSE
  )
  source_rows[, provider_row_number := seq_len(.N)]
  source_rows <- source_rows[
    `GEMS Station Number` %in% gems_station_ids &
      `Parameter Code` %in% one_map$parameter_code
  ]
  source_rows %>%
    tibble::as_tibble() %>%
    dplyr::transmute(
      Stream_Name = `GEMS Station Number`,
      date = as.Date(`Sample Date`),
      depth = suppressWarnings(as.numeric(Depth)),
      parameter_code = `Parameter Code`,
      value_flag = trimws(`Value Flags`),
      source_value = suppressWarnings(as.numeric(Value)),
      source_units = trimws(Unit)
    ) %>%
    dplyr::inner_join(one_map, by = "parameter_code")
}) %>%
  dplyr::bind_rows() %>%
  dplyr::filter(
    source_units == provider_units,
    is.finite(source_value),
    !is.na(date)
  ) %>%
  dplyr::mutate(censored = !is.na(value_flag) & value_flag == "<")

# the gemstat catalogue defines these as total fractions or different species
# do not substitute them for the dissolved or standard variables used here
# source: https://gemstat.org/data-gemstat/data-portal/custom-data-request/
gems_unsafe_map <- tibble::tribble(
  ~source_entry, ~parameter_code, ~provider_units, ~variable, ~units,
  "Aluminium.csv", "Al-Tot", "mg/l", "Al", "uM",
  "Chloride.csv", "Cl-Tot", "mg/l", "Cl", "uM",
  "Silicon.csv", "SiO2-Tot", "mg/l", "DSi", "uM",
  "Fluoride.csv", "F-Tot", "mg/l", "F", "uM",
  "Iron.csv", "Fe-Tot", "mg/l", "Fe", "uM",
  "Manganese.csv", "Mn-Tot", "mg/l", "Mn", "uM",
  "Other_Nitrogen.csv", "TON", "mg/l", "TN", "uM",
  "Other_Nitrogen.csv", "DKN", "mg/l", "tot kjeldahl N", "uM",
  "Phosphorus.csv", "TIP", "mg/l", "TP", "uM",
  "Zinc.csv", "Zn-Tot", "mg/l", "Zn", "uM"
)

gems_unsafe_source <- lapply(
  unique(gems_unsafe_map$source_entry),
  function(entry) {
    one_map <- gems_unsafe_map %>% dplyr::filter(source_entry == entry)
    source_rows <- data.table::fread(
      cmd = sprintf(
        "unzip -p %s %s",
        shQuote(gems_provider_path),
        shQuote(entry)
      ),
      select = c(
        "GEMS Station Number",
        "Sample Date",
        "Parameter Code",
        "Value",
        "Unit"
      ),
      na.strings = "",
      encoding = "Latin-1",
      showProgress = FALSE
    )
    source_rows[
      `GEMS Station Number` %in% gems_station_ids &
        `Parameter Code` %in% one_map$parameter_code
    ] %>%
      tibble::as_tibble() %>%
      dplyr::transmute(
        Stream_Name = `GEMS Station Number`,
        date = as.Date(`Sample Date`),
        parameter_code = `Parameter Code`,
        source_value = suppressWarnings(as.numeric(Value)),
        source_units = trimws(Unit)
      ) %>%
      dplyr::inner_join(one_map, by = "parameter_code")
  }
) %>%
  dplyr::bind_rows() %>%
  dplyr::filter(
    source_units == provider_units,
    is.finite(source_value),
    !is.na(date)
  )

gems_safe_keys <- gems_source %>%
  dplyr::filter(safe) %>%
  dplyr::distinct(Stream_Name, date, variable)

gems_unsafe_keys <- gems_unsafe_source %>%
  dplyr::distinct(
    Stream_Name,
    date,
    variable,
    units,
    parameter_code
  ) %>%
  dplyr::anti_join(
    gems_safe_keys,
    by = c("Stream_Name", "date", "variable")
  )

if (
  nrow(gems_unsafe_keys) != 6637L ||
    sum(gems_unsafe_keys$parameter_code == "TON") != 441L ||
    sum(gems_unsafe_keys$parameter_code != "TON") != 6196L
) {
  stop("the reviewed GEMS unsafe fallback keys changed")
}

gems_current_unsafe <- gems_current %>%
  dplyr::semi_join(
    gems_unsafe_keys,
    by = c(
      "Stream_Name",
      "date",
      "solute" = "variable",
      "solute_units" = "units"
    )
  )

if (nrow(gems_current_unsafe) != 0L) {
  stop("unsafe GEMS fallback rows were found in the prepared input")
}

gems_current <- gems_current %>%
  dplyr::anti_join(
    gems_unsafe_keys,
    by = c(
      "Stream_Name",
      "date",
      "solute" = "variable",
      "solute_units" = "units"
    )
  )

if (
  nrow(gems_source) != 601656L ||
    sum(gems_source$censored) != 7443L ||
    sum(gems_source$censored & gems_source$safe) != 4467L ||
    sum(gems_source$censored & !gems_source$safe) != 2976L
) {
  stop("the reviewed GEMS provider rows changed")
}

# keep the reported silicon form visible through daily averaging
gems_silicon_code_conflicts <- gems_source %>%
  dplyr::filter(safe, variable == "DSi") %>%
  dplyr::group_by(Stream_Name, date) %>%
  dplyr::summarize(
    source_codes = dplyr::n_distinct(parameter_code),
    .groups = "drop"
  ) %>%
  dplyr::filter(source_codes > 1L)

if (nrow(gems_silicon_code_conflicts) > 0L) {
  stop("more than one GEMS silicon form occurs on the same site-date")
}

# 2,976 flags occur on ordinary italian field values such as temperature and ph
# keep their numeric values and record that the flag meaning still needs review

# include confirmed below-limit chemistry at half its reported threshold
gems_safe_depth_review <- gems_source %>%
  dplyr::filter(safe) %>%
  dplyr::group_by(Stream_Name, date, variable) %>%
  dplyr::filter(priority == min(priority)) %>%
  dplyr::mutate(
    depth_order = dplyr::if_else(is.na(depth), Inf, depth),
    selected_depth = min(depth_order),
    multiple_sample_depths = dplyr::n_distinct(depth[!is.na(depth)]) > 1L
  )
gems_deeper_rows <- gems_safe_depth_review %>%
  dplyr::filter(depth_order != selected_depth)
gems_multiple_depth_groups <- gems_safe_depth_review %>%
  dplyr::filter(multiple_sample_depths) %>%
  dplyr::distinct(Stream_Name, date, variable)

if (
  nrow(gems_deeper_rows) != 748L ||
    nrow(gems_multiple_depth_groups) != 165L
) {
  stop("the reviewed GEMS sample-depth rows changed")
}

gems_daily <- gems_safe_depth_review %>%
  dplyr::filter(depth_order == selected_depth) %>%
  dplyr::mutate(
    any_censored = any(censored),
    all_censored = all(censored),
    mixed = any_censored & !all_censored
  ) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(
    Stream_Name,
    date,
    variable,
    units,
    any_censored,
    all_censored,
    mixed
  ) %>%
  dplyr::summarize(
    below_count = sum(censored),
    observation_count = dplyr::n(),
    value = daily_limit_mean(source_value * conversion, ifelse(censored, "<", "")),
    value_qualifier = NA_character_,
    source_method_flag = combine_method_flags(
      dplyr::case_when(
        dplyr::first(parameter_code) == "SiO2-Dis" ~
          "reported as SiO2-Dis",
        dplyr::first(parameter_code) == "Si-Dis" ~
          "reported as Si-Dis",
        TRUE ~ NA_character_
      ),
      dplyr::if_else(
        dplyr::first(multiple_sample_depths),
        shallowest_depth_method_flag,
        NA_character_
      )
    ),
    .groups = "drop"
  )

if (sum(gems_daily$below_count) != sum(gems_safe_depth_review$censored[
    gems_safe_depth_review$depth_order == gems_safe_depth_review$selected_depth]) ||
    sum(gems_daily$observation_count) != sum(
      gems_safe_depth_review$depth_order == gems_safe_depth_review$selected_depth)) {
  stop("the GEMS daily substitution counts lost source observations")
}

gems_replacements <- gems_daily %>%
  dplyr::filter(any_censored) %>%
  dplyr::transmute(
    LTER = "GEMS",
    Stream_Name,
    date,
    solute = variable,
    solute_units = units,
    amount = format_amount(value, value_qualifier),
    all_censored,
    mixed
  )

gems_review <- gems_replacements %>%
  dplyr::left_join(
    gems_current %>%
      dplyr::mutate(current_value = suppressWarnings(as.numeric(amount))) %>%
      dplyr::select(
        Stream_Name,
        date,
        solute,
        solute_units,
        current_value
    ),
    by = c("Stream_Name", "date", "solute", "solute_units")
  )

if (
  nrow(gems_replacements) != 4299L ||
    sum(gems_replacements$all_censored) != 4290L ||
    sum(gems_replacements$mixed) != 9L ||
    sum(!is.na(gems_review$current_value[gems_review$all_censored])) != 56L ||
    sum(is.na(gems_review$current_value[gems_review$all_censored])) != 4234L ||
    any(is.na(gems_review$current_value[gems_review$mixed])) ||
    any(!is.finite(suppressWarnings(as.numeric(gems_review$amount))))
) {
  stop(
    "the reviewed GEMS replacement values changed: replacements=",
    nrow(gems_replacements),
    ", all below=", sum(gems_replacements$all_censored),
    ", mixed=", sum(gems_replacements$mixed),
    ", matched old all-below=",
    sum(!is.na(gems_review$current_value[gems_review$all_censored])),
    ", missing old all-below=",
    sum(is.na(gems_review$current_value[gems_review$all_censored])),
    ", missing old mixed=",
    sum(is.na(gems_review$current_value[gems_review$mixed])),
    ", invalid new means=",
    sum(!is.finite(suppressWarnings(as.numeric(gems_review$amount))))
  )
}

gems_safe_variables <- unique(gems_map$variable[gems_map$safe])
gems_safe_output <- gems_daily %>%
  dplyr::transmute(
    LTER = "GEMS",
    Stream_Name,
    date,
    solute = variable,
    solute_units = units,
    amount = format_amount(value, value_qualifier),
    source_method_flag
  )
gems_non_safe_current <- gems_current %>%
  dplyr::filter(!solute %in% gems_safe_variables)
gems_physical_depth_review <- gems_source %>%
  dplyr::filter(!safe) %>%
  dplyr::group_by(
    Stream_Name,
    date,
    variable,
    units
  ) %>%
  dplyr::mutate(
    depth_order = dplyr::if_else(is.na(depth), Inf, depth),
    selected_depth = min(depth_order),
    multiple_sample_depths = dplyr::n_distinct(depth[!is.na(depth)]) > 1L
  )
gems_physical_multiple_depth_groups <- gems_physical_depth_review %>%
  dplyr::filter(multiple_sample_depths) %>%
  dplyr::distinct(Stream_Name, date, variable, units)
gems_physical_deeper_rows <- gems_physical_depth_review %>%
  dplyr::filter(depth_order != selected_depth)

if (
  nrow(gems_physical_multiple_depth_groups) != 105L ||
    nrow(gems_physical_deeper_rows) != 487L
) {
  stop("the reviewed GEMS physical sample-depth rows changed")
}

gems_physical_flagged_output <- gems_physical_depth_review %>%
  dplyr::filter(depth_order == selected_depth) %>%
  dplyr::filter(any(censored)) %>%
  dplyr::summarize(
    amount = format_amount(mean(source_value * conversion)),
    source_lab_flag = "provider < flag; meaning not confirmed",
    source_method_flag = dplyr::if_else(
      dplyr::first(multiple_sample_depths),
      shallowest_depth_method_flag,
      NA_character_
    ),
    .groups = "drop"
  ) %>%
  dplyr::transmute(
    LTER = "GEMS",
    Stream_Name,
    date,
    solute = variable,
    solute_units = units,
    amount,
    source_lab_flag,
    source_method_flag
  )
gems_physical_depth_flags <- gems_physical_multiple_depth_groups %>%
  dplyr::transmute(
    Stream_Name,
    date,
    solute = variable,
    source_method_flag = shallowest_depth_method_flag
  )
if (anyDuplicated(gems_physical_depth_flags[c(
  "Stream_Name", "date", "solute"
)]) > 0L) {
  stop("duplicate GEMS physical sample-depth flags remain")
}
gems_non_safe_current <- gems_non_safe_current %>%
  dplyr::left_join(
    gems_physical_depth_flags,
    by = c("Stream_Name", "date", "solute")
  )
gems_non_safe_output <- gems_non_safe_current %>%
  dplyr::anti_join(
    gems_physical_flagged_output,
    by = c("Stream_Name", "date", "solute", "solute_units")
  ) %>%
  dplyr::bind_rows(gems_physical_flagged_output) %>%
  dplyr::arrange(Stream_Name, date, solute, solute_units)

if (
  nrow(gems_safe_output) != 334647L ||
    nrow(gems_non_safe_current) != 527443L ||
    nrow(gems_non_safe_output) != 530355L ||
    sum(gems_non_safe_output$source_lab_flag ==
      "provider < flag; meaning not confirmed", na.rm = TRUE) != 2912L ||
    sum(has_method_flag(
      gems_non_safe_output$source_method_flag,
      shallowest_depth_method_flag
    )) != 105L ||
    dplyr::n_distinct(
      gems_non_safe_output$Stream_Name,
      gems_non_safe_output$date,
      gems_non_safe_output$solute,
      gems_non_safe_output$solute_units
    ) != 530355L
) {
  stop(
    "the reviewed GEMS safe or retained physical rows changed: safe=",
    nrow(gems_safe_output),
    ", current physical=", nrow(gems_non_safe_current),
    ", flagged physical=", nrow(gems_physical_flagged_output),
    ", final physical=", nrow(gems_non_safe_output),
    ", physical flags=", sum(
      gems_non_safe_output$source_lab_flag ==
        "provider < flag; meaning not confirmed",
      na.rm = TRUE
    ),
    ", depth flags=", sum(has_method_flag(
      gems_non_safe_output$source_method_flag,
      shallowest_depth_method_flag
    )),
    ", final physical keys=", dplyr::n_distinct(
      gems_non_safe_output$Stream_Name,
      gems_non_safe_output$date,
      gems_non_safe_output$solute,
      gems_non_safe_output$solute_units
    )
  )
}

gems_output <- dplyr::bind_rows(
  gems_non_safe_output,
  gems_safe_output
) %>%
  dplyr::arrange(Stream_Name, date, solute, solute_units)

# check that the retained records match the original source values
gems_suspect_rows <- tibble::tribble(
  ~source_entry, ~provider_row_number, ~Stream_Name, ~date, ~parameter_code,
  ~source_value, ~source_units, ~variable, ~units, ~conversion,
  "Phosphorus.csv", 562348L, "IND02355", "1995-01-05", "TP",
  90, "mg/l", "TP", "uM", 1000 / P_mw,
  "Phosphorus.csv", 574788L, "IND02427", "2007-06-01", "TP",
  9.48, "mg/l", "TP", "uM", 1000 / P_mw,
  "Oxidized_Nitrogen.csv", 696171L, "IND02268", "2013-02-01", "NOxN",
  355.014, "mg/l", "NOx", "uM", 1000 / N_mw,
  "Phosphorus.csv", 527161L, "IND02160", "1999-02-01", "TP",
  37, "mg/l", "TP", "uM", 1000 / P_mw,
  "Iron.csv", 272048L, "IND02263", "1995-01-08", "Fe-Dis",
  93, "mg/l", "Fe", "uM", 1000 / Fe_mw,
  "Chloride.csv", 242417L, "IND02242", "1995-10-04", "Cl-Dis",
  3501, "mg/l", "Cl", "uM", 1000 / Cl_mw,
  "Potassium.csv", 181920L, "IND02268", "1980-06-02", "K-Dis",
  262, "mg/l", "K", "uM", 1000 / K_mw,
  "Other_Nitrogen.csv", 422115L, "IND02197", "1992-07-01", "NH4N",
  14.04, "mg/l", "NHX", "uM", 1000 / N_mw,
  "Fluoride.csv", 98048L, "IND02355", "1987-01-04", "F-Dis",
  95, "mg/l", "F", "uM", 1000 / F_mw,
  "Fluoride.csv", 73494L, "IND02208", "1992-03-02", "F-Dis",
  48, "mg/l", "F", "uM", 1000 / F_mw,
  "Fluoride.csv", 96669L, "IND02341", "1995-02-01", "F-Dis",
  92, "mg/l", "F", "uM", 1000 / F_mw,
  "Aluminium.csv", 151324L, "IND02295", "1999-06-21", "Al-Dis",
  25, "mg/l", "Al", "uM", 1000 / Al_mw,
  "Oxidized_Nitrogen.csv", 634953L, "IND02125", "2007-04-02", "NOxN",
  322.511, "mg/l", "NOx", "uM", 1000 / N_mw,
  "Oxidized_Nitrogen.csv", 732383L, "IND02355", "2011-02-05", "NOxN",
  394, "mg/l", "NOx", "uM", 1000 / N_mw,
  "Potassium.csv", 161840L, "IND02184", "1996-04-01", "K-Dis",
  195.5, "mg/l", "K", "uM", 1000 / K_mw
) %>%
  dplyr::mutate(date = as.Date(date))

gems_retained_duplicates <- tibble::tribble(
  ~source_entry, ~provider_row_number, ~Stream_Name, ~date, ~parameter_code,
  ~source_value, ~source_units, ~variable, ~units, ~conversion,
  "Phosphorus.csv", 562347L, "IND02355", "1995-01-05", "TP",
  0.001, "mg/l", "TP", "uM", 1000 / P_mw,
  "Fluoride.csv", 98049L, "IND02355", "1987-01-04", "F-Dis",
  0.44, "mg/l", "F", "uM", 1000 / F_mw,
  "Oxidized_Nitrogen.csv", 732382L, "IND02355", "2011-02-05", "NOxN",
  1.958, "mg/l", "NOx", "uM", 1000 / N_mw
) %>%
  dplyr::mutate(date = as.Date(date))

gems_review_rows <- dplyr::bind_rows(
  gems_suspect_rows %>%
    dplyr::mutate(review_action = "retain_source_value"),
  gems_retained_duplicates %>%
    dplyr::mutate(review_action = "retain_source_value")
)

read_gems_review_rows <- function(entry) {
  expected <- gems_review_rows %>% dplyr::filter(source_entry == entry)
  source_rows <- data.table::fread(
    cmd = sprintf(
      "unzip -p %s %s",
      shQuote(gems_provider_path),
      shQuote(entry)
    ),
    select = c(
      "GEMS Station Number",
      "Sample Date",
      "Parameter Code",
      "Value",
      "Unit"
    ),
    colClasses = "character",
    na.strings = "",
    encoding = "Latin-1",
    showProgress = FALSE
  )
  source_rows[, provider_row_number := seq_len(.N)]
  source_rows[provider_row_number %in% expected$provider_row_number] %>%
    tibble::as_tibble() %>%
    dplyr::transmute(
      source_entry = entry,
      provider_row_number,
      Stream_Name = `GEMS Station Number`,
      date = as.Date(`Sample Date`),
      parameter_code = `Parameter Code`,
      source_value = suppressWarnings(as.numeric(Value)),
      source_units = trimws(Unit)
    )
}

observed_gems_review_rows <- lapply(
  unique(gems_review_rows$source_entry),
  read_gems_review_rows
) %>%
  dplyr::bind_rows()

review_identity <- c(
  "source_entry",
  "provider_row_number",
  "Stream_Name",
  "date",
  "parameter_code",
  "source_units"
)
matched_gems_review_rows <- gems_review_rows %>%
  dplyr::inner_join(observed_gems_review_rows, by = review_identity)

if (
  nrow(gems_suspect_rows) != 15L ||
    nrow(gems_retained_duplicates) != 3L ||
    nrow(observed_gems_review_rows) != 18L ||
    nrow(matched_gems_review_rows) != 18L ||
    any(!dplyr::near(
      matched_gems_review_rows$source_value.x,
      matched_gems_review_rows$source_value.y
    ))
) {
  stop("the reviewed GEMS suspect or retained provider rows changed")
}

gems_quality_keys <- gems_suspect_rows %>%
  dplyr::transmute(
    Stream_Name,
    date,
    solute = variable,
    solute_units = units
  )

gems_quality_current <- gems_output %>%
  dplyr::semi_join(
    gems_quality_keys,
    by = c("Stream_Name", "date", "solute", "solute_units")
  )

if (
  nrow(gems_quality_current) != 15L ||
    dplyr::n_distinct(
      gems_quality_current$Stream_Name,
      gems_quality_current$date,
      gems_quality_current$solute,
      gems_quality_current$solute_units
    ) != 15L
) {
  stop("the reviewed GEMS suspect daily rows changed")
}

gems_quality_expected <- dplyr::bind_rows(
  gems_suspect_rows,
  gems_retained_duplicates
) %>%
  dplyr::group_by(Stream_Name, date, variable, units) %>%
  dplyr::summarize(
    expected_amount = mean(source_value * conversion),
    .groups = "drop"
  )
gems_quality_retained <- gems_quality_current %>%
  dplyr::inner_join(
    gems_quality_expected,
    by = c(
      "Stream_Name",
      "date",
      "solute" = "variable",
      "solute_units" = "units"
    )
  )
gems_quality_mismatch <- !dplyr::near(
  suppressWarnings(as.numeric(gems_quality_retained$amount)),
  gems_quality_retained$expected_amount
)
if (
  nrow(gems_quality_retained) != 15L ||
    any(gems_quality_mismatch)
) {
  mismatch_rows <- gems_quality_retained[gems_quality_mismatch, ]
  stop(
    "the retained GEMS reviewed daily values changed: ",
    paste(
      mismatch_rows$Stream_Name,
      mismatch_rows$date,
      mismatch_rows$solute,
      mismatch_rows$amount,
      mismatch_rows$expected_amount,
      sep = "|",
      collapse = ";"
    )
  )
}

if (
  nrow(gems_output) != 865002L ||
    dplyr::n_distinct(
      gems_output$Stream_Name,
      gems_output$date,
      gems_output$solute,
      gems_output$solute_units
    ) != 865002L ||
    any(grepl("^<", gems_output$amount)) ||
    nrow(dplyr::filter(gems_output, solute == "NHX")) != 26668L ||
    any(grepl("^<", dplyr::filter(gems_output, solute == "NHX")$amount)) ||
    any(gems_output$solute == "NH4")
) {
  stop(
    "the prepared GEMS input changed: rows=", nrow(gems_output),
    ", keys=", dplyr::n_distinct(
      gems_output$Stream_Name,
      gems_output$date,
      gems_output$solute,
      gems_output$solute_units
    ),
    ", less-than=", sum(grepl("^<", gems_output$amount)),
    ", NHX=", nrow(dplyr::filter(gems_output, solute == "NHX")),
    ", NHX less-than=", sum(grepl(
      "^<",
      dplyr::filter(gems_output, solute == "NHX")$amount
    )),
    ", NH4=", sum(gems_output$solute == "NH4")
  )
}

gems_source_flags <- gems_daily %>%
  dplyr::transmute(
    Stream_Name,
    date,
    solute = variable,
    solute_units = units,
    source_safe_lab_flag = half_limit_flag(below_count, observation_count)
  ) %>%
  dplyr::distinct()

gems_output <- gems_output %>%
  dplyr::left_join(
    gems_source_flags,
    by = c("Stream_Name", "date", "solute", "solute_units")
  ) %>%
  dplyr::mutate(
    source_lab_flag = dplyr::coalesce(
      source_lab_flag,
      source_safe_lab_flag
    )
  ) %>%
  dplyr::select(-source_safe_lab_flag) %>%
  dplyr::relocate(source_lab_flag, source_method_flag, .before = date)

if (
  nrow(gems_output) != 865002L ||
    sum(is_half_limit_flag(gems_output$source_lab_flag)) != 4299L ||
    sum(gems_output$source_lab_flag ==
      "provider < flag; meaning not confirmed", na.rm = TRUE) != 2912L ||
    sum(has_method_flag(
      gems_output$source_method_flag,
      "reported as SiO2-Dis"
    )) != 39106L ||
    sum(has_method_flag(
      gems_output$source_method_flag,
      "reported as Si-Dis"
    )) != 161L ||
    sum(has_method_flag(
      gems_output$source_method_flag,
      shallowest_depth_method_flag
    )) != 270L
) {
  stop("the prepared GEMS lab or method flags changed")
}

# ---- danube ----

danube_provider_path <- file.path(
  provider_dir,
  "danube_icpdr",
  "danube_wq_v1.csv"
)

if (!file.exists(danube_provider_path)) {
  stop("the reviewed Danube provider file is unavailable")
}

danube_map <- tibble::tribble(
  ~determinand_code, ~variable, ~conversion, ~priority,
  "2.1.10", "NH4", 1000 / N_mw, 1L,
  "2.1.20", "NO2", 1000 / N_mw, 1L,
  "2.1.30", "NO3", 1000 / N_mw, 1L,
  "2.1.40", "tot kjeldahl N", 1000 / N_mw, 1L,
  "2.1.50", "TN", 1000 / N_mw, 1L,
  "2.2.10", "PO4", 1000 / P_mw, 1L,
  "2.2.20", "TP", 1000 / P_mw, 1L,
  "2.2.21", "tdp", 1000 / P_mw, 1L,
  # keep only the silica variable explicitly called dissolved in the original source
  "2.3.11", "DSi", 1000 / SiO2_mw, 1L,
  "3.1.10", "Na", 1000 / Na_mw, 1L,
  "3.1.20", "K", 1000 / K_mw, 1L,
  "3.1.30", "Ca", 1000 / Ca_mw, 1L,
  "3.1.40", "Mg", 1000 / Mg_mw, 1L,
  "3.2.10", "Cl", 1000 / Cl_mw, 1L,
  "3.2.20", "SO4", 1000 / SO4_mw, 1L
)

danube_provider <- data.table::fread(
  danube_provider_path,
  select = c(
    "id",
    "value",
    "value_calc",
    "station_code",
    "date_of_sampling",
    "analytical_method_code",
    "unit",
    "detection_limit",
    "determinand_code"
  ),
  na.strings = "",
  showProgress = FALSE
) %>%
  tibble::as_tibble() %>%
  dplyr::transmute(
    Stream_Name = station_code,
    date = as.Date(substr(date_of_sampling, 1, 10)),
    determinand_code,
    source_value = suppressWarnings(as.numeric(value)),
    value_calc = suppressWarnings(as.numeric(value_calc)),
    detection_limit = suppressWarnings(as.numeric(detection_limit)),
    source_units = trimws(unit)
  )

danube_source <- danube_provider %>%
  dplyr::inner_join(danube_map, by = "determinand_code") %>%
  dplyr::filter(
    source_units == "mg/l",
    is.finite(source_value),
    is.finite(value_calc),
    is.finite(detection_limit),
    !is.na(date)
  ) %>%
  dplyr::mutate(censored = source_value < detection_limit) %>%
  dplyr::group_by(Stream_Name, date, variable) %>%
  dplyr::filter(priority == min(priority)) %>%
  dplyr::ungroup()

# use half the detection limit for each below-limit sample before averaging
danube_daily <- danube_source %>%
  dplyr::group_by(Stream_Name, date, variable) %>%
  dplyr::summarize(
    any_censored = any(censored),
    all_censored = all(censored),
    mixed = any_censored & !all_censored,
    below_count = sum(censored),
    observation_count = dplyr::n(),
    value = daily_limit_mean(
      ifelse(censored, detection_limit, value_calc) * conversion,
      ifelse(censored, "<", "")),
    value_qualifier = NA_character_,
    source_method_flag = NA_character_,
    .groups = "drop"
  )

if (sum(danube_daily$below_count) != sum(danube_source$censored) ||
    sum(danube_daily$observation_count) != nrow(danube_source)) {
  stop("the Danube daily substitution counts lost source observations")
}

# build the prepared file from the original source download
# do not rely on older generated files for individual sites
danube_output <- danube_daily %>%
  dplyr::transmute(
    LTER = "Danube",
    Stream_Name,
    source_lab_flag = half_limit_flag(below_count, observation_count),
    source_method_flag,
    date,
    solute = variable,
    solute_units = "uM",
    amount = format_amount(value, value_qualifier)
  ) %>%
  dplyr::arrange(Stream_Name, date, solute, solute_units)

if (
  sum(danube_daily$all_censored) != 5496L ||
    sum(danube_daily$mixed) != 757L ||
    any(danube_daily$all_censored & danube_daily$mixed) ||
    nrow(danube_output) != 239963L ||
    dplyr::n_distinct(
      danube_output$Stream_Name,
      danube_output$date,
      danube_output$solute,
      danube_output$solute_units
    ) != 239963L ||
    any(grepl("^<", danube_output$amount)) ||
    any(grepl("^>", danube_output$amount)) ||
    sum(is_half_limit_flag(danube_output$source_lab_flag)) != 6253L ||
    any(!is.na(danube_output$source_method_flag))
) {
  stop("the reviewed Danube daily qualifier groups changed")
}

# ---- keys ----

data_key <- read.csv(data_key_path, na.strings = "")

provider_files <- c(canada_file, gems_file, danube_file)
required_long_columns <- c(
  "LTER", "Stream_Name", "date", "solute", "solute_units", "amount"
)

provider_key_counts <- data_key %>%
  dplyr::filter(Raw_Filename %in% provider_files) %>%
  dplyr::count(Raw_Filename, name = "rows")

provider_key_expected <- tibble::tribble(
  ~Raw_Filename, ~rows,
  canada_file, 8L,
  gems_file, 8L,
  danube_file, 8L
)

if (
  nrow(provider_key_counts) != 3L ||
    !setequal(provider_key_counts, provider_key_expected) ||
    any(grepl(gems_pattern, data_key$Raw_Filename)) ||
    any(grepl(danube_pattern, data_key$Raw_Filename))
) {
  stop("the reviewed provider data-key file set changed")
}

provider_key_columns <- data_key %>%
  dplyr::filter(Raw_Filename %in% provider_files) %>%
  dplyr::filter(!Standardized_Column_Name %in% c(
    "source_lab_flag",
    "source_method_flag"
  )) %>%
  dplyr::group_by(Raw_Filename) %>%
  dplyr::summarize(
    columns = paste(sort(Standardized_Column_Name), collapse = "|"),
    .groups = "drop"
  )

if (any(provider_key_columns$columns !=
  paste(sort(required_long_columns), collapse = "|"))) {
  stop("the reviewed provider data-key columns changed")
}

unexpected_provider_columns <- data_key %>%
  dplyr::filter(
    Raw_Filename %in% provider_files,
    !Standardized_Column_Name %in% c(
      required_long_columns,
      "source_lab_flag",
      "source_method_flag"
    )
  )

if (nrow(unexpected_provider_columns) > 0L) {
  stop("an unexpected provider data-key column remains")
}

flag_key_rows <- data_key %>%
  dplyr::filter(
    Raw_Filename %in% provider_files,
    Raw_Column_Name == "source_lab_flag",
    Standardized_Column_Name == "source_lab_flag"
  )

method_flag_key_rows <- data_key %>%
  dplyr::filter(
    Raw_Filename %in% provider_files,
    Raw_Column_Name == "source_method_flag",
    Standardized_Column_Name == "source_method_flag"
  )

data_key_output <- data_key

if (
  nrow(flag_key_rows) != 3L ||
    sum(data_key_output$Raw_Column_Name == "source_lab_flag") != 3L ||
    nrow(method_flag_key_rows) != 3L ||
    !setequal(
      method_flag_key_rows$Raw_Filename,
      provider_files
    ) ||
    sum(
      data_key_output$Raw_Filename %in% provider_files &
        data_key_output$Raw_Column_Name == "source_method_flag",
      na.rm = TRUE
    ) != 3L
) {
  stop("the provider lab or method flag key rows changed")
}

# ---- prepared source files ----

data.table::fwrite(
  canada_output,
  file.path(raw_output_dir, canada_file),
  na = ""
)
data.table::fwrite(
  gems_output,
  file.path(raw_output_dir, gems_file),
  na = ""
)
data.table::fwrite(
  danube_output,
  file.path(raw_output_dir, danube_file),
  na = ""
)
