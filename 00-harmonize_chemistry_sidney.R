# ---- chemistry harmonization and wrangling ----
# original script authors: nick j lyon, kathi jo jankowski, and paul julian
# workflow updates: sidney bush
# purpose: combine and harmonize chemistry files into an analysis-ready master file

# ---- setup ----
librarian::shelf(
  googledrive, purrr, readxl, stringr, supportR, tidyverse,
  quiet = TRUE
)
options(dplyr.summarise.inform = FALSE)
script_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
workflow_dir <- if (length(script_argument) == 1L) {
  dirname(normalizePath(sub("^--file=", "", script_argument)))
} else {
  normalizePath(getwd())
}
source(file.path(workflow_dir, "chemistry_rules.R"))
source(file.path(workflow_dir, "chemistry_functions.R"))
use_local_test <- read_boolean_setting("SISYN_CHEM_LOCAL_TEST", FALSE)
download_shared_inputs <- read_boolean_setting("SISYN_CHEM_DOWNLOAD_SHARED", TRUE)
upload_output <- read_boolean_setting("SISYN_CHEM_UPLOAD_OUTPUT", FALSE)
stop_if(upload_output && use_local_test, "output upload is not available during a local test")
stop_if(upload_output && !download_shared_inputs, "output upload requires fresh inputs")
if (use_local_test) {
  root_dir <- read_required_setting("SISYN_CHEM_LOCAL_ROOT")
  raw_input_dir <- Sys.getenv("SISYN_CHEM_LOCAL_INPUT_DIR", file.path(root_dir, "chem_raw_subset"))
} else {
  root_dir <- scicomptools::wd_loc(
    local = FALSE,
    remote_path = file.path("/", "home", "shares", "lter-si", "WRTDS")
  )
  raw_input_dir <- file.path(root_dir, "Master_Chemistry_Inputs_CURRENT")
}

# ---- shared inputs ----
inputs <- load_chemistry_inputs(root_dir, raw_input_dir, download_shared_inputs)
data_key <- inputs$data_key
units_key <- inputs$units_key
site_reference <- inputs$site_reference
raw_files <- inputs$raw_files
source_headers <- inputs$source_headers
output_dir <- inputs$output_dir

# ---- source harmonizing ----
mean_measurement <- function(values) {
  missing_tokens <- unique(c(data_key$NA_indicator, fixed_missing_tokens))
  mean_qualified_measurement(values, missing_tokens)
}
harmonize_source <- function(file_name) {
  key_rows <- data_key |>
    dplyr::filter(
      Raw_Filename == file_name,
      !Standardized_Column_Name %in% c("Dataset", "Raw_Filename"),
      !is.na(Standardized_Column_Name),
      Standardized_Column_Name != ""
    )
  raw_data <- read_selected_source(
    raw_input_dir, file_name, key_rows$Raw_Column_Name, source_headers
  ) |>
    dplyr::select(-dplyr::any_of(c("Dataset", "Raw_Filename")))
  raw_data <- apply_source_review_rules(raw_data, file_name)
  raw_data <- raw_data[unique(key_rows$Raw_Column_Name)]
  keyed_data <- raw_data |>
    dplyr::mutate(
      row_num = dplyr::row_number(),
      Raw_Filename = file_name,
      .before = dplyr::everything()
    ) |>
    dplyr::mutate(dplyr::across(dplyr::everything(), as.character)) |>
    tidyr::pivot_longer(
      -row_num:-Raw_Filename,
      names_to = "Raw_Column_Name",
      values_to = "values"
    ) |>
    dplyr::left_join(key_rows, by = c("Raw_Filename", "Raw_Column_Name")) |>
    dplyr::filter(!is.na(Standardized_Column_Name)) |>
    dplyr::mutate(
      Units = dplyr::na_if(normalize_unit_label(Units), ""),
      unit_suffix = gsub("\\/| |\\-", "_", Units),
      standard_name = dplyr::if_else(
        is.na(unit_suffix),
        Standardized_Column_Name,
        paste0(Standardized_Column_Name, "_", unit_suffix)
      )
    ) |>
    dplyr::select(row_num, Dataset, Raw_Filename, standard_name, values) |>
    tidyr::pivot_wider(names_from = standard_name, values_from = values) |>
    dplyr::select(-row_num) |>
    dplyr::distinct()
  if (!identical(unique(key_rows$Dataset_orientation), "long")) {
    return(keyed_data)
  }
  source_units <- units_key |>
    dplyr::filter(Raw_Filename == file_name) |>
    dplyr::mutate(
      Units = gsub("/| |\\-", "_", normalize_unit_label(Units)),
      solute = tolower(gsub("/| |\\-", "_", Variable))
    ) |>
    dplyr::distinct(Raw_Filename, solute, Units)
  if (file_name == deduplicated_filename) {
    source_units <- dplyr::bind_rows(source_units, deduplicated_raw_aliases) |>
      dplyr::distinct(Raw_Filename, solute, Units)
  }
  has_row_units <- "solute_units" %in% names(keyed_data)
  if (!has_row_units) {
    keyed_data$solute_units <- NA_character_
  }
  long_rows <- keyed_data |>
    dplyr::mutate(
      solute = tolower(gsub("/| |\\-", "_", solute)),
      solute_units = gsub("/| |\\-", "_", normalize_unit_label(solute_units)),
      solute_units = dplyr::na_if(solute_units, "")
    ) |>
    dplyr::group_by(dplyr::across(-amount)) |>
    dplyr::summarise(amount = mean_measurement(amount), .groups = "drop")
  if (file_name == deduplicated_filename) {
    long_rows$solute_units[
      long_rows$solute == "nh4_mgl" & long_rows$solute_units == "mg_L"
    ] <- "mg_NH4_L"
  }
  unit_counts <- source_units |>
    dplyr::count(Raw_Filename, solute, name = "unit_count")
  keyed_rows <- long_rows |>
    dplyr::filter(solute %in% source_units$solute)
  joined_units <- keyed_rows |>
    dplyr::left_join(unit_counts, by = c("Raw_Filename", "solute")) |>
    dplyr::left_join(
      source_units,
      by = c("Raw_Filename", "solute"),
      relationship = "many-to-many"
    ) |>
    dplyr::filter(
      (unit_count == 1L & (
        !has_row_units |
          (is.na(solute_units) & (is.na(Units) | Units == "")) |
          (!is.na(solute_units) & solute_units == Units)
      )) |
        (unit_count > 1L & has_row_units & solute_units == Units)
    )
  stop_if(
    nrow(joined_units) != nrow(keyed_rows),
    paste0("row-level units did not resolve in '", file_name, "'")
  )
  joined_units |>
    dplyr::mutate(
      standard_name = dplyr::if_else(
        is.na(Units) | Units == "",
        solute,
        paste0(solute, "_", Units)
      )
    ) |>
    dplyr::select(-solute, -solute_units, -Units, -unit_count) |>
    tidyr::pivot_wider(
      names_from = standard_name,
      values_from = amount,
      values_fn = mean_measurement
    )
}
chemistry_wide <- purrr::map(raw_files, harmonize_source) |>
  dplyr::bind_rows()
if (!"source_lab_flag" %in% names(chemistry_wide)) {
  chemistry_wide$source_lab_flag <- NA_character_
}
if (!"source_method_flag" %in% names(chemistry_wide)) {
  chemistry_wide$source_method_flag <- NA_character_
}
chemistry_wide <- chemistry_wide |>
  dplyr::relocate(source_lab_flag, source_method_flag, .before = date)

# ---- qualifiers and units ----
chemistry_wide <- chemistry_wide |>
  dplyr::mutate(
    LTER = dplyr::coalesce(LTER, Dataset),
    Dataset = dplyr::coalesce(Dataset, LTER),
    Stream_Name = dplyr::case_when(
      Raw_Filename == "NigerRiver.csv" & is.na(Stream_Name) ~ "Niger",
      Raw_Filename == "ElbeRiverChem.csv" ~ "Elbe",
      TRUE ~ Stream_Name
    )
  ) |>
  dplyr::select(-dplyr::ends_with("_NA"), -dplyr::any_of("Dataset_orientation")) |>
  drop_empty_columns() |>
  dplyr::mutate(row_num = dplyr::row_number(), .before = dplyr::everything())
id_columns <- names(chemistry_wide)[seq_len(match("date", names(chemistry_wide)))]
chemistry_long <- chunked_pivot_longer(
  chemistry_wide,
  id_columns,
  "solute",
  "measurement"
) |>
  dplyr::mutate(
    measurement = dplyr::case_when(
      trimws(measurement) %in% unique(c(
        data_key$NA_indicator,
        fixed_missing_tokens
      )) ~ NA_character_,
      suppressWarnings(as.numeric(trimws(measurement))) == -9999 ~ NA_character_,
      TRUE ~ measurement
    )
  ) |>
  dplyr::filter(!is.na(measurement), measurement != "")
rm(chemistry_wide)
unparsed <- chemistry_long |>
  dplyr::filter(is.na(suppressWarnings(as.complex(
    stringr::str_remove(measurement, "^\\s*[<>]\\s*")
  ))))
stop_if(
  nrow(unparsed) > 0L,
  paste("unparsed chemistry values remain:", paste(unique(unparsed$measurement), collapse = ", "))
)
# store the reported less-than or greater-than sign while units are converted
chemistry_wide <- chemistry_long |>
  dplyr::mutate(
    value = as.complex(stringr::str_remove(measurement, "^\\s*[<>]\\s*")) +
      (stringr::str_detect(measurement, "^\\s*<") -
         stringr::str_detect(measurement, "^\\s*>")) * 1i
  ) |>
  dplyr::select(-measurement) |>
  dplyr::arrange(solute) |>
  tidyr::pivot_wider(names_from = solute, values_from = value) |>
  dplyr::select(-row_num)
rm(chemistry_long, unparsed)
stop_if_columns_have_values(
  chemistry_wide,
  c(
    "ammonia_n_mg_L", "nh4_mgl_mg_L", "no3_mg_L", "no2_mg_L",
    "nox_mg_L", "po4_mg_L", "srp_mg_L", "tn_mg_L", "tn_ug_L",
    "tdn_mg_L", "tkn_mg_L", "tp_mg_L", "tp_ug_L", "tdp_mg_L", "don_mg_L"
  ),
  "nutrient mass units are missing an N or P reporting basis"
)
stop_if_columns_have_values(
  chemistry_wide,
  c(
    "doc_mg_L", "doc_mgl_mg_L", "carbon_dissolved_organic_mg_L",
    "dic_mg_L", "toc_mg_L", "toc_mgl_mg_L", "so4_mg_L", "so4_ug_L"
  ),
  "carbon or sulfate mass units are missing a C, SO4, or S reporting basis"
)
# use field measurements first and keep laboratory values as a fallback
chemistry_wide$pH <- dplyr::if_else(
  chemistry_wide$Dataset == "ColoradoAlpine",
  first_present_value(
    chemistry_wide,
    c("fldph", "fldph_", "labph", "labph_", ph_columns)
  ),
  first_present_value(chemistry_wide, ph_columns)
)
chemistry_wide <- chemistry_wide |>
  dplyr::select(-dplyr::any_of(setdiff(ph_columns, "pH")))
chemistry_wide$conductivity_uS_cm <- dplyr::case_when(
  chemistry_wide$Dataset == "Guadeloupe" ~ first_present_value(
    chemistry_wide, "conductivity_at_25°c_uS_cm"
  ),
  chemistry_wide$Dataset == "ColoradoAlpine" ~ first_present_value(
    chemistry_wide, c("fldcond_uscm_uS_cm", "labcond_uscm_uS_cm")
  ),
  TRUE ~ first_present_value(chemistry_wide, conductivity_columns)
)
chemistry_wide <- chemistry_wide |>
  dplyr::select(-dplyr::any_of(setdiff(conductivity_columns, "conductivity_uS_cm"))) |>
  apply_merge_rules(merge_rules)
for (column in optional_measurement_columns) {
  if (!column %in% names(chemistry_wide)) {
    chemistry_wide[[column]] <- NA_complex_
  }
}
# convert each reported chemical basis to the standard output unit
chemistry_wide <- apply_conversion_rules(chemistry_wide, conversion_rules)
east_rows <- chemistry_wide$Raw_Filename == "EastRiverSFA_chem.csv"
stop_if(
  !any(east_rows & !is.na(chemistry_wide$nhx_uM)) ||
    any(east_rows & !is.na(chemistry_wide$nh4_uM)),
  "East River ammonia-N was not retained as combined ammonia"
)

# ---- variables, sites, and dates ----
chemistry_wide <- merge_columns(
  chemistry_wide,
  "susp_partic_matter_mg_L",
  c("spm_mg_L", "tss_mg_L")
) |>
  dplyr::select(-dplyr::contains("_q_"))
stop_if(
  anyDuplicated(names(chemistry_wide)) > 0L,
  "duplicate column names remain before the final long-format reshape"
)
id_columns <- names(chemistry_wide)[seq_len(match("date", names(chemistry_wide)))]
chemistry_long <- chunked_pivot_longer(
  chemistry_wide,
  id_columns,
  "raw_variable",
  "value"
) |>
  dplyr::mutate(
    value_qualifier = dplyr::case_when(
      Im(value) > 0 ~ "<",
      Im(value) < 0 ~ ">",
      TRUE ~ NA_character_
    ),
    value = Re(value),
    variable_units = stringr::str_replace(
      raw_variable,
      "_(uM|ueq_L|uS_cm|ug(?:_[A-Za-z0-9]+)*_L|mg(?:_[A-Za-z0-9]+)*_L|PCU|C|NTU|FNU|percent|ppt)$",
      "--\\1"
    )
  ) |>
  tidyr::separate_wider_delim(
    variable_units,
    delim = "--",
    names = c("raw_name", "raw_units"),
    too_few = "align_start"
  ) |>
  dplyr::mutate(
    units = gsub("_", "/", raw_units),
    variable = dplyr::coalesce(
      unname(variable_names[raw_name]),
      dplyr::case_when(
        nchar(raw_name) == 1 ~ toupper(raw_name),
        nchar(raw_name) == 2 ~ stringr::str_to_title(raw_name),
        nchar(raw_name) < 4 & stringr::str_detect(raw_name, "x") ~ toupper(raw_name),
        stringr::str_detect(raw_name, "[[:digit:]]") ~ toupper(raw_name),
        TRUE ~ gsub("_", " ", raw_name)
      )
    ),
    variable = gsub("Ph", "pH", variable),
    variable = gsub("Ec", "EC", variable),
    variable = gsub("NOX", "NOx", variable)
  ) |>
  dplyr::select(-raw_variable, -raw_name, -raw_units) |>
  dplyr::relocate(value, value_qualifier, .after = dplyr::everything())
rm(chemistry_wide)
drop_nitrogen <- with(
  chemistry_long,
  (LTER %in% drop_no3_lters & variable == "NO3") |
    (Stream_Name %in% drop_nox_streams & variable == "NOx") |
    (Stream_Name %in% drop_no3_streams & variable == "NO3") |
    (
      Raw_Filename == "CAMELS_USGS_N_P.csv" &
        Stream_Name %in% drop_camels_no3_streams & variable == "NO3"
    )
)
drop_phosphorus <- with(
  chemistry_long,
  (LTER %in% drop_srp_lters & variable == "SRP") |
    (Stream_Name %in% drop_srp_streams & variable == "SRP")
)
# keep the reviewed source when two files cover the same chemical series
chemistry_long <- chemistry_long |>
  dplyr::filter(!drop_nitrogen, !drop_phosphorus) |>
  dplyr::filter(!(
    Stream_Name == "Sopchoppy River" & Raw_Filename %in% sopchoppy_superseded_files
  )) |>
  dplyr::mutate(
    Stream_Name = dplyr::if_else(
      Raw_Filename == "CAMELS_USGS_N_P.csv" & Stream_Name == "Sopchoppy River",
      "SOPCHOPPY RIVER",
      Stream_Name
    )
  ) |>
  dplyr::filter(!(
    Stream_Name == "SOPCHOPPY RIVER" &
      Raw_Filename == "20221030_masterdata_chem_V2.csv" &
      variable %in% sopchoppy_legacy_variables
  ))
rm(drop_nitrogen, drop_phosphorus)
name_lookup <- dplyr::bind_rows(
  site_reference |>
    dplyr::transmute(
      target_lter = LTER,
      target_name = Stream_Name,
      source_name = Stream_Name
    ),
  site_reference |>
    dplyr::transmute(
      target_lter = LTER,
      target_name = Stream_Name,
      source_name = Alt_Stream_Name
    ) |>
    tidyr::separate_longer_delim(source_name, delim = ";"),
  site_reference |>
    dplyr::transmute(
      target_lter = LTER,
      target_name = Stream_Name,
      source_name = Original_Stream_Name
    )
) |>
  dplyr::filter(!is.na(source_name), trimws(source_name) != "") |>
  dplyr::mutate(name_key = stringr::str_to_lower(stringr::str_squish(source_name))) |>
  dplyr::distinct(name_key, target_lter, target_name)
stop_if(
  anyDuplicated(name_lookup$name_key) > 0L,
  "the site reference contains ambiguous primary or alternate names"
)
chemistry_long <- chemistry_long |>
  dplyr::mutate(name_key = stringr::str_to_lower(stringr::str_squish(Stream_Name))) |>
  dplyr::left_join(name_lookup, by = "name_key")
unmatched_sites <- chemistry_long |>
  dplyr::filter(is.na(target_name)) |>
  dplyr::group_by(Raw_Filename, Stream_Name) |>
  dplyr::summarise(has_dsi = any(variable == "DSi"), .groups = "drop")
stop_if(
  nrow(dplyr::anti_join(
    unmatched_sites, reviewed_unmatched_sites,
    by = c("Raw_Filename", "Stream_Name")
  )) > 0L ||
    nrow(dplyr::anti_join(
      reviewed_unmatched_sites, unmatched_sites,
      by = c("Raw_Filename", "Stream_Name")
    )) > 0L,
  "the reviewed unmatched-site set changed"
)
unmatched_sites <- unmatched_sites |>
  dplyr::left_join(reviewed_unmatched_sites, by = c("Raw_Filename", "Stream_Name"))
stop_if(
  any(unmatched_sites$reason == "no_dsi" & unmatched_sites$has_dsi) ||
    any(unmatched_sites$reason == "not_river" & !unmatched_sites$has_dsi),
  "the reviewed unmatched-site scope changed"
)
chemistry_long <- chemistry_long |>
  dplyr::anti_join(reviewed_unmatched_sites, by = c("Raw_Filename", "Stream_Name"))
stop_if(
  anyNA(chemistry_long$target_name),
  "unreviewed site names are missing from the reference table"
)
chemistry_long <- chemistry_long |>
  dplyr::mutate(Stream_Name = target_name, LTER = target_lter) |>
  dplyr::select(-name_key, -target_name, -target_lter) |>
  dplyr::distinct() |>
  dplyr::mutate(
    date_text = gsub(" [[:digit:]]{1,2}\\:[[:digit:]]{1,2}", "", date),
    date_text = gsub("\\_|\\-", "/", date_text),
    date_text = gsub("\\:00", "", date_text)
  )
date_tokens <- stringr::str_split_fixed(chemistry_long$date_text, "/", 3)
year_token_count <- rowSums(
  apply(date_tokens, 2, stringr::str_detect, "^[0-9]{4}$")
)
stop_if(
  any(year_token_count != 1L),
  "chemistry dates must contain exactly one four-digit year token"
)
date_formats <- chemistry_long |>
  dplyr::group_by(Raw_Filename) |>
  dplyr::summarise(date_format = detect_date_format(date_text), .groups = "drop")
stop_if(
  any(!date_formats$date_format %in% c("ymd", "mdy", "dmy")),
  "one or more source files have an ambiguous date format"
)
chemistry_long <- chemistry_long |>
  dplyr::left_join(date_formats, by = "Raw_Filename") |>
  tidyr::separate_wider_delim(
    date_text,
    delim = "/",
    names = c("date1", "date2", "date3"),
    too_many = "merge"
  ) |>
  dplyr::mutate(
    dplyr::across(date1:date3, as.numeric),
    day = dplyr::case_when(
      date_format == "ymd" ~ date3,
      date_format == "mdy" ~ date2,
      TRUE ~ date1
    ),
    month = dplyr::case_when(
      date_format == "ymd" ~ date2,
      date_format == "mdy" ~ date1,
      TRUE ~ date2
    ),
    year = dplyr::case_when(
      date_format == "ymd" ~ date1,
      TRUE ~ date3
    ),
    date = as.Date(paste(day, month, year, sep = "-"), "%d-%m-%Y")
  ) |>
  dplyr::select(-date1, -date2, -date3, -day, -month, -year, -date_format)
stop_if(anyNA(chemistry_long$date), "unparsed or missing chemistry dates remain")

# ---- daily master chemistry ----
stop_if(
  any(
    !is.na(chemistry_long$source_lab_flag) &
      chemistry_long$source_lab_flag != "" &
      !chemistry_long$source_lab_flag %in% allowed_source_lab_flags
  ),
  "unsupported source lab flags remain"
)
for (row in seq_len(nrow(source_method_exclusions))) {
  remove_rows <- chemistry_long$source_method_flag ==
    source_method_exclusions$source_method_flag[[row]]
  remove_rows[is.na(remove_rows)] <- FALSE
  check_reviewed_count(
    sum(remove_rows),
    source_method_exclusions$allowed_counts[[row]],
    "a reviewed source-method count changed"
  )
  chemistry_long <- chemistry_long[!remove_rows, , drop = FALSE]
}
stop_if(
  any(
    !is.na(chemistry_long$source_method_flag) &
      chemistry_long$source_method_flag != "" &
      !chemistry_long$source_method_flag %in% allowed_source_method_flags
  ),
  "unsupported source method flags remain"
)
# average detected replicates and keep limits only when every result is limited
daily_rows <- chemistry_long |>
  dplyr::filter(!is.na(value)) |>
  dplyr::group_by(Dataset, Raw_Filename, LTER, Stream_Name, date, variable, units) |>
  dplyr::summarise(
    detected = sum(is.na(value_qualifier)),
    below = sum(value_qualifier == "<", na.rm = TRUE),
    above = sum(value_qualifier == ">", na.rm = TRUE),
    value = if (detected > 0L) mean(value[is.na(value_qualifier)]) else mean(value),
    value_qualifier = if (detected > 0L) NA_character_ else dplyr::first(value_qualifier),
    source_lab_flag = reduce_daily_flag(source_lab_flag, "source lab"),
    source_method_flag = reduce_daily_flag(source_method_flag, "source method"),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    calculated_lab_flag = purrr::pmap_chr(
      list(detected, below, above),
      daily_lab_flag
    )
  )
stop_if(
  any(
    !is.na(daily_rows$calculated_lab_flag) &
      !is.na(daily_rows$source_lab_flag) &
      daily_rows$calculated_lab_flag != daily_rows$source_lab_flag
  ),
  "source and qualifier-derived lab flags conflict"
)
daily_rows <- daily_rows |>
  dplyr::mutate(Lab_Flag = dplyr::coalesce(calculated_lab_flag, source_lab_flag)) |>
  dplyr::select(-detected, -below, -above, -calculated_lab_flag, -source_lab_flag)
preferred_rows <- daily_rows |>
  dplyr::filter(Raw_Filename == deduplicated_filename)
preferred_series <- preferred_rows |>
  dplyr::distinct(LTER, Stream_Name, variable)
master_chemistry <- daily_rows |>
  dplyr::filter(Raw_Filename != deduplicated_filename) |>
  dplyr::anti_join(preferred_series, by = c("LTER", "Stream_Name", "variable")) |>
  dplyr::bind_rows(preferred_rows) |>
  dplyr::distinct()
rm(chemistry_long, daily_rows, preferred_rows, preferred_series)
# exclude silicon methods that did not measure filtered dissolved water
extractable_si <- master_chemistry$variable == "DSi" &
  master_chemistry$Raw_Filename == canada_extractable_filename
check_reviewed_count(
  sum(extractable_si),
  canada_extractable_allowed_counts,
  "the reviewed Canada extractable-silicon count changed"
)
unfiltered_hbr_si <- master_chemistry$LTER == "HBR" &
  master_chemistry$variable == "DSi" &
  master_chemistry$date < hbr_filter_start
check_reviewed_count(
  sum(unfiltered_hbr_si),
  hbr_unfiltered_si_count,
  "the reviewed Hubbard Brook unfiltered-silicon count changed"
)
master_chemistry <- master_chemistry[!extractable_si & !unfiltered_hbr_si, ]
master_chemistry <- master_chemistry |>
  dplyr::left_join(
    date_notes,
    by = c("Dataset", "Raw_Filename", "LTER", "Stream_Name", "date")
  ) |>
  dplyr::relocate(Date_Flag, Date_Note, .after = date) |>
  dplyr::mutate(Method_Flag = dplyr::na_if(source_method_flag, ""))
flagged_date_events <- master_chemistry |>
  dplyr::filter(!is.na(Date_Flag)) |>
  dplyr::distinct(Dataset, Raw_Filename, LTER, Stream_Name, date)
stop_if(
  nrow(dplyr::anti_join(
    dplyr::select(date_notes, Dataset, Raw_Filename, LTER, Stream_Name, date),
    flagged_date_events,
    by = c("Dataset", "Raw_Filename", "LTER", "Stream_Name", "date")
  )) > 0L,
  "a reviewed inferred-date event is missing before site filtering"
)
canada_site_si <- master_chemistry$variable == "DSi" &
  grepl("^Canada__Canada__", master_chemistry$Raw_Filename)
hbr_filtered_si <- master_chemistry$LTER == "HBR" &
  master_chemistry$variable == "DSi" &
  master_chemistry$date >= hbr_filter_start
master_chemistry$Method_Flag[
  is.na(master_chemistry$Method_Flag) & canada_site_si
] <- "dissolved silicon, filtered"
master_chemistry$Method_Flag[
  is.na(master_chemistry$Method_Flag) & hbr_filtered_si
] <- "filtered Si, 0.7 micrometer GF/F"
master_chemistry <- master_chemistry |>
  dplyr::left_join(
    dplyr::select(method_rules, Method_Flag, Method_Note),
    by = "Method_Flag"
  ) |>
  dplyr::select(-source_method_flag) |>
  dplyr::relocate(Method_Flag, Method_Note, .after = Lab_Flag)
stop_if(
  anyNA(master_chemistry$date) || anyNA(master_chemistry$value) ||
    any(!is.finite(master_chemistry$value)),
  "missing or nonfinite dates or values remain"
)
stop_if(
  any(is.na(master_chemistry$Date_Flag) != is.na(master_chemistry$Date_Note)) ||
    any(is.na(master_chemistry$Method_Flag) != is.na(master_chemistry$Method_Note)),
  "sample flags and notes do not match"
)
stop_if(
  any(!is.na(master_chemistry$value_qualifier) &
        !master_chemistry$value_qualifier %in% allowed_value_qualifiers) ||
    any(!is.na(master_chemistry$Lab_Flag) &
          !master_chemistry$Lab_Flag %in% valid_lab_flags) ||
    any(!is.na(master_chemistry$Date_Flag) &
          !master_chemistry$Date_Flag %in% allowed_date_flags) ||
    any(!is.na(master_chemistry$Method_Flag) &
          !master_chemistry$Method_Flag %in% valid_method_flags),
  "unsupported sample flags remain"
)
for (row in seq_len(nrow(method_rules))) {
  wrong_variable <- master_chemistry$Method_Flag == method_rules$Method_Flag[[row]] &
    !master_chemistry$variable %in% method_rules$expected_variables[[row]]
  stop_if(any(wrong_variable, na.rm = TRUE), "a method flag is attached to another variable")
}
stop_if(
  any(master_chemistry$value_qualifier == "<" &
        master_chemistry$Lab_Flag != "below reported limit", na.rm = TRUE) ||
    any(master_chemistry$value_qualifier == ">" &
          master_chemistry$Lab_Flag != "above reported limit", na.rm = TRUE) ||
    any(!is.na(master_chemistry$value_qualifier) & master_chemistry$value <= 0),
  "value qualifiers and lab flags do not match"
)
stop_if(
  anyDuplicated(master_chemistry) > 0L ||
    any(master_chemistry$variable != "pH" &
          (is.na(master_chemistry$units) | trimws(master_chemistry$units) == "")),
  "duplicate rows or blank units remain"
)
stop_if(
  any(master_chemistry$variable == "tdp" & master_chemistry$units != "uM", na.rm = TRUE),
  "nonstandard total dissolved phosphorus units remain"
)
stop_if(
  any(
    master_chemistry$variable == "susp partic matter" &
      master_chemistry$units != "mg/L",
    na.rm = TRUE
  ),
  "nonstandard suspended particulate matter units remain"
)
master_chemistry <- dplyr::anti_join(
  master_chemistry,
  arctic_lake_events,
  by = c("LTER", "Stream_Name", "date")
)
si_sites <- master_chemistry |>
  dplyr::filter(variable == "DSi") |>
  dplyr::distinct(LTER, Stream_Name)
# keep only flowing-water sites that also have dissolved silicon data
included_sites <- site_reference |>
  dplyr::filter(Waterbody %in% flowing_types, Include_in_Master_Chem == "Yes") |>
  dplyr::distinct(LTER, Stream_Name) |>
  dplyr::semi_join(si_sites, by = c("LTER", "Stream_Name"))
master_chemistry <- dplyr::semi_join(
  master_chemistry,
  included_sites,
  by = c("LTER", "Stream_Name")
)
stop_if(nrow(master_chemistry) == 0L, "no chemistry rows remain after site checks")
# carry site notes onto every row and combine the sample-level flags
master_chemistry <- add_site_and_sample_flags(master_chemistry, site_reference)
for (row in seq_len(nrow(method_rules))) {
  if (!is.na(method_rules$expected_count[[row]])) {
    check_reviewed_count(
      sum(master_chemistry$Method_Flag == method_rules$Method_Flag[[row]], na.rm = TRUE),
      method_rules$expected_count[[row]],
      "a reviewed method count changed"
    )
  }
}
stop_if(
  any(master_chemistry$variable == "DSi" &
        master_chemistry$Raw_Filename == canada_extractable_filename) ||
    any(master_chemistry$LTER == "HBR" & master_chemistry$variable == "DSi" &
          master_chemistry$date < hbr_filter_start),
  "excluded silicon methods entered the final master"
)
release_counts <- c(
  inferred_date_rows = sum(!is.na(master_chemistry$Date_Flag)),
  less_than_rows = sum(master_chemistry$value_qualifier == "<", na.rm = TRUE),
  greater_than_rows = sum(master_chemistry$value_qualifier == ">", na.rm = TRUE),
  mixed_below_daily_rows = sum(
    master_chemistry$Lab_Flag == "below-limit results excluded from daily mean",
    na.rm = TRUE
  ),
  final_rows = nrow(master_chemistry),
  final_sites = dplyr::n_distinct(master_chemistry$LTER, master_chemistry$Stream_Name),
  final_silicon_sites = dplyr::n_distinct(
    master_chemistry$LTER[master_chemistry$variable == "DSi"],
    master_chemistry$Stream_Name[master_chemistry$variable == "DSi"]
  ),
  final_silicon_rows = sum(master_chemistry$variable == "DSi")
)
stop_if(
  !identical(release_counts, expected_release_counts),
  "one or more reviewed final chemistry counts changed"
)

# ---- output ----
output_file <- file.path(output_dir, paste0(gsub("-", "", Sys.Date()), "_masterdata_chem.csv"))
write.csv(master_chemistry, output_file, na = "", row.names = FALSE)
if (upload_output) {
  googledrive::drive_upload(
    media = output_file,
    overwrite = TRUE,
    path = googledrive::as_id(
      "https://drive.google.com/drive/u/0/folders/1dTENIB5W2ClgW0z-8NbjqARiaGO2_A7W"
    )
  )
}
