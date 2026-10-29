# ---- combine chemistry files ----
# original script authors: nick j lyon, kathi jo jankowski, and paul julian
# workflow updates: sidney bush
# combine chemistry files using the shared variable names and units
# read the 77 files created by the preparation scripts
# leave the downloaded chemistry and original source files unchanged
# open the full chemistry runner in rstudio and click source
# that runner calls this script after preparing the working inputs

# ---- setup ----
librarian::shelf(
  purrr, readxl, stringr, tidyverse,
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
prepared_workflow_run <- read_boolean_setting(
  "SISYN_CHEM_INTERNAL_PREPARED_RUN",
  FALSE
)
stop_if(
  !prepared_workflow_run,
  paste(
    "run run_full_chemistry_workflow.R so the source-review rules create",
    "fresh working inputs before chemistry is harmonized"
  )
)
root_dir <- read_required_setting("SISYN_CHEM_LOCAL_ROOT")
run_input_dir <- Sys.getenv(
  "SISYN_CHEM_LOCAL_INPUT_DIR",
  file.path(root_dir, "chem_raw_subset")
)

# ---- shared inputs ----
inputs <- load_chemistry_inputs(root_dir, run_input_dir)
data_key <- inputs$data_key
units_key <- inputs$units_key
site_reference <- inputs$site_reference
method_rules <- inputs$method_rules
chemistry_files <- inputs$chemistry_files
source_headers <- inputs$source_headers
output_dir <- inputs$output_dir
hbr_rules <- method_rules |>
  dplyr::filter(
    rule_lter == "HBR",
    vapply(expected_variables, function(x) "DSi" %in% x, logical(1)),
    !is.na(start_date)
  )
stop_if(
  nrow(hbr_rules) != 1L || !is.na(hbr_rules$end_date[[1]]),
  "the workflow-rules lookup needs one open-ended HBR dissolved-Si rule"
)
hbr_documented_start <- hbr_rules$start_date[[1]]
rm(hbr_rules)
allowed_source_method_flags <- method_rules$source_method_flag[
  !is.na(method_rules$source_method_flag)
]
valid_method_flags <- method_rules$Method_Flag

# ---- prepare each source ----
# process each source separately to avoid one large table of mostly empty columns
standardize_source <- function(file_name) {
  raw_samples <- harmonize_source(file_name, data_key, units_key, run_input_dir, source_headers)
  has_repeats <- isTRUE(attr(raw_samples, "has_repeated_samples"))
  samples <- standardize_source_data(raw_samples, data_key) |>
    dplyr::mutate(averaging_input = "samples")
  if (!has_repeats) return(samples)
  # keep the old grouping available so unaffected daily results do not change
  existing <- harmonize_source(file_name, data_key, units_key, run_input_dir, source_headers,
    keep_repeats = FALSE) |>
    standardize_source_data(data_key) |>
    dplyr::mutate(averaging_input = "existing")
  dplyr::bind_rows(samples, existing)
}

# ---- variables, sites, and dates ----
chemistry_long <- purrr::map(chemistry_files, standardize_source) |>
  dplyr::bind_rows()
# compare nutrient forms only at the same site on the same date
site_day <- paste(
  chemistry_long$LTER,
  chemistry_long$Stream_Name,
  chemistry_long$date,
  sep = "\r"
)
has_no3 <- site_day %in% site_day[chemistry_long$variable == "NO3"]
has_nox <- site_day %in% site_day[chemistry_long$variable == "NOx"]
has_po4 <- site_day %in% site_day[chemistry_long$variable == "PO4"]
drop_nitrogen <- with(
  chemistry_long,
  (LTER %in% drop_no3_lters & variable == "NO3" & has_nox) |
    (Stream_Name %in% drop_nox_streams & variable == "NOx" & has_no3) |
    (Stream_Name %in% drop_no3_streams & variable == "NO3" & has_nox) |
    (
      Raw_Filename == "CAMELS_USGS_N_P.csv" &
        Stream_Name %in% drop_camels_no3_streams & variable == "NO3" & has_nox
    )
)
drop_phosphorus <- with(
  chemistry_long,
  (
    (LTER %in% drop_srp_lters | Stream_Name %in% drop_srp_streams) &
      variable == "SRP" & has_po4
  )
)
# keep the reviewed nutrient form only where both occur on the same day
chemistry_long <- chemistry_long |>
  dplyr::filter(!drop_nitrogen, !drop_phosphorus) |>
  dplyr::mutate(
    Stream_Name = dplyr::if_else(
      Raw_Filename == "CAMELS_USGS_N_P.csv" & Stream_Name == "Sopchoppy River",
      "SOPCHOPPY RIVER",
      Stream_Name
    )
  )
rm(
  site_day, has_no3, has_nox, has_po4,
  drop_nitrogen, drop_phosphorus
)
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

# choose individual samples only where the half-limit rule is needed
chemistry_long <- select_daily_inputs(chemistry_long)

# retain unverified seine si with a flag and remove confirmed unfiltered si
chemistry_long <- review_seine_si(chemistry_long, site_reference,
  file.path(root_dir, "keys", "seine_silica_provider_20261022.rds"))

# ---- daily master chemistry ----
stop_if(
  any(
    !is.na(chemistry_long$source_lab_flag) &
      chemistry_long$source_lab_flag != "" &
      !lab_flags_supported(chemistry_long$source_lab_flag, allowed_source_lab_flags)
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
unknown_method_flags <- unsupported_method_flags(
  chemistry_long$source_method_flag,
  allowed_source_method_flags
)
stop_if(
  length(unknown_method_flags) > 0L,
  paste("unsupported source method flags:", paste(unknown_method_flags, collapse = ", "))
)
# daily averages are calculated after unit conversion and date checks
# include less-than samples at half their threshold and count them in the flag
# prepared daily estimates are already substituted and are not halved again
# greater-than handling stays unchanged
daily_rows <- chemistry_long |>
  dplyr::filter(!is.na(value)) |>
  dplyr::group_by(Dataset, Raw_Filename, LTER, Stream_Name, date, variable, units) |>
  dplyr::summarise(
    Lab_Flag = daily_lab_flag(value_qualifier, source_lab_flag),
    value = daily_limit_mean(value, value_qualifier),
    value_qualifier = if (all(!is.na(value_qualifier) & value_qualifier == ">")) ">" else NA_character_,
    source_method_flag = reduce_daily_flag(source_method_flag, "source method"),
    .groups = "drop"
  ) |>
  dplyr::relocate(Lab_Flag, .after = source_method_flag)
# use the combined file only on dates it actually covers
preferred_rows <- daily_rows |>
  dplyr::filter(Raw_Filename == deduplicated_filename)
preferred_keys <- preferred_rows |>
  dplyr::distinct(LTER, Stream_Name, date, variable, units)
master_chemistry <- daily_rows |>
  dplyr::filter(Raw_Filename != deduplicated_filename) |>
  dplyr::anti_join(
    preferred_keys,
    by = c("LTER", "Stream_Name", "date", "variable", "units")
  ) |>
  dplyr::bind_rows(preferred_rows) |>
  dplyr::distinct()
rm(chemistry_long, daily_rows, preferred_rows, preferred_keys)
# exclude three hbr si values that predate the documented dissolved series
hbr_si_before_documented_period <- master_chemistry$LTER == "HBR" &
  master_chemistry$variable == "DSi" &
  master_chemistry$date < hbr_documented_start
check_reviewed_count(
  sum(hbr_si_before_documented_period),
  hbr_undocumented_si_count,
  "the reviewed Hubbard Brook Si count before the documented period changed"
)
master_chemistry <- master_chemistry[!hbr_si_before_documented_period, ]
# record the documented one-day camrex date correction in the final rows
camrex_corrected <- master_chemistry$Raw_Filename ==
  "CAMREX_filled_template.csv" |
  (
    master_chemistry$Raw_Filename == "Manacapuru_chemistry_combined.csv" &
      master_chemistry$date %in% camrex_manacapuru_dates
  )
check_reviewed_count(
  sum(camrex_corrected),
  camrex_corrected_rows,
  "the reviewed CAMREX corrected-date row count changed"
)
camrex_date_notes <- master_chemistry[camrex_corrected, ] |>
  dplyr::distinct(Dataset, Raw_Filename, LTER, Stream_Name, date) |>
  dplyr::mutate(
    Date_Flag = "corrected",
    Date_Note = camrex_date_note
  )
check_reviewed_count(
  nrow(camrex_date_notes),
  camrex_corrected_events,
  "the reviewed CAMREX corrected-date event count changed"
)
date_notes <- dplyr::bind_rows(date_notes, camrex_date_notes)
master_chemistry <- master_chemistry |>
  dplyr::left_join(
    date_notes,
    by = c("Dataset", "Raw_Filename", "LTER", "Stream_Name", "date")
  ) |>
  dplyr::relocate(Date_Flag, Date_Note, .after = date) |>
  dplyr::mutate(source_method_flag = dplyr::na_if(source_method_flag, "")) |>
  map_source_method_flags(method_rules) |>
  apply_interval_method_rules(method_rules)
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
master_chemistry <- master_chemistry |>
  add_method_notes(method_rules) |>
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
          !lab_flags_supported(master_chemistry$Lab_Flag, valid_lab_flags)) ||
    any(!is.na(master_chemistry$Date_Flag) &
          !master_chemistry$Date_Flag %in% allowed_date_flags) ||
    !source_method_flags_supported(
      master_chemistry$Method_Flag,
      valid_method_flags
    ),
  "unsupported sample flags remain"
)
for (row in seq_len(nrow(method_rules))) {
  has_flag <- method_flag_present(
    master_chemistry$Method_Flag,
    method_rules$Method_Flag[[row]]
  )
  outside_scope <- !master_chemistry$variable %in% method_rules$expected_variables[[row]]
  if (!is.na(method_rules$rule_lter[[row]])) {
    outside_scope <- outside_scope | master_chemistry$LTER != method_rules$rule_lter[[row]]
  }
  if (!is.na(method_rules$start_date[[row]])) {
    outside_scope <- outside_scope | master_chemistry$date < method_rules$start_date[[row]]
  }
  if (!is.na(method_rules$end_date[[row]])) {
    outside_scope <- outside_scope | master_chemistry$date > method_rules$end_date[[row]]
  }
  wrong_pairs <- master_chemistry[which(has_flag & outside_scope), c("Raw_Filename", "variable")] |>
    dplyr::distinct()
  stop_if(
    nrow(wrong_pairs) > 0L,
    paste(
      "method flag", method_rules$Method_Flag[[row]], "is outside its variable, network, or date rule:",
      paste(paste(wrong_pairs$Raw_Filename, wrong_pairs$variable, sep = " / "), collapse = ", ")
    )
  )
}
stop_if(
  any(master_chemistry$value_qualifier == "<", na.rm = TRUE) ||
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
arctic_lake_rows <- master_chemistry |>
  dplyr::semi_join(
    arctic_lake_events,
    by = c("LTER", "Stream_Name", "date")
  )
check_reviewed_count(
  nrow(arctic_lake_rows),
  arctic_lake_measurement_count,
  "the provider-confirmed ARC lake-water measurement count changed"
)
master_chemistry <- dplyr::anti_join(
  master_chemistry,
  arctic_lake_events,
  by = c("LTER", "Stream_Name", "date")
)
rm(arctic_lake_rows)
si_sites <- master_chemistry |>
  dplyr::filter(variable == "DSi") |>
  dplyr::distinct(LTER, Stream_Name)
# keep river and stream sites with si after the method checks
included_sites <- site_reference |>
  dplyr::filter(Waterbody %in% flowing_types, Include_in_Master_Chem == "Yes") |>
  dplyr::distinct(LTER, Stream_Name) |>
  dplyr::semi_join(si_sites, by = c("LTER", "Stream_Name"))
master_chemistry <- dplyr::semi_join(
  master_chemistry,
  included_sites,
  by = c("LTER", "Stream_Name")
)
master_chemistry <- apply_preferred_source_rules(
  master_chemistry,
  preferred_source_rules,
  expected_superseded_rows
)
daily_key <- c("LTER", "Stream_Name", "date", "variable", "units")
stop_if(
  anyDuplicated(master_chemistry[daily_key]) > 0L,
  "duplicate site-day-variable-unit rows remain after reviewed source selection"
)
stop_if(nrow(master_chemistry) == 0L, "no chemistry rows remain after site checks")
# carry site notes onto every row and combine the sample-level flags
master_chemistry <- add_site_and_sample_flags(master_chemistry, site_reference)
checked_methods <- which(!is.na(method_rules$expected_count))
observed_method_counts <- vapply(checked_methods, function(row) {
  sum(method_flag_present(master_chemistry$Method_Flag, method_rules$Method_Flag[[row]]))
}, integer(1))
check_reviewed_counts(
  observed_method_counts,
  method_rules$expected_count[checked_methods],
  paste("method count:", method_rules$Method_Flag[checked_methods])
)
stop_if(
  any(master_chemistry$variable == "DSi" &
        master_chemistry$Raw_Filename == canada_extractable_filename) ||
    any(master_chemistry$variable == "DSi" &
          master_chemistry$Raw_Filename == "EastRiverSFA_chem.csv") ||
    any(master_chemistry$LTER == "HBR" &
          master_chemistry$variable == "DSi" &
          master_chemistry$date < hbr_documented_start),
  "silicon with an undocumented sample fraction entered the final master"
)
release_counts <- c(
  corrected_date_rows = sum(master_chemistry$Date_Flag == "corrected", na.rm = TRUE),
  inferred_date_rows = sum(master_chemistry$Date_Flag == "inferred", na.rm = TRUE),
  restored_date_rows = sum(master_chemistry$Date_Flag == "restored", na.rm = TRUE),
  less_than_rows = sum(master_chemistry$value_qualifier == "<", na.rm = TRUE),
  greater_than_rows = sum(master_chemistry$value_qualifier == ">", na.rm = TRUE),
  final_rows = nrow(master_chemistry),
  final_sites = dplyr::n_distinct(master_chemistry$LTER, master_chemistry$Stream_Name),
  final_silicon_sites = dplyr::n_distinct(
    master_chemistry$LTER[master_chemistry$variable == "DSi"],
    master_chemistry$Stream_Name[master_chemistry$variable == "DSi"]
  ),
  final_silicon_rows = sum(master_chemistry$variable == "DSi")
)
# keep the reviewed counts while the new averaging rule awaits a full comparison
if (!identical(release_counts[names(expected_release_counts)], expected_release_counts)) {
  count_text <- paste(
    names(release_counts),
    release_counts,
    sep = "=",
    collapse = ", "
  )
  stop("reviewed final chemistry counts changed: ", count_text, call. = FALSE)
}

# ---- output ----
output_file <- file.path(output_dir, paste0(gsub("-", "", Sys.Date()), "_masterdata_chem.csv"))

# keep long explanations once in metadata and leave all flags on chemistry rows
date_note_keys <- c("Dataset", "Raw_Filename", "LTER", "Stream_Name", "date", "Date_Flag")
output_date_notes <- master_chemistry |>
  dplyr::filter(!is.na(Date_Note)) |>
  dplyr::distinct(dplyr::across(dplyr::all_of(c(date_note_keys, "Date_Note"))))
output_method_notes <- master_chemistry |>
  dplyr::filter(!is.na(Method_Note)) |>
  dplyr::distinct(Method_Flag, Method_Note)
stop_if(
  anyDuplicated(output_date_notes[date_note_keys]) > 0L ||
    anyDuplicated(output_method_notes$Method_Flag) > 0L,
  "the metadata needs one explanation per date event or method flag"
)
metadata <- list(date_notes = output_date_notes, method_notes = output_method_notes)
metadata_dir <- file.path(dirname(output_dir), "metadata")
dir.create(metadata_dir, showWarnings = FALSE)
metadata_file <- file.path(
  metadata_dir,
  sub("[.]csv$", "_metadata.csv", basename(output_file))
)
write_chemistry_metadata(metadata, metadata_file)

# removing notes does not change values, row order, or flags
master_output <- dplyr::select(master_chemistry, -Date_Note, -Method_Note)
write.csv(master_output, output_file, na = "", row.names = FALSE)
