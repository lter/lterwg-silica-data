## ---------------------------------------------------------- ##
            # Chemistry - Harmonization & Wrangling
## ---------------------------------------------------------- ##
# Script author(s): Nick J Lyon, Kathi Jo Jankowski, Paul Julian
# workflow updates: sidney bush

# PURPOSE:
## Ingest all chemistry files and perform harmonization necessary for creation of 'master' file
## Wrangle the first version of the 'master' file into a tidy, analysis-ready format

## -------------------------------------------- ##
                # Housekeeping ----
## -------------------------------------------- ##


# ---- proposed update: setup, shared inputs, and source reshaping (commented for review) ----
# # load libraries
# librarian::shelf(googledrive, purrr, readxl, stringr, supportR, tidyverse)
#
# # clear environment
# rm(list = ls())
#
# # silence group-wise summarization messages
# options(dplyr.summarise.inform = F)
#
# use_local_test <- identical(
#   tolower(Sys.getenv("SISYN_CHEM_LOCAL_TEST", unset = "false")),
#   "true"
# )
# download_shared_inputs <- identical(
#   tolower(Sys.getenv("SISYN_CHEM_DOWNLOAD_SHARED", unset = "true")),
#   "true"
# )
# upload_requested <- identical(
#   tolower(Sys.getenv("SISYN_CHEM_UPLOAD_OUTPUT", unset = "false")),
#   "true"
# )
# upload_output <- upload_requested
#
# if (upload_output && use_local_test) {
#   stop("output upload is not available during a local test")
# }
#
# if (upload_output && !download_shared_inputs) {
#   stop("output upload requires a fresh shared-input download")
# }
#
# if (use_local_test) {
#   path <- Sys.getenv("SISYN_CHEM_LOCAL_ROOT", unset = "")
#   if (path == "") {
#     stop("set SISYN_CHEM_LOCAL_ROOT before running a local test")
#   }
#   raw_input_dir <- Sys.getenv(
#     "SISYN_CHEM_LOCAL_INPUT_DIR",
#     unset = file.path(path, "chem_raw_subset")
#   )
# } else {
#   path <- scicomptools::wd_loc(
#     local = FALSE,
#     remote_path = file.path("/", "home", "shares", "lter-si", "WRTDS")
#   )
#   raw_input_dir <- file.path(path, "Master_Chemistry_Inputs_CURRENT")
# }
#
# dir.create(file.path(path, "keys"), recursive = TRUE, showWarnings = FALSE)
# dir.create(raw_input_dir, recursive = TRUE, showWarnings = FALSE)
# dir.create(path = file.path(path, "tidy"), showWarnings = F)
#
# ## -------------------------------------------- ##
#               # data acquisition ----
# ## -------------------------------------------- ##
#
# if (download_shared_inputs) {
#   key_drive <- googledrive::drive_ls(
#     path = googledrive::as_id(
#       "https://drive.google.com/drive/folders/1hbkUsTdo4WAEUnlPReOUuXdeeXm92mg-"
#     )
#   )
#
#   purrr::walk(
#     c("SiSyn_Data_Key", "SiSyn_Chem_Units_Key"),
#     function(key_name) {
#       key_drive %>%
#         dplyr::filter(name == key_name) %>%
#         googledrive::drive_download(
#           file = .$id,
#           path = file.path(path, "keys", .$name),
#           overwrite = TRUE,
#           type = "csv"
#         )
#     }
#   )
#
#   googledrive::drive_ls(
#     path = googledrive::as_id(
#       "https://drive.google.com/drive/u/1/folders/0AIPkWhVuXjqFUk9PVA"
#     )
#   ) %>%
#     dplyr::filter(name == "Site_Reference_Table") %>%
#     googledrive::drive_download(
#       file = .$id,
#       path = file.path(path, "keys", .$name),
#       overwrite = TRUE,
#       type = "csv"
#     )
# }
#
# required_key_files <- file.path(
#   path,
#   "keys",
#   c("SiSyn_Data_Key.csv", "SiSyn_Chem_Units_Key.csv", "Site_Reference_Table.csv")
# )
#
# if (any(!file.exists(required_key_files))) {
#   stop(
#     "required key files are missing: ",
#     paste(basename(required_key_files[!file.exists(required_key_files)]), collapse = ", ")
#   )
# }
#
# key_v1 <- read.csv(required_key_files[[1]], na.strings = "") %>%
#   dplyr::filter(Data_type == "chemistry")
# units_key_v1 <- read.csv(required_key_files[[2]], na.strings = "") %>%
#   dplyr::filter(Data_type == "chemistry")
# # keep site identifiers as text so leading zeros and long ids survive csv import
# ref_table <- read.csv(
#   required_key_files[[3]],
#   na.strings = "",
#   colClasses = c(
#     USGSGageNumber = "character",
#     Discharge_Site_Name = "character"
#   )
# )
#
# expected_raw_files <- key_v1$Raw_Filename %>%
#   as.character() %>%
#   trimws() %>%
#   unique() %>%
#   stats::na.omit() %>%
#   as.character() %>%
#   purrr::discard(~ .x == "") %>%
#   sort()
#
# # download the current run-ready files selected by the data key
# # drive location: chemistry_files; the data key selects the run-ready files
# if (download_shared_inputs) {
#   chem_drive <- googledrive::drive_ls(
#     path = googledrive::as_id(
#       "https://drive.google.com/drive/u/1/folders/1da8AkSvforPehp-gPJsmA7-pPAJ913mz"
#     ),
#     n_max = Inf
#   )
#
#   missing_drive_files <- setdiff(expected_raw_files, chem_drive$name)
#   if (length(missing_drive_files) > 0L) {
#     stop(
#       "data-key files are missing from the chemistry folder: ",
#       paste(missing_drive_files, collapse = ", ")
#     )
#   }
#
#   selected_drive_files <- chem_drive %>%
#     dplyr::filter(name %in% expected_raw_files)
#   if (
#     nrow(selected_drive_files) != length(expected_raw_files) ||
#       anyDuplicated(selected_drive_files$name) > 0L ||
#       !setequal(selected_drive_files$name, expected_raw_files)
#   ) {
#     stop("the chemistry folder needs one copy of each data-key file")
#   }
#
#   purrr::walk2(
#     selected_drive_files$id,
#     selected_drive_files$name,
#     ~ googledrive::drive_download(
#       file = .x,
#       overwrite = TRUE,
#       path = file.path(raw_input_dir, .y)
#     )
#   )
# }
#
# actual_raw_files <- sort(list.files(raw_input_dir, pattern = "[.]csv$"))
#
# if (!identical(actual_raw_files, expected_raw_files)) {
#   stop("the local master chemistry inputs do not match the data key")
# }
#
# # every prepared flag column must be named in the data key exactly once
# flag_files <- expected_raw_files[vapply(
#   file.path(raw_input_dir, expected_raw_files),
#   function(source_file) {
#     source_names <- names(read.csv(
#       source_file,
#       nrows = 0,
#       check.names = FALSE
#     ))
#     any(source_names %in% c("Source_Lab_Flag", "source_lab_flag"))
#   },
#   logical(1)
# )]
# flag_key_counts <- key_v1 %>%
#   dplyr::filter(Standardized_Column_Name == "source_lab_flag") %>%
#   dplyr::count(Raw_Filename, name = "mapping_count")
# if (
#   !setequal(flag_files, flag_key_counts$Raw_Filename) ||
#     any(flag_key_counts$mapping_count != 1L)
# ) {
#   stop("every flag-bearing chemistry input needs one source_lab_flag mapping")
# }
#
# method_flag_files <- expected_raw_files[vapply(
#   file.path(raw_input_dir, expected_raw_files),
#   function(source_file) {
#     source_names <- names(read.csv(
#       source_file,
#       nrows = 0,
#       check.names = FALSE
#     ))
#     any(source_names %in% c("Source_Method_Flag", "source_method_flag"))
#   },
#   logical(1)
# )]
# method_flag_key_counts <- key_v1 %>%
#   dplyr::filter(Standardized_Column_Name == "source_method_flag") %>%
#   dplyr::count(Raw_Filename, name = "mapping_count")
# if (
#   !setequal(method_flag_files, method_flag_key_counts$Raw_Filename) ||
#     any(method_flag_key_counts$mapping_count != 1L)
# ) {
#   stop("every method-flag chemistry input needs one source_method_flag mapping")
# }
#
#
# ## -------------------------------------------- ##
#               # data harmonizing ----
# ## -------------------------------------------- ##
#
# # process the data key object as needed
# key <- key_v1 %>%
#   # drop fields not used during reshaping
#   dplyr::select(-dplyr::any_of(c("Data_type", "Notes"))) %>%
#   # match names created by read.csv
#   dplyr::mutate(Raw_Column_Name = gsub(pattern = " |\\+|\\(|\\)|\\/|\\-", replacement = ".",
#                                        x = Raw_Column_Name))
#
# # re-check structure
# dplyr::glimpse(key)
#
# # do the same for the units key
# units_key <- units_key_v1 %>%
#   dplyr::select(-dplyr::any_of(c("Data_type", "Notes", "X"))) %>%
#   dplyr::mutate(Variable = tolower(Variable))
#
# # re-check structure
# dplyr::glimpse(units_key)
#
# # normalize common spellings without changing chemical basis labels
# normalize_unit_label <- function(x) {
#   x <- trimws(as.character(x))
#   x <- stringr::str_replace_all(x, c("µ" = "u", "μ" = "u"))
#
#   dplyr::case_when(
#     is.na(x) | x == "" ~ x,
#     tolower(x) == "na" ~ NA_character_,
#     x %in% c("-", "/") ~ "",
#     tolower(x) %in% c("mg/l", "milligrams/litre", "milligramsperliter") ~ "mg/L",
#     tolower(x) == "mg(s)/l" ~ "mg S/L",
#     tolower(x) == "mg(so4)/l" ~ "mg SO4/L",
#     tolower(x) %in% c("ug/l", "micrograms/litre") ~ "ug/L",
#     tolower(x) %in% c(
#       "microsiemens/cm", "microsiemens cm", "microsiemenspercentimeter"
#     ) ~ "uS/cm",
#     tolower(x) %in% c("deg c", "oc", "degrees celsius") ~ "C",
#     x == "%" ~ "percent",
#     tolower(x) == "pcu" ~ "PCU",
#     tolower(x) == "fnu" ~ "FNU",
#     tolower(x) == "ntu" ~ "NTU",
#     tolower(x) == "ueq/l" ~ "ueq/L",
#     TRUE ~ x
#   )
# }
#
# # retain qualifiers when repeated source records share a key
# mean_measurement <- function(x) {
#   x <- as.character(x)
#   missing_tokens <- unique(c(
#     key$NA_indicator,
#     "", "NA", "NaN", "N/A", "n/a", "<LOD", "wrong filtering",
#     "no filter", "nd"
#   ))
#   less_than <- stringr::str_detect(x, "^\\s*<")
#   greater_than <- stringr::str_detect(x, "^\\s*>")
#   number <- suppressWarnings(
#     as.numeric(stringr::str_remove(x, "^\\s*[<>]\\s*"))
#   )
#   unparsed <- !is.na(x) & !trimws(x) %in% missing_tokens & is.na(number)
#
#   if (any(unparsed)) {
#     stop(
#       "unparsed repeated chemistry values remain: ",
#       paste(unique(x[unparsed]), collapse = ", ")
#     )
#   }
#
#   detected <- number[!is.na(number) & !less_than & !greater_than]
#   upper_limits <- number[!is.na(number) & less_than]
#   lower_limits <- number[!is.na(number) & greater_than]
#
#   if (length(detected) > 0) {
#     return(as.character(mean(detected)))
#   }
#   if (length(upper_limits) > 0) {
#     if (length(lower_limits) > 0) {
#       stop("mixed less-than and greater-than repeated measurements remain")
#     }
#     return(paste0("<", mean(upper_limits)))
#   }
#   if (length(lower_limits) > 0) {
#     return(paste0(">", mean(lower_limits)))
#   }
#   NA_character_
# }
#
# # use the exact key-derived file list
# raw_files <- expected_raw_files
#
# # make an empty list to store re-formatted raw data
# df_list <- list()
#
# # process each raw file
# for(j in 1:length(raw_files)){
#
#   # grab its name
#   focal_raw <- raw_files[j]
#
#   # report useful loop progress
#   message("Harmonizing '", focal_raw, "' (file ",  j, " of ", length(raw_files), ")")
#
#   # subset the key object a bit
#   # there are values in some datasets ("dataset" and "raw_filename" that i need to remove)
#   rows_to_remove <- c("Dataset","Raw_Filename")
#
#   key_sub <- key %>%
#     # only this file's section
#     dplyr::filter(!Standardized_Column_Name %in% rows_to_remove) %>%
#     dplyr::filter(Raw_Filename == focal_raw) %>%
#     # and only columns that have a synonymized equivalent
#     dplyr::filter(!is.na(Standardized_Column_Name) & nchar(Standardized_Column_Name) != 0)
#
#   # remove columns redundant with data key
#   cols_to_remove <- c("Dataset","Raw_Filename")
#
#   # load in that file
#   raw_df_v1 <- read.csv(
#     file = file.path(raw_input_dir, focal_raw),
#     na.strings = ""
#   ) %>%
#     dplyr::select(-dplyr::any_of(cols_to_remove))
#
#   # translate documented east river nondetect zeros in memory
#   if (focal_raw == "EastRiverSFA_chem.csv") {
#     east_ammonia <- raw_df_v1$solute == "ammonia_n"
#     east_ammonia[is.na(east_ammonia)] <- FALSE
#     east_zero <- east_ammonia &
#       trimws(as.character(raw_df_v1$value)) == "0"
#     east_missing <- east_ammonia &
#       trimws(as.character(raw_df_v1$value)) == "-9999"
#     east_zero[is.na(east_zero)] <- FALSE
#     east_missing[is.na(east_missing)] <- FALSE
#
#     if (
#       sum(east_ammonia) != 1114L ||
#         sum(east_zero) != 32L ||
#         sum(east_missing) != 1L
#     ) {
#       stop("the documented East River ammonia source codes changed")
#     }
#
#     # keep -9999 for the generic missing-value rule below
#     raw_df_v1$value[east_zero] <- "<0.0077"
#   }
#
#   # keep the six reviewed cameroon samples for the 2022 date correction
#
#   # verify the single prepared catalina sample with an inferred date
#   if (focal_raw == "CatalinaJemez_chemistry_2009-2019_V2.csv") {
#     inferred_sample <- raw_df_v1$SampleCode == "AZ_S1793"
#     inferred_sample_key <- inferred_sample &
#       raw_df_v1$SiteCode == "MGWEIR" &
#       as.Date(raw_df_v1$DateTime) == as.Date("2018-04-19")
#     same_site_date <- raw_df_v1$SiteCode == "MGWEIR" &
#       as.Date(raw_df_v1$DateTime) == as.Date("2018-04-19")
#     if (
#       sum(inferred_sample, na.rm = TRUE) != 1L ||
#         sum(inferred_sample_key, na.rm = TRUE) != 1L ||
#         sum(same_site_date, na.rm = TRUE) != 1L
#     ) {
#       stop("the reviewed Catalina inferred-date sample changed")
#     }
#   }
#
#   # verify the source-restored guadeloupe events are present
#   guadeloupe_dates <- list(
#     "guadeloupe_GRCVHSAV_chem.csv" = as.Date(c(
#       "2010-10-05", "2009-01-08", "2007-06-05", "2007-03-05",
#       "2006-11-10", "2006-10-12", "2006-09-25", "2006-09-22",
#       "2006-06-06", "2006-05-06"
#     )),
#     "guadeloupe_GRCBDPBD_chem.csv" = as.Date("2007-09-28")
#   )
#   guadeloupe_rows <- c(
#     "guadeloupe_GRCVHSAV_chem.csv" = 304L,
#     "guadeloupe_GRCBDPBD_chem.csv" = 1856L
#   )
#   if (focal_raw %in% names(guadeloupe_dates)) {
#     source_dates <- as.Date(raw_df_v1$datetime)
#     restored_counts <- vapply(
#       guadeloupe_dates[[focal_raw]],
#       function(source_date) sum(source_dates == source_date, na.rm = TRUE),
#       integer(1)
#     )
#     if (
#       nrow(raw_df_v1) != guadeloupe_rows[[focal_raw]] ||
#         anyNA(source_dates) ||
#         any(restored_counts != 16L)
#     ) {
#       stop("the reviewed Guadeloupe source-restored dates changed")
#     }
#   }
#
#   # process it to ready for integration with other raw files
#   raw_df_v2 <- raw_df_v1 %>%
#     # create a row number column and a column for the original file
#     dplyr::mutate(row_num = 1:nrow(.),
#                   Raw_Filename = focal_raw,
#                   .before = dplyr::everything()) %>%
#     # make all columns into character columns
#     dplyr::mutate(dplyr::across(.cols = dplyr::everything(), .fns = as.character)) %>%
#     # now pivot everything into ultimate long format
#     ## note: if column class differs this step can't be done
#     ## that is why we convert everything into characters in the previous step
#     tidyr::pivot_longer(cols = -row_num:-Raw_Filename,
#                         names_to = "Raw_Column_Name",
#                         values_to = "values")
#
#   # identify any columns that are in the data key but (apparently) not in the data
#   missing_cols <- setdiff(x = key_sub$Raw_Column_Name, y = unique(raw_df_v2$Raw_Column_Name))
#
#   # stop before a partial file can enter the output
#   if(length(missing_cols) != 0){
#     stop(
#       "the data key expects missing columns in '", focal_raw, "': ",
#       paste0("'", missing_cols, "'", collapse = " & ")
#     )
#   }
#
#   # drop this object (if it exists) to avoid false warning with the next run of the loop
#   if(exists("missing_cols") == T){ rm(list = "missing_cols") }
#
#   # integrate synonymized column names from key
#   raw_df_v3 <- raw_df_v2 %>%
#     # attach revised column names
#     dplyr::left_join(key_sub, by = c("Raw_Filename", "Raw_Column_Name")) %>%
#     # drop any columns that don't have a synonymized equivalent
#     dplyr::filter(!is.na(Standardized_Column_Name)) %>%
#     # normalize units before building measurement names
#     dplyr::mutate(
#       Units = normalize_unit_label(Units),
#       Units = ifelse(nchar(Units) == 0, yes = NA, no = Units)
#     ) %>%
#     # handle concentration units characters that can't be in column names
#     dplyr::mutate(units_fix = gsub(pattern = "\\/| |\\-", replacement = "_", x = Units)) %>%
#     # combine concentration units with column name (where conc units are provided)
#     dplyr::mutate(names_actual = ifelse(test = !is.na(units_fix),
#                                         yes = paste0(Standardized_Column_Name, "_", units_fix),
#                                         no = Standardized_Column_Name)) %>%
#     # pare down to only needed columns (implicitly removes unspecified columns)
#     dplyr::select(row_num, Dataset, Raw_Filename, names_actual, values) %>%
#     # pivot back to wide format with revised column names
#     tidyr::pivot_wider(names_from = names_actual, values_from = values, values_fill = NA) %>%
#     # drop row number column
#     dplyr::select(-row_num) %>%
#     # drop non-unique rows (there shouldn't be any but better safe than sorry)
#     dplyr::distinct()
#
#   # match long-format rows to the units key without discarding row-level units
#   if (unique(key_sub$Dataset_orientation) == "long") {
#     units_sub <- units_key %>%
#       dplyr::filter(Raw_Filename == focal_raw) %>%
#       dplyr::mutate(
#         Units = normalize_unit_label(Units),
#         Units = gsub("/| |\\-", "_", Units),
#         solute = tolower(gsub("/| |\\-", "_", Variable))
#       ) %>%
#       dplyr::select(-dplyr::any_of(c("Variable", "Dataset"))) %>%
#       dplyr::distinct(Raw_Filename, solute, Units)
#
#     # keep the restored combined source unchanged
#     # these aliases expose its raw labels to the existing conversion section
#     if (focal_raw == "Deduplicated_Sites_AllSites_chemistry.csv") {
#       deduplicated_raw_aliases <- tibble::tribble(
#         ~solute, ~Units,
#         "ca_mgl", "mg_L",
#         "cl_mgl", "mg_L",
#         "sio2_mgl", "mg_SiO2_L",
#         "k_mgl", "mg_L",
#         "mg_mgl", "mg_L",
#         "nh4_mgl", "mg_NH4_L",
#         "na_mgl", "mg_L",
#         "po4_mgl", "mg_PO4_L",
#         "so4_mgl", "mg_SO4_L",
#         "tp_ugl", "ug_P_L",
#         "doc_mgl", "mg_C_L",
#         "no3_mgl", "mg_NO3_L",
#         "al_ugl", "ug_L",
#         "f_mgl", "mg_L",
#         "fe_ugl", "ug_L",
#         "mn_ugl", "ug_L",
#         "anc_ueql", "ueq_L",
#         "temp_c", "C",
#         "tdn_mgl", "mg_N_L"
#       ) %>%
#         dplyr::mutate(Raw_Filename = focal_raw) %>%
#         dplyr::select(Raw_Filename, solute, Units)
#       units_sub <- dplyr::bind_rows(
#         units_sub,
#         deduplicated_raw_aliases
#       ) %>%
#         dplyr::distinct(Raw_Filename, solute, Units)
#     }
#
#     source_has_row_units <- "solute_units" %in% names(raw_df_v3)
#     if (!source_has_row_units) {
#       raw_df_v3$solute_units <- NA_character_
#     }
#
#     raw_df_v4 <- raw_df_v3 %>%
#       dplyr::mutate(
#         solute = tolower(gsub("/| |\\-", "_", solute)),
#         solute_units = normalize_unit_label(solute_units),
#         solute_units = dplyr::na_if(solute_units, ""),
#         solute_units = gsub("/| |\\-", "_", solute_units)
#       ) %>%
#       dplyr::group_by(dplyr::across(c(-amount))) %>%
#       dplyr::summarize(amount = mean_measurement(amount)) %>%
#       dplyr::ungroup() %>%
#       dplyr::mutate(amount = as.character(amount))
#
#     if (focal_raw == "Deduplicated_Sites_AllSites_chemistry.csv") {
#       raw_df_v4 <- raw_df_v4 %>%
#         dplyr::mutate(
#           solute_units = dplyr::if_else(
#             solute == "nh4_mgl" & solute_units == "mg_L",
#             "mg_NH4_L",
#             solute_units
#           )
#         )
#     }
#
#     unit_choice_counts <- units_sub %>%
#       dplyr::count(Raw_Filename, solute, name = "unit_choice_count")
#     keyed_rows <- raw_df_v4 %>%
#       dplyr::filter(solute %in% units_sub$solute)
#     units_joined <- keyed_rows %>%
#       dplyr::left_join(unit_choice_counts, by = c("Raw_Filename", "solute")) %>%
#       dplyr::left_join(
#         units_sub,
#         by = c("Raw_Filename", "solute"),
#         relationship = "many-to-many"
#       ) %>%
#       dplyr::filter(
#         (
#           unit_choice_count == 1L &
#             (
#               !source_has_row_units |
#                 (
#                   is.na(solute_units) &
#                     (is.na(Units) | Units == "")
#                 ) |
#                 (!is.na(solute_units) & solute_units == Units)
#             )
#         ) |
#           (
#             unit_choice_count > 1L &
#               source_has_row_units &
#               !is.na(solute_units) & solute_units == Units
#           )
#       )
#
#     if (nrow(units_joined) != nrow(keyed_rows)) {
#       stop("row-level units did not resolve in '", focal_raw, "'")
#     }
#
#     raw_df <- units_joined %>%
#       dplyr::mutate(
#         solute_actual = dplyr::if_else(
#           is.na(Units) | Units == "",
#           solute,
#           paste0(solute, "_", Units)
#         )
#       ) %>%
#       dplyr::select(-solute, -solute_units, -Units, -unit_choice_count) %>%
#       tidyr::pivot_wider(
#         names_from = solute_actual,
#         values_from = amount,
#         values_fill = NA,
#         values_fn = mean_measurement
#       )
#   } else {
#     raw_df <- raw_df_v3
#   }
#
#   # add to list
#   df_list[[focal_raw]] <- raw_df
#
# } # close loop
#
# # unlist the list we just generated
# tidy_v0 <- df_list %>%
#   purrr::list_rbind(x = .)
#
# if (!"source_lab_flag" %in% names(tidy_v0)) {
#   tidy_v0$source_lab_flag <- NA_character_
# }
# if (!"source_method_flag" %in% names(tidy_v0)) {
#   tidy_v0$source_method_flag <- NA_character_
# }
# tidy_v0 <- tidy_v0 %>%
#   dplyr::relocate(source_lab_flag, source_method_flag, .before = date)
#
# # check that out
# dplyr::glimpse(tidy_v0)
#
#
# ---- kj's original: setup, shared inputs, and source reshaping ----
# Load libraries
# install.packages("librarian")
librarian::shelf(googledrive, purrr, readxl, stringr, supportR, tidyverse)

# Clear environment
rm(list = ls())

# Silence group-wise summarization messages from `dplyr::summarize`
options(dplyr.summarise.inform = F)

# set up path to store files
(path <- scicomptools::wd_loc(local = FALSE, remote_path = file.path('/', "home", "shares", "lter-si", "WRTDS")))

# Create a folder for any needed data keys
dir.create(path = file.path(path, "keys"), showWarnings = F)

# Create a folder for inputs & and outputs
dir.create(path = file.path(path, "chem_raw"), showWarnings = F)
dir.create(path = file.path(path, "chem_raw_subset_092126"), showWarnings = F)
dir.create(path = file.path(path, "tidy"), showWarnings = F)

## -------------------------------------------- ##
              # Data Acquisition ----
## -------------------------------------------- ##

# Identify and download the data key
googledrive::drive_ls(path = googledrive::as_id("https://drive.google.com/drive/folders/1hbkUsTdo4WAEUnlPReOUuXdeeXm92mg-")) %>%
  dplyr::filter(name == "SiSyn_Data_Key") %>%
  googledrive::drive_download(file = .$id, path = file.path(path, "keys", .$name), overwrite = T, type="csv")

# Read in the key
#key_v2 <- readxl::read_excel(path = file.path(path, "keys", "SiSyn_Data_Key.xlsx")) %>%
  # Subset to only chemistry data
 #dplyr::filter(Data_type == "chemistry")

key_v1 <- read.csv(file = file.path(path, "keys", paste0("SiSyn_Data_Key", ".csv"))) %>%
  # Subset to only chemistry data
  dplyr::filter(Data_type == "chemistry")

# Check structure
dplyr::glimpse(key_v1)

# Identify and download the **units key** as well
googledrive::drive_ls(path = googledrive::as_id("https://drive.google.com/drive/folders/1hbkUsTdo4WAEUnlPReOUuXdeeXm92mg-")) %>%
  dplyr::filter(name == "SiSyn_Chem_Units_Key") %>%
  googledrive::drive_download(file = .$id, path = file.path(path, "keys", .$name), overwrite = T, type = "csv")


# Read it in
units_key_v1 <- read.csv(file = file.path(path, "keys", paste0("SiSyn_Chem_Units_Key", ".csv"))) %>%
  # Subset to only chemistry data
  dplyr::filter(Data_type == "chemistry")

# Check structure
dplyr::glimpse(units_key_v1)

# Identify all raw chemistry files
chem_drive <- googledrive::drive_ls(path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1da8AkSvforPehp-gPJsmA7-pPAJ913mz"))

# Check which files are in the data key but not the Drive and vice versa
supportR::diff_check(old = unique(key_v1$Raw_Filename), new = unique(chem_drive$name))

# Filter the Drive files to only those in the data key
chem_drive_actual <- chem_drive %>%
  dplyr::filter(name %in% key_v1$Raw_Filename)

# Download these files
purrr::walk2(.x = chem_drive_actual$id, .y = chem_drive_actual$name,
             .f = ~ googledrive::drive_download(file = .x, overwrite = T,
                                                path = file.path(path, "chem_raw_subset_092126", .y)))

# check what is in chemistry files folder
list.files(path = file.path(path,"chem_raw_subset_092126"))

# site reference table
googledrive::drive_ls(path = googledrive::as_id("https://drive.google.com/drive/u/1/folders/0AIPkWhVuXjqFUk9PVA")) %>%
  dplyr::filter(name == "Site_Reference_Table") %>%
  googledrive::drive_download(file = .$id, path = file.path(path, "keys", .$name), overwrite = T, type="csv")

ref_table <- read.csv(file = file.path(path, "keys", paste0("Site_Reference_Table", ".csv")))


## -------------------------------------------- ##
              # Data Harmonizing ----
## -------------------------------------------- ##

# Process the data key object as needed
key <- key_v1 %>%
  # Drop unwanted columns
  dplyr::select(-Data_type, -Notes) %>%
  # Mangle raw column names as they will be by reading in the CSVs
  dplyr::mutate(Raw_Column_Name = gsub(pattern = " |\\+|\\(|\\)|\\/|\\-", replacement = ".",
                                       x = Raw_Column_Name))

# Re-check structure
dplyr::glimpse(key)

# Do the same for the units key
units_key <- units_key_v1 %>%
  dplyr::select(-Data_type, -Notes,-X) %>%
  dplyr::mutate(Variable = tolower(Variable))

# Re-check structure
dplyr::glimpse(units_key)

# Identify all downloaded files
( raw_files <- dir(path = file.path(path, "chem_raw_subset_092126")) )

# Make an empty list to store re-formatted raw data
df_list <- list()

# For each raw file...
for(j in 1:length(raw_files)){

  # Grab its name
  focal_raw <- raw_files[j]

  # Message procesing start
  message("Harmonizing '", focal_raw, "' (file ",  j, " of ", length(raw_files), ")")

  # Subset the key object a bit
  # there are values in some datasets ("Dataset" and "Raw_Filename" that I need to remove)
  rows_to_remove <- c("Dataset","Raw_Filename")

  key_sub <- key %>%
    # Only this file's section
    dplyr::filter(!Standardized_Column_Name %in% rows_to_remove) %>%
    dplyr::filter(Raw_Filename == focal_raw) %>%
    # And only columns that have a synonymized equivalent
    dplyr::filter(!is.na(Standardized_Column_Name) & nchar(Standardized_Column_Name) != 0)

  # remove columns redundant with data key
  cols_to_remove <- c("Dataset","Raw_Filename")

  # Load in that file
  raw_df_v1 <- read.csv(file = file.path(path, "chem_raw_subset_092126", focal_raw)) %>%
    select(-any_of(cols_to_remove))

  # Process it to ready for integration with other raw files
  raw_df_v2 <- raw_df_v1 %>%
    # Create a row number column and a column for the original file
    dplyr::mutate(row_num = 1:nrow(.),
                  Raw_Filename = focal_raw,
                  .before = dplyr::everything()) %>%
    # Make all columns into character columns
    dplyr::mutate(dplyr::across(.cols = dplyr::everything(), .fns = as.character)) %>%
    # Now pivot everything into ultimate long format
    ## Note: if column class differs this step can't be done
    ## That is why we convert everything into characters in the previous step
    tidyr::pivot_longer(cols = -row_num:-Raw_Filename,
                        names_to = "Raw_Column_Name",
                        values_to = "values")

  # Identify any columns that are in the data key but (apparently) not in the data
  missing_cols <- setdiff(x = key_sub$Raw_Column_Name, y = unique(raw_df_v2$Raw_Column_Name))

  # If any are found, print a warning for whoever is running this
  if(length(missing_cols) != 0){
    message("Not all expected columns in '", focal_raw, "' are in *data* key!")
    message("Check (and fix if needed) raw columns: ",
            paste0("'", missing_cols, "'", collapse = " & ")) }

  # Drop this object (if it exists) to avoid false warning with the next run of the loop
  if(exists("missing_cols") == T){ rm(list = "missing_cols") }

  # Integrate synonymized column names from key
  raw_df_v3 <- raw_df_v2 %>%
    # Attach revised column names
    dplyr::left_join(key_sub, by = c("Raw_Filename", "Raw_Column_Name")) %>%
    # Drop any columns that don't have a synonymized equivalent
    dplyr::filter(!is.na(Standardized_Column_Name)) %>%
    # Pick a standard 'not provided' entry for concentration units
    dplyr::mutate(Units = ifelse(nchar(Units) == 0, yes = NA, no = Units)) %>%
    # Handle concentration units characters that can't be in column names
    dplyr::mutate(units_fix = gsub(pattern = "\\/| |\\-", replacement = "_", x = Units)) %>%
    # Combine concentration units with column name (where conc units are provided)
    dplyr::mutate(names_actual = ifelse(test = !is.na(units_fix),
                                        yes = paste0(Standardized_Column_Name, "_", units_fix),
                                        no = Standardized_Column_Name)) %>%
    # Pare down to only needed columns (implicitly removes unspecified columns)
    dplyr::select(row_num, Dataset, Raw_Filename, names_actual, values) %>%
    # Pivot back to wide format with revised column names
    tidyr::pivot_wider(names_from = names_actual, values_from = values, values_fill = NA) %>%
    # Drop row number column
    dplyr::select(-row_num) %>%
    # Drop non-unique rows (there shouldn't be any but better safe than sorry)
    dplyr::distinct()

  # Do additional processing if the dataset is long format
  if(unique(key_sub$Dataset_orientation) == "long"){

    # Get just the relevant bit of the units key
    units_sub <- units_key %>%
      # Filter to only this site
      dplyr::filter(Raw_Filename == focal_raw) %>%
      # Make all units ready to become column headers
      dplyr::mutate(Units = gsub(pattern = "\\/| |\\-", replacement = "_", x = Units)) %>%
      # Rename variable column & tweak so it's ready to be a column name
      dplyr::mutate(solute = gsub(pattern = "\\/| |\\-", replacement = "_", x = Variable)) %>%
      dplyr::select(-Variable) %>%
      # Also drop Dataset column
      dplyr::select(-Dataset)

    # Needed wrangling
    raw_df_v4 <- raw_df_v3 %>%
      # Make solute column lowercase to increase chances of pattern match
      dplyr::mutate(solute = tolower(gsub(pattern = "\\/| |\\-",
                                          replacement = "_", x = solute))) %>%
      # Make some other specific changes (conditionally)
      dplyr::mutate(solute = dplyr::case_when(
        ## Canada_WQ_dat.csv
        # Dataset == "Canada" ~ gsub(pattern = " ", replacement = "_", x = solute),
        ## NT_NSW_Chem_Cleaned.csv
        # Dataset == "Australia" ~ gsub(pattern = "\\/| |\\-", replacement = "_", x = solute),
        ## 20221030_masterdata_chem.csv
        Dataset == "Master_2022" & solute == "daily.avg.q.(discharge)" ~ "daily_q",
        Dataset == "Master_2022" & solute == "instantaneous.q.(discharge)" ~ "instant_q",
        Dataset == "Master_2022" & solute == "spec.cond" ~ "specific_conductivity",
        Dataset == "Master_2022" & solute == "temp.c" ~ "temp",
        Dataset == "Master_2022" & solute == "suspended.chl" ~ "suspended_chl",
        ## Chem_HYBAM.csv
        Dataset == "HYBAM" & solute == "si" ~ "dsi",
        # If no conditional provided, no mismatch found so keep regular solute column
        TRUE ~ solute)) %>%
      # Make the amount column truly numeric
      dplyr::mutate(amount = as.numeric(amount)) %>%
      # Group by everything and get an average
      ## In case any solute is measured more than once in a day
      dplyr::group_by(dplyr::across(c(-amount))) %>%
      dplyr::summarize(amount = mean(amount, na.rm = T)) %>%
      dplyr::ungroup() %>%
      # Make amount back into a character (needed for later)
      dplyr::mutate(amount = as.character(amount))

    # Identify whether any units mismatch the data key units
    mismatch_chem <- setdiff(x = unique(units_sub$solute),
                             y = unique(raw_df_v4$solute))

    # If any mismatches are found, print a warning for whoever is running this
    if(length(mismatch_chem) != 0){
      message("Solutes found in *Units* key that are absent from '", focal_raw, "'!")
      message("Fix the following solutes: ",
              paste0("'", mismatch_chem, "'", collapse = " & ")) }

    # Drop this object (if it exists) to avoid false warning with the next run of the loop
    if(exists("mismatch_chem") == T){ rm(list = "mismatch_chem") }

    # Wrangling to get long format data into wide format
    raw_df <- raw_df_v4 %>%
      # Drop built-in units column if one exists
      dplyr::select(-dplyr::ends_with("solute_units")) %>%
      # Attach units from key
      dplyr::left_join(y = units_sub, by = c("Raw_Filename", "solute")) %>%
      # Filter to only solutes included specifically in units key
      dplyr::filter(solute %in% unique(units_sub$solute)) %>%
      # Attach solute and solute units into one column
      dplyr::mutate(solute_actual = paste0(solute, "_", Units)) %>%
      # Drop unwanted columns
      dplyr::select(-solute, -Units) %>%
      # Reshape wider averaging within groups if duplicate values are found
      tidyr::pivot_wider(names_from = solute_actual, values_from = amount, values_fill = NA,
                         values_fn = ~ as.character(mean(x = as.numeric(.x), na.rm = T)))

    } else { raw_df <- raw_df_v3 }

  # Add to list
  df_list[[focal_raw]] <- raw_df

} # Close loop

# Unlist the list we just generated
tidy_v0 <- df_list %>%
  purrr::list_rbind(x = .)

# Check that out
dplyr::glimpse(tidy_v0)

# ---- proposed update: cleanup, source preparation, and qualifier parsing (commented for review) ----
# rm(list = setdiff(
#   ls(),
#   c(
#     "key", "tidy_v0", "ref_table", "path",
#     "raw_input_dir", "use_local_test", "upload_output"
#   )
# ))
#
# tidy_v1b <- tidy_v0 %>%
#   dplyr::mutate(
#     LTER = dplyr::case_when(
#       is.na(LTER) & !is.na(Dataset) ~ Dataset,
#       TRUE ~ LTER
#     ),
#     Dataset = dplyr::case_when(
#       is.na(Dataset) & !is.na(LTER) ~ LTER,
#       TRUE ~ Dataset
#     ),
#     Stream_Name = dplyr::case_when(
#       Raw_Filename == "NigerRiver.csv" & is.na(Stream_Name) ~ "Niger",
#       Raw_Filename == "ElbeRiverChem.csv" ~ "Elbe",
#       TRUE ~ Stream_Name
#     )
#   ) %>%
#   dplyr::select(-dplyr::ends_with("_NA")) %>%
#   dplyr::select(-dplyr::any_of("Dataset_orientation"))
#
# tidy_v1c <- tidy_v1b %>%
#   dplyr::select(dplyr::where(~ !(all(is.na(.)) | all(. == ""))))
#
# # keep every prepared source and standardize names later from the site reference
# tidy_v1e <- tidy_v1c
#
# # reshape in smaller chunks and drop empty cells as it goes
# tidy_v2a_wide <- tidy_v1e %>%
#   dplyr::mutate(row_num = 1:nrow(.), .before = dplyr::everything())
# tidy_v2a_chunks <- split(
#   seq_len(nrow(tidy_v2a_wide)),
#   (seq_len(nrow(tidy_v2a_wide)) - 1L) %/% 10000L
# )
# tidy_v2a <- tidy_v2a_chunks %>%
#   purrr::map(function(rows) {
#     tidy_v2a_wide %>%
#       dplyr::slice(rows) %>%
#       tidyr::pivot_longer(
#         cols = -row_num:-date,
#         names_to = "solute",
#         values_to = "measurement",
#         values_drop_na = TRUE
#       )
#   }) %>%
#   purrr::list_rbind()
# rm(tidy_v2a_wide, tidy_v2a_chunks)
#
# invisible(suppressMessages(
#   supportR::num_check(data = tidy_v2a, col = "measurement")
# ))
#
# missing_measurements <- unique(c(
#   key$NA_indicator,
#   "NA", "NaN", "N/A", "n/a", "<LOD", "wrong filtering", "no filter", "nd"
# ))
#
# tidy_v2b <- tidy_v2a %>%
#   dplyr::mutate(measurement = dplyr::case_when(
#     trimws(measurement) %in% missing_measurements ~ NA_character_,
#     suppressWarnings(as.numeric(trimws(measurement))) == -9999 ~ NA_character_,
#     TRUE ~ measurement
#   )) %>%
#   dplyr::filter(!is.na(measurement), measurement != "")
#
# unparsed_measurements <- tidy_v2b %>%
#   dplyr::mutate(parsed = suppressWarnings(
#     as.complex(stringr::str_remove(measurement, "^\\s*[<>]\\s*"))
#   )) %>%
#   dplyr::filter(is.na(parsed))
#
# if (nrow(unparsed_measurements) > 0) {
#   stop(
#     "unparsed chemistry values remain: ",
#     paste(unique(unparsed_measurements$measurement), collapse = ", ")
#   )
# }
#
# # store qualifier direction in the imaginary component during linear conversions
# tidy_v2c <- tidy_v2b %>%
#   dplyr::mutate(
#     less_than = stringr::str_detect(measurement, "^\\s*<"),
#     greater_than = stringr::str_detect(measurement, "^\\s*>"),
#     measure_actual = as.complex(
#       stringr::str_remove(measurement, "^\\s*[<>]\\s*")
#     ) + (as.numeric(less_than) - as.numeric(greater_than)) * 1i
#   ) %>%
#   dplyr::select(-measurement, -less_than, -greater_than) %>%
#   dplyr::arrange(solute) %>%
#   tidyr::pivot_wider(names_from = solute, values_from = measure_actual) %>%
#   dplyr::select(-row_num)
#
# # stop if a future key omits the nitrogen or phosphorus reporting basis
# ambiguous_basis_columns <- c(
#   "ammonia_n_mg_L", "nh4_mgl_mg_L", "no3_mg_L", "no2_mg_L", "nox_mg_L",
#   "po4_mg_L", "srp_mg_L", "tn_mg_L", "tn_ug_L", "tdn_mg_L",
#   "tkn_mg_L", "tp_mg_L", "tp_ug_L", "tdp_mg_L", "don_mg_L"
# )
# ambiguous_basis_columns <- intersect(ambiguous_basis_columns, names(tidy_v2c))
# ambiguous_basis_counts <- vapply(
#   tidy_v2c[ambiguous_basis_columns],
#   function(values) sum(!is.na(values)),
#   integer(1)
# )
# if (any(ambiguous_basis_counts > 0L)) {
#   stop(
#     "nutrient mass units are missing an N or P reporting basis: ",
#     paste(names(ambiguous_basis_counts)[ambiguous_basis_counts > 0L], collapse = ", ")
#   )
# }
#
# # stop if a future key omits the carbon or sulfate reporting basis
# ambiguous_carbon_sulfate_columns <- c(
#   "doc_mg_L", "doc_mgl_mg_L", "carbon_dissolved_organic_mg_L",
#   "dic_mg_L", "toc_mg_L", "toc_mgl_mg_L", "so4_mg_L", "so4_ug_L"
# )
# ambiguous_carbon_sulfate_columns <- intersect(
#   ambiguous_carbon_sulfate_columns,
#   names(tidy_v2c)
# )
# ambiguous_carbon_sulfate_counts <- vapply(
#   tidy_v2c[ambiguous_carbon_sulfate_columns],
#   function(values) sum(!is.na(values)),
#   integer(1)
# )
# if (any(ambiguous_carbon_sulfate_counts > 0L)) {
#   stop(
#     "carbon or sulfate mass units are missing a C, SO4, or S reporting basis: ",
#     paste(
#       names(ambiguous_carbon_sulfate_counts)[
#         ambiguous_carbon_sulfate_counts > 0L
#       ],
#       collapse = ", "
#     )
#   )
# }
#
# ---- kj's original: cleanup, source preparation, and qualifier parsing ----
# Clean up environment (i.e., drop everything prior to this object)
rm(list = setdiff(ls(), c("key", "tidy_v0", "ref_table")))

## -------------------------------------------- ##
              # Big-Picture Checks ----
## -------------------------------------------- ##

# Begin by taking a look at our 'version 0' data object's structure for macro issues
dplyr::glimpse(tidy_v0)

# Any unit-less columns?
tidy_v0 %>%
  dplyr::select(dplyr::ends_with("_NA")) %>%
  names()

# Any missing dataset-level info?
tidy_v0 %>%
  dplyr::filter(is.na(Dataset) | is.na(Raw_Filename) | is.na(LTER)) %>%
  dplyr::select(Dataset, Raw_Filename, LTER) %>%
  dplyr::distinct()

# Any missing stream names?
tidy_v0 %>%
  dplyr::select(Dataset, Raw_Filename, Stream_Name) %>%
  dplyr::distinct() %>%
  dplyr::filter(is.na(Stream_Name))

# Do needed wrangling
tidy_v1b <- tidy_v0 %>%
  # Fix missing LTER information
  dplyr::mutate(LTER = dplyr::case_when(
    is.na(LTER) & !is.na(Dataset) ~ Dataset,
    T ~ LTER)) %>%
  # Fix missing dataset info using the reverse operation
  dplyr::mutate(Dataset = dplyr::case_when(
    is.na(Dataset) & !is.na(LTER) ~ LTER,
    T ~ Dataset)) %>%
  # Fill in missing stream names - added "Stream_Name" to original datasets 1/2/24
  dplyr::mutate(Stream_Name = dplyr::case_when(
    Raw_Filename == "NigerRiver.csv" & is.na(Stream_Name) ~ "Niger",
    #Raw_Filename == "MCM_Chem_clean.csv" ~ "McMurdo",
    Raw_Filename == "ElbeRiverChem.csv" ~ "Elbe",
    T ~ Stream_Name)) %>%
  # Drop any unitless columns that we don't want to retain
  dplyr::select(-dplyr::ends_with("_NA")) %>%
  # Also drop 'dataset orientation' column
  dplyr::select(-Dataset_orientation)

# Check what was lost/gained
supportR::diff_check(old = names(tidy_v0), new = names(tidy_v1b))

# Repeat earlier checks to make sure everything is fixed
## Missing dataset-level info?
tidy_v1b %>%
  dplyr::filter(is.na(Dataset) | is.na(Raw_Filename) | is.na(LTER)) %>%
  dplyr::select(Dataset, Raw_Filename, LTER) %>%
  dplyr::distinct()

## Missing stream names?
tidy_v1b %>%
  dplyr::select(Dataset, Raw_Filename, Stream_Name) %>%
  dplyr::distinct() %>%
  dplyr::filter(is.na(Stream_Name))

# Finally, remove any columns that are entirely NA (possibly due to structure of data key
tidy_v1c <- tidy_v1b %>%
  # Also drop any columns that don't include any values
  dplyr::select(dplyr::where(~ !(all(is.na(.)) | all(. == ""))))

# See if that actually removed anything
supportR::diff_check(old = names(tidy_v1b), new = names(tidy_v1c))

# remove Coal Creek from original "master chemistry"
tidy_v1d <- tidy_v1c %>%
  filter(LTER != "DOE SFA East River") %>%
  filter(LTER != "East River SFA")

# change name of Catalina Jemez streams which contain "_" so doesn't cause problems later
tidy_v1e <- tidy_v1d %>%
  mutate(Stream_Name = case_when(Stream_Name == "MG_WEIR" ~ "MGWEIR",
                                 Stream_Name == "OR_low" ~ "ORlow",
                                 .default = Stream_Name))


# Check the structure of the remaining data
dplyr::glimpse(tidy_v1e)

## -------------------------------------------- ##
               # Numeric Checks ----
## -------------------------------------------- ##

# Before we can do units conversions we need to do numeric checks (quality control)
tidy_v2a <- tidy_v1e %>%
  # Add a row number column
  dplyr::mutate(row_num = 1:nrow(.), .before = dplyr::everything()) %>%
  # Reshape the data to get all numeric columns into long format
  tidyr::pivot_longer(cols = -row_num:-date,
                      names_to = "solute",
                      values_to = "measurement")

# Check for any malformed numbers
supportR::num_check(data = tidy_v2a, col = "measurement")

# Check for unreasonable numbers too
range(suppressWarnings(as.numeric(tidy_v2a$measurement)), na.rm = T)

# Having identified any unreasonable / malformed numbers, we can now fix them
tidy_v2b <- tidy_v2a %>%
  dplyr::mutate(measurement = dplyr::case_when(
    ## Malformed numbers / non-numbers
    measurement %in% unique(key$NA_indicator) ~ NA,
    measurement == "NA" ~ NA,
    measurement == "NaN" ~ NA,
    measurement == "N/A" ~ NA,
    measurement == "<LOD" ~ NA,
    measurement == "wrong filtering" ~ NA,
    measurement == "no filter" ~ NA,
    measurement == "n/a"~ NA,
    # Catalina Jemez has many versions of -9999
    measurement == -9999.00 ~ NA,
    measurement == -9999.000 ~ NA,
    measurement == -9999.0000 ~ NA,
    ## Unreasonable numbers
    ### None yet!
    # If not broken, leave alone!
    T ~ measurement)) %>%
  # remove values that contain [] from swedish dataset
  dplyr::filter(!str_detect(measurement,"]")) %>%
  dplyr::filter(measurement != "") %>%
  # Filter out all NAs (don't worry we'll get them back momentarily)
  dplyr::filter(!is.na(measurement))


# Re-check for malformed numbers / unreasonable numbers
supportR::num_check(data = tidy_v2b, col = "measurement")
range(suppressWarnings(as.numeric(tidy_v2b$measurement)), na.rm = T)

# Now change class of measurements to numeric and re-claim wide format
tidy_v2c <- tidy_v2b %>%
  # Make measurement column numeric
  dplyr::mutate(measure_actual = as.numeric(measurement)) %>%
  dplyr::select(-measurement) %>%
  # Sort solutes (likely to make unit conversions easier because like columns will group together)
  dplyr::arrange(solute) %>%
  # Re-claim original format
  tidyr::pivot_wider(names_from = solute, values_from = measure_actual) %>%
  # Then drop row number column
  dplyr::select(-row_num)

# Re-check structure
dplyr::glimpse(tidy_v2c)

## -------------------------------------------- ##
        # Column Name Standardization ----
## -------------------------------------------- ##

# Check current columns in data (not site info columns)
tidy_v2c %>%
  dplyr::select(-Dataset:-date) %>%
  names()

# Check structure of pH column variants
tidy_v2c %>%
  dplyr::select(dplyr::contains("pH")) %>%
  summary()

# ---- proposed update: variable names and within-source precedence (commented for review) ----
# merge_columns <- function(data, target, precedence) {
#   present <- intersect(unique(c(target, precedence)), names(data))
#   if (length(present) == 0) return(data)
#   data[[target]] <- purrr::reduce(data[present], dplyr::coalesce)
#   dplyr::select(data, -dplyr::any_of(setdiff(present, target)))
# }
#
# coalesce_present <- function(data, columns) {
#   present <- intersect(columns, names(data))
#   if (length(present) == 0) return(rep(NA_complex_, nrow(data)))
#   purrr::reduce(data[present], dplyr::coalesce)
# }
#
# ph_columns <- c(
#   "pH", "ph", "pH_pH", "ph_SU", "ph_", "fldph", "fldph_", "labph", "labph_"
# )
# tidy_v3a <- tidy_v2c %>%
#   dplyr::mutate(pH_actual = dplyr::if_else(
#     Dataset == "ColoradoAlpine",
#     coalesce_present(tidy_v2c, c("fldph", "fldph_", "labph", "labph_", ph_columns)),
#     coalesce_present(tidy_v2c, ph_columns)
#   )) %>%
#   dplyr::select(-dplyr::any_of(ph_columns)) %>%
#   dplyr::rename(pH = pH_actual)
#
# conductivity_columns <- c(
#   "conduct_uS_cm", "conductivity_uS_cm", "cond_uscm_uS_cm",
#   "fldcond_uscm_uS_cm", "labcond_uscm_uS_cm", "conductivity_us_cm",
#   "ec_uS_cm", "specificconductance_uS_cm", "conductivity_at_25°c_uS_cm"
# )
# tidy_v3a$conductivity_uS_cm <- dplyr::case_when(
#   tidy_v3a$Dataset == "Guadeloupe" ~ coalesce_present(
#     tidy_v3a, "conductivity_at_25°c_uS_cm"
#   ),
#   tidy_v3a$Dataset == "ColoradoAlpine" ~ coalesce_present(
#     tidy_v3a, c("fldcond_uscm_uS_cm", "labcond_uscm_uS_cm")
#   ),
#   TRUE ~ coalesce_present(tidy_v3a, conductivity_columns)
# )
# tidy_v3a <- dplyr::select(
#   tidy_v3a,
#   -dplyr::any_of(setdiff(conductivity_columns, "conductivity_uS_cm"))
# )
#
# merge_specs <- list(
#   alkalinity_ueq_L = c("alkalinity_ueq_L", "alkalinity_uEq_l"),
#   anc_ueq_L = c("anc_ueq_L", "anc_ueql_ueq_L", "anc_ueq_l_ueq_L"),
#   do_percent = c("do_percent", "do_pct_percent", "do_percent_percent", "do_percent_%"),
#   colour_PCU = c("colour_PCU", "colour_pcu", "colour_pcu_", "colour_pcu_PCU"),
#   do_uM = c("do_uM", "dissolved_oxygen_uM"), don_uM = c("don_uM", "dissolved_org_n_uM"),
#   din_uM = c("din_uM", "dissolved_inorg_n_uM"), srp_uM = c("srp_uM", "soluble_reactive_p_uM"),
#   tdn_uM = c("tdn_uM", "tot_dissolved_n_uM"), tn_uM = c("tn_uM", "tot_n_uM"),
#   tkn_uM = c("tkn_uM", "tot_kjeldahl_n_uM"), tp_uM = c("tp_uM", "tot_p_uM"),
#   al_ug_L = c("al_ug_L", "al_ugl_ug_L"), fe_ug_L = c("fe_ug_L", "fe_ugl_ug_L"),
#   nh4_mg_NH4_L = c(
#     "nh4_mg_NH4_L", "nh4_mgl_mg_NH4_L"
#   ),
#   tdn_mg_N_L = c("tdn_mg_N_L", "tdn_mgl_mg_N_L", "tdn_mg_L"),
#   chla_ug_L = c("chl_a_ug_L", "chla_ug_L", "suspended_chl_ug_L", "chla_ugl_ug_L", "chlorophyll_a_ug_L"),
#   do_mg_L = c("do_mg_L", "do_mg_O2_L"),
#   ca_mg_L = c("ca_mgl_mg_L", "ca_mg_L", "calcium_mg_L"),
#   ca_ug_L = c("ca_ug_L", "calcium_ug_L"), cl_mg_L = c("cl_mgl_mg_L", "cl_mg_L"),
#   cl_uM = c("chloride_uM", "cl_uM"),
#   doc_mg_C_L = c(
#     "doc_mgl_mg_C_L", "doc_mg_C_L", "carbon_dissolved_organic_mg_C_L"
#   ),
#   doc_uM = c("dissolved_org_c_uM", "doc_uM"),
#   dic_mg_C_L = "dic_mg_C_L",
#   dic_uM = c("dic_uM", "dissolved_inorg_c_uM"),
#   toc_mg_C_L = c("toc_mgl_mg_C_L", "toc_mg_C_L"),
#   f_mg_L = c("f_mgl_mg_L", "f_mg_L"), hco3_mg_L = c("hco3_mgl_mg_L", "hco3_mg_L"),
#   k_mg_L = c("k_mgl_mg_L", "k_mg_L", "potassium_mg_L"),
#   mg_mg_L = c("mg_mg_L", "mg_mgl_mg_L", "magnesium_mg_L"),
#   mg_ug_L = c("magnesium_ug_L", "mg_ug_L"), mn_ug_L = c("mn_ugl_ug_L", "mn_ug_L"),
#   na_mg_L = c("na_mgl_mg_L", "na_mg_L", "na._mg_L"),
#   nh4_mg_NH4_N_L = c(
#     "nh4_mgl_mg_NH4_N_L", "nh4_mg_NH4_N_L",
#     "nh4___n_mg_NH4_N_L"
#   ),
#   nh4_ug_NH4_N_L = "nh4_ug_NH4_N_L",
#   nhx_mg_N_L = c(
#     "nhx_mg_N_L", "ammonia_n_mg_N_L", "ammonia_dissolved_mg_N_L"
#   ),
#   nhx_ug_N_L = "nh4_n_ug_NH4_N_L",
#   no3_mg_NO3_L = c("no3_mgl_mg_NO3_L", "no3_mg_NO3_L"), no3_uM = c("no3_uM", "nitrate_uM"),
#   # keep combined nitrate plus nitrite as nox while using its reported n basis
#   nox_mg_NO3_N_L = c(
#     "nox_mg_NO3_N_L", "nox_mg_N_L", "no3+no2___n_mg_NO3_N_L"
#   ),
#   no3_mg_NO3_N_L = c("no3_mgl_mg_NO3_N_L", "no3_mg_NO3_N_L", "dissolved_nitrogen_nitrate_mg_NO3_N_L"),
#   no2_mg_NO2_N_L = c("no2_mgl_mg_NO2_N_L", "no2_mg_NO2_N_L"),
#   po4_mg_PO4_L = c("po4_mg_PO4_L", "po4_mgl_mg_PO4_L"), po4_uM = c("phosphate_uM", "po4_uM"),
#   po4_mg_P_L = c("po4_mg_PO4_P_L", "po4_mg_P_L", "ortho___p_mg_PO4_P_L"),
#   ssc_mg_L = c("ssc_mg_L", "susp_sediment_conc_mg_L"),
#   dsi_mg_SiO2_L = c(
#     "si_mg_SiO2_L", "sio2_mgl_mg_SiO2_L", "dsi_mg_SiO2_L",
#     "sio2_mg_SiO2_L", "sio2_reactive_dissolved_mgl_mg_SiO2_L"
#   ),
#   so4_mg_SO4_L = c("so4_mg_SO4_L", "so4_mgl_mg_SO4_L"),
#   so4_ug_SO4_L = "so4_ug_SO4_L",
#   so4_uM = c("so4_uM", "sulfate_uM"),
#   temp_C = c("temp_C", "temp_c_C", "water_temp_C", "twater_(°c)_C"),
#   toc_uM = c("toc_uM", "tot_org_c_uM"), tp_ug_P_L = c("tp_ug_L", "tp_ugl_ug_P_L"),
#   turbidity_NTU = c("turbidity_NTU", "turb_NTU"),
#   tds_mg_L = c("tds_mg_L", "tot_dissolved_solids_mg_L"),
#   tss_mg_L = c("tss___dry_mass_mg_L", "tss_mg_L"),
#   vss_mg_L = c("volatile_susp_solids_mg_L", "vss_mg_L"),
#   tdn_mg_L = "nitrogen_total_dissolved_mg_N_L",
#   tn_mg_L = c("tn_mg_N_L", "nitrogen_total_mg_N_L", "ntot_mgl_mg_N_L"),
#   tp_mg_L = "phosphorus_total_mg_P_L", tdp_mg_L = "phosphorus_total_dissolved_mg_P_L",
#   dsi_mg_Si_L = c(
#     "dsi_mg_Si_L", "si_mg_Si_L", "silicon_extractable_mg_Si_L"
#   ),
#   dsi_ug_Si_L = c(
#     "dsi_ug_Si_L", "si_ug_Si_L", "silicon_ug_Si_L"
#   )
# )
#
# tidy_v3c <- tidy_v3a
# for (target_name in names(merge_specs)) {
#   tidy_v3c <- merge_columns(tidy_v3c, target_name, merge_specs[[target_name]])
# }
# for (optional_column in c("don_mg_L", "so4_mg_S_L", "zn_mg_L")) {
#   if (!optional_column %in% names(tidy_v3c)) tidy_v3c[[optional_column]] <- NA_complex_
# }
#
# ---- kj's original: variable names and within-source precedence ----
# With partially-tided columns, we can now fix any name standardization issues
tidy_v3a <- tidy_v2c %>%
  # Many variants on pH column naming
  ## Combine into one
  dplyr::mutate(ph_actual = dplyr::coalesce(pH, ph, pH_pH, ph_SU, ph_,fldph_,labph_), .after = date) %>%
  ## Delete old columns
  dplyr::select(-pH, -pH_pH, -ph, -ph_SU, -ph_,-fldph_,-labph_) %>%
  ## Rename new one simply
  dplyr::rename(pH = ph_actual)

# Did that fix the pH issues?
summary(tidy_v3a$pH) ## Yes! Many fewer NAs

tidy_v3a %>%
  dplyr::select(dplyr::contains("pH")) %>%
  summary()

# Fix some of the element columns too
tidy_v3b <- tidy_v3a %>%
  # Alkalinity (uEq/L)
  dplyr::mutate(alka_actual = dplyr::coalesce(alkalinity_uEq_l, alkalinity_ueq_L),
                .after = alkalinity_ueq_L) %>%
  dplyr::select(-alkalinity_uEq_l, -alkalinity_ueq_L) %>%
  dplyr::rename(alkalinity_ueq_L = alka_actual) %>%
  # Conductivity (uS/cm)
  dplyr::mutate(cond_actual = dplyr::coalesce(conduct_uS_cm,conductivity_uS_cm, cond_uscm_uS_cm,
                                              `conductivity_at_25°c_uS_cm`,fldcond_uscm_uS_cm,
                                              labcond_uscm_uS_cm,conductivity_us_cm,ec_uS_cm,
                                              specificconductance_uS_cm),
                .after = conductivity_uS_cm) %>%
  dplyr::select(-conduct_uS_cm, -conductivity_uS_cm, -fldcond_uscm_uS_cm,-conductivity_us_cm,-cond_uscm_uS_cm,
                -`conductivity_at_25°c_uS_cm`,-labcond_uscm_uS_cm,-ec_uS_cm,
                -specificconductance_uS_cm) %>%
  dplyr::rename(conductivity_uS_cm = cond_actual) %>%
  # Chl a
  dplyr::mutate(chla_actual = dplyr::coalesce(chl_a_ug_L, chla_ug_L, suspended_chl_ug_L,chla_ugl_ug_L,chlorophyll_a_ug_L), .after = chla_ug_L) %>%
  dplyr::select(-chl_a_ug_L, -chla_ug_L,-suspended_chl_ug_L,-chla_ugl_ug_L,-chlorophyll_a_ug_L) %>%
  dplyr::rename(chla_ug_L = chla_actual) %>%
  # DO
  dplyr::mutate(do_actual = dplyr::coalesce(do_mg_L, do_mg_O2_L), .after = do_mg_O2_L) %>%
  dplyr::select(-do_mg_L, -do_mg_O2_L) %>%
  dplyr::rename(do_mg_L = do_actual) |>
  # DO percent
  dplyr::rename(do_percent = "do_percent_%") |>
  dplyr::mutate(do_pct_actual = dplyr::coalesce(do_percent, do_pct_percent), .after = do_pct_percent) %>%
  dplyr::select(-do_percent, -do_pct_percent) %>%
  dplyr::rename(do_percent = do_pct_actual) %>%
  # DO - uM
  dplyr::rename(do_uM = dissolved_oxygen_uM)  %>%
  # aluminum
  dplyr::rename(al_ug_L = al_ugl_ug_L) |>
  # anc
  dplyr::rename(anc_ueq_L = anc_ueql_ueq_L) |>
  # calcium
  dplyr::mutate(ca_actual = dplyr::coalesce(ca_mgl_mg_L,ca_mg_L,calcium_mg_L)) %>%
  dplyr::select(-ca_mgl_mg_L,-ca_mg_L,-calcium_mg_L) %>%
  dplyr::rename(ca_mg_L = ca_actual) |>
  # calcium - ug/L
  dplyr::mutate(ca_actual_ug = dplyr::coalesce(ca_ug_L,calcium_ug_L)) %>%
  dplyr::select(-ca_ug_L,-calcium_ug_L) %>%
  dplyr::rename(ca_ug_L = ca_actual_ug) |>
  # chloride
  dplyr::mutate(cl_actual = dplyr::coalesce(cl_mgl_mg_L,cl_mg_L)) %>%
  dplyr::select(-cl_mgl_mg_L,-cl_mg_L) %>%
  dplyr::rename(cl_mg_L = cl_actual) |>
  dplyr::mutate(cl_actual_uM = dplyr::coalesce(chloride_uM,cl_uM)) %>%
  dplyr::select(-chloride_uM,-cl_uM) %>%
  dplyr::rename(cl_uM = cl_actual_uM) |>
  # Colour
  dplyr::mutate(colour_actual = dplyr::coalesce(colour_PCU,colour_pcu_,colour_pcu_PCU)) %>%
  dplyr::select(-colour_PCU,-colour_pcu_,-colour_pcu_PCU) %>%
  dplyr::rename(colour_pcu = colour_actual) %>%
  # DOC
  dplyr::mutate(doc_actual = dplyr::coalesce(doc_mgl_mg_C_L,doc_mg_C_L,doc_mg_L,doc_mgl_mg_L)) %>%
  dplyr::select(-doc_mgl_mg_C_L,-doc_mg_C_L,-doc_mg_L,-doc_mgl_mg_L) %>%
  dplyr::rename(doc_mg_L = doc_actual) |>
  # DOC -uM
  dplyr::mutate(doc_actual = dplyr::coalesce(dissolved_org_c_uM,doc_uM)) %>%
  dplyr::select(-dissolved_org_c_uM,-doc_uM) %>%
  dplyr::rename(doc_uM = doc_actual) |>
  # DIC
  dplyr::mutate(dic_actual = dplyr::coalesce(dic_mg_C_L,dic_mg_L)) %>%
  dplyr::select(-dic_mg_C_L,-dic_mg_L) %>%
  dplyr::rename(dic_mg_L = dic_actual) |>
  # DIC - uM
  dplyr::mutate(dic_actual = dplyr::coalesce(dic_uM,dissolved_inorg_c_uM)) %>%
  dplyr::select(-dic_uM,-dissolved_inorg_c_uM) %>%
  dplyr::rename(dic_uM = dic_actual) |>
  # TOC
  dplyr::mutate(toc_actual = dplyr::coalesce(toc_mgl_mg_L,toc_mg_L,toc_mg_C_L)) %>%
  dplyr::select(-toc_mgl_mg_L,-toc_mg_L,-toc_mg_C_L) %>%
  dplyr::rename(toc_mg_L = toc_actual) |>
  # Fluorine
  dplyr::mutate(f_actual = dplyr::coalesce(f_mgl_mg_L,f_mg_L)) %>%
  dplyr::select(-f_mgl_mg_L,-f_mg_L) %>%
  dplyr::rename(f_mg_L = f_actual) |>
  # Bicarbonate (HCO3)
  dplyr::mutate(hco3_actual = dplyr::coalesce(hco3_mgl_mg_L,hco3_mg_L)) %>%
  dplyr::select(-hco3_mgl_mg_L,-hco3_mg_L) %>%
  dplyr::rename(hco3_mg_L = hco3_actual) |>
  # Iron
  dplyr::rename(fe_ug_L = fe_ugl_ug_L) |>
  # potassium
  dplyr::mutate(k_actual = dplyr::coalesce(k_mgl_mg_L,k_mg_L,potassium_mg_L)) %>%
  dplyr::select(-k_mgl_mg_L,-k_mg_L,-potassium_mg_L) %>%
  dplyr::rename(k_mg_L = k_actual) |>
  # magnesium
dplyr::mutate(mg_actual = dplyr::coalesce(mg_mg_L,mg_mgl_mg_L,magnesium_mg_L)) %>%
  dplyr::select(-mg_mgl_mg_L,-mg_mg_L,-magnesium_mg_L) %>%
  dplyr::rename(mg_mg_L = mg_actual) |>
  dplyr::mutate(mg_actual_ug = dplyr::coalesce(magnesium_ug_L,mg_ug_L)) %>%
  dplyr::select(-magnesium_ug_L,-mg_ug_L) %>%
  dplyr::rename(mg_ug_L = mg_actual_ug) |>
  # Manganese
  dplyr::mutate(mn_actual = dplyr::coalesce(mn_ugl_ug_L,mn_ug_L)) %>%
  dplyr::select(-mn_ugl_ug_L,-mn_ug_L) %>%
  dplyr::rename(mn_ug_L = mn_actual) |>
  # Sodium
  dplyr::mutate(na_actual = dplyr::coalesce(na_mgl_mg_L,na_mg_L,na._mg_L,NA_mg_L)) %>%
  dplyr::select(-na_mgl_mg_L,-na_mg_L,-na._mg_L,-NA_mg_L) %>%
  dplyr::rename(na_mg_L = na_actual) |>
# NH4
dplyr::mutate(nh4_actual = dplyr::coalesce(nh4_mgl_mg_L,nh4_mg_NH4_L)) %>%
  dplyr::select(-nh4_mgl_mg_L,-nh4_mg_NH4_L) %>%
  dplyr::rename(nh4_mg_NH4_L = nh4_actual) |>
  # NH4 - N
  dplyr::mutate(nh4_N_actual = dplyr::coalesce(nh4_mgl_mg_NH4_N_L,nh4_mg_NH4_N_L,nh4___n_mg_NH4_N_L,ammonia_n_mg_L)) %>%
  dplyr::select(-nh4_mgl_mg_NH4_N_L,-nh4_mg_NH4_N_L,-nh4___n_mg_NH4_N_L,-ammonia_n_mg_L) %>%
  dplyr::rename(nh4_mg_NH4_N_L = nh4_N_actual) |>
  # NH4-N micrograms
  dplyr::mutate(nh4_N_actual_ug = dplyr::coalesce(nh4_n_ug_NH4_N_L,nh4_ug_NH4_N_L)) %>%
  dplyr::select(-nh4_ug_NH4_N_L,-nh4_n_ug_NH4_N_L) %>%
  dplyr::rename(nh4_ug_NH4_N_L = nh4_N_actual_ug) |>
  # NO3
  dplyr::mutate(no3_actual = dplyr::coalesce(no3_mgl_mg_NO3_L,no3_mg_NO3_L,no3_mgl_mg_NO3_L)) %>%
  dplyr::select(-no3_mgl_mg_NO3_L,-no3_mg_NO3_L,-no3_mgl_mg_NO3_L) %>%
  dplyr::rename(no3_mg_NO3_L = no3_actual) |>
  # NO3 micromolar
  dplyr::mutate(no3_actual_uM = dplyr::coalesce(no3_uM,nitrate_uM)) %>%
  dplyr::select(-no3_uM,-nitrate_uM) %>%
  dplyr::rename(no3_uM = no3_actual_uM) |>
  # NO3-N
  dplyr::rename(no3_mg_NO3N_L = "no3+no2___n_mg_NO3_N_L") %>%
  dplyr::mutate(no3_N_actual = dplyr::coalesce(no3_mgl_mg_NO3_N_L,no3_mg_NO3_N_L,no3_mg_NO3N_L)) %>%
  dplyr::select(-no3_mg_NO3_N_L,-no3_mgl_mg_NO3_N_L,-no3_mg_NO3N_L ) %>%
  dplyr::rename(no3_mg_NO3_N_L = no3_N_actual) |>
  #NO2
  dplyr::mutate(no2_actual = dplyr::coalesce(no2_mgl_mg_NO2_N_L,no2_mg_NO2_N_L)) %>%
  dplyr::select(-no2_mgl_mg_NO2_N_L,-no2_mg_NO2_N_L) %>%
  dplyr::rename(no2_mg_NO2_N_L = no2_actual) |>
  # DON
  dplyr::rename(don_uM = dissolved_org_n_uM) %>%
  # PO4
  dplyr::mutate(po4_actual = dplyr::coalesce(po4_mg_PO4_L,po4_mgl_mg_PO4_L)) %>%
  dplyr::select(-po4_mgl_mg_PO4_L,-po4_mg_PO4_L) %>%
  dplyr::rename(po4_mg_PO4_L = po4_actual) |>
  # PO4 - uM
  dplyr::mutate(po4_actual_uM = dplyr::coalesce(phosphate_uM,po4_uM)) %>%
  dplyr::select(-phosphate_uM,-po4_uM) %>%
  dplyr::rename(po4_uM = po4_actual_uM) |>
  # PO4-P
  dplyr::mutate(po4_p_actual = dplyr::coalesce(po4_mg_PO4_P_L,po4_mg_P_L,ortho___p_mg_PO4_P_L)) %>%
  dplyr::select(-po4_mg_PO4_P_L,-po4_mg_P_L,-ortho___p_mg_PO4_P_L) %>%
  dplyr::rename(po4_mg_P_L = po4_p_actual) |>
  # SSC
  dplyr::mutate(ssc_actual = dplyr::coalesce(ssc_mg_L, susp_sediment_conc_mg_L)) %>%
  dplyr::select(-ssc_mg_L, -susp_sediment_conc_mg_L) %>%
  dplyr::rename(ssc_mg_L = ssc_actual) %>%
  # SiO2 (units in Si are corrected below with Canadian solute names)
  dplyr::mutate(sio2_actual = dplyr::coalesce(si_mg_SiO2_L,sio2_mgl_mg_SiO2_L,dsi_mg_SiO2_L,
                                              sio2_mg_SiO2_L)) %>%
  dplyr::select(-sio2_mgl_mg_SiO2_L,-si_mg_SiO2_L,-dsi_mg_SiO2_L,-sio2_mg_SiO2_L) %>%
  dplyr::rename(dsi_mg_SiO2_L = sio2_actual) |>
  # SO4
  dplyr::mutate(so4_actual = dplyr::coalesce(so4_mg_SO4_L,so4_mgl_mg_SO4_L,so4_mg_L)) %>%
  dplyr::select(-so4_mgl_mg_SO4_L,-so4_mg_SO4_L,-so4_mg_L) %>%
  dplyr::rename(so4_mg_SO4_L = so4_actual) |>
  # so4 - micromolar
  dplyr::mutate(so4_actual_uM = dplyr::coalesce(so4_uM, sulfate_uM)) %>%
  dplyr::select(-so4_uM, -sulfate_uM) %>%
  dplyr::rename(so4_uM = so4_actual_uM) |>
  # TDN
  dplyr::mutate(tdn_actual = dplyr::coalesce(tdn_mg_N_L,tdn_mgl_mg_N_L)) %>%
  dplyr::select(-tdn_mgl_mg_N_L,-tdn_mg_N_L) %>%
  dplyr::rename(tdn_mg_N_L = tdn_actual) |>
  # TDN
  dplyr::mutate(tdn_uM_actual = dplyr::coalesce(tdn_uM,tot_dissolved_n_uM)) %>%
  dplyr::select(-tdn_uM,-tot_dissolved_n_uM) %>%
  dplyr::rename(tdn_uM = tdn_uM_actual) |>
  # TKN
  dplyr::rename(tkn_uM = tot_kjeldahl_n_uM) %>%
  # temperature
  dplyr::mutate(temp_actual = dplyr::coalesce(temp_C,temp_c_C,`twater_(°c)_C`)) %>%
  dplyr::select(-temp_C,-temp_c_C,-`twater_(°c)_C`) %>%
  dplyr::rename(temp_C = temp_actual) |>
  # TOC
  dplyr::mutate(toc_actual = dplyr::coalesce(toc_uM,tot_org_c_uM)) %>%
  dplyr::select(-toc_uM,-tot_org_c_uM) %>%
  dplyr::rename(toc_uM = toc_actual) %>%
  # total P
  dplyr::mutate(tp_actual = dplyr::coalesce(tp_ug_L,tp_ugl_ug_P_L)) %>%
  dplyr::select(-tp_ug_L, -tp_ugl_ug_P_L) %>%
  dplyr::rename(tp_ug_P_L = tp_actual) %>%
  # turbidity
  dplyr::mutate(turb_actual = dplyr::coalesce(turbidity_NTU,turb_NTU)) %>%
  dplyr::select(-turbidity_NTU,-turb_NTU) %>%
  dplyr::rename(turbidity_NTU = turb_actual) %>%
  # TDS
  dplyr::mutate(tds_actual = dplyr::coalesce(tds_mg_L,tot_dissolved_solids_mg_L)) %>%
  dplyr::select(-tds_mg_L,-tot_dissolved_solids_mg_L) %>%
  dplyr::rename(tds_mg_L = tds_actual) %>%
  # TSS
  dplyr::mutate(tss_actual = dplyr::coalesce(tss___dry_mass_mg_L,tss_mg_L)) %>%
  dplyr::select(-tss___dry_mass_mg_L,-tss_mg_L) %>%
  dplyr::rename(tss_mg_L = tss_actual) %>%
  dplyr::rename(tss_ug_L = tss___dry_mass_ug) %>%
  # temporary - need to remove this column not using
  select(-contains("nicb")) %>%
  # VSS
  dplyr::mutate(vss_actual = dplyr::coalesce(volatile_susp_solids_mg_L,vss_mg_L)) %>%
  dplyr::select(-volatile_susp_solids_mg_L,-vss_mg_L) %>%
  dplyr::rename(vss_mg_L = vss_actual)


# Re-check remaining columns
tidy_v3b %>%
  dplyr::select(-Dataset:-date) %>%
  names() %>% sort()

# Now handle the Canadian solutes (they use un-abbreviated names)
tidy_v3c <- tidy_v3b %>%
  ## DOC
  dplyr::mutate(doc_actual = dplyr::coalesce(doc_mg_L, carbon_dissolved_organic_mg_L)) %>%
  dplyr::select(-doc_mg_L, -carbon_dissolved_organic_mg_L) %>%
  dplyr::rename(doc_mg_L = doc_actual) %>%
  ## TDN
  dplyr::rename(tdn_mg_L = nitrogen_total_dissolved_mg_N_L) %>%
  ## TN
  dplyr::mutate(tn_actual = dplyr::coalesce(tn_mg_N_L, nitrogen_total_mg_N_L,ntot_mgl_mg_N_L)) %>%
  dplyr::select(-tn_mg_N_L, -nitrogen_total_mg_N_L,-ntot_mgl_mg_N_L) %>%
  dplyr::rename(tn_mg_L = tn_actual) %>%
  ## NH4
  dplyr::mutate(nh4_actual = dplyr::coalesce(nh4_mg_NH4_L, ammonia_dissolved_mg_NH4_L)) %>%
  dplyr::select(-nh4_mg_NH4_L, -ammonia_dissolved_mg_NH4_L) %>%
  dplyr::rename(nh4_mg_NH4_L = nh4_actual) %>%
  ## NO3
  dplyr::mutate(no3_actual = dplyr::coalesce(no3_mg_NO3_L, dissolved_nitrogen_nitrate_mg_NO3_L)) %>%
  dplyr::select(-no3_mg_NO3_L, -dissolved_nitrogen_nitrate_mg_NO3_L) %>%
  dplyr::rename(no3_mg_NO3_L = no3_actual) %>%
  ## TP
  dplyr::rename(tp_mg_L = phosphorus_total_mg_P_L) %>%
  ## TDP
  dplyr::rename(tdp_mg_L = phosphorus_total_dissolved_mg_P_L) %>%
  ## DSi
  dplyr::mutate(dsi_actual = dplyr::coalesce(dsi_mg_Si_L, silicon_extractable_mg_Si_L,si_mg_Si_L)) %>%
  dplyr::select(-dsi_mg_Si_L, -silicon_extractable_mg_Si_L,-si_mg_Si_L) %>%
  dplyr::rename(dsi_mg_Si_L = dsi_actual)

# Check again
tidy_v3c %>%
  dplyr::select(-Dataset:-date) %>%
  names() %>% sort()


## -------------------------------------------- ##
              # Unit Conversions ----
## -------------------------------------------- ##

# Check what units are in the data
tidy_v3c %>%
  dplyr::select(-Dataset:-date) %>%
  names() %>% sort()

# ---- proposed update: unit conversions (commented for review) ----
# # a suffix such as no3-n or nh4-n describes the reported mass basis
# # it does not change the measured analyte into a different chemical variable
# # no3, no2, nh4, and po4 each contain one n or p atom per ion
# # dividing an as-n or as-p value by that element's mass gives umol/l of the ion
# # values reported as the whole ion use the full molecular mass instead
# # ammonia-n methods that do not separate nh3 from nh4 stay nhx
# # combined nitrate plus nitrite stays nox through conversion
# # doc, dic, and toc mass concentrations use carbon atomic mass
# # sulfate uses whole-ion mass unless the key explicitly reports sulfur mass
# merge_converted <- function(data, target, conversions) {
#   if (!target %in% names(data)) data[[target]] <- NA_complex_
#   present <- intersect(names(conversions), names(data))
#   candidates <- c(
#     list(data[[target]]),
#     lapply(present, function(column) conversions[[column]](data[[column]]))
#   )
#   data[[target]] <- purrr::reduce(candidates, dplyr::coalesce)
#   dplyr::select(data, -dplyr::any_of(present))
# }
#
# atomic_mass <- c(
#   Al = 26.981539, Br = 79.904, C = 12.011, Ca = 40.078, Cl = 35.453,
#   F = 18.998403, Fe = 55.845, H = 1.00784, K = 39.0983, Li = 6.941,
#   Mg = 24.305, Mn = 54.938044, N = 14.0067, Na = 22.989769,
#   O = 15.999, P = 30.973762, S = 32.065, Si = 28.0855,
#   Sr = 87.62, Zn = 65.38
# )
# molecule_mass <- c(
#   HCO3 = atomic_mass[["H"]] + atomic_mass[["C"]] + (atomic_mass[["O"]] * 3),
#   NH3 = atomic_mass[["N"]] + (atomic_mass[["H"]] * 3),
#   NH4 = atomic_mass[["N"]] + (atomic_mass[["H"]] * 4),
#   NO2 = atomic_mass[["N"]] + (atomic_mass[["O"]] * 2),
#   NO3 = atomic_mass[["N"]] + (atomic_mass[["O"]] * 3),
#   PO4 = atomic_mass[["P"]] + (atomic_mass[["O"]] * 4),
#   SO4 = atomic_mass[["S"]] + (atomic_mass[["O"]] * 4),
#   SiO2 = atomic_mass[["Si"]] + (atomic_mass[["O"]] * 2)
# )
#
# conversion_specs <- list(
#   alkalinity_ueq_L = list(
#     alkalinity_uM = identity,
#     alkalinity_meq_L = function(x) x * 1000,
#     alkalinity_ug_L = function(x) x / 50.04345,
#     alkalinity_mg_CaCO3_L = function(x) x * 10^3 / 50.04345
#   ),
#   conductivity_uS_cm = list(conductivity_mS_m = function(x) x * 10),
#   al_uM = list(al_mg_L = function(x) (x / atomic_mass[["Al"]]) * 10^3, al_ug_L = function(x) x / atomic_mass[["Al"]]),
#   br_uM = list(br_mg_L = function(x) (x / atomic_mass[["Br"]]) * 10^3),
#   ca_uM = list(ca_mg_L = function(x) (x / atomic_mass[["Ca"]]) * 10^3, ca_ug_L = function(x) x / atomic_mass[["Ca"]]),
#   cl_uM = list(cl_mg_L = function(x) (x / atomic_mass[["Cl"]]) * 10^3, cl_ug_L = function(x) x / atomic_mass[["Cl"]]),
#   doc_uM = list(doc_mg_C_L = function(x) (x / atomic_mass[["C"]]) * 10^3, doc_ug_C_L = function(x) x / atomic_mass[["C"]]),
#   dic_uM = list(dic_mg_C_L = function(x) (x / atomic_mass[["C"]]) * 10^3),
#   do_uM = list(do_mg_L = function(x) (x / (atomic_mass[["O"]] * 2)) * 10^3),
#   dsi_uM = list(
#     dsi_mg_Si_L = function(x) (x / atomic_mass[["Si"]]) * 10^3,
#     dsi_ug_Si_L = function(x) x / atomic_mass[["Si"]],
#     dsi_mg_SiO2_L = function(x) (x / molecule_mass[["SiO2"]]) * 10^3,
#     sio2_ug_SiO2_L = function(x) x / molecule_mass[["SiO2"]]
#   ),
#   f_uM = list(f_mg_L = function(x) (x / atomic_mass[["F"]]) * 10^3),
#   fe_uM = list(fe_mg_L = function(x) (x / atomic_mass[["Fe"]]) * 10^3, fe_ug_L = function(x) x / atomic_mass[["Fe"]]),
#   hco3_uM = list(hco3_mg_L = function(x) (x / molecule_mass[["HCO3"]]) * 10^3),
#   k_uM = list(k_mg_L = function(x) (x / atomic_mass[["K"]]) * 10^3, k_ug_L = function(x) x / atomic_mass[["K"]], potassium_ug_L = function(x) x / atomic_mass[["K"]]),
#   li_uM = list(li_mg_L = function(x) (x / atomic_mass[["Li"]]) * 10^3),
#   mg_uM = list(mg_mg_L = function(x) (x / atomic_mass[["Mg"]]) * 10^3, mg_ug_L = function(x) x / atomic_mass[["Mg"]]),
#   mn_uM = list(mn_mg_L = function(x) (x / atomic_mass[["Mn"]]) * 10^3, mn_ug_L = function(x) x / atomic_mass[["Mn"]]),
#   na_uM = list(na_mg_L = function(x) (x / atomic_mass[["Na"]]) * 10^3, na_ug_L = function(x) x / atomic_mass[["Na"]], sodium_ug_L = function(x) x / atomic_mass[["Na"]]),
#   nh3_uM = list(nh3_mg_NH3_N_L = function(x) (x / atomic_mass[["N"]]) * 10^3, nh3_mg_NH3_L = function(x) (x / molecule_mass[["NH3"]]) * 10^3),
#   nh4_uM = list(nh4_mg_NH4_N_L = function(x) (x / atomic_mass[["N"]]) * 10^3, nh4_ug_NH4_N_L = function(x) x / atomic_mass[["N"]], nh4_mg_NH4_L = function(x) (x / molecule_mass[["NH4"]]) * 10^3),
#   nhx_uM = list(
#     nhx_mg_NH4_N_L = function(x) (x / atomic_mass[["N"]]) * 10^3,
#     nhx_mg_N_L = function(x) (x / atomic_mass[["N"]]) * 10^3,
#     nhx_ug_N_L = function(x) x / atomic_mass[["N"]]
#   ),
#   no3_uM = list(no3_mg_NO3_L = function(x) (x / molecule_mass[["NO3"]]) * 10^3, no3_ug_NO3_L = function(x) x / molecule_mass[["NO3"]], no3_mg_NO3_N_L = function(x) (x / atomic_mass[["N"]]) * 10^3, no3_mg_N_L = function(x) (x / atomic_mass[["N"]]) * 10^3, no3_ug_NO3_N_L = function(x) x / atomic_mass[["N"]]),
#   nox_uM = list(nox_mg_NO3_N_L = function(x) (x / atomic_mass[["N"]]) * 10^3, nox_ug_NO3_N_L = function(x) x / atomic_mass[["N"]]),
#   no2_uM = list(no2_mg_NO2_N_L = function(x) (x / atomic_mass[["N"]]) * 10^3),
#   po4_uM = list(po4_mg_PO4_L = function(x) (x / molecule_mass[["PO4"]]) * 10^3, po4_mg_P_L = function(x) (x / atomic_mass[["P"]]) * 10^3, po4_p_ug_PO4_P_L = function(x) x / atomic_mass[["P"]], po4_ug_PO4_P_L = function(x) x / atomic_mass[["P"]]),
#   so4_uM = list(so4_ug_SO4_L = function(x) x / molecule_mass[["SO4"]], so4_ug_S_L = function(x) x / atomic_mass[["S"]], so4_mg_SO4_L = function(x) (x / molecule_mass[["SO4"]]) * 10^3, so4_mg_S_L = function(x) (x / atomic_mass[["S"]]) * 10^3),
#   sr_uM = list(sr_mg_L = function(x) (x / atomic_mass[["Sr"]]) * 10^3, sr_ug_L = function(x) x / atomic_mass[["Sr"]]),
#   srp_uM = list(srp_mg_P_L = function(x) (x / atomic_mass[["P"]]) * 10^3, srp_ug_P_L = function(x) x / atomic_mass[["P"]]),
#   tdn_uM = list(tdn_mg_L = function(x) (x / atomic_mass[["N"]]) * 10^3, tdn_mg_N_L = function(x) (x / atomic_mass[["N"]]) * 10^3, tdn_ug_N_L = function(x) x / atomic_mass[["N"]]),
#   tkn_uM = list(tkn_mg_N_L = function(x) (x / atomic_mass[["N"]]) * 10^3),
#   tn_uM = list(
#     tn_mg_L = function(x) (x / atomic_mass[["N"]]) * 10^3,
#     tn_ug_L = function(x) x / atomic_mass[["N"]],
#     tn_ug_N_L = function(x) x / atomic_mass[["N"]]
#   ),
#   don_uM = list(don_mg_L = function(x) (x / atomic_mass[["N"]]) * 10^3), toc_uM = list(toc_mg_C_L = function(x) (x / atomic_mass[["C"]]) * 10^3),
#   tp_uM = list(tp_mg_P_L = function(x) x / atomic_mass[["P"]] * 10^3, tp_mg_L = function(x) x / atomic_mass[["P"]] * 10^3, tp_ug_P_L = function(x) x / atomic_mass[["P"]]),
#   tdp_uM = list(tdp_mg_P_L = function(x) (x / atomic_mass[["P"]]) * 10^3, tdp_mg_L = function(x) (x / atomic_mass[["P"]]) * 10^3, tdp_ug_P_L = function(x) x / atomic_mass[["P"]]),
#   zn_uM = list(zn_mg_L = function(x) (x / atomic_mass[["Zn"]]) * 10^3, zn_ug_L = function(x) x / atomic_mass[["Zn"]])
# )
#
# tidy_v4b <- tidy_v3c
# for (target_name in names(conversion_specs)) {
#   tidy_v4b <- merge_converted(tidy_v4b, target_name, conversion_specs[[target_name]])
# }
#
# # keep east river ammonia-n as combined ammonia rather than ammonium
# east_river_rows <- tidy_v4b$Raw_Filename == "EastRiverSFA_chem.csv"
# if (!any(east_river_rows & !is.na(tidy_v4b$nhx_uM))) {
#   stop("East River ammonia-N did not convert to NHX")
# }
# if (any(east_river_rows & !is.na(tidy_v4b$nh4_uM))) {
#   stop("East River ammonia-N was incorrectly routed to NH4")
# }
#
# ---- kj's original: unit conversions ----
# First, do any non-elemental unit conversions
tidy_v4a <- tidy_v3c %>%
  # Alkalinity (uM)
  ## Pretty sure 1 uEq/L == 1 uM
  ## UPDATED ##
  # dplyr::mutate(alkalinity_uM = dplyr::coalesce(alkalinity_uM, alkalinity_ueq_L)) %>%
  # dplyr::select(-alkalinity_ueq_L) %>%
  dplyr::mutate(alkalinity_uM = dplyr::coalesce(
    alkalinity_uM,
    alkalinity_ueq_L,
    anc_ueq_L,anc_ueq_l_ueq_L,
    alkalinity_ug_L / 50.04345
  )) %>%
  dplyr::mutate(alkalinity_uM = dplyr::coalesce(alkalinity_uM, alkalinity_ueq_L)) %>%
  dplyr::select(-alkalinity_ueq_L) %>%
  # Conductivity (uS/cm)
  dplyr::mutate(conductivity_uS_cm = ifelse(test = (is.na(conductivity_uS_cm) == T),
                                            yes = (conductivity_mS_m * 10^3 * 0.01),
                                            no = conductivity_uS_cm)) %>%
  dplyr::select(-conductivity_mS_m) %>%
  # Turbidity
  dplyr::mutate(turbidity_NTU = dplyr::coalesce(turbidity_NTU)) %>%
  # Relocate all of these columns to the left of the elemental columns
  dplyr::relocate(temp_C, alkalinity_uM, conductivity_uS_cm,
                  specific_conductivity_uS_cm, turbidity_NTU,
                  tds_mg_L, dplyr::starts_with("tss_"), dplyr::starts_with("vss_"),
                  .after = pH) %>%
  # Also moving discharge columns to left
  dplyr::relocate(dplyr::contains("_q"), .before = pH)

# Re-check remaining columns
tidy_v4a %>%
  dplyr::select(-Dataset:-vss_mg_L) %>%
  names() %>% sort()

# Define any needed molecular weights here
Al_mw <- 26.981539
Br_mw <- 79.904
C_mw <- 12.011
Ca_mw <- 40.078
Cl_mw <- 35.453
F_mw <- 18.998403
Fe_mw <- 55.845
H_mw <- 1.00784
K_mw <- 39.0983
Li_mw <- 6.941
Mg_mw <- 24.305
Mn_mw <- 54.938044
N_mw <- 14.0067
Na_mw <- 22.989769
O_mw <- 15.999
P_mw <- 30.973762
S_mw <- 32.065
Si_mw <- 28.0855
Sr_mw <- 87.62
Zn_mw <- 65.38

# Calculate any needed molecules' molecular weights here
HCO3_mw <- H_mw + C_mw + (O_mw * 3)
NH3_mw <- N_mw + (H_mw * 3)
NH4_mw <- N_mw + (H_mw * 4)
NO2_mw <- N_mw + (O_mw * 2)
NO3_mw <- N_mw + (O_mw * 3)
PO4_mw <- P_mw + (O_mw * 4)
SO4_mw <- S_mw + (O_mw * 4)

# Need to do unit conversions to get each metric into a single, desired unit
tidy_v4b <- tidy_v4a %>%
  # alumninum
  dplyr::mutate(al_uM = dplyr::case_when(
    !is.na(al_mg_L) ~ al_mg_L/Al_mw * 10^3,
    !is.na(al_ug_L) ~ al_ug_L / Al_mw,
    T ~ NA)) %>%
  dplyr::select(-al_mg_L,-al_ug_L) %>%
  # Bromine
  dplyr::mutate(br_uM = (br_mg_L / Br_mw) * 10^3,
                .after = br_mg_L) %>%
  dplyr::select(-br_mg_L) %>%
  # calcium
  dplyr::mutate(ca_uM = dplyr::case_when(
    !is.na(ca_uM) ~ ca_uM,
    is.na(ca_uM) & !is.na(ca_mg_L) ~ (ca_mg_L / Ca_mw) * 10^3,
    is.na(ca_uM) & !is.na(ca_ug_L) ~ ca_ug_L / Ca_mw,
    T ~ NA)) %>%
    dplyr::select(-ca_mg_L,-ca_ug_L) %>%
  # Chloride
  dplyr::mutate(cl_uM = dplyr::case_when(
    !is.na(cl_uM)  ~ cl_uM,
    is.na(cl_uM) & !is.na(cl_mg_L) ~ (cl_mg_L / Cl_mw) * 10^3,
    is.na(cl_uM) & !is.na(cl_ug_L) ~ (cl_ug_L / Cl_mw),
    T ~ NA)) %>%
  dplyr::select(-cl_mg_L,-cl_ug_L) %>%
  # Dissolved Organic Carbon
  dplyr::mutate(doc_uM = dplyr::case_when(
    !is.na(doc_uM) ~ doc_uM,
    is.na(doc_uM) & !is.na(doc_mg_L) ~ (doc_mg_L / C_mw) * 10^3,
    is.na(doc_uM) & !is.na(doc_ug_C_L) ~ (doc_ug_C_L / C_mw),
    T ~ NA)) %>%
  dplyr::select(-doc_mg_L,-doc_ug_C_L) %>%
  # Dissolved Inorganic Carbon
  dplyr::mutate(dic_uM = dplyr::case_when(
    !is.na(dic_uM) ~ dic_uM,
    #is.na(dic_uM) & !is.na(dic_mg_C_L) ~ (dic_mg_C_L / C_mw) * 10^3,
    is.na(dic_uM) & !is.na(dic_mg_L) ~ (dic_mg_L / C_mw)*10^3,
    T ~ NA)) %>%
  dplyr::select(-dic_mg_L) %>%
  # Dissolved Oxygen
  dplyr::mutate(do_uM = (do_mg_L / (O_mw * 2)) * 10^3,
                .after = do_mg_L) %>%
  dplyr::select(-do_mg_L) %>%
  # Silica (!)
  dplyr::mutate(dsi_uM = dplyr::case_when(
    !is.na(dsi_uM) ~ dsi_uM,
    is.na(dsi_uM) & !is.na(dsi_mg_Si_L) ~ (dsi_mg_Si_L / Si_mw) * 10^3,
    is.na(dsi_uM) & !is.na(dsi_ug_Si_L) ~ dsi_ug_Si_L / Si_mw,
    is.na(dsi_uM) & !is.na(si_ug_L) ~ si_ug_L / Si_mw,
    is.na(dsi_uM) & !is.na(silicon_ug_L) ~ (silicon_ug_L / Si_mw),
    is.na(dsi_uM) & !is.na(dsi_mg_SiO2_L) ~ (dsi_mg_SiO2_L / (Si_mw + (O_mw * 2))) * 10^3,
    is.na(dsi_uM) & !is.na(sio2_ug_SiO2_L) ~ (sio2_ug_SiO2_L / (Si_mw+(O_mw*2))),
    T ~ NA)) %>%
  dplyr::select(-dsi_mg_SiO2_L, -dsi_mg_Si_L,-dsi_ug_Si_L,-sio2_ug_SiO2_L,-silicon_ug_L,-si_ug_L) %>%
  # Fluorine
  dplyr::mutate(f_uM = ifelse(test = (is.na(f_uM) == T), # NOTE this is ifelse(), more than one needs case_when
                              yes = (f_mg_L / F_mw) * 10^3,
                              no = f_uM)) %>%
  dplyr::select(-f_mg_L) %>%
  # Iron
  #dplyr::mutate(fe_uM = (fe_mg_L / Fe_mw) * 10^3, .after = fe_mg_L) %>%
  dplyr::mutate(fe_uM = dplyr::case_when(
    #!is.na(fe_uM) ~ fe_uM,
    !is.na(fe_mg_L) ~ (fe_mg_L / Fe_mw) * 10^3,
    !is.na(fe_ug_L) ~ fe_ug_L / Fe_mw,
    T ~ NA)) %>%
  dplyr::select(-fe_mg_L,-fe_ug_L) %>%
  # Bicarbonate (HCO3)
  dplyr::mutate(hco3_uM = ifelse(test = (is.na(hco3_uM) == T),
                                 yes = (hco3_mg_L / HCO3_mw) * 10^3,
                                 no = hco3_uM)) %>%
  dplyr::select(-hco3_mg_L) %>%
  # Potassium
  dplyr::mutate(k_uM = dplyr::case_when(
    !is.na(k_uM) ~ k_uM,
    is.na(k_uM) & !is.na(k_mg_L) ~ (k_mg_L / K_mw) * 10^3,
    is.na(k_uM) & !is.na(k_ug_L) ~ k_ug_L / K_mw,
    is.na(k_uM) & !is.na(potassium_ug_L) ~ potassium_ug_L / K_mw,
    T ~ NA)) %>%
  dplyr::select(-k_mg_L,-k_ug_L,-potassium_ug_L) %>%
  # Lithium
  dplyr::mutate(li_uM = (li_mg_L / Li_mw) * 10^3, .after = li_mg_L) %>%
  dplyr::select(-li_mg_L) %>%
  # Magnesium
  dplyr::mutate(mg_uM = dplyr::case_when(
    !is.na(mg_uM) ~ mg_uM,
    is.na(mg_uM) & !is.na(mg_mg_L) ~ (mg_mg_L / Mg_mw) * 10^3,
    is.na(mg_uM) & !is.na(mg_ug_L) ~ mg_ug_L / Mg_mw,
    T ~ NA)) %>%
  dplyr::select(-mg_mg_L,-mg_ug_L) %>%
  # Manganese
  dplyr::mutate(mn_uM = dplyr::case_when(
    !is.na(mn_mg_L) ~ (mn_mg_L / Mn_mw) * 10^3,
    !is.na(mn_ug_L) ~ mn_ug_L / Mn_mw,
    T ~ NA)) %>%
  dplyr::select(-mn_mg_L,-mn_ug_L) %>%
  # Sodium
  dplyr::mutate(na_uM = dplyr::case_when(
    !is.na(na_uM) ~ na_uM,
    is.na(na_uM) & !is.na(na_mg_L) ~ (na_mg_L / Na_mw) * 10^3,
    is.na(na_uM) & !is.na(na_ug_L) ~ na_ug_L / Na_mw,
    is.na(na_uM) & !is.na(sodium_ug_L) ~ sodium_ug_L / Na_mw,
    T ~ NA)) %>%
  dplyr::select(-na_mg_L,-na_ug_L,-sodium_ug_L) %>%
  # Ammonia (NH3)
  dplyr::mutate(nh3_uM = dplyr::case_when(
    !is.na(nh3_mg_NH3_N_L) ~ (nh3_mg_NH3_N_L / N_mw) * 10^3,
    !is.na(nh3_mg_NH3_L) ~ (nh3_mg_NH3_L / NH3_mw) * 10^3,
    T ~ NA), .after = nh3_mg_NH3_N_L) %>%
  dplyr::select(-nh3_mg_NH3_N_L,-nh3_mg_NH3_L) %>%
  # Ammonium (NH4)
  dplyr::mutate(nh4_uM = dplyr::case_when(
    !is.na(nh4_uM) ~ nh4_uM,
    is.na(nh4_uM) & !is.na(nh4_mg_NH4_N_L) ~ (nh4_mg_NH4_N_L / N_mw) * 10^3,
    is.na(nh4_uM) & !is.na(nh4_ug_NH4_N_L) ~ nh4_ug_NH4_N_L / N_mw,
    is.na(nh4_uM) & !is.na(nh4_mg_NH4_L) ~ (nh4_mg_NH4_L / NH4_mw) * 10^3,
    T ~ NA)) %>%
  dplyr::select(-nh4_mg_NH4_N_L, -nh4_ug_NH4_N_L,-nh4_mg_NH4_L) %>%
  # Ammoni__ (NHx)
  dplyr::mutate(nhx_uM = (nhx_mg_NH4_N_L / N_mw) * 10^3,
                .after = nhx_mg_NH4_N_L) %>%
  dplyr::select(-nhx_mg_NH4_N_L) %>%
  # Nitrate (NO3)
  dplyr::mutate(no3_uM = dplyr::case_when(
    !is.na(no3_uM) ~ no3_uM,
    is.na(no3_uM) & !is.na(no3_mg_NO3_L) ~ (no3_mg_NO3_L / NO3_mw) * 10^3,
    is.na(no3_uM) & !is.na(no3_ug_NO3_L) ~ (no3_ug_NO3_L / NO3_mw),
    is.na(no3_uM) & !is.na(no3_mg_NO3_N_L) ~ (no3_mg_NO3_N_L / N_mw) * 10^3,
    is.na(no3_uM) & !is.na(no3_mg_N_L) ~ (no3_mg_N_L / N_mw) * 10^3,
    ## see here for how we fixed this!
    is.na(no3_uM) & !is.na(no3_ug_NO3_N_L) ~ (no3_ug_NO3_N_L / N_mw),
    T ~ NA)) %>%
  dplyr::select(-no3_mg_NO3_L,-no3_mg_N_L, -no3_ug_NO3_N_L, -no3_ug_NO3_L,-no3_mg_NO3_N_L) %>% ## make sure to add all original units here!!
  # Nitr__ (NOx)
  dplyr::mutate(nox_uM = dplyr::case_when(
    !is.na(nox_uM) ~ nox_uM,
    is.na(nox_uM) & !is.na(nox_mg_NO3_N_L) ~ (nox_mg_NO3_N_L / N_mw) * 10^3,
    is.na(nox_uM) & !is.na(nox_ug_NO3_N_L) ~ nox_ug_NO3_N_L / N_mw,
    #is.na(nox_uM) & !is.na(nox_mg_L) ~ (nox_mg_L / NO3_mw) * 10^3,
    T ~ NA)) %>%
  dplyr::select(-nox_mg_NO3_N_L, -nox_ug_NO3_N_L) %>%
  # Nitrite (NO2)
  dplyr::mutate(no2_uM = dplyr::case_when(
    !is.na(no2_uM)  ~ no2_uM,
    is.na(no2_uM) & !is.na(no2_mg_NO2_N_L) ~ (no2_mg_NO2_N_L / N_mw) * 10^3,
    T ~ NA)) %>%
  dplyr::select(-no2_mg_NO2_N_L) %>%
  # Phosphate (PO4)
  dplyr::mutate(po4_uM = dplyr::case_when(
    !is.na(po4_uM) ~ po4_uM,
    is.na(po4_uM) & !is.na(po4_mg_PO4_L) ~ (po4_mg_PO4_L / PO4_mw) * 10^3,
    is.na(po4_uM) & !is.na(po4_mg_P_L) ~ (po4_mg_P_L / P_mw) * 10^3,
    is.na(po4_uM) & !is.na(po4_p_ug_PO4_P_L) ~ (po4_p_ug_PO4_P_L/P_mw),
    is.na(po4_uM) & !is.na(po4_ug_PO4_P_L) ~ (po4_ug_PO4_P_L / P_mw),
    T ~ NA)) %>%
  dplyr::select(-po4_mg_PO4_L,-po4_mg_P_L, -po4_ug_PO4_P_L,-po4_ug_PO4_P_L, -po4_p_ug_PO4_P_L) %>%
  # Sulfate (SO4)
  ## UPDATED ##
  # dplyr::mutate(so4_uM = dplyr::case_when(
  #   !is.na(so4_uM) ~ so4_uM,
  #   is.na(so4_uM) & !is.na(so4_ug_SO4_L) ~ (so4_ug_SO4_L / SO4_mw),
  #   is.na(so4_uM) & !is.na(so4_ug_L) ~ (so4_ug_L / SO4_mw),
  #   is.na(so4_uM) & !is.na(so4_mg_SO4_L) ~ (so4_mg_SO4_L / SO4_mw) * 10^3,
  #   is.na(so4_uM) & !is.na(sulfates_mg_S_L) ~ (sulfates_mg_S_L/ S_mw) * 10^3,
  #   T ~ NA)) %>%
  # dplyr::select(-so4_mg_SO4_L,-so4_ug_SO4_L,-sulfates_mg_S_L,-so4_ug_L) %>%
  dplyr::mutate(so4_uM = dplyr::case_when(
    !is.na(so4_uM) ~ so4_uM,
    is.na(so4_uM) & !is.na(so4_ug_L) ~ (so4_ug_L / SO4_mw),
    is.na(so4_uM) & !is.na(so4_ug_S_L) ~ (so4_ug_S_L / S_mw),
    is.na(so4_uM) & !is.na(so4_mg_SO4_L) ~ (so4_mg_SO4_L / SO4_mw) * 10^3,
    is.na(so4_uM) & !is.na(sulfates_mg_S_L) ~ (sulfates_mg_S_L/ S_mw) * 10^3,
    T ~ NA)) %>%
  dplyr::select(-so4_mg_SO4_L, -so4_ug_L, -so4_ug_S_L, -sulfates_mg_S_L) %>%
  # Strontium (Sr)
  dplyr::mutate(sr_uM = (sr_mg_L / Sr_mw) * 10^3,
                .before = sr_mg_L) %>%
  dplyr::select(-sr_mg_L) %>%
  # Soluble Reactive Phosphorus (SRP)
  dplyr::mutate(srp_uM = dplyr::case_when(
    !is.na(srp_uM) ~ srp_uM,
    is.na(srp_uM) & !is.na(srp_mg_P_L) ~ (srp_mg_P_L / P_mw) * 10^3,
    is.na(srp_uM) & !is.na(srp_ug_P_L) ~ (srp_ug_P_L / P_mw),
    T ~ NA)) %>%
  dplyr::select(-srp_mg_P_L, -srp_ug_P_L) %>%
  # Total Dissolved Nitrogen (TDN)
  dplyr::mutate(tdn_uM = dplyr::case_when(
    !is.na(tdn_uM) ~ tdn_uM,
    is.na(tdn_uM) & !is.na(tdn_mg_L) ~ (tdn_mg_L / N_mw) * 10^3,
    is.na(tdn_uM) & !is.na(tdn_mg_N_L) ~ (tdn_mg_N_L / N_mw) * 10^3,
    is.na(tdn_uM) & !is.na(tdn_ug_N_L) ~ (tdn_ug_N_L / N_mw),
    T ~ NA)) %>%
  dplyr::select(-tdn_mg_L,-tdn_mg_N_L,-tdn_ug_N_L) %>%
  # total Kjeldahl nitrogen (TKN)
  dplyr::mutate(tkn_uM = (tkn_mg_N_L / N_mw) * 10^3,
                .after = tkn_mg_N_L) %>%
  dplyr::select(-tkn_mg_N_L) %>%
  # Total Nitrogen (TN)
  ## UPDATED ##
  # dplyr::mutate(tn_uM = ifelse(test = is.na(tn_uM) == T,
  #                              yes = (tn_mg_L / N_mw) * 10^3,
  #                              no = tn_uM)) %>%
  # dplyr::select(-tn_mg_L) %>%
  dplyr::mutate(tn_uM = dplyr::case_when(
    !is.na(tn_uM) ~ tn_uM,
    !is.na(tn_mg_L) ~ (tn_mg_L / N_mw) * 10^3,
    !is.na(tn_ug_L) ~ tn_ug_L / N_mw,
    T ~ NA_real_
  )) %>%
  dplyr::select(-tn_mg_L,-tn_ug_L) %>%
  # Dissolved Organic Nitrogen (TON)
  #dplyr::mutate(don_uM = (don_mg_L / N_mw) * 10^3,
   #             .after = don_mg_L) %>%
  #dplyr::select(-don_mg_L) %>%
  # Total Organic Carbon (TOC)
  dplyr::mutate(toc_uM = dplyr::case_when(
    !is.na(toc_uM) ~ toc_uM,
    #is.na(toc_uM) & !is.na(toc_mg_C_L) ~ (toc_mg_C_L / C_mw) * 10^3,
    is.na(toc_uM) & !is.na(toc_mg_L) ~ (toc_mg_L / C_mw) * 10^3,
    T ~ NA)) %>%
  dplyr::select(-toc_mg_L) %>%
  # Total Phosphorus (TP)
  dplyr::mutate(tp_uM = dplyr::case_when(
    !is.na(tp_uM) ~ tp_uM,
    is.na(tp_uM) & !is.na(tp_mg_P_L) ~ tp_mg_P_L / P_mw * 10^3,
    is.na(tp_uM) & !is.na(tp_mg_L) ~ tp_mg_L / P_mw * 10^3,
    is.na(tp_uM) & !is.na(tp_ug_P_L) ~ tp_ug_P_L / P_mw,
    #is.na(tp_uM) & !is.na(tp_ug_L) ~ tp_ug_L / P_mw,
    T ~ NA)) %>%
  dplyr::select(-tp_mg_P_L, -tp_mg_L,-tp_ug_P_L) %>%
  # Total Dissolved Phosphorus (TDP)
  dplyr::mutate(tdp_uM = dplyr::case_when(
    !is.na(tdp_mg_L) ~ (tdp_mg_L / P_mw) * 10^3,
    !is.na(tdp_ug_P_L) ~ (tdp_ug_P_L / P_mw),
    T ~ NA)) %>%
  dplyr::select(-tdp_mg_L,-tdp_ug_P_L) %>%
  # Zinc
  dplyr::mutate(zn_uM = (zn_mg_L / Zn_mw) * 10^3,
                .after = zn_mg_L) %>%
  dplyr::select(-zn_mg_L)

# Re-check names
tidy_v4b %>%
  dplyr::select(-Dataset:-vss_mg_L) %>%
  names() %>%
  sort()

# Examine full structure too
dplyr::glimpse(tidy_v4b)

## -------------------------------------------- ##
            # Clarification Tweaks ----
## -------------------------------------------- ##

# ---- proposed update: final names and long-format reshape (commented for review) ----
# tidy_v5 <- merge_columns(
#   tidy_v4b,
#   "susp_partic_matter_mg_L",
#   c("spm_mg_L", "tss_mg_L")
# )
# if (anyDuplicated(names(tidy_v5)) > 0) {
#   stop("duplicate column names remain before the final long-format reshape")
# }
#
# variable_names <- c(
#   ph = "pH", dsi = "DSi", do = "dissolved oxygen", chla = "chlorophyll a",
#   dic = "dissolved inorg C", din = "dissolved inorg N",
#   doc = "dissolved org C", don = "dissolved org N", srp = "SRP",
#   ssc = "susp sediment conc", tdn = "tot dissolved N",
#   tds = "tot dissolved solids", tkn = "tot kjeldahl N", tn = "TN",
#   toc = "tot org C", tp = "TP", vss = "volatile susp solids"
# )
#
# tidy_v6_wide <- tidy_v5 %>%
#   dplyr::select(-dplyr::contains("_q_"))
# tidy_v6_chunks <- split(
#   seq_len(nrow(tidy_v6_wide)),
#   (seq_len(nrow(tidy_v6_wide)) - 1L) %/% 10000L
# )
# tidy_v6 <- tidy_v6_chunks %>%
#   purrr::map(function(rows) {
#     tidy_v6_wide %>%
#       dplyr::slice(rows) %>%
#       tidyr::pivot_longer(
#         cols = -Dataset:-date,
#         names_to = "vars_raw",
#         values_to = "value",
#         values_drop_na = TRUE
#       )
#   }) %>%
#   purrr::list_rbind() %>%
#   dplyr::mutate(
#     value_qualifier = dplyr::case_when(
#       Im(value) > 0 ~ "<",
#       Im(value) < 0 ~ ">",
#       TRUE ~ NA_character_
#     ),
#     value = Re(value)
#   ) %>%
#   dplyr::mutate(vars_std = stringr::str_replace(
#     vars_raw,
#     "_(uM|ueq_L|uS_cm|ug(?:_[A-Za-z0-9]+)*_L|mg(?:_[A-Za-z0-9]+)*_L|PCU|C|NTU|FNU|percent|ppt)$",
#     "--\\1"
#   )) %>%
#   tidyr::separate_wider_delim(
#     vars_std,
#     delim = "--",
#     too_few = "align_start",
#     names = c("vars_only", "units_only")
#   ) %>%
#   dplyr::mutate(
#     units = gsub("_", "/", units_only),
#     variable = dplyr::coalesce(
#       unname(variable_names[vars_only]),
#       dplyr::case_when(
#         nchar(vars_only) == 1 ~ toupper(vars_only),
#         nchar(vars_only) == 2 ~ stringr::str_to_title(vars_only),
#         nchar(vars_only) < 4 & stringr::str_detect(vars_only, "x") ~ toupper(vars_only),
#         stringr::str_detect(vars_only, "[[:digit:]]") ~ toupper(vars_only),
#         TRUE ~ gsub("_", " ", vars_only)
#       )
#     ),
#     variable = gsub("Ph", "pH", variable),
#     variable = gsub("Ec", "EC", variable),
#     variable = gsub("NOX", "NOx", variable)
#   ) %>%
#   dplyr::select(-vars_raw, -dplyr::ends_with("_only")) %>%
#   dplyr::relocate(value, value_qualifier, .after = dplyr::everything())
# rm(tidy_v6_wide, tidy_v6_chunks)
#
# ---- kj's original: final names and long-format reshape ----
# Some of these column names are heavily abbreviated
# Can expand them to make them a little more intuitive
tidy_v5 <- tidy_v4b %>%
  # And consensus was that TSS & SPM are equivalent
  dplyr::mutate(spm_actual = dplyr::coalesce(spm_mg_L, tss_mg_L,susp_partic_matter_mg_L)) %>%
  dplyr::select(-spm_mg_L, -tss_mg_L,-susp_partic_matter_mg_L) %>%
  # Expand column names to be more descriptive
  dplyr::rename(
    # DO
    dissolved_oxygen_uM = do_uM,
    # DOC/DIC
    dissolved_org_c_uM = doc_uM,
    dissolved_inorg_c_uM = dic_uM,
    # DON/DIN
    dissolved_org_n_uM = don_uM,
    dissolved_inorg_n_uM = din_uM,
    # SRP
    soluble_reactive_p_uM = srp_uM,
    # SPM / TSS
    susp_partic_matter_uM = spm_actual,
    # SSC
    susp_sediment_conc_mg_L = ssc_mg_L,
    # TDS
    tot_dissolved_solids_mg_L = tds_mg_L,
    # TDN
    tot_dissolved_n_uM = tdn_uM,
    # TN
    tot_n_uM = tn_uM,
    # TKN
    tot_kjeldahl_n_uM = tkn_uM,
    # TOC
    tot_org_c_uM = toc_uM,
    # TP
    tot_p_uM = tp_uM,
    # VSS
    volatile_susp_solids_mg_L = vss_mg_L) %>%
  # Reorder these slightly
  dplyr::relocate(dplyr::starts_with("susp_"), dplyr::starts_with("tot_"),
                  dplyr::starts_with("dissolved_"), soluble_reactive_p_uM,
                  .after = volatile_susp_solids_mg_L)

# Re-check structure
dplyr::glimpse(tidy_v5)

## -------------------------------------------- ##
            # Change Data Shape ----
## -------------------------------------------- ##

# Data need to be in long format so we'll do that here
tidy_v6 <- tidy_v5 %>%
  # Drop discharge columns
  dplyr::select(-dplyr::contains("_q_")) %>%
  # Flip variables into long format
  tidyr::pivot_longer(cols = -Dataset:-date,
                      names_to = "vars_raw", values_to = "value") %>%
  # Filter out all the NAs that introduces
  dplyr::filter(!is.na(value)) %>%
  # Create a standard character between variable name and units
  dplyr::mutate(vars_std = gsub(pattern = "_uM", replacement = "--uM", x = vars_raw)) %>%
  dplyr::mutate(vars_std = gsub(pattern = "_uS", replacement = "--uS", x = vars_std)) %>%
  dplyr::mutate(vars_std = gsub(pattern = "_ug", replacement = "--ug", x = vars_std)) %>%
  dplyr::mutate(vars_std = gsub(pattern = "_mg", replacement = "--mg", x = vars_std)) %>%
  dplyr::mutate(vars_std = gsub(pattern = "_C", replacement = "--C", x = vars_std)) %>%
  dplyr::mutate(vars_std = gsub(pattern = "_NTU", replacement = "--NTU", x = vars_std)) %>%
  # Separate solute/units into different columns
  tidyr::separate_wider_delim(cols = vars_std, delim = "--", too_few = "align_start",
                              names = c("vars_only", "units_only")) %>%
  # Process the units formatting slightly
  dplyr::mutate(units = gsub(pattern = "_", replacement = "/", x = units_only)) %>%
  # Now wrangle the variable names
  dplyr::mutate(variable = dplyr::case_when(
    ## Fix pH
    vars_only == "ph" ~ "pH",
    ## Fix Silica!
    vars_only == "dsi" ~ "DSi",
    ## If one or two letters (i.e., is an element), make the first capital
    nchar(vars_only) == 1 ~ toupper(vars_only),
    nchar(vars_only) == 2 ~ stringr::str_to_title(vars_only),
    ## If there's an X or a number (i.e., is a molecule), make the whole thing uppercase
    nchar(vars_only) < 4 &
      stringr::str_detect(string = vars_only, pattern = "x") ~ toupper(vars_only),
    stringr::str_detect(string = vars_only, pattern = "[[:digit:]]") ~ toupper(vars_only),
    ## Handle idiosyncratic fixes
    vars_only == "chla" ~ "chlorophyll a",
    vars_only == "dissolved_inorg_c" ~ "dissolved inorg C",
    vars_only == "dissolved_inorg_n" ~ "dissolved inorg N",
    vars_only == "dissolved_org_c" ~ "dissolved org C",
    vars_only == "dissolved_org_n" ~ "dissolved org N",
    vars_only == "soluble_reactive_p" ~ "SRP",
    vars_only == "tot_dissolved_n" ~ "tot dissolved N",
    vars_only == "tot_kjeldahl_n" ~ "tot kjeldahl N",
    vars_only == "tot_n" ~ "TN",
    vars_only == "tot_org_c" ~ "tot org C",
    vars_only == "tot_p" ~ "TP",
    ## Otherwise just swap underscores for spaces
    T ~ gsub(pattern = "_", replacement = " ", x = vars_only)),
    .before = units) %>%
  # Fix pernicious issues with a few oddballs
  dplyr::mutate(variable = gsub(pattern = "Ph", replacement = "pH", x = variable),
                variable = gsub(pattern = "Ec", replacement = "EC", x = variable),
                variable = gsub(pattern = "NOX", replacement = "NOx", x = variable)) %>%
  # Drop columns we made to get here
  dplyr::select(-vars_raw, -dplyr::ends_with("_only")) %>%
  # Move value to the end
  dplyr::relocate(value, .after = dplyr::everything())

# Happy with resulting variables/units?
sort(unique(tidy_v6$variable))
sort(unique(tidy_v6$units))

# Check structure
dplyr::glimpse(tidy_v6)

## -------------------------------------------- ##
      # Handle Outliers, Variables Names ----
## -------------------------------------------- ##

# ---- proposed update: outliers and site-name matching (commented for review) ----
# # keep reported values in master chemistry
# # outliers are reviewed later with the shared outlier procedure
# tidy_v7b <- tidy_v6
#
# # keep kj's reviewed choices where no3 and nox overlap
# drop_nitrogen <- with(
#   tidy_v7b,
#   (LTER == "Krycklan" & variable == "NO3") |
#     (Stream_Name == "Biscuit Brook" & variable == "NOx") |
#     (
#       Stream_Name %in% c(
#         "Frio River",
#         "Cane Creek",
#         "Merced River",
#         "Turkey River",
#         "James River",
#         "Corral Gulch",
#         "Flat Brook",
#         "Choptank River",
#         "Green River",
#         "Smith Creek",
#         "Sopchoppy River"
#       ) & variable == "NO3"
#     ) |
#     (
#       Raw_Filename == "CAMELS_USGS_N_P.csv" &
#         Stream_Name %in% c(
#           "North Slyamore Creek",
#           "Vallecito Creek",
#           "West Clear Creek"
#         ) &
#         variable == "NO3"
#     )
# )
# drop_nitrogen[is.na(drop_nitrogen)] <- FALSE
# tidy_v7c <- tidy_v7b %>%
#   dplyr::filter(!drop_nitrogen)
#
# # keep kj's reviewed choices where srp and po4 overlap
# drop_phosphorus <- with(
#   tidy_v7c,
#   (LTER == "Krycklan" & variable == "SRP") |
#     (Stream_Name %in% c("MGWEIR", "ORlow") & variable == "SRP")
# )
# drop_phosphorus[is.na(drop_phosphorus)] <- FALSE
# tidy_v7d <- tidy_v7c %>%
#   dplyr::filter(!drop_phosphorus)
#
# # keep kj's reviewed Sopchoppy source and name choices
# tidy_v8a <- tidy_v7d %>%
#   dplyr::filter(!(
#     Stream_Name == "Sopchoppy River" &
#       Raw_Filename %in% c(
#         "USGS_geogenic_solutes.csv",
#         "20221030_masterdata_chem_V2.csv"
#       )
#   )) %>%
#   dplyr::mutate(
#     Stream_Name = dplyr::if_else(
#       Raw_Filename == "CAMELS_USGS_N_P.csv" &
#         Stream_Name == "Sopchoppy River",
#       "SOPCHOPPY RIVER",
#       Stream_Name
#     )
#   ) %>%
#   dplyr::filter(!(
#     Stream_Name == "SOPCHOPPY RIVER" &
#       Raw_Filename == "20221030_masterdata_chem_V2.csv" &
#       variable %in% c("NOx", "PO4", "dissolved org C")
#   ))
#
# primary_names <- ref_table %>%
#   dplyr::transmute(target_lter = LTER, target_name = Stream_Name, source_name = Stream_Name)
# alternate_names <- ref_table %>%
#   dplyr::transmute(target_lter = LTER, target_name = Stream_Name, source_name = Alt_Stream_Name) %>%
#   tidyr::separate_longer_delim(source_name, delim = ";")
# original_names <- ref_table %>%
#   dplyr::transmute(target_lter = LTER, target_name = Stream_Name, source_name = Original_Stream_Name)
#
# reference_name_lookup <- dplyr::bind_rows(
#   primary_names,
#   alternate_names,
#   original_names
# ) %>%
#   dplyr::filter(!is.na(source_name), trimws(source_name) != "") %>%
#   dplyr::mutate(name_key = stringr::str_to_lower(stringr::str_squish(source_name))) %>%
#   dplyr::distinct(name_key, target_lter, target_name)
#
# ambiguous_names <- reference_name_lookup %>%
#   dplyr::count(name_key) %>%
#   dplyr::filter(n > 1L)
# if (nrow(ambiguous_names) > 0) {
#   stop("the site reference contains ambiguous primary or alternate names")
# }
#
# tidy_v8f <- tidy_v8a %>%
#   dplyr::mutate(name_key = stringr::str_to_lower(stringr::str_squish(Stream_Name))) %>%
#   dplyr::left_join(reference_name_lookup, by = "name_key")
#
# # exclude only the reviewed unmatched sites that cannot enter the river-and-dsi scope
# reviewed_unmatched_sites <- tibble::tribble(
#   ~Raw_Filename, ~Stream_Name, ~reason,
#   "CatalinaJemez_chemistry_2009-2019_V2.csv", "MG_S_SEEP", "not_river",
#   "WesternAus_AllSites_Filtered_v2.csv", "Collie River - Central Collie", "no_dsi",
#   "Finnish_riverine_data_07032025.csv", "Kisko 14 Vanhak va6111", "no_dsi",
#   "Finnish_riverine_data_07032025.csv", "Uske 16 Salon yp va6101", "no_dsi",
#   "Finnish_riverine_data_07032025.csv", "Pajo 44 Isosilta va6301", "no_dsi",
#   "Finnish_riverine_data_07032025.csv", "Aura 54 ohikulku va6401", "no_dsi",
#   "Finnish_riverine_data_07032025.csv", "Eura 42 Pori-Rma va6900", "no_dsi",
#   "Finnish_riverine_data_07032025.csv", "Kojo 35 Pori-Tre", "no_dsi",
#   "Finnish_riverine_data_07032025.csv", "Merikarvianjoki Vaadinni", "no_dsi",
#   "UK_nonSi_solutes.csv", "EDEN AT SHEEPMOUNT", "no_dsi",
#   "UK_nonSi_solutes.csv", "TWEED AT NORHAM", "no_dsi",
#   "UK_nonSi_solutes.csv", "ESK AT SLEIGHTS", "no_dsi",
#   "UK_nonSi_solutes.csv", "CALDER AT METHLEY", "no_dsi",
#   "Krycklan_Cation_Clean.csv", "Site 3", "no_dsi",
#   "Krycklan_Cation_Clean.csv", "Site 8", "no_dsi",
#   "Krycklan_Cation_Clean.csv", "Site 11", "no_dsi",
#   "Krycklan_Cation_Clean.csv", "Site 12", "no_dsi",
#   "Krycklan_Cation_Clean.csv", "Site 15", "no_dsi",
#   "Krycklan_Cation_Clean.csv", "Site 17", "no_dsi",
#   "Krycklan_Cation_Clean.csv", "Site 18", "no_dsi",
#   "Krycklan_Cation_Clean.csv", "Site 19", "no_dsi",
#   "Krycklan_Cation_Clean.csv", "Site 20", "no_dsi",
#   "Krycklan_Cation_Clean.csv", "Site 21", "no_dsi"
# )
#
# unmatched_site_scope <- tidy_v8f %>%
#   dplyr::filter(is.na(target_name)) %>%
#   dplyr::group_by(Raw_Filename, Stream_Name) %>%
#   dplyr::summarise(has_dsi = any(variable == "DSi"), .groups = "drop")
#
# unreviewed_unmatched <- dplyr::anti_join(
#   unmatched_site_scope,
#   reviewed_unmatched_sites,
#   by = c("Raw_Filename", "Stream_Name")
# )
# missing_reviewed_unmatched <- dplyr::anti_join(
#   reviewed_unmatched_sites,
#   unmatched_site_scope,
#   by = c("Raw_Filename", "Stream_Name")
# )
# if (nrow(unreviewed_unmatched) > 0L || nrow(missing_reviewed_unmatched) > 0L) {
#   stop("the reviewed unmatched-site set changed")
# }
#
# unmatched_site_scope <- unmatched_site_scope %>%
#   dplyr::left_join(
#     reviewed_unmatched_sites,
#     by = c("Raw_Filename", "Stream_Name")
#   )
# if (any(unmatched_site_scope$reason == "no_dsi" & unmatched_site_scope$has_dsi) ||
#     any(unmatched_site_scope$reason == "not_river" & !unmatched_site_scope$has_dsi)) {
#   stop("the reviewed unmatched-site scope changed")
# }
#
# tidy_v8f <- dplyr::anti_join(
#   tidy_v8f,
#   reviewed_unmatched_sites,
#   by = c("Raw_Filename", "Stream_Name")
# )
# if (anyNA(tidy_v8f$target_name)) {
#   stop("unreviewed site names are missing from the reference table")
# }
#
# tidy_v8h <- tidy_v8f %>%
#   dplyr::mutate(Stream_Name = target_name, LTER = target_lter) %>%
#   dplyr::select(-name_key, -target_name, -target_lter) %>%
#   dplyr::distinct()
#
# ---- kj's original: outliers and site-name matching ----
# Now we need to identify / handle outlier values
tidy_v7a <- tidy_v6 %>%
  # Group by everything except value and date
  dplyr::group_by(Dataset, Raw_Filename, LTER, Stream_Name, variable, units) %>%
  # Calculate some needed metrics
  dplyr::mutate(mean_value = mean(value, na.rm = T),
                sd_value = sd(value, na.rm = T),
                outlier_thresh = (abs(mean_value) + abs(sd_value * 4)) ) %>%
  # Identify outliers
  dplyr::mutate(is_outlier = ifelse(test = abs(value) > outlier_thresh,
                                    yes = TRUE, no = FALSE)) %>%
  # Ungroup
  dplyr::ungroup()

# Identify percent outliers
(nrow(dplyr::filter(tidy_v7a, is_outlier == T)) / nrow(tidy_v6)) * 100

# what is it identifying as outliers?
# alot of Australia values
tidy_v7a |>
  filter(is_outlier == "TRUE") |>
  aggregate(value~Dataset, FUN=length)

# Handle outliers
tidy_v7b <- tidy_v7a %>%
  # Currently handling outliers by removing them
  dplyr::filter(is_outlier == FALSE | is.na(is_outlier)) %>%
  # Then drop the columns we needed for this filtering step
  dplyr::select(-mean_value, -sd_value, -outlier_thresh, -is_outlier)

# Did that work? Should lose some rows
nrow(tidy_v7a) - nrow(tidy_v7b)

# Glimpse it
dplyr::glimpse(tidy_v7b)

## Remove duplicate NOx and NO3 data from select sites
tidy_v7c <- tidy_v7b %>%
  dplyr::mutate(value = dplyr::case_when(
    LTER == "Krycklan" & variable == "NO3" ~ NA,
    Stream_Name == "Biscuit Brook" & variable == "NOx"~ NA,
    Stream_Name == "Frio River" & variable == "NO3"~ NA,
    Stream_Name == "Cane Creek" & variable == "NO3"~ NA,
    Stream_Name == "Merced River" & variable == "NO3"~ NA,
    Stream_Name == "Turkey River" & variable == "NO3"~ NA,
    Stream_Name == "James River" & variable == "NO3"~ NA,
    Stream_Name == "Corral Gulch" & variable == "NO3"~ NA,
    Stream_Name == "Flat Brook" & variable == "NO3"~ NA,
    Stream_Name == "Choptank River" & variable == "NO3" ~ NA,
    Stream_Name == "Green River" & variable == "NO3" ~ NA,
    ## UPDATED ##
    # Stream_Name == "North Slyamore Creek" & variable == "NO3" ~ NA,
    Raw_Filename == "CAMELS_USGS_N_P.csv" &
      Stream_Name == "North Slyamore Creek" & variable == "NO3" ~ NA,
    Stream_Name == "Smith Creek" & variable == "NO3" ~ NA,
    Stream_Name == "Sopchoppy River" & variable == "NO3" ~ NA,
    ## UPDATED ##
    # Stream_Name == "Vallecito Creek" & variable == "NO3" ~ NA,
    Raw_Filename == "CAMELS_USGS_N_P.csv" &
      Stream_Name == "Vallecito Creek" & variable == "NO3" ~ NA,
    ## UPDATED ##
    # Stream_Name == "West Clear Creek" & variable == "NO3" ~ NA,
    Raw_Filename == "CAMELS_USGS_N_P.csv" &
      Stream_Name == "West Clear Creek" & variable == "NO3" ~ NA,
    .default = value
  ))

# remove duplicate SRP/PO4
tidy_v7d <- tidy_v7c %>%
  dplyr::mutate(value = dplyr::case_when(
    LTER == "Krycklan" & variable == "SRP" ~ NA,
    Stream_Name == "MG_WEIR" & variable == "SRP"~ NA,
    Stream_Name == "OR_low" & variable == "SRP"~ NA,
    .default = value
  ))

## -------------------------------------------- ##
# Fix duplicate stream names ----
## -------------------------------------------- ##


### Condense Finnish site synonym names ###
## A given site has one name for silica and a diff name for all other chemicals
tidy_v8a <- tidy_v7d |>
  dplyr::mutate(Stream_Name = dplyr::case_when(
  Stream_Name == "Site 1069" ~ "Mustionjoki 4,9  15500",
  Stream_Name == "Site 11310" ~ "Virojoki 006 3020",
  Stream_Name == "Site 11523" ~ "Kymijoki Ahvenkoski 001",
  Stream_Name == "Site 11532" ~ "Kymijoki Kokonkoski 014",
  Stream_Name == "Site 11564" ~ "Kymij Huruksela 033 5600",
  Stream_Name == "Site 227" ~ "Koskenkylanjoki 6030",
  Stream_Name == "Site 26534" ~ "Lapuanjoki 9900",
  Stream_Name == "Site 26740" ~ "Perhonjoki 10600",
  Stream_Name == "Site 26935" ~ "Lestijoki 10800 8-tien s",
  Stream_Name == "Site 27095" ~ "Kalajoki 11000",
  Stream_Name == "Site 27697" ~ "Pyhajoki Hourunk 11400",
  Stream_Name == "Site 27880" ~ "Siikajoki 8-tien s 11600",
  Stream_Name == "Site 28208" ~ "Oulujoki 13000",
  Stream_Name == "Site 28414" ~ "Kiiminkij 13010 4-tien s",
  Stream_Name == "Site 28639" ~ "Iijoki Raasakan voimal",
  Stream_Name == "Site 36177" ~ "SIMOJOKI AS. 13500",
  Stream_Name == "Site 397" ~ "Porvoonjoki 11,5  6022",
  Stream_Name == "Site 39892" ~ "KEMIJOKI ISOHAARA 14000",
  Stream_Name == "Site 39974" ~ "TORNIONJ KUKKOLA 14310",
  Stream_Name == "Site 4081" ~ "Myllykanava vp 9100",
  Stream_Name == "Site 4381" ~ "Skatila vp 9600",
  Stream_Name == "Site 567" ~ "Mustijoki 4,2  6010",
  Stream_Name == "Site 605" ~ "Vantaa 4,2  6040",
  Stream_Name == "Site 69038" ~ "Narpionjoki mts 6761",
  TRUE ~ Stream_Name)) |>
  dplyr::mutate(Stream_Name = dplyr::case_match(Stream_Name,
                                                "N<e4>rpi<f6>njoki mts 6761" ~  "Narpionjoki mts 6761",
                                                "Pyh<e4>joki Hourunk 11400" ~ "Pyhajoki Hourunk 11400",
                                                "Koskenkyl<e4>njoki 6030" ~ "Koskenkylanjoki 6030",
                                                #"SIMOJOKI AS. 13500" ~ "SIMOJOKI AS 13500",
                                                #"Lestijoki 10800 8-tien s" ~ "Lestijoki 10800 8tien s",
                                                #"Porvoonjoki 11,5  6022" ~ "Porvoonjoki 115  6022",
                                                #"Mustionjoki 4,9  15500"~"Mustionjoki 49  15500",
                                                #"Mustijoki 4,2  6010"~"Mustijoki 42  6010",
                                                #"Vantaa 4,2  6040"~"Vantaa 42  6040",
                                                .default = Stream_Name)) %>%
  # correcting some duplicates
  dplyr::mutate(Stream_Name = dplyr::case_when(
    Stream_Name == "Koskenkylänjoki 6030"~ "Koskenkylanjoki 6030",
    Stream_Name == "Närpiönjoki mts 6761" ~"Narpionjoki mts 6761",
    Stream_Name == "Pyhäjoki Hourunk 11400" ~ "Pyhajoki Hourunk 11400",
    Stream_Name == "SIMOJOKI AS. 13500      "  ~ "SIMOJOKI AS. 13500",
    Stream_Name == "TORNIONJ KUKKOLA 14310  " ~ "TORNIONJ KUKKOLA 14310",
    TRUE ~ Stream_Name)
  )

# check Finnish name conversions
tidy_v8a |>
  filter(LTER == "Finnish Environmental Institute") |>
  filter(variable == "DSi") |>
  group_by(Stream_Name) |>
  dplyr::summarize(n=n()) |>
  print(n=30)

### Fix Sopchoppy River duplicate names and solutes ###

## remove "Sopchoppy River" from USGS geogenic solutes dataset - SOPCHOPPY RIVER and Sopchoppy River have same data
tidy_v8b <- tidy_v8a %>%
  filter(!(Stream_Name == "Sopchoppy River" & Raw_Filename == "USGS_geogenic_solutes.csv"))

unique(filter(tidy_v8b, Raw_Filename == "USGS_geogenic_solutes.csv")$Stream_Name)
nrow(tidy_v8a)-nrow(tidy_v8b)

## remove "Sopchoppy River" from old masterdata dataset - SOPCHOPPY RIVER and Sopchoppy River have similar data for DSi, but SOPCHOPPY is longer
tidy_v8c <- tidy_v8b %>%
  filter(!(Stream_Name == "Sopchoppy River" & Raw_Filename == "20221030_masterdata_chem_V2.csv"))

unique(filter(tidy_v8c, Raw_Filename == "20221030_masterdata_chem_V2.csv")$Stream_Name)
nrow(tidy_v8b)-nrow(tidy_v8c)

## rename Sopchoppy  River to SOPCHOPPY RIVER in CAMELS dataset
tidy_v8d <- tidy_v8c %>%
  mutate(Stream_Name = case_when(Raw_Filename == "CAMELS_USGS_N_P.csv" & Stream_Name == "Sopchoppy River" ~ "SOPCHOPPY RIVER",
                                 .default = Stream_Name))

##  Remove duplicate solutes from masterdata and keep CAMELS/geogenic values
tidy_v8e <- tidy_v8d %>%
  filter(!(Stream_Name == "SOPCHOPPY RIVER" & Raw_Filename == "20221030_masterdata_chem_V2.csv" & variable == "NOx")) %>%
  filter(!(Stream_Name == "SOPCHOPPY RIVER" & Raw_Filename == "20221030_masterdata_chem_V2.csv"& variable == "PO4")) %>%
  filter(!(Stream_Name == "SOPCHOPPY RIVER" & Raw_Filename == "20221030_masterdata_chem_V2.csv"& variable == "dissolved org C"))

# check SOPCHOPPY vs Sopchoppy River
tidy_v8e %>%
  filter(Stream_Name == "Sopchoppy River")

tidy_v8e %>%
  filter(Stream_Name == "SOPCHOPPY RIVER")

### Filter to only what sites are in the Site Reference Table ###
tidy_v8f <- tidy_v8e %>%
  filter(Stream_Name %in% ref_table$Stream_Name)

# check how many sites removed
length(unique(tidy_v8e$Stream_Name))-length(unique(tidy_v8f$Stream_Name))


### Replace larger dataset values with values from "deduplicated sites ###

# Load data ----------------------------------------------------

master <- tidy_v8f %>%
  filter(Raw_Filename != "Deduplicated_Sites_AllSites_chemistry.csv")

dedup <- tidy_v8f %>%
  filter(Raw_Filename ==  "Deduplicated_Sites_AllSites_chemistry.csv")

required_cols <- c("Dataset", "Raw_Filename", "LTER", "Stream_Name",
                   "date", "variable", "units", "value")

stopifnot(all(required_cols %in% names(master)))
stopifnot(all(required_cols %in% names(dedup)))

# Normalize keys for matching -----------------------------------
# (trims whitespace so " Ca" and "Ca" are treated as the same match)
norm <- function(x) trimws(as.character(x))

master <- master %>%
  mutate(.match_key = paste(norm(Stream_Name), norm(variable), sep = "||"))

dedup <- dedup %>%
  mutate(.match_key = paste(norm(Stream_Name), norm(variable), sep = "||"))

dedup_keys <- unique(dedup$.match_key)

# Split master into kept vs. removed ----------------------------
master_kept    <- master %>% filter(!(.match_key %in% dedup_keys))
master_removed <- master %>% filter(.match_key %in% dedup_keys)

# Combine kept master rows with all dedup rows -------------------
merged <- bind_rows(
  master_kept %>% select(all_of(required_cols)),
  dedup       %>% select(all_of(required_cols))
)

# --- 5. Diagnostics (prints a summary, mirrors the checks done earlier) -
cat("Master rows (total):           ", nrow(master), "\n")
cat("Master rows removed (matched): ", nrow(master_removed), "\n")
cat("Master rows kept (unmatched):  ", nrow(master_kept), "\n")
cat("Dedup rows inserted:           ", nrow(dedup), "\n")
cat("Final merged row count:        ", nrow(merged), "\n")

new_pairs <- setdiff(dedup_keys, unique(master$.match_key))
cat("Dedup pairs with no match in master (net-new, still inserted):",
    length(new_pairs), "\n")

tidy_v8g <- merged


# remove totally duplicated rows

n_dup <- sum(duplicated(tidy_v8g)) # >12,000 rows were duplicated?!

tidy_v8h <- tidy_v8g %>%
  distinct()


## -------------------------------------------- ##
                # Wrangle Dates ----
## -------------------------------------------- ##

# ---- proposed update: date handling (commented for review) ----
# tidy_v9a <- tidy_v8h %>%
#   dplyr::mutate(
#     date_v2 = gsub(
#       pattern = " [[:digit:]]{1,2}\\:[[:digit:]]{1,2}",
#       replacement = "",
#       x = date
#     ),
#     date_v3 = gsub(pattern = "\\_|\\-", replacement = "/", x = date_v2),
#     date_v4 = gsub(pattern = "\\:00", replacement = "", x = date_v3),
#     .after = date
#   ) %>%
#   dplyr::rename(date_v1 = date)
#
# date_tokens <- stringr::str_split_fixed(tidy_v9a$date_v4, "/", 3)
# four_digit_year_tokens <- rowSums(
#   apply(date_tokens, 2, stringr::str_detect, "^[0-9]{4}$")
# )
# if (any(four_digit_year_tokens != 1L)) {
#   stop("chemistry dates must contain exactly one four-digit year token")
# }
#
# detect_date_format <- function(date_strings, delim = "/") {
#   parts <- stringr::str_split_fixed(
#     date_strings,
#     stringr::fixed(delim),
#     3
#   )
#   if (ncol(parts) < 3 || all(parts == "")) return("NO_DELIM_MATCH")
#
#   c1 <- suppressWarnings(as.numeric(parts[, 1]))
#   c2 <- suppressWarnings(as.numeric(parts[, 2]))
#   c3 <- suppressWarnings(as.numeric(parts[, 3]))
#   if (all(is.na(c1)) && all(is.na(c2)) && all(is.na(c3))) {
#     return("UNPARSEABLE")
#   }
#
#   components <- list(c1, c2, c3)
#   looks_like_year <- function(values) {
#     any(nchar(trimws(as.character(values))) == 4 & !is.na(values))
#   }
#   year_positions <- which(vapply(components, looks_like_year, logical(1)))
#   if (length(year_positions) != 1L) return("AMBIGUOUS_YEAR_POSITION")
#
#   year_position <- year_positions[[1]]
#   if (year_position == 1L) return("ymd")
#   if (year_position == 3L) {
#     max_c1 <- max(c1, na.rm = TRUE)
#     max_c2 <- max(c2, na.rm = TRUE)
#     if (max_c1 > 12 && max_c2 <= 12) return("dmy")
#     if (max_c2 > 12 && max_c1 <= 12) return("mdy")
#     if (max_c1 > 12 && max_c2 > 12) return("CONFLICTING_BOTH_OVER_12")
#     return("AMBIGUOUS_DAY_MONTH")
#   }
#   "UNEXPECTED_YEAR_POSITION"
# }
#
# detected_formats <- tidy_v9a %>%
#   dplyr::group_by(Raw_Filename) %>%
#   dplyr::summarise(
#     detected_format = detect_date_format(date_v4),
#     .groups = "drop"
#   )
#
# # require one clear date format for each file
# needs_review <- detected_formats %>%
#   dplyr::filter(!detected_format %in% c("ymd", "mdy", "dmy"))
# if (nrow(needs_review) > 0) {
#   stop("prepare ambiguous source dates for: ", paste(needs_review$Raw_Filename, collapse = ", "))
# }
# date_format_lookup <- detected_formats %>%
#   dplyr::select(Raw_Filename, date_format = detected_format)
#
# tidy_v9b <- tidy_v9a %>%
#   dplyr::left_join(date_format_lookup, by = "Raw_Filename")
#
# tidy_v9c <- tidy_v9b %>%
#   tidyr::separate_wider_delim(
#     cols = date_v4,
#     delim = "/",
#     names = c("date1", "date2", "date3"),
#     too_few = "align_start",
#     too_many = "merge",
#     cols_remove = FALSE
#   ) %>%
#   dplyr::mutate(dplyr::across(date1:date3, as.numeric))
#
# tidy_v9d <- tidy_v9c %>%
#   dplyr::mutate(
#     day = dplyr::case_when(
#       date_format == "ymd" ~ date3,
#       date_format == "mdy" ~ date2,
#       date_format == "dmy" ~ date1,
#       TRUE ~ NA_real_
#     ),
#     month = dplyr::case_when(
#       date_format == "ymd" ~ date2,
#       date_format == "mdy" ~ date1,
#       date_format == "dmy" ~ date2,
#       TRUE ~ NA_real_
#     ),
#     year = dplyr::case_when(
#       date_format == "ymd" ~ date1,
#       date_format == "mdy" ~ date3,
#       date_format == "dmy" ~ date3,
#       TRUE ~ NA_real_
#     ),
#     .after = date_v4
#   )
#
# tidy_v9e <- tidy_v9d %>%
#   dplyr::mutate(
#     date_v5 = paste(day, month, year, sep = "-"),
#     date_actual = as.Date(date_v5, format = "%d-%m-%Y"),
#     .after = date_v4
#   )
#
# tidy_v9f <- tidy_v9e %>%
#   dplyr::rename(date = date_actual) %>%
#   dplyr::select(
#     -date1, -date2, -date3, -day, -month, -year,
#     -dplyr::starts_with("date_")
#   )
#
# # do not guess two-digit years because source dates are prepared upstream
# if (anyNA(tidy_v9f$date)) stop("unparsed or missing chemistry dates remain")
#
# ---- kj's original: date handling ----
# check the available dates without printing the full list
invisible(sort(unique(tidy_v8h$date)))

# Try to standardize date formatting a bit
tidy_v9a <- tidy_v8h %>%
  # Remove time stamps where present
  dplyr::mutate(date_v2 = gsub(pattern = " [[:digit:]]{1,2}\\:[[:digit:]]{1,2}",
                               replacement = "", x = date),
                .after = date) %>%
  # Standardize to only using slashes between numbers
  dplyr::mutate(date_v3 = gsub(pattern = "\\_|\\-", replacement = "/", x = date_v2),
                .after = date_v2) %>%
  # Remove bizarre trailing ":00" on some dates
  dplyr::mutate(date_v4 = gsub(pattern = "\\:00", replacement = "", x = date_v3),
                .after = date_v3) %>%
  # Rename original date column
  dplyr::rename(date_v1 = date)


## function to automate date detection in by raw filename
detect_date_format <- function(date_strings, delim = "/") {

  parts <- stringr::str_split_fixed(date_strings, stringr::fixed(delim), 3)
  if (ncol(parts) < 3 || all(parts == "")) return("NO_DELIM_MATCH")

  c1 <- suppressWarnings(as.numeric(parts[, 1]))
  c2 <- suppressWarnings(as.numeric(parts[, 2]))
  c3 <- suppressWarnings(as.numeric(parts[, 3]))

  if (all(is.na(c1)) && all(is.na(c2)) && all(is.na(c3))) return("UNPARSEABLE")

  comps <- list(c1, c2, c3)

  # Step 1: find year position -- 4-digit values, or values > 31
  # (can't be a day or month), anywhere in the column
  looks_like_year <- function(x) any(nchar(trimws(as.character(x))) == 4 & !is.na(x)) |
    any(x > 31, na.rm = TRUE)

  year_candidates <- which(vapply(comps, looks_like_year, logical(1)))

  if (length(year_candidates) != 1) {
    return("AMBIGUOUS_YEAR_POSITION")
  }
  year_pos <- year_candidates

  # Only 3 layouts are considered, matching your existing pipeline's
  # assumptions: year-first (ymd), or year-last (mdy or dmy)
  if (year_pos == 1) {
    return("ymd")  # assumes month, day order after year -- flip if you use ydm anywhere
  }

  if (year_pos == 3) {
    max_c1 <- max(c1, na.rm = TRUE)
    max_c2 <- max(c2, na.rm = TRUE)

    if (max_c1 > 12 & max_c2 <= 12) return("dmy")
    if (max_c2 > 12 & max_c1 <= 12) return("mdy")
    if (max_c1 > 12 & max_c2 > 12) return("CONFLICTING_BOTH_OVER_12") # shouldn't happen if truly day/month
    return("AMBIGUOUS_DAY_MONTH")  # both components always <= 12 -- can't disambiguate from data
  }

  return("UNEXPECTED_YEAR_POSITION")  # year in position 2 -- unusual, worth a manual look
}


detected_formats <- tidy_v9a %>%
  dplyr::group_by(Raw_Filename) %>%
  dplyr::summarize(
    n_rows            = dplyr::n(),
    detected_format   = detect_date_format(date_v4, delim = "/"),
    example_dates     = paste(head(unique(date_v4), 3), collapse = "; "),
    .groups = "drop"
  )

# Files that resolved cleanly
detected_formats %>%
  dplyr::filter(detected_format %in% c("ymd", "mdy", "dmy")) %>%
  dplyr::count(detected_format)

# Files needing manual review -- either genuinely ambiguous from the data,
# or hit an unexpected pattern (wrong delimiter, missing data, etc.)
needs_review <- detected_formats %>%
  dplyr::filter(!detected_format %in% c("ymd", "mdy", "dmy"))

dplyr::glimpse(needs_review)

reviewed  <- needs_review %>%
  mutate(detected_format = case_when(detected_format == "AMBIGUOUS_YEAR_POSITION" ~ "mdy"))

detected_formats_v2 <- detected_formats %>%
  bind_rows(reviewed)

# Create date format lookup table
date_format_lookup <- detected_formats_v2 %>%
  dplyr::filter(detected_format %in% c("ymd", "mdy", "dmy")) %>%
  dplyr::select(Raw_Filename, date_format = detected_format)

# Sanity check: any duplicate filenames with conflicting formats?
date_format_lookup %>%
  dplyr::count(Raw_Filename) %>%
  dplyr::filter(n > 1)

# Now join data with detected date formats
tidy_v9b <- tidy_v9a %>%
  dplyr::left_join(date_format_lookup, by = "Raw_Filename") %>%
  dplyr::mutate(date_format = tidyr::replace_na(date_format, "UNKNOWN"))

# Check remaining date formats
tidy_v9b %>%
  dplyr::group_by(date_format) %>%
  dplyr::summarize(files = paste(unique(Raw_Filename), collapse = "; "))

# Let's do the last (hopefully) little bit of date wrangling
tidy_v9c <- tidy_v9b %>%
  # Break apart the date information
  tidyr::separate_wider_delim(cols = date_v4, delim = "/", names = c("date1", "date2", "date3"),
                              too_few = "align_start", too_many = "merge", cols_remove = F) %>%
  # If year is two-digits, fill up to 4
  dplyr::mutate(date3 = dplyr::case_when(
    # Anything after '24 is definitely "19__"
    date_format == "mdy" & nchar(date3) == 2 & as.numeric(date3) >= 24 ~ paste0("19", date3),
    # Less than '24 is likely early 2000s (rather than really early 1900s)
    date_format == "mdy"  & nchar(date3) == 2 & as.numeric(date3) < 24 ~ paste0("20", date3),
    date_format == "dmy" & nchar(date3) == 2 & as.numeric(date3) >= 24 ~ paste0("19", date3),
    # Less than '24 is likely early 2000s (rather than really early 1900s)
    date_format == "dmy"  & nchar(date3) == 2 & as.numeric(date3) < 24 ~ paste0("20", date3),
    T ~ date3)) %>%
  # Do the same fixes for the other date format
  dplyr::mutate(date1 = dplyr::case_when(
    date_format == "ymd" & nchar(date1) == 2 & as.numeric(date1) >= 24 ~ paste0("19", date1),
    date_format == "ymd" & nchar(date1) == 2 & as.numeric(date1) < 24 ~ paste0("20", date1),
    T ~ date1)) %>%
  # Make the three components into numbers
  dplyr::mutate(dplyr::across(.cols = date1:date3,
                              .fns = as.numeric))

# Any non 4-digit years remaining?
tidy_v9c %>%
  dplyr::filter((date_format == "ymd" & nchar(date1) != 4) |
                  (date_format == "mdy" & nchar(date3) != 4)) %>%
  dplyr::glimpse()


# Use formats identified above to determine day, month, and year
tidy_v9d <- tidy_v9c %>%
  dplyr::mutate(
    ## Days
    day = dplyr::case_when(
      date_format == "ymd" ~ date3,
      date_format == "mdy" ~ date2,
      date_format == "dmy" ~ date1,
      T ~ NA),
    ## Months
    month = dplyr::case_when(
      date_format == "ymd" ~ date2,
      date_format == "mdy" ~ date1,
      date_format == "dmy" ~ date2,
      T ~ NA),
    ## Years
    year = dplyr::case_when(
      date_format == "ymd" ~ date1,
      date_format == "mdy" ~ date3,
      date_format == "dmy" ~ date3,
      T ~ NA),
    .after = date_v4)

# How's that look?
dplyr::glimpse(tidy_v9d)

# Assemble back into a real date
tidy_v9e <- tidy_v9d %>%
  dplyr::mutate(date_v5 = paste(day, month, year, sep = "-"),
                .after = date_v4) %>%
  # Make it truly a date
  dplyr::mutate(date_actual = as.Date(x = date_v5, format = "%d-%m-%Y"),
                .after = date_v5)

# Glimpse it
dplyr::glimpse(tidy_v9e)

# Check date range quickly
range(tidy_v9e$date_actual, na.rm = T)

# Date from early 1900s?
filter(tidy_v9e, year < 1930) %>%
  distinct(Raw_Filename)

# Final (actually this time) date wrangling
tidy_v9f <- tidy_v9e %>%
  # Rename final date column
  dplyr::rename(date = date_actual) %>%
  # Drop all intermediary columns
  dplyr::select(-date1, -date2, -date3, -day, -month, -year,
                -dplyr::starts_with("date_"))

# Check structure
dplyr::glimpse(tidy_v9f)


# ---- proposed update: daily aggregation, site scope, checks, and export (commented for review) ----
# # release the wide and intermediate tables before daily aggregation
# rm(list = setdiff(
#   ls(),
#   c("tidy_v9f", "ref_table", "path", "upload_output")
# ))
# gc()
#
# daily_key <- c("LTER", "Stream_Name", "date", "variable", "units")
# stop_if <- function(condition, text) if (isTRUE(condition)) stop(text)
#
# allowed_source_lab_flags <- c(
#   "below reported limit",
#   "above reported limit",
#   "below-limit results excluded from daily mean",
#   "above-limit results excluded from daily mean"
# )
# stop_if(
#   any(
#     !is.na(tidy_v9f$source_lab_flag) &
#       tidy_v9f$source_lab_flag != "" &
#       !tidy_v9f$source_lab_flag %in% allowed_source_lab_flags
#   ),
#   "unsupported source lab flags remain"
# )
#
# # exclude source methods that do not represent dissolved or filtered Si
# canada_unfiltered_si <- tidy_v9f$source_method_flag ==
#   "ECCC SiO2 method 2328, VMV 14109"
# danube_unspecified_si <- tidy_v9f$source_method_flag ==
#   "silica as SiO2, fraction unspecified"
# canada_unfiltered_si[is.na(canada_unfiltered_si)] <- FALSE
# danube_unspecified_si[is.na(danube_unspecified_si)] <- FALSE
# stop_if(
#   !sum(canada_unfiltered_si) %in% c(0L, 135L),
#   "the reviewed Canada unfiltered-silicon count changed"
# )
# stop_if(
#   !sum(danube_unspecified_si) %in% c(0L, 4908L),
#   "the reviewed Danube unspecified-fraction silicon count changed"
# )
# tidy_v9f <- tidy_v9f %>%
#   dplyr::filter(!canada_unfiltered_si, !danube_unspecified_si)
#
# allowed_source_method_flags <- c(
#   "ECCC SiO2 method 2329, VMV 14108",
#   "soluble reactive Si, filtered ICP-OES",
#   "dissolved SiO2, filtered",
#   "dissolved Si, reporting basis under review",
#   "field pH",
#   "laboratory pH fallback",
#   "field conductivity",
#   "laboratory conductivity fallback",
#   "total alkalinity",
#   "bicarbonate alkalinity fallback"
# )
# stop_if(
#   any(
#     !is.na(tidy_v9f$source_method_flag) &
#       tidy_v9f$source_method_flag != "" &
#       !tidy_v9f$source_method_flag %in% allowed_source_method_flags
#   ),
#   "unsupported source method flags remain"
# )
#
# mixed_qualifiers <- tidy_v9f %>%
#   dplyr::filter(!is.na(value), !is.na(value_qualifier)) %>%
#   dplyr::group_by(Dataset, Raw_Filename, LTER, Stream_Name, date, variable, units) %>%
#   dplyr::summarize(n = dplyr::n_distinct(value_qualifier), .groups = "drop") %>%
#   dplyr::filter(n > 1L)
# stop_if(
#   nrow(mixed_qualifiers) > 0,
#   paste(
#     "Master chemistry was not created because one or more source-day",
#     "groups contain both '<' and '>' qualifiers. Review those source",
#     "records; the workflow will not average opposing limits."
#   )
# )
#
# # detected replicates control mixed days; all-qualified days retain their direction
# daily_rows <- tidy_v9f %>%
#   dplyr::filter(!is.na(value)) %>%
#   dplyr::group_by(Dataset, Raw_Filename, LTER, Stream_Name, date, variable, units) %>%
#   dplyr::summarize(
#     detected = sum(is.na(value_qualifier)),
#     below = sum(value_qualifier == "<", na.rm = TRUE),
#     above = sum(value_qualifier == ">", na.rm = TRUE),
#     value = if (detected > 0L) mean(value[is.na(value_qualifier)], na.rm = TRUE) else mean(value, na.rm = TRUE),
#     value_qualifier = if (detected > 0L) NA_character_ else dplyr::first(value_qualifier),
#     source_lab_flag = {
#       source_flags <- unique(source_lab_flag[
#         !is.na(source_lab_flag) & source_lab_flag != ""
#       ])
#       if (length(source_flags) > 1L) {
#         stop("conflicting source lab flags remain within a daily source key")
#       }
#       if (length(source_flags) == 0L) NA_character_ else source_flags
#     },
#     source_method_flag = {
#       method_flags <- unique(source_method_flag[
#         !is.na(source_method_flag) & source_method_flag != ""
#       ])
#       if (length(method_flags) > 1L) {
#         stop("conflicting source method flags remain within a daily source key")
#       }
#       if (length(method_flags) == 0L) NA_character_ else method_flags
#     },
#     .groups = "drop"
#   ) %>%
#   dplyr::mutate(calculated_lab_flag = dplyr::case_when(
#     detected > 0L & below > 0L ~
#       "below-limit results excluded from daily mean",
#     detected > 0L & above > 0L ~
#       "above-limit results excluded from daily mean",
#     detected == 0L & below > 0L ~ "below reported limit",
#     detected == 0L & above > 0L ~ "above reported limit",
#     TRUE ~ NA_character_
#   ))
#
# stop_if(
#   any(
#     !is.na(daily_rows$calculated_lab_flag) &
#       !is.na(daily_rows$source_lab_flag) &
#       daily_rows$calculated_lab_flag != daily_rows$source_lab_flag
#   ),
#   "source and qualifier-derived lab flags conflict"
# )
#
# daily_rows <- daily_rows %>%
#   dplyr::mutate(
#     Lab_Flag = dplyr::coalesce(calculated_lab_flag, source_lab_flag)
#   ) %>%
#   dplyr::select(
#     -detected, -below, -above, -calculated_lab_flag, -source_lab_flag
#   )
#
# # use kj's reviewed combined series wherever that site-variable is present
# dedup_filename <- "Deduplicated_Sites_AllSites_chemistry.csv"
# dedup_rows <- daily_rows %>%
#   dplyr::filter(Raw_Filename == dedup_filename)
# dedup_series <- dedup_rows %>%
#   dplyr::select(LTER, Stream_Name, variable) %>%
#   dplyr::distinct()
#
# tidy_final <- daily_rows %>%
#   dplyr::filter(Raw_Filename != dedup_filename) %>%
#   dplyr::anti_join(
#     dedup_series,
#     by = c("LTER", "Stream_Name", "variable")
#   ) %>%
#   dplyr::bind_rows(dedup_rows) %>%
#   dplyr::distinct()
#
# # exclude extractable silicon before applying the site scope
# # this also protects runs made with an older prepared Canada file
# excluded_total_si <- tidy_final$variable == "DSi" &
#   tidy_final$Raw_Filename == "Canada_WQ_dat.csv"
# stop_if(
#   !sum(excluded_total_si) %in% c(0L, 1549L),
#   "the reviewed Canada extractable-silicon count changed"
# )
# tidy_final <- tidy_final %>%
#   dplyr::filter(!excluded_total_si)
#
# # hubbard brook did not filter samples before june 2013
# hbr_filter_start <- as.Date("2013-06-01")
# unfiltered_hbr_si <- tidy_final$LTER == "HBR" &
#   tidy_final$variable == "DSi" &
#   tidy_final$date < hbr_filter_start
# stop_if(
#   sum(unfiltered_hbr_si) != 3922L,
#   "the reviewed Hubbard Brook unfiltered-silicon count changed"
# )
# tidy_final <- tidy_final %>%
#   dplyr::filter(!unfiltered_hbr_si)
#
# # retain inferred dates while making their origin explicit
# catalina_date_provenance <- tibble::tibble(
#   Dataset = "CZO-Catalina Jemez",
#   Raw_Filename = "CatalinaJemez_chemistry_2009-2019_V2.csv",
#   LTER = "Catalina Jemez",
#   Stream_Name = "MGWEIR",
#   date = as.Date("2018-04-19"),
#   Date_Flag = "inferred",
#   Date_Note = paste0(
#     "Source sample AZ_S1793 had no usable provider date (#VALUE!); ",
#     "2018-04-19 was inferred from paired sample AZ_S1792 and the ",
#     "surrounding weekly sampling sequence."
#   )
# )
#
# cameroon_date_provenance <- tibble::tribble(
#   ~Stream_Name, ~workbook_row,
#   "Mbalmayo", 514L,
#   "Messam", 495L,
#   "Nsimi_outlet", 515L,
#   "Nsimi_spring", 547L,
#   "Olama", 511L,
#   "Pont_So'o", 540L
# ) %>%
#   dplyr::transmute(
#     Dataset = "Cameroon",
#     Raw_Filename = "Chem_Cameroon.csv",
#     LTER = "Cameroon",
#     Stream_Name,
#     date = as.Date("2022-06-16"),
#     Date_Flag = "inferred",
#     Date_Note = paste0(
#       "Provider workbook row ", workbook_row,
#       " reports 2021-06-16; date set to 2022-06-16 because the row ",
#       "falls between 2022-05-31 and 2022-07-06."
#     )
#   )
#
# date_provenance <- dplyr::bind_rows(
#   catalina_date_provenance,
#   cameroon_date_provenance
# )
# tidy_final <- tidy_final %>%
#   dplyr::left_join(
#     date_provenance,
#     by = c("Dataset", "Raw_Filename", "LTER", "Stream_Name", "date")
#   ) %>%
#   dplyr::relocate(Date_Flag, Date_Note, .after = date)
# flagged_date_events <- tidy_final %>%
#   dplyr::filter(!is.na(Date_Flag)) %>%
#   dplyr::distinct(Dataset, Raw_Filename, LTER, Stream_Name, date)
# stop_if(
#   nrow(dplyr::anti_join(
#     date_provenance %>%
#       dplyr::distinct(Dataset, Raw_Filename, LTER, Stream_Name, date),
#     flagged_date_events,
#     by = c("Dataset", "Raw_Filename", "LTER", "Stream_Name", "date")
#   )) > 0L,
#   "a reviewed inferred-date event is missing before site filtering"
# )
#
# # retain dissolved and filtered silicon method details in the final data
# canada_site_silicon <- tidy_final$variable == "DSi" &
#   grepl("^Canada__Canada__", tidy_final$Raw_Filename)
# hbr_filtered_silicon <- tidy_final$LTER == "HBR" &
#   tidy_final$variable == "DSi" &
#   tidy_final$date >= hbr_filter_start
# tidy_final <- tidy_final %>%
#   dplyr::mutate(
#     Method_Flag = dplyr::na_if(source_method_flag, "")
#   )
# tidy_final$Method_Flag[
#   is.na(tidy_final$Method_Flag) & canada_site_silicon
# ] <- "dissolved silicon, filtered"
# tidy_final$Method_Flag[
#   is.na(tidy_final$Method_Flag) & hbr_filtered_silicon
# ] <- "filtered Si, 0.7 micrometer GF/F"
#
# tidy_final <- tidy_final %>%
#   dplyr::mutate(
#     Method_Note = dplyr::case_when(
#       source_method_flag == "ECCC SiO2 method 2329, VMV 14108" ~ paste0(
#         "ECCC VMV 14108/method 2329, colourimetry using heteropoly blue ",
#         "on an autoanalyser. The provider identifies this as reactive ",
#         "dissolved silica from a field-filtered 0.45 micrometer sample. ",
#         "It is reported in mg SiO2/L and converted using SiO2 molecular mass."
#       ),
#       source_method_flag ==
#         "soluble reactive Si, filtered ICP-OES" ~ paste0(
#         "ECCC VMV 109113/method 3628. The provider labels this soluble ",
#         "reactive silica and analyzes a filtered sample by ICP-OES. The ",
#         "workflow treats the reported mg/L as mg SiO2/L based on continuity ",
#         "with the earlier ECCC SiO2 series, then converts it using SiO2 ",
#         "molecular mass."
#       ),
#       source_method_flag == "dissolved SiO2, filtered" ~ paste0(
#         "GEMStat reports this as SiO2-Dis in mg/L and defines it as ",
#         "dissolved silica passing through a 0.45 micrometer filter. The ",
#         "workflow converts it using SiO2 molecular mass."
#       ),
#       source_method_flag ==
#         "dissolved Si, reporting basis under review" ~ paste0(
#         "GEMStat reports this as Si-Dis in mg/L and defines it as dissolved ",
#         "silicon passing through a 0.45 micrometer filter. At seven Belgian ",
#         "stations, this code replaces SiO2-Dis only during 2016-2017, while ",
#         "the reported values remain continuous across the switch. GEMStat ",
#         "does not define the analytical method code. The workflow currently ",
#         "treats the value as mg Si/L and converts it using Si atomic mass, ",
#         "but the reporting basis still needs provider confirmation."
#       ),
#       hbr_filtered_silicon ~ paste0(
#         "Hubbard Brook samples collected from June 1, 2013 onward were ",
#         "vacuum filtered through a 0.7 micrometer GF/F filter. Earlier ",
#         "unfiltered SiO2 samples are excluded from master chemistry."
#       ),
#       source_method_flag == "field pH" ~ paste0(
#         "This value was measured in the field. Field pH takes priority ",
#         "when a laboratory pH result exists on the same site-date."
#       ),
#       source_method_flag == "laboratory pH fallback" ~ paste0(
#         "This laboratory pH value was retained because no field pH ",
#         "measurement was available on the same site-date."
#       ),
#       source_method_flag == "field conductivity" ~ paste0(
#         "This value was measured in the field. Field conductivity takes ",
#         "priority when a laboratory result exists on the same site-date."
#       ),
#       source_method_flag == "laboratory conductivity fallback" ~ paste0(
#         "This laboratory conductivity value was retained because no field ",
#         "measurement was available on the same site-date."
#       ),
#       source_method_flag == "total alkalinity" ~ paste0(
#         "Total alkalinity as CaCO3 takes priority when bicarbonate ",
#         "alkalinity is also available on the same site-date."
#       ),
#       source_method_flag == "bicarbonate alkalinity fallback" ~ paste0(
#         "Bicarbonate alkalinity as CaCO3 was retained because total ",
#         "alkalinity was unavailable on the same site-date."
#       ),
#       canada_site_silicon ~ paste0(
#         "ECCC provider variable SILICON DISSOLVED; VMV 1175/method 10320 ",
#         "or VMV 13812/method 4399."
#       ),
#       TRUE ~ NA_character_
#     )
#   ) %>%
#   dplyr::select(-source_method_flag) %>%
#   dplyr::relocate(Method_Flag, Method_Note, .after = Lab_Flag)
#
# valid_flags <- c(
#   "below reported limit", "above reported limit",
#   "below-limit results excluded from daily mean",
#   "above-limit results excluded from daily mean"
# )
# stop_if(anyNA(tidy_final$date) || anyNA(tidy_final$value) || any(!is.finite(tidy_final$value)),
#         "missing or nonfinite dates or values remain")
# stop_if(any(!is.na(tidy_final$value_qualifier) & !tidy_final$value_qualifier %in% c("<", ">")),
#         "invalid value qualifiers remain")
# stop_if(any(!is.na(tidy_final$Lab_Flag) & !tidy_final$Lab_Flag %in% valid_flags),
#         "invalid lab flags remain")
# stop_if(any(is.na(tidy_final$Date_Flag) != is.na(tidy_final$Date_Note)),
#         "date flags and notes do not match")
# stop_if(any(!is.na(tidy_final$Date_Flag) & tidy_final$Date_Flag != "inferred"),
#         "invalid date flags remain")
# valid_method_flags <- c(
#   "ECCC SiO2 method 2329, VMV 14108",
#   "soluble reactive Si, filtered ICP-OES",
#   "dissolved silicon, filtered",
#   "dissolved SiO2, filtered",
#   "dissolved Si, reporting basis under review",
#   "filtered Si, 0.7 micrometer GF/F",
#   "field pH",
#   "laboratory pH fallback",
#   "field conductivity",
#   "laboratory conductivity fallback",
#   "total alkalinity",
#   "bicarbonate alkalinity fallback"
# )
# stop_if(any(is.na(tidy_final$Method_Flag) != is.na(tidy_final$Method_Note)),
#         "method flags and notes do not match")
# stop_if(any(!is.na(tidy_final$Method_Flag) &
#               !tidy_final$Method_Flag %in% valid_method_flags),
#         "invalid method flags remain")
# silicon_method_flags <- c(
#   "ECCC SiO2 method 2329, VMV 14108",
#   "soluble reactive Si, filtered ICP-OES",
#   "dissolved silicon, filtered",
#   "dissolved SiO2, filtered",
#   "dissolved Si, reporting basis under review",
#   "filtered Si, 0.7 micrometer GF/F"
# )
# pH_method_flags <- c("field pH", "laboratory pH fallback")
# conductivity_method_flags <- c(
#   "field conductivity", "laboratory conductivity fallback"
# )
# alkalinity_method_flags <- c(
#   "total alkalinity", "bicarbonate alkalinity fallback"
# )
# stop_if(any(tidy_final$Method_Flag %in% silicon_method_flags &
#               tidy_final$variable != "DSi", na.rm = TRUE),
#         "a silicon method flag is attached to another variable")
# stop_if(any(tidy_final$Method_Flag %in% pH_method_flags &
#               tidy_final$variable != "pH", na.rm = TRUE),
#         "a pH method flag is attached to another variable")
# stop_if(any(tidy_final$Method_Flag %in% conductivity_method_flags &
#               !tidy_final$variable %in%
#                 c("conductivity", "specific conductivity"),
#             na.rm = TRUE),
#         "a conductivity method flag is attached to another variable")
# stop_if(any(tidy_final$Method_Flag %in% alkalinity_method_flags &
#               tidy_final$variable != "alkalinity", na.rm = TRUE),
#         "an alkalinity method flag is attached to another variable")
# stop_if(any(tidy_final$value_qualifier == "<" & tidy_final$Lab_Flag != "below reported limit", na.rm = TRUE),
#         "less-than qualifiers do not match lab flags")
# stop_if(any(tidy_final$value_qualifier == ">" & tidy_final$Lab_Flag != "above reported limit", na.rm = TRUE),
#         "greater-than qualifiers do not match lab flags")
# stop_if(any(!is.na(tidy_final$value_qualifier) & tidy_final$value <= 0),
#         "nonpositive qualified limits remain")
# stop_if(anyDuplicated(tidy_final) > 0L,
#         "fully duplicated final chemistry rows remain")
# stop_if(any(tidy_final$variable != "pH" & (is.na(tidy_final$units) | trimws(tidy_final$units) == "")),
#         "non-pH rows with blank units remain")
# stop_if(any(tidy_final$variable == "tdp" & tidy_final$units != "uM", na.rm = TRUE),
#         "nonstandard total dissolved phosphorus units remain")
# stop_if(any(tidy_final$variable == "susp partic matter" & tidy_final$units != "mg/L", na.rm = TRUE),
#         "nonstandard suspended particulate matter units remain")
#
# # these 2012 arc samples are classified as lake water in the source supplement
# # www.rsc.org/suppdata/em/c3/c3em00596h/c3em00596h.pdf, table s1
# arctic_lake_events <- tibble::tribble(
#   ~LTER, ~Stream_Name, ~date,
#   "ARC", "I Swamp Outlet", as.Date("2012-08-08"),
#   "ARC", "I1 Outlet", as.Date("2012-08-07"),
#   "ARC", "I2 Outlet", as.Date("2012-08-07"),
#   "ARC", "I3 Outlet", as.Date("2012-08-07"),
#   "ARC", "I4 Outlet", as.Date("2012-08-07"),
#   "ARC", "I5 Outlet", as.Date("2012-08-07"),
#   "ARC", "I6 Headwater Lake Outlet", as.Date("2012-08-07"),
#   "ARC", "I6 Outlet", as.Date("2012-08-08"),
#   "ARC", "I7 Outlet", as.Date("2012-08-08"),
#   "ARC", "I8 Headwater", as.Date("2012-08-07"),
#   "ARC", "I8 Outlet", as.Date("2012-08-08"),
#   "ARC", "Toolik Outlet", as.Date("2012-06-04")
# )
# tidy_final <- dplyr::anti_join(tidy_final, arctic_lake_events, by = c("LTER", "Stream_Name", "date"))
#
# flowing_types <- c("River", "Creek", "Stream", "Brook")
# dsi_sites <- tidy_final %>%
#   dplyr::filter(variable == "DSi") %>%
#   dplyr::distinct(LTER, Stream_Name)
# scope_sites <- ref_table %>%
#   dplyr::filter(
#     Waterbody %in% flowing_types,
#     Include_in_Master_Chem == "Yes"
#   ) %>%
#   dplyr::distinct(LTER, Stream_Name) %>%
#   dplyr::semi_join(dsi_sites, by = c("LTER", "Stream_Name"))
# tidy_final <- dplyr::semi_join(tidy_final, scope_sites, by = c("LTER", "Stream_Name"))
# stop_if(nrow(tidy_final) == 0L, "no chemistry rows remain after site-scope checks")
#
# # carry site-level context into every chemistry row
# site_flag_lookup <- ref_table %>%
#   dplyr::transmute(
#     LTER,
#     Stream_Name,
#     Site_Flag = dplyr::na_if(trimws(as.character(Flag)), "")
#   ) %>%
#   dplyr::distinct()
#
# stop_if(
#   anyDuplicated(site_flag_lookup[c("LTER", "Stream_Name")]) > 0L,
#   "the site reference has duplicate site flags"
# )
# stop_if(
#   nrow(dplyr::anti_join(
#     dplyr::distinct(tidy_final, LTER, Stream_Name),
#     site_flag_lookup,
#     by = c("LTER", "Stream_Name")
#   )) > 0L,
#   "some final chemistry sites are missing from the site reference"
# )
#
# rows_before_site_flags <- nrow(tidy_final)
# daily_keys_before_site_flags <- tidy_final[daily_key]
#
# tidy_final <- tidy_final %>%
#   dplyr::left_join(
#     site_flag_lookup,
#     by = c("LTER", "Stream_Name")
#   ) %>%
#   dplyr::mutate(
#     Sample_Flag = paste0(
#       dplyr::if_else(
#         !is.na(Lab_Flag), paste0("lab: ", Lab_Flag, "; "), ""
#       ),
#       dplyr::if_else(
#         !is.na(Date_Flag), paste0("date: ", Date_Flag, "; "), ""
#       ),
#       dplyr::if_else(
#         !is.na(Method_Flag), paste0("method: ", Method_Flag, "; "), ""
#       )
#     ),
#     Sample_Flag = sub("; $", "", Sample_Flag),
#     Sample_Flag = dplyr::na_if(Sample_Flag, "")
#   ) %>%
#   dplyr::relocate(Site_Flag, Sample_Flag, .after = Stream_Name)
#
# stop_if(
#   nrow(tidy_final) != rows_before_site_flags,
#   "adding site flags changed the number of chemistry rows"
# )
# stop_if(
#   !identical(tidy_final[daily_key], daily_keys_before_site_flags),
#   "adding site flags changed the chemistry row order or daily keys"
# )
#
# has_any_sample_flag <- with(
#   tidy_final,
#   !is.na(Lab_Flag) |
#     !is.na(Date_Flag) |
#     !is.na(Method_Flag)
# )
# stop_if(
#   any(is.na(tidy_final$Sample_Flag) != !has_any_sample_flag),
#   "the sample flag does not match the detailed sample flags"
# )
# stop_if(sum(!is.na(tidy_final$Date_Flag)) != 79L,
#         "reviewed inferred-date row count changed after site filtering")
# stop_if(sum(tidy_final$Method_Flag == "field pH", na.rm = TRUE) != 12667L,
#         "reviewed field pH count changed")
# stop_if(sum(tidy_final$Method_Flag == "laboratory pH fallback", na.rm = TRUE) != 780L,
#         "reviewed laboratory pH fallback count changed")
# stop_if(sum(tidy_final$Method_Flag == "field conductivity", na.rm = TRUE) != 11075L,
#         "reviewed field conductivity count changed")
# stop_if(sum(tidy_final$Method_Flag == "laboratory conductivity fallback", na.rm = TRUE) != 0L,
#         "reviewed laboratory conductivity fallback count changed")
# stop_if(sum(tidy_final$Method_Flag == "total alkalinity", na.rm = TRUE) != 2063L,
#         "reviewed total alkalinity count changed")
# stop_if(sum(tidy_final$Method_Flag == "bicarbonate alkalinity fallback", na.rm = TRUE) != 27L,
#         "reviewed bicarbonate alkalinity fallback count changed")
# stop_if(sum(tidy_final$Method_Flag ==
#               "ECCC SiO2 method 2329, VMV 14108", na.rm = TRUE) != 3802L,
#         "reviewed Canada method 2329 silica count changed")
# stop_if(sum(tidy_final$Method_Flag ==
#               "soluble reactive Si, filtered ICP-OES", na.rm = TRUE) != 4219L,
#         "reviewed Canada soluble-reactive silica count changed")
# stop_if(any(tidy_final$variable == "DSi" &
#               tidy_final$Raw_Filename == "Canada_WQ_dat.csv"),
#         "extractable Canada silicon entered the final master")
# stop_if(sum(tidy_final$Method_Flag ==
#               "dissolved SiO2, filtered", na.rm = TRUE) != 39040L,
#         "reviewed GEMS dissolved SiO2 count changed")
# stop_if(sum(tidy_final$Method_Flag ==
#               "dissolved Si, reporting basis under review", na.rm = TRUE) !=
#           161L,
#         "reviewed GEMS reporting-basis review count changed")
# stop_if(sum(tidy_final$Method_Flag ==
#               "filtered Si, 0.7 micrometer GF/F", na.rm = TRUE) != 464L,
#         "reviewed Hubbard Brook filtered silicon count changed")
# stop_if(any(tidy_final$LTER == "HBR" &
#               tidy_final$variable == "DSi" &
#               tidy_final$date < hbr_filter_start),
#         "unfiltered Hubbard Brook silicon entered the final master")
# stop_if(sum(tidy_final$value_qualifier == "<", na.rm = TRUE) != 47977L,
#         "reviewed less-than qualifier count changed")
# stop_if(sum(tidy_final$value_qualifier == ">", na.rm = TRUE) != 43L,
#         "reviewed greater-than qualifier count changed")
# stop_if(sum(tidy_final$Lab_Flag ==
#               "below-limit results excluded from daily mean", na.rm = TRUE) != 814L,
#         "reviewed mixed daily-limit flag count changed")
# stop_if(nrow(tidy_final) != 4816311L,
#         "reviewed final chemistry row count changed")
# stop_if(dplyr::n_distinct(tidy_final$LTER, tidy_final$Stream_Name) != 1156L,
#         "reviewed final chemistry site count changed")
# stop_if(dplyr::n_distinct(
#   tidy_final$LTER[tidy_final$variable == "DSi"],
#   tidy_final$Stream_Name[tidy_final$variable == "DSi"]
# ) != 1156L, "reviewed final DSi site count changed")
# stop_if(sum(tidy_final$variable == "DSi") != 311341L,
#         "reviewed final silicon row count changed")
# stop_if(nrow(dplyr::anti_join(
#   dplyr::distinct(tidy_final, LTER, Stream_Name),
#   dplyr::distinct(ref_table, LTER, Stream_Name),
#   by = c("LTER", "Stream_Name")
# )) > 0L, "final site names or lter labels do not match the site reference")
#
# date_stamp <- gsub("-", "", Sys.Date())
# chem_output <- file.path(path, "tidy", paste0(date_stamp, "_masterdata_chem.csv"))
# write.csv(tidy_final, chem_output, na = "", row.names = FALSE)
# if (upload_output) {
#   googledrive::drive_upload(
#     media = chem_output,
#     overwrite = TRUE,
#     path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1dTENIB5W2ClgW0z-8NbjqARiaGO2_A7W")
#   )
# }
#
# ---- kj's original: daily aggregation, site scope, checks, and export ----
# expected data present?
main_solutes <- c("Ca","SO4","Na","Cl","NO3","NOx","PO4","SRP","DSi","K")

solute_check <- tidy_v9f %>%
  filter(variable %in% main_solutes) %>%
  group_by(Raw_Filename, variable) %>%
  dplyr::summarise(n=n())

glimpse(solute_check)

## -------------------------------------------- ##
                  # Export ----
## -------------------------------------------- ##

# Create one final tidy object
tidy_final <- tidy_v9f

# Check structure
dplyr::glimpse(tidy_final)

# Grab today's date
date <- gsub(pattern = "-", replacement = "", x = Sys.Date())

# Generate a date-stamped file name for this file
( chem_filename <- paste0(date, "_masterdata_chem.csv") )

# Export locally

(path <- scicomptools::wd_loc(local = FALSE, remote_path = file.path('/', "home", "shares", "lter-si", "WRTDS")))
write.csv(x = tidy_final, file = file.path(path,"tidy", chem_filename), na = '', row.names = F)

# Export to Drive
googledrive::drive_upload(media = file.path(path,"tidy", chem_filename), overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1dTENIB5W2ClgW0z-8NbjqARiaGO2_A7W"))

# End ----



# OLD CODE MAYBE USEFUL ---------------------------------------------------

### SKIP ####
# Look at general date format per LTER
tidy_v9a %>%
  dplyr::group_by(Raw_Filename) %>%
  dplyr::summarize(dates = paste(unique(date_v4), collapse = "; ")) %>%
  tidyr::pivot_wider(names_from = Raw_Filename, values_from = dates) %>%
  dplyr::glimpse()

# Identify format for each file name based on **human eye/judgement**
tidy_v9b <- tidy_v9a %>%
  dplyr::mutate(date_format = dplyr::case_when(
    Raw_Filename == "20221030_masterdata_chem_V2.csv" ~ "ymd",
    # I think this one from PJulian might be obselete
    Raw_Filename == "Australia_MurrayBasin_PJulian_071723.csv" ~ "ymd",
    Raw_Filename == "CAMELS_USGS_N_P.csv" ~ "ymd",
    Raw_Filename == "CAMREX_filled_template.csv" ~ "mdy",
    Raw_Filename == "Canada_WQ_dat.csv" ~ "ymd",
    Raw_Filename == "Chem_Cameroon.csv" ~ "mdy",
    Raw_Filename == "Chem_HYBAM.csv" ~ "mdy",
    Raw_Filename == "ElbeRiverChem.csv" ~ "dmy",
    Raw_Filename == "Krycklan_NP.csv" ~ "mdy",
    Raw_Filename == "MCM_Chem_clean.csv" ~ "mdy",
    Raw_Filename == "MurrayDarlingChem_Merged.csv" ~ "ymd",
    Raw_Filename == "NIVA_Water_chemistry.csv" ~ "mdy",
    Raw_Filename == "NT_NSW_Chem_Cleaned.csv" ~ "dmy",
    Raw_Filename == "NigerRiver.csv" ~ "mdy",
    Raw_Filename == "SiSyn_DataTemplate_Sweden_102423.csv" ~ "mdy",
    Raw_Filename == "UK_Si.csv" ~ "dmy",
    Raw_Filename == "UK_nonSi_solutes.csv" ~ "ymd",
    Raw_Filename == "UMR_si_new_sites.csv" ~ "ymd",
    Raw_Filename == "UMR_si_update_existing_sites.csv" ~ "ymd",
    Raw_Filename == "USGS_NWQA_Chemistry_MissRiverSites.csv" ~ "ymd",
    Raw_Filename == "USGS_geogenic_solutes.csv" ~ "ymd",
    #Raw_Filename == "NEON_Chem.csv" ~"ymd",
    Raw_Filename == "WalkerBranch_Chem.csv" ~"ymd",
    Raw_Filename == "CatalinaJemez_chemistry_2009-2019_V2.csv"~ "mdy",
    Raw_Filename == "Alpine_Clean.csv"~ "ymd",
    Raw_Filename == "Finnish_riverine_data_07032025.csv"~ "mdy",
    Raw_Filename == "NIVA_geogenic_clean.csv" ~ "ymd",
    Raw_Filename == "Cleaned_Seine_Data.csv"~ "mdy",
    Raw_Filename == "Seine_baseGeo_Naides.csv" ~ "ymd",
    Raw_Filename == "Swedish_Geo_Updated_10062025.csv"~ "ymd",
    Raw_Filename == "guadeloupe_GRCBDMDF_chem.csv" ~ "ymd",
    Raw_Filename == "guadeloupe_GRCBDPBD_chem.csv" ~ "ymd",
    Raw_Filename == "guadeloupe_GRCBDQCK_chem.csv" ~ "ymd",
    Raw_Filename == "guadeloupe_GRCCEDIG_chem.csv" ~ "ymd",
    Raw_Filename == "guadeloupe_GRCVHBAR_chem.csv" ~ "ymd",
    Raw_Filename == "guadeloupe_GRCVHSAV_chem.csv" ~ "ymd",
    Raw_Filename == "yzeron_301502401_chem.csv"~ "ymd",
    Raw_Filename == "yzeron_v301502402_chem.csv" ~ "ymd",
    Raw_Filename == "yzeron_v3015810_chem.csv" ~ "ymd",
    Raw_Filename == "Krycklan_Cation_Clean.csv" ~ "mdy",
    Raw_Filename == "GRO_Obidos.csv" ~ "ymd",
    Raw_Filename == "WesternAus_AllSites_Filtered_v2.csv" ~ "ymd",
    Raw_Filename == "EastRiverSFA_chem.csv" ~ "ymd",
    Raw_Filename == "Como_Creek_chem.csv" ~ "ymd",
    Raw_Filename %in% c(
      "Canada_ECCC_AllSites_chemistry.csv",
      "Chile_DGA_AllSites_chemistry.csv",
      "danube_dsi_ready_v1.csv",
      "Deduplicated_Sites_AllSites_chemistry.csv",
      "gems_dsi_ready_v1.csv",
      "Japan_MLIT_AllSites_chemistry.csv",
      "Netherlands_RWS_AllSites_chemistry.csv",
      "Switzerland_CAMELS_CH_AllSites_chemistry.csv",
      "WesternAus_AllSites_Filtered_v2.csv"
    ) ~ "ymd",
    # Raw_Filename == "" ~ "",
    T ~ "UNKNOWN"))

######
