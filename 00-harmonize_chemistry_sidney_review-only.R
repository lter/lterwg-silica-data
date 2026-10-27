# ---- about this comparison ----
# kathi jo's original code and comments are kept below without changes
# sidney's current replacement code is commented beside the matching sections
# each block names the active file it comes from
# preparation and shared functions are included in full rather than repeated at every call
# use the rstudio section menu to move between the replacement blocks

## ---------------------------------------------------------- ##
            # Chemistry - Harmonization & Wrangling
## ---------------------------------------------------------- ##
# Script author(s): Nick J Lyon, Kathi Jo Jankowski, Paul Julian

# PURPOSE:
## Ingest all chemistry files and perform harmonization necessary for creation of 'master' file
## Wrangle the first version of the 'master' file into a tidy, analysis-ready format

## -------------------------------------------- ##
                # Housekeeping ----
## -------------------------------------------- ##

# ---- sidney: run and download the chemistry workflow ----
# the runner downloads the shared files and keys, prepares the inputs, and calls the master script
# it saves the master and metadata csvs and uploads them after the checks pass
# current file: run_full_chemistry_workflow.R
# ---- begin replacement: runner ----
# # ---- full chemistry workflow ----
# # workflow updates: sidney bush
# # source files stay unchanged
#
# # ---- start here in rstudio ----
# # keep this file with the rest of the repository and open it in rstudio
# # click source, choose a save folder, and sign in to google when prompted
# # there are no paths to edit or files to download by hand
# # the script installs missing packages and downloads the shared inputs
# # it checks those inputs before preparing and combining the chemistry
# # each run gets a new dated folder so earlier files are left alone
# # the final csv is saved in chemistry-run/master/tidy inside that folder
# # keep rstudio open and the computer awake until the run finishes
# # if the run fails, the error gives the saved log location
# # the master and its notes file are saved here and uploaded to the shared drive
#
# # ---- download the shared inputs ----
#
# # download source files, supporting data, and live keys from the shared drive
# download_chemistry_inputs <- function(input_root) {
#   if (!requireNamespace("googledrive", quietly = TRUE)) {
#     stop("install the googledrive package before downloading the inputs", call. = FALSE)
#   }
#   googledrive::local_drive_quiet()
#   input_root <- path.expand(input_root)
#   if (file.exists(input_root)) {
#     stop("choose a new input folder so existing downloads are not overwritten", call. = FALSE)
#   }
#   # these shared locations are also linked in the readme
#   folder_id <- "1da8AkSvforPehp-gPJsmA7-pPAJ913mz"
#   support_name <- "Chemistry_workflow_support_20261022.zip"
#   shared <- googledrive::drive_ls(googledrive::as_id(folder_id), n_max = Inf)
#   source_rows <- grepl("[.]csv$", shared$name)
#   support_rows <- shared$name == support_name
#   if (sum(source_rows) != 236L || sum(support_rows) != 1L ||
#       anyDuplicated(tolower(shared$name[source_rows]))) {
#     stop("the shared chemistry file list differs from the reviewed release", call. = FALSE)
#   }
#   source_dir <- file.path(input_root, "Chemistry_Files")
#   key_dir <- file.path(input_root, "keys")
#   evidence_dir <- file.path(input_root, "evidence")
#   for (directory in c(source_dir, key_dir, evidence_dir)) {
#     if (!dir.create(directory, recursive = TRUE)) {
#       stop("the download folder could not be created: ", directory, call. = FALSE)
#     }
#   }
#
#   # the fixed file count describes the reviewed october 2026 input set
#   # download source csvs only, not older sheets or subfolders
#   sources <- shared[source_rows, ]
#   for (row in seq_len(nrow(sources))) {
#     path <- file.path(source_dir, sources$name[row])
#     googledrive::drive_download(sources[row, ], path = path)
#     expected <- sources$drive_resource[[row]]$md5Checksum
#     if (is.null(expected) || !identical(unname(tools::md5sum(path)), expected)) {
#       stop("the downloaded source failed its file check: ", basename(path), call. = FALSE)
#     }
#     if (row %% 25L == 0L || row == nrow(sources)) {
#       message("chemistry files downloaded: ", row, " of ", nrow(sources))
#     }
#   }
#   archive <- tempfile(fileext = ".zip")
#   on.exit(unlink(archive), add = TRUE)
#   support <- shared[support_rows, ]
#   googledrive::drive_download(support, path = archive)
#   expected <- support$drive_resource[[1]]$md5Checksum
#   if (is.null(expected) || !identical(unname(tools::md5sum(archive)), expected)) {
#     stop("the supporting-data download failed its file check", call. = FALSE)
#   }
#   entries <- utils::unzip(archive, list = TRUE)$Name
#   if (any(!grepl("^(providers|evidence)/", entries)) ||
#       any(grepl("(^|[/\\\\])\\.\\.([/\\\\]|$)", entries))) {
#     stop("the supporting archive contains unexpected paths", call. = FALSE)
#   }
#   utils::unzip(archive, exdir = input_root)
#
#   # export the live keys rather than downloading older saved copies
#   keys <- data.frame(
#     id = c("1gKe7zdMtFHeBZJPfzu5l4jjtvP0AFsfrdCreB7OZvkU",
#       "15KDcbCAs4ZzZWoPN2g4MpgOMnYQFaxtAuZnQzwAAONI",
#       "11t9YYTzN_T12VAQhHuY5TpVjGS50ymNmKznJK4rKTIU",
#       "1PshqCYF2hAR9WhpgN65KLBm0qiqcBmihwkGdSesxI3Q"),
#     path = c(file.path(key_dir, c("SiSyn_Data_Key.csv", "SiSyn_Chem_Units_Key.csv",
#       "Site_Reference_Table.csv")),
#       file.path(evidence_dir, "Chemistry_detection_limits_and_methods.xlsx")),
#     type = c("csv", "csv", "csv", "xlsx")
#   )
#   for (row in seq_len(nrow(keys))) {
#     googledrive::drive_download(googledrive::as_id(keys$id[row]),
#       path = keys$path[row], type = keys$type[row])
#   }
#   invisible(list(
#     source_dir = source_dir,
#     provider_dir = file.path(input_root, "providers"),
#     key_dir = key_dir,
#     evidence_dir = evidence_dir,
#     work_dir = file.path(input_root, "chemistry-run")
#   ))
# }
#
# # ---- run the workflow ----
#
# run_chemistry <- function(
#   # downloaded csv files directly inside chemistry_files
#   source_dir,
#   # original source downloads from the shared supporting-data zip
#   provider_dir,
#   # current data key, units key, and site reference exports
#   key_dir,
#   # lab workbook and supporting records from the shared download
#   evidence_dir,
#   # a new output folder, separate from the downloaded inputs
#   work_dir,
#   workflow_dir = getwd(),
#   check_only = FALSE,
#   # the rstudio run uploads the master; background tests leave this off
#   upload = FALSE
# ) {
#   workflow_dir <- normalizePath(workflow_dir, winslash = "/", mustWork = TRUE)
#   if (isTRUE(upload) && !requireNamespace("googledrive", quietly = TRUE)) {
#     stop("the googledrive package is needed to upload the master", call. = FALSE)
#   }
#
#   # keep preparation code with the workflow in github
#   preparation_dir <- file.path(workflow_dir, "chemistry-inputs")
#   if (!dir.exists(preparation_dir)) {
#     stop("the chemistry-inputs folder is missing", call. = FALSE)
#   }
#   helper_file <- file.path(workflow_dir, "chemistry_functions.R")
#   if (!file.exists(helper_file)) {
#     stop("chemistry_functions.R is missing", call. = FALSE)
#   }
#   source(helper_file, local = TRUE)
#
#   source_dir <- normalizePath(source_dir, winslash = "/", mustWork = TRUE)
#   provider_dir <- normalizePath(provider_dir, winslash = "/", mustWork = TRUE)
#   key_dir <- normalizePath(key_dir, winslash = "/", mustWork = TRUE)
#   evidence_dir <- normalizePath(evidence_dir, winslash = "/", mustWork = TRUE)
#   work_parent <- normalizePath(
#     dirname(path.expand(work_dir)),
#     winslash = "/",
#     mustWork = TRUE
#   )
#   work_dir <- file.path(work_parent, basename(path.expand(work_dir)))
#   if (file.exists(work_dir)) {
#     stop("the work folder must not already exist", call. = FALSE)
#   }
#
#   read_only_dirs <- c(source_dir, provider_dir, key_dir, evidence_dir)
#   if (any(vapply(
#     read_only_dirs,
#     function(input_dir) paths_overlap(work_dir, input_dir),
#     logical(1)
#   ))) {
#     stop(
#       "the work folder must be separate from every read-only input folder",
#       call. = FALSE
#     )
#   }
#
#   builder <- file.path(preparation_dir, "prepare_chemistry.R")
#   master_script <- file.path(workflow_dir, "00-harmonize_chemistry_sidney.R")
#   helper_files <- file.path(
#     workflow_dir,
#     c("chemistry_functions.R", "chemistry_rules.R")
#   )
#   method_file <- file.path(
#     evidence_dir,
#     "Chemistry_detection_limits_and_methods.xlsx"
#   )
#   seine_file <- file.path(provider_dir, "france_seine", "seine_silica_provider_20261022.rds")
#   key_files <- file.path(
#     key_dir,
#     c("SiSyn_Data_Key.csv", "SiSyn_Chem_Units_Key.csv", "Site_Reference_Table.csv")
#   )
#   required_files <- c(builder, master_script, helper_files, method_file, seine_file, key_files)
#   if (any(!file.exists(required_files))) {
#     stop(
#       "required workflow files are missing: ",
#       paste(basename(required_files[!file.exists(required_files)]), collapse = ", "),
#       call. = FALSE
#     )
#   }
#
#   top_level_files <- sort(list.files(
#     source_dir,
#     pattern = "[.]csv$",
#     full.names = TRUE
#   ))
#   if (length(top_level_files) != 236L) {
#     stop(
#       "the source folder must contain exactly 236 chemistry csv files",
#       call. = FALSE
#     )
#   }
#   source_files <- top_level_files
#
#   source_names <- basename(source_files)
#   repeated_names <- unique(source_names[
#     duplicated(tolower(source_names)) |
#       duplicated(tolower(source_names), fromLast = TRUE)
#   ])
#   gems_files <- grep("^GEMS__GEMS__.*[.]csv$", source_names, value = TRUE)
#   other_files <- setdiff(source_names, gems_files)
#   if (length(repeated_names) > 0L) {
#     stop(
#       "source chemistry filenames are repeated: ",
#       paste(repeated_names, collapse = ", "),
#       call. = FALSE
#     )
#   }
#   if (
#     length(source_names) != 236L ||
#       length(other_files) != 76L ||
#       length(gems_files) != 160L ||
#       "Canada_ECCC_AllSites_chemistry.csv" %in% source_names
#   ) {
#     stop(
#       paste(
#         "the source folder must provide exactly 76 main files and 160 GEMS",
#         "site files, without the generated combined Canada file"
#       ),
#       call. = FALSE
#     )
#   }
#
#   # check all supporting files and packages before starting preparation
#   check_chemistry_setup(provider_dir, evidence_dir, preparation_dir)
#
#   # check the setup without creating outputs or starting preparation
#   if (isTRUE(check_only)) {
#     return(invisible(list(
#       source_dir = source_dir,
#       provider_dir = provider_dir,
#       key_dir = key_dir,
#       evidence_dir = evidence_dir,
#       work_dir = work_dir,
#       source_files = length(source_files)
#     )))
#   }
#
#   # record every input folder so the complete run can verify it stayed unchanged
#   input_dirs <- c(
#     source = source_dir,
#     provider = provider_dir,
#     keys = key_dir,
#     evidence = evidence_dir
#   )
#   input_states <- record_input_states(input_dirs)
#   verify_inputs <- function() {
#     check_input_files(
#       input_states,
#       input_dirs,
#       context = "the full chemistry workflow"
#     )
#   }
#   on.exit(verify_inputs(), add = TRUE)
#
#   dir.create(work_dir)
#   source_input_dir <- source_dir
#   run_input_dir <- file.path(work_dir, "run-inputs")
#   master_root <- file.path(work_dir, "master")
#   master_key_dir <- file.path(master_root, "keys")
#   dir.create(master_key_dir, recursive = TRUE)
#   dir.create(file.path(master_root, "tidy"))
#
#   # prepare each file before combining the chemistry
#   run_r_script(
#     builder,
#     c(source_input_dir, provider_dir, key_dir, evidence_dir, run_input_dir),
#     settings = c(SISYN_CHEM_LOG_DIR = file.path(work_dir, "logs")),
#     log_prefix = "chemistry-inputs-"
#   )
#
#   # save the keys used for this run beside the master output
#   copied_keys <- file.copy(
#     c(key_files, method_file, seine_file),
#     master_key_dir,
#     overwrite = TRUE,
#     copy.mode = FALSE
#   )
#   if (!all(copied_keys)) {
#     stop("the working copies of the shared keys could not be created", call. = FALSE)
#   }
#
#   # convert units, calculate daily averages, add flags, and save the master
#   run_r_script(
#     master_script,
#     settings = c(
#       SISYN_CHEM_LOG_DIR = file.path(work_dir, "logs"),
#       SISYN_CHEM_INTERNAL_PREPARED_RUN = "true",
#       SISYN_CHEM_LOCAL_ROOT = master_root,
#       SISYN_CHEM_LOCAL_INPUT_DIR = run_input_dir
#     ),
#     log_prefix = "master-chemistry-"
#   )
#
#   verify_inputs()
#   output_files <- list.files(
#     file.path(master_root, "tidy"),
#     pattern = "[.]csv$",
#     full.names = TRUE
#   )
#   if (length(output_files) != 1L) {
#     stop("the workflow did not create exactly one master file", call. = FALSE)
#   }
#   metadata_file <- file.path(
#     master_root, "metadata",
#     sub("[.]csv$", "_metadata.csv", basename(output_files[[1]]))
#   )
#   if (!file.exists(metadata_file)) {
#     stop("the master chemistry metadata file is missing", call. = FALSE)
#   }
#   message("master chemistry written to: ", output_files[[1]])
#   message("chemistry notes written to: ", metadata_file)
#
#   # upload the notes first, then the finished master after all checks pass
#   # keep any existing drive file with the same name rather than replacing it
#   if (isTRUE(upload)) {
#     destination <- googledrive::drive_get(
#       googledrive::as_id("1dTENIB5W2ClgW0z-8NbjqARiaGO2_A7W")
#     )
#     upload_files <- c(metadata_file, output_files[[1]])
#     existing <- googledrive::drive_ls(destination, n_max = Inf)
#     if (any(basename(upload_files) %in% existing$name)) {
#       stop("a master or metadata file with this name is already on drive; local files were kept",
#         call. = FALSE)
#     }
#     for (file in upload_files) {
#       uploaded <- googledrive::drive_upload(media = file, path = destination, overwrite = FALSE)
#       message(basename(file), " uploaded to: https://drive.google.com/file/d/",
#         uploaded$id[[1]], "/view")
#     }
#   }
# }
#
# # ---- rstudio startup ----
#
# # use the open script to locate the workflow, not a computer-specific path
# start_chemistry <- function() {
#   if (!requireNamespace("rstudioapi", quietly = TRUE)) {
#     utils::install.packages("rstudioapi", repos = "https://cloud.r-project.org")
#   }
#   if (!rstudioapi::isAvailable()) {
#     stop("open this script in rstudio and click source", call. = FALSE)
#   }
#   script <- rstudioapi::getSourceEditorContext()$path
#   if (!identical(basename(script), "run_full_chemistry_workflow.R")) {
#     stop("open the full chemistry runner in rstudio before clicking source", call. = FALSE)
#   }
#   workflow_dir <- dirname(normalizePath(script, mustWork = TRUE))
#   parent <- rstudioapi::selectDirectory(caption = "choose where to save chemistry inputs and results")
#   if (is.null(parent) || !nzchar(parent)) return(invisible(NULL))
#
#   packages <- c("googledrive", "librarian", "tidyverse", "readxl", "data.table", "jsonlite")
#   missing <- packages[!vapply(packages, requireNamespace, logical(1), quietly = TRUE)]
#   if (length(missing)) {
#     utils::install.packages(missing, repos = "https://cloud.r-project.org")
#   }
#   input_root <- file.path(parent, paste0("sisyn-chemistry-", format(Sys.time(), "%Y%m%d-%H%M%S")))
#   settings <- download_chemistry_inputs(input_root)
#   settings$workflow_dir <- workflow_dir
#   do.call(run_chemistry, c(settings, list(check_only = TRUE)))
#   do.call(run_chemistry, c(settings, list(upload = TRUE)))
# }
#
# # clicking source in rstudio starts the guided run
# # command-line runs use the five folder paths listed below
# if (interactive()) {
#   start_chemistry()
# } else if (sys.nframe() == 0L) {
#   args <- commandArgs(trailingOnly = TRUE)
#   if (length(args) != 5L) {
#     stop(
#       "supply the source, provider, key, evidence, and new work folders",
#       call. = FALSE
#     )
#   }
#   script_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
#   if (length(script_argument) != 1L) {
#     stop("the workflow folder could not be identified", call. = FALSE)
#   }
#   run_chemistry(
#     source_dir = args[[1]],
#     provider_dir = args[[2]],
#     key_dir = args[[3]],
#     evidence_dir = args[[4]],
#     work_dir = args[[5]],
#     workflow_dir = dirname(normalizePath(sub("^--file=", "", script_argument)))
#   )
# }
# ---- end replacement: runner ----

# ---- sidney: setup and shared keys ----
# the main script reads the prepared files and shared method rules
# current file: 00-harmonize_chemistry_sidney.R
# ---- begin replacement: main-setup ----
# # ---- combine chemistry files ----
# # original script authors: nick j lyon, kathi jo jankowski, and paul julian
# # workflow updates: sidney bush
# # combine chemistry files using the shared variable names and units
# # read the 77 files created by the preparation scripts
# # leave the downloaded chemistry and original source files unchanged
# # open the full chemistry runner in rstudio and click source
# # that runner calls this script after preparing the working inputs
#
# # ---- setup ----
# librarian::shelf(
#   purrr, readxl, stringr, tidyverse,
#   quiet = TRUE
# )
# options(dplyr.summarise.inform = FALSE)
# script_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
# workflow_dir <- if (length(script_argument) == 1L) {
#   dirname(normalizePath(sub("^--file=", "", script_argument)))
# } else {
#   normalizePath(getwd())
# }
# source(file.path(workflow_dir, "chemistry_rules.R"))
# source(file.path(workflow_dir, "chemistry_functions.R"))
# prepared_workflow_run <- read_boolean_setting(
#   "SISYN_CHEM_INTERNAL_PREPARED_RUN",
#   FALSE
# )
# stop_if(
#   !prepared_workflow_run,
#   paste(
#     "run run_full_chemistry_workflow.R so the source-review rules create",
#     "fresh working inputs before chemistry is harmonized"
#   )
# )
# root_dir <- read_required_setting("SISYN_CHEM_LOCAL_ROOT")
# run_input_dir <- Sys.getenv(
#   "SISYN_CHEM_LOCAL_INPUT_DIR",
#   file.path(root_dir, "chem_raw_subset")
# )
#
# # ---- shared inputs ----
# inputs <- load_chemistry_inputs(root_dir, run_input_dir)
# data_key <- inputs$data_key
# units_key <- inputs$units_key
# site_reference <- inputs$site_reference
# method_rules <- inputs$method_rules
# chemistry_files <- inputs$chemistry_files
# source_headers <- inputs$source_headers
# output_dir <- inputs$output_dir
# hbr_rules <- method_rules |>
#   dplyr::filter(
#     rule_lter == "HBR",
#     vapply(expected_variables, function(x) "DSi" %in% x, logical(1)),
#     !is.na(start_date)
#   )
# stop_if(
#   nrow(hbr_rules) != 1L || !is.na(hbr_rules$end_date[[1]]),
#   "the workflow-rules lookup needs one open-ended HBR dissolved-Si rule"
# )
# hbr_documented_start <- hbr_rules$start_date[[1]]
# rm(hbr_rules)
# allowed_source_method_flags <- method_rules$source_method_flag[
#   !is.na(method_rules$source_method_flag)
# ]
# valid_method_flags <- method_rules$Method_Flag
#
# ---- end replacement: main-setup ----

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

# ---- sidney: preparation order and source overlap ----
# preparation runs before the master reads any chemistry
# direct eccc and usgs results take priority over gems for every mapped variable
# gems fills missing dates and keeps reactive-si, estimated-value, and daily-average flags
# current file: chemistry-inputs/prepare_chemistry.R
# ---- begin replacement: preparation-order ----
# # ---- preparation overview ----
# # called automatically by the full chemistry runner
# # the function below runs preparation in the reviewed source order
# # amazon, source selection, and file assembly are defined later in this file
# # canada, gems, danube, qualifiers, and other source rules stay in their named files
#
# # ---- shared functions ----
#
# script_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
# if (length(script_argument) != 1L) {
#   stop("the script folder could not be identified")
# }
# preparation_dir <- dirname(normalizePath(sub("^--file=", "", script_argument)))
# source(file.path(dirname(preparation_dir), "chemistry_functions.R"))
# source(file.path(dirname(preparation_dir), "chemistry_rules.R"))
#
# # ---- preparation order ----
#
# prepare_chemistry <- function(args) {
#   if (length(args) != 5L) {
#     stop(
#       paste(
#         "supply the read-only source snapshot, provider-download folder,",
#         "key folder, evidence folder, and a new output folder"
#       )
#     )
#   }
#
#   raw_dir <- normalizePath(args[[1]], winslash = "/", mustWork = TRUE)
#   provider_dir <- normalizePath(args[[2]], winslash = "/", mustWork = TRUE)
#   key_dir <- normalizePath(args[[3]], winslash = "/", mustWork = TRUE)
#   evidence_dir <- normalizePath(args[[4]], winslash = "/", mustWork = TRUE)
#
#   output_parent <- normalizePath(
#     dirname(path.expand(args[[5]])),
#     winslash = "/",
#     mustWork = TRUE
#   )
#   output_dir <- file.path(output_parent, basename(path.expand(args[[5]])))
#
#   if (file.exists(output_dir)) {
#     stop("the output folder must not already exist")
#   }
#
#   runner_argument <- grep("^--file=", commandArgs(), value = TRUE)
#   if (length(runner_argument) != 1L) {
#     stop("the script folder could not be identified")
#   }
#   script_dir <- dirname(normalizePath(
#     sub("^--file=", "", runner_argument),
#     winslash = "/",
#     mustWork = TRUE
#   ))
#   input_dirs <- c(raw_dir, provider_dir, key_dir, evidence_dir, script_dir)
#   if (any(vapply(
#     input_dirs,
#     function(input_dir) paths_overlap(output_dir, input_dir),
#     logical(1)
#   ))) {
#     stop("the output folder must not overlap an input folder")
#   }
#
#   # ---- required files ----
#
#   provider_script <- file.path(script_dir, "prepare_eccc_gems_danube.R")
#   qualifier_script <- file.path(script_dir, "prepare_qualifier_inputs.R")
#   source_script <- file.path(script_dir, "prepare_other_sources.R")
#   legacy_script <- file.path(script_dir, "prepare_usgs_legacy.R")
#   data_key <- file.path(key_dir, "SiSyn_Data_Key.csv")
#   units_key <- file.path(key_dir, "SiSyn_Chem_Units_Key.csv")
#   site_key <- file.path(key_dir, "Site_Reference_Table.csv")
#
#   clinton_file <- file.path(
#     evidence_dir,
#     "usgs_05420500_tn_tds_20261009.csv"
#   )
#   exclusion_file <- file.path(
#     evidence_dir,
#     "usgs_tds_as_tn_exclusions.rds"
#   )
#   carey_file <- file.path(evidence_dir, "SiSyn_DataTemplate_Carey.xlsx")
#   japan_provider_file <- file.path(
#     evidence_dir,
#     "yanase_chemistry_tables.json"
#   )
#   netherlands_provider_file <- file.path(
#     evidence_dir,
#     "netherlands_rws_target_chemistry.rds"
#   )
#   nt_nsw_provider_file <- file.path(
#     provider_dir,
#     "australia_mdba",
#     "MDBA_RMWQMP_physico-chemical-data_20260914.csv"
#   )
#
#   required_files <- c(
#     provider_script,
#     qualifier_script,
#     source_script,
#     legacy_script,
#     data_key,
#     units_key,
#     site_key,
#     clinton_file,
#     exclusion_file,
#     carey_file,
#     japan_provider_file,
#     netherlands_provider_file,
#     nt_nsw_provider_file
#   )
#
#   check_required_files(required_files, "required files")
#
#   provider_paths <- c(
#     file.path(provider_dir, "canada_eccc"),
#     file.path(provider_dir, "gems_gemstat"),
#     file.path(provider_dir, "danube_icpdr"),
#     file.path(provider_dir, "amazon_hybam"),
#     file.path(provider_dir, "cameroon_mtropics"),
#     file.path(provider_dir, "australia_mdba"),
#     file.path(provider_dir, "usa_usgs_samples", "responses")
#   )
#
#   if (any(!dir.exists(provider_paths))) {
#     stop(
#       "provider folders are missing: ",
#       paste(basename(provider_paths[!dir.exists(provider_paths)]), collapse = ", ")
#     )
#   }
#
#   cameroon_zip <- file.path(
#     provider_dir,
#     "cameroon_mtropics",
#     "Cameroon_MTROPICS_HYDROCHEMICAL_DATA_NYONG_20260915.zip"
#   )
#
#   if (!file.exists(cameroon_zip)) {
#     stop("the reviewed cameroon provider file is missing")
#   }
#
#   # ---- input folder checks ----
#
#   read_only_dirs <- c(
#     source = raw_dir,
#     provider = provider_dir,
#     keys = key_dir,
#     evidence = evidence_dir
#   )
#   input_states <- record_input_states(read_only_dirs)
#   verify_inputs <- function() {
#     check_input_files(
#       input_states,
#       read_only_dirs,
#       context = "chemistry preparation"
#     )
#   }
#   on.exit(verify_inputs(), add = TRUE)
#
#   # ---- temporary preparation folders ----
#
#   work_dir <- tempfile(pattern = "chemistry-input-preparation-")
#   dir.create(work_dir)
#   on.exit(unlink(work_dir, recursive = TRUE, force = TRUE), add = TRUE)
#
#   provider_output <- file.path(work_dir, "provider")
#   amazon_output <- file.path(work_dir, "amazon")
#   selection_output <- file.path(work_dir, "selected-sources")
#   qualifier_output <- file.path(work_dir, "qualifiers")
#   source_output <- file.path(work_dir, "source")
#
#   # ---- prepare inputs ----
#
#   # recover original source details for canada, gems, and danube
#   run_r_script(
#     provider_script,
#     c(raw_dir, provider_dir, data_key, provider_output),
#     log_prefix = "provider-inputs-"
#   )
#
#   # combine the amazon sources without losing their separate date ranges
#   prepare_amazon(
#     source_dir = raw_dir,
#     provider_dir = file.path(provider_dir, "amazon_hybam"),
#     output_dir = amazon_output
#   )
#
#   # remove confirmed overlap and tds-as-tn records from the prepared inputs
#   select_source_files(
#     input_dir = raw_dir,
#     output_dir = selection_output,
#     usgs_exclusion_file = exclusion_file
#   )
#
#   # restore reported qualifiers and documented numeric detection limits
#   run_r_script(
#     qualifier_script,
#     settings = c(
#       SISYN_CHEM_INPUT_DIR = raw_dir,
#       SISYN_CHEM_SELECTION_INPUT_DIR = selection_output,
#       SISYN_CHEM_QUALIFIER_OUTPUT_DIR = qualifier_output,
#       SISYN_CHEM_KEY_DIR = key_dir
#     ),
#     log_prefix = "qualifier-inputs-"
#   )
#
#   # apply the remaining unit, date, name, and method choices for each source
#   run_r_script(
#     source_script,
#     settings = c(
#       SISYN_CHEM_INPUT_DIR = raw_dir,
#       SISYN_CHEM_SELECTION_INPUT_DIR = selection_output,
#       SISYN_CHEM_DIRECT_OUTPUT_DIR = source_output,
#       SISYN_CHEM_QUALIFIER_OUTPUT_DIR = qualifier_output,
#       SISYN_CHEM_KEY_DIR = key_dir,
#       SISYN_CHEM_CANADA_PROVIDER_DIR = file.path(provider_dir, "canada_eccc"),
#       SISYN_CAMEROON_PROVIDER_ZIP = cameroon_zip,
#       SISYN_CHEM_USGS_PROVIDER_DIR = file.path(
#         provider_dir,
#         "usa_usgs_samples",
#         "responses"
#       ),
#       SISYN_CHEM_USGS_CLINTON_FILE = clinton_file,
#       SISYN_CHEM_USGS_EXCLUSION_FILE = exclusion_file,
#       SISYN_CHEM_NT_NSW_PROVIDER_FILE = nt_nsw_provider_file,
#       SISYN_CHEM_JAPAN_PROVIDER_FILE = japan_provider_file,
#       SISYN_CHEM_NETHERLANDS_PROVIDER_FILE = netherlands_provider_file,
#       SISYN_CHEM_REVIEWED_SUSQUEHANNA_FILE = file.path(
#         raw_dir,
#         "USGS__USGS__SUSQUEHANNA_RIVER__4db2531edf53.csv"
#       ),
#       SISYN_CHEM_CAREY_FILE = carey_file
#     ),
#     log_prefix = "source-inputs-"
#   )
#
#   # collect the prepared versions into the 77 inputs used by the master script
#   assemble_inputs(
#     raw_dir = raw_dir,
#     selection_dir = selection_output,
#     qualifier_dir = qualifier_output,
#     direct_dir = source_output,
#     provider_dir = file.path(provider_output, "chem_raw_subset"),
#     amazon_dir = amazon_output,
#     key_file = data_key,
#     output_dir = output_dir
#   )
#
#   # use gems only where eccc or usgs lacks that variable on that date
#   additions <- prepare_gems_chemistry_gaps(output_dir, provider_dir, key_dir)
#   gems_path <- file.path(output_dir, "GEMS_AllSites_chemistry.csv")
#   gems <- read.csv(gems_path, colClasses = "character", check.names = FALSE)
#   if (any(gems$LTER %in% c("Canada", "USGS"))) {
#     stop("GEMS chemistry gap filling has already been applied")
#   }
#   data.table::fwrite(dplyr::bind_rows(gems, additions), gems_path, na = "")
#
#   # ---- final checks ----
#
#   data_key_table <- read.csv(
#     data_key,
#     check.names = FALSE,
#     stringsAsFactors = FALSE
#   )
#   chemistry_rows <- !is.na(data_key_table$Data_type) &
#     data_key_table$Data_type == "chemistry" &
#     !is.na(data_key_table$Raw_Filename) &
#     data_key_table$Raw_Filename != ""
#   expected_files <- sort(unique(data_key_table$Raw_Filename[chemistry_rows]))
#
#   actual_files <- sort(list.files(output_dir, pattern = "[.]csv$"))
#   if (!identical(actual_files, expected_files)) {
#     stop("the prepared chemistry inputs do not match the data key")
#   }
#
#   verify_inputs()
# }
#
# # ---- amazon sources ----
#
# prepare_amazon <- function(source_dir, provider_dir, output_dir) {
#   source_dir <- normalizePath(source_dir, winslash = "/", mustWork = TRUE)
#   provider_dir <- normalizePath(provider_dir, winslash = "/", mustWork = TRUE)
#   output_parent <- normalizePath(dirname(path.expand(output_dir)), winslash = "/", mustWork = TRUE)
#   output_dir <- file.path(output_parent, basename(path.expand(output_dir)))
#
#   check_work_folder(output_dir, c(source_dir, provider_dir), "amazon preparation")
#
#   camrex_path <- file.path(source_dir, "CAMREX_filled_template.csv")
#   hybam_path <- file.path(source_dir, "Chem_HYBAM.csv")
#   provider_path <- file.path(provider_dir, "Manacapuru.csv")
#   required_files <- c(camrex_path, hybam_path, provider_path)
#   check_required_files(required_files, "required manacapuru files")
#
#   source_state <- data.frame(
#     path = required_files,
#     size = unname(file.info(required_files)$size),
#     md5 = unname(tools::md5sum(required_files)),
#     stringsAsFactors = FALSE
#   )
#   check_sources <- function() {
#     current <- data.frame(
#       path = required_files,
#       size = unname(file.info(required_files)$size),
#       md5 = unname(tools::md5sum(required_files)),
#       stringsAsFactors = FALSE
#     )
#     if (!identical(source_state, current)) {
#       stop("a manacapuru source file changed during preparation")
#     }
#   }
#   on.exit(check_sources(), add = TRUE)
#
#   # ---- amazon helpers ----
#
#   parse_mdy_year <- function(value) {
#     parts <- strsplit(trimws(value), "/", fixed = TRUE)
#     valid <- lengths(parts) == 3L
#     if (!all(valid)) {
#       stop("a HYBAM date does not have month/day/year format")
#     }
#     month <- integer(length(parts))
#     day <- integer(length(parts))
#     year_text <- character(length(parts))
#     for (row in seq_along(parts)) {
#       month[row] <- as.integer(parts[[row]][1L])
#       day[row] <- as.integer(parts[[row]][2L])
#       year_text[row] <- parts[[row]][3L]
#     }
#     year <- as.integer(year_text)
#     short_year <- nchar(year_text) == 2L
#     # two-digit years use the same 1950 cutoff as the reviewed hybam dates
#     year[short_year & year >= 50L] <- year[short_year & year >= 50L] + 1900L
#     year[short_year & year < 50L] <- year[short_year & year < 50L] + 2000L
#     date <- as.Date(sprintf("%04d-%02d-%02d", year, month, day))
#     if (anyNA(date)) {
#       stop("a HYBAM date could not be parsed")
#     }
#     date
#   }
#
#   apply_conversion <- function(data, rules) {
#     rule_row <- match(data$source_variable, rules$source_variable)
#     if (anyNA(rule_row)) {
#       stop("a manacapuru variable lacks a conversion rule")
#     }
#     value <- suppressWarnings(as.numeric(data$source_value))
#     if (anyNA(value) || any(!is.finite(value))) {
#       stop("a manacapuru chemistry value is not numeric")
#     }
#     data.frame(
#       LTER = "HYBAM",
#       Stream_Name = "Manacapuru",
#       date = as.character(data$date),
#       solute = rules$solute[rule_row],
#       solute_units = rules$solute_units[rule_row],
#       amount = format_amount(
#         value * rules$factor[rule_row],
#         data$qualifier
#       ),
#       stringsAsFactors = FALSE
#     )
#   }
#
#   # molecular and atomic masses match the chemistry units workflow
#   C_mw <- 12.011
#   Ca_mw <- 40.078
#   Cl_mw <- 35.453
#   F_mw <- 18.998403163
#   Fe_mw <- 55.845
#   H_mw <- 1.00784
#   K_mw <- 39.0983
#   Mg_mw <- 24.305
#   Mn_mw <- 54.938044
#   N_mw <- 14.0067
#   Na_mw <- 22.989769
#   O_mw <- 15.999
#   S_mw <- 32.065
#   Si_mw <- 28.0855
#   Sr_mw <- 87.62
#   Zn_mw <- 65.38
#   Al_mw <- 26.9815385
#   HCO3_mw <- H_mw + C_mw + 3 * O_mw
#   NO3_mw <- N_mw + 3 * O_mw
#   SO4_mw <- S_mw + 4 * O_mw
#
#   # ---- current source data ----
#
#   provider <- data.table::fread(
#     provider_path,
#     skip = 12L,
#     sep = "\t",
#     check.names = FALSE,
#     colClasses = "character",
#     na.strings = "",
#     showProgress = FALSE
#   )
#   names(provider) <- trimws(names(provider))
#
#   provider_columns <- c(
#     Date = 1L,
#     F = 3L,
#     Zn = 8L,
#     SO4 = 10L,
#     temp = 11L,
#     Al = 14L,
#     DOC = 20L,
#     Fe = 22L,
#     K = 24L,
#     pH = 25L,
#     Na = 33L,
#     Mn = 36L,
#     DSi = 38L,
#     HCO3 = 39L,
#     Cl = 40L,
#     NO3 = 43L,
#     Sr = 45L,
#     Ca = 47L,
#     Mg = 55L,
#     conductivity = 56L
#   )
#   expected_provider_headers <- c(
#     "Date",
#     "Mass concentration (F^{-}) (mg/L)",
#     "Mass concentration (Zn) (µg/L)",
#     "Mass concentration (SO_{4}^{2-}) (mg/L)",
#     "Temperature (Temperature) (°C)",
#     "Mass concentration (Al) (µg/L)",
#     "Mass concentration (COD) (mg/L)",
#     "Mass concentration (Fe) (µg/L)",
#     "Mass concentration (K) (mg/L)",
#     "Chemistry property (pH)",
#     "Mass concentration (Na) (mg/L)",
#     "Mass concentration (Mn) (µg/L)",
#     "Mass concentration (Si) (mg/L)",
#     "Mass concentration (HCO_{3}^{-}) (mg/L)",
#     "Mass concentration (Cl^{-}) (mg/L)",
#     "Mass concentration (NO_{3}^{-}) (mg/L)",
#     "Mass concentration (Sr) (µg/L)",
#     "Mass concentration (Ca) (mg/L)",
#     "Mass concentration (Mg) (mg/L)",
#     "Electronics and electromagnetism property (Conductivity) (µs/cm)"
#   )
#   if (
#     ncol(provider) != 60L ||
#       !identical(names(provider)[provider_columns], expected_provider_headers)
#   ) {
#     stop("the reviewed manacapuru provider columns changed")
#   }
#
#   provider_values <- provider[, provider_columns[-1L], with = FALSE]
#   provider_long <- data.frame(
#     date = rep(provider[[provider_columns[["Date"]]]], times = ncol(provider_values)),
#     source_variable = rep(names(provider_columns)[-1L], each = nrow(provider)),
#     source_value = unlist(provider_values, use.names = FALSE),
#     qualifier = NA_character_,
#     stringsAsFactors = FALSE
#   )
#   provider_long <- provider_long[!is_blank(provider_long$source_value), , drop = FALSE]
#   provider_long$date <- as.Date(provider_long$date, format = "%Y/%m/%d")
#   if (anyNA(provider_long$date)) {
#     stop("a provider value has an invalid date")
#   }
#   assert_count(nrow(provider_long), 3794L, "mapped provider rows")
#
#   provider_rules <- data.frame(
#     source_variable = names(provider_columns)[-1L],
#     solute = c(
#       "F", "Zn", "SO4", "temp", "Al", "dissolved org C", "Fe", "K",
#       "pH", "Na", "Mn", "DSi", "HCO3", "Cl", "NO3", "Sr", "Ca",
#       "Mg", "conductivity"
#     ),
#     solute_units = c(
#       "uM", "uM", "uM", "C", "uM", "uM", "uM", "uM", NA_character_,
#       "uM", "uM", "uM", "uM", "uM", "uM", "uM", "uM", "uM", "uS/cm"
#     ),
#     factor = c(
#       1000 / F_mw,
#       1 / Zn_mw,
#       1000 / SO4_mw,
#       1,
#       1 / Al_mw,
#       1000 / C_mw,
#       1 / Fe_mw,
#       1000 / K_mw,
#       1,
#       1000 / Na_mw,
#       1 / Mn_mw,
#       1000 / Si_mw,
#       1000 / HCO3_mw,
#       1000 / Cl_mw,
#       1000 / NO3_mw,
#       1 / Sr_mw,
#       1000 / Ca_mw,
#       1000 / Mg_mw,
#       1
#     ),
#     stringsAsFactors = FALSE
#   )
#   provider_prepared <- apply_conversion(provider_long, provider_rules)
#   provider_key <- provider_prepared[c("date", "solute", "solute_units")]
#   if (anyDuplicated(provider_key) > 0L) {
#     stop("the provider contains repeated manacapuru date-variable keys")
#   }
#   if (!identical(
#     range(provider_prepared$date),
#     c("2003-04-08", "2025-02-21")
#   )) {
#     stop("the provider manacapuru date range changed")
#   }
#
#   # ---- earlier hybam data ----
#
#   hybam <- read.csv(
#     hybam_path,
#     check.names = FALSE,
#     stringsAsFactors = FALSE,
#     na.strings = "",
#     colClasses = "character",
#     comment.char = ""
#   )
#   assert_count(nrow(hybam), 117075L, "full HYBAM source rows")
#
#   legacy_rules <- data.frame(
#     source_variable = c(
#       "Al", "Ca", "Cl", "DOC", "Si", "F", "Fe", "HCO3", "K", "Mg",
#       "Mn", "Na", "NO3", "SO4", "Sr", "Zn"
#     ),
#     solute = c(
#       "Al", "Ca", "Cl", "dissolved org C", "DSi", "F", "Fe", "HCO3",
#       "K", "Mg", "Mn", "Na", "NO3", "SO4", "Sr", "Zn"
#     ),
#     solute_units = rep("uM", 16L),
#     factor = c(
#       1000 / Al_mw,
#       1000 / Ca_mw,
#       1000 / Cl_mw,
#       1000 / C_mw,
#       1000 / Si_mw,
#       1000 / F_mw,
#       1000 / Fe_mw,
#       1000 / HCO3_mw,
#       1000 / K_mw,
#       1000 / Mg_mw,
#       1 / Mn_mw,
#       1000 / Na_mw,
#       1000 / NO3_mw,
#       1000 / SO4_mw,
#       1000 / Sr_mw,
#       1 / Zn_mw
#     ),
#     stringsAsFactors = FALSE
#   )
#
#   legacy <- hybam[
#     hybam$nom == "Manacapuru" &
#       hybam$solute %in% legacy_rules$source_variable,
#     ,
#     drop = FALSE
#   ]
#   assert_count(nrow(legacy), 2631L, "mapped earlier HYBAM rows")
#   legacy$date <- parse_mdy_year(legacy$date)
#
#   blank_value <- is_blank(legacy$valeur)
#   source_code <- trimws(legacy$code)
#   numeric_limit <- grepl(
#     "^<\\s*(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)$",
#     source_code
#   )
#   special_limit <- blank_value & source_code == "<50ppb" & legacy$solute == "NO3"
#   if (any(blank_value & !(numeric_limit | special_limit))) {
#     stop("an earlier HYBAM limit lacks a numeric threshold")
#   }
#   assert_count(sum(blank_value), 34L, "earlier HYBAM limit rows")
#
#   legacy_value <- legacy$valeur
#   legacy_value[blank_value & numeric_limit] <- sub("^<\\s*", "", source_code[
#     blank_value & numeric_limit
#   ])
#   legacy_value[special_limit] <- "0.05"
#   legacy_long <- data.frame(
#     date = legacy$date,
#     source_variable = legacy$solute,
#     source_value = legacy_value,
#     qualifier = ifelse(blank_value, "<", NA_character_),
#     stringsAsFactors = FALSE
#   )
#   legacy_prepared <- apply_conversion(legacy_long, legacy_rules)
#   legacy_key <- legacy_prepared[c("date", "solute", "solute_units")]
#   if (anyDuplicated(legacy_key) > 0L) {
#     stop("the earlier HYBAM data contain repeated manacapuru keys")
#   }
#
#   shared_key <- merge(
#     transform(legacy_key, legacy_row = seq_len(nrow(legacy_key))),
#     transform(provider_key, provider_row = seq_len(nrow(provider_key))),
#     by = c("date", "solute", "solute_units"),
#     sort = FALSE
#   )
#   assert_count(nrow(shared_key), 2508L, "provider and earlier HYBAM overlap")
#   assert_count(
#     sum(grepl("^<", legacy_prepared$amount[shared_key$legacy_row])),
#     1L,
#     "limited earlier HYBAM overlap"
#   )
#
#   legacy_only <- legacy_prepared[-shared_key$legacy_row, , drop = FALSE]
#   assert_count(nrow(legacy_only), 123L, "earlier-only HYBAM rows")
#   assert_count(sum(grepl("^<", legacy_only$amount)), 33L, "earlier-only HYBAM limits")
#   hybam_combined <- rbind(legacy_only, provider_prepared)
#   assert_count(nrow(hybam_combined), 3917L, "provider-precedence HYBAM rows")
#
#   # ---- earlier camrex data ----
#
#   camrex <- read.csv(
#     camrex_path,
#     check.names = FALSE,
#     stringsAsFactors = FALSE,
#     na.strings = "",
#     colClasses = "character",
#     comment.char = ""
#   )
#   assert_count(nrow(camrex), 215L, "full CAMREX source rows")
#   camrex <- camrex[camrex$Site.Stream.Name == "Manacapuru", , drop = FALSE]
#   assert_count(nrow(camrex), 12L, "CAMREX manacapuru events")
#
#   camrex_date <- as.Date(camrex$Sampling.Date, format = "%m/%d/%Y") - 1L
#   if (
#     anyNA(camrex_date) ||
#       !identical(range(camrex_date), as.Date(c("1982-05-12", "1991-08-30")))
#   ) {
#     stop("the corrected CAMREX manacapuru dates changed")
#   }
#
#   camrex_rules <- data.frame(
#     source_variable = c(
#       "DSi", "TN", "NOx", "TP", "PO4", "DOC", "TOC", "alkalinity",
#       "pH", "Na", "K", "Ca", "Mg", "Cl"
#     ),
#     solute = c(
#       "DSi", "TN", "NOx", "TP", "SRP", "dissolved org C", "tot org C",
#       "alkalinity", "pH", "Na", "K", "Ca", "Mg", "Cl"
#     ),
#     solute_units = c(
#       rep("uM", 8L), NA_character_, rep("uM", 5L)
#     ),
#     factor = c(
#       rep(1, 5L), 1000 / C_mw, 1000 / C_mw, rep(1, 7L)
#     ),
#     stringsAsFactors = FALSE
#   )
#
#   camrex_values <- camrex[camrex_rules$source_variable]
#   camrex_long <- data.frame(
#     date = rep(camrex_date, times = ncol(camrex_values)),
#     source_variable = rep(names(camrex_values), each = nrow(camrex)),
#     source_value = unlist(camrex_values, use.names = FALSE),
#     qualifier = NA_character_,
#     stringsAsFactors = FALSE
#   )
#   camrex_long <- camrex_long[!is_blank(camrex_long$source_value), , drop = FALSE]
#   assert_count(nrow(camrex_long), 134L, "mapped CAMREX manacapuru values")
#   camrex_prepared <- apply_conversion(camrex_long, camrex_rules)
#
#   # ---- combined working input ----
#
#   combined <- rbind(camrex_prepared, hybam_combined)
#   combined <- combined[
#     order(combined$date, combined$solute, combined$solute_units),
#     ,
#     drop = FALSE
#   ]
#   rownames(combined) <- NULL
#   assert_count(nrow(combined), 4051L, "prepared manacapuru rows")
#   assert_count(sum(grepl("^<", combined$amount)), 33L, "prepared manacapuru limits")
#   assert_count(sum(combined$solute == "DSi"), 246L, "prepared manacapuru Si rows")
#   if (anyDuplicated(combined[c("date", "solute", "solute_units")]) > 0L) {
#     stop("the prepared manacapuru input contains repeated daily keys")
#   }
#
#   dir.create(output_dir, recursive = TRUE)
#   output_path <- file.path(output_dir, "Manacapuru_chemistry_combined.csv")
#   write.csv(combined, output_path, row.names = FALSE, na = "")
#
#   written <- read.csv(
#     output_path,
#     check.names = FALSE,
#     stringsAsFactors = FALSE,
#     na.strings = "",
#     colClasses = "character"
#   )
#   if (!identical(combined, written)) {
#     stop("the prepared manacapuru values changed when read again")
#   }
#
#   check_sources()
# }
#
# # ---- source overlap and usgs identity checks ----
#
# select_source_files <- function(input_dir, output_dir, usgs_exclusion_file) {
#   source_files <- c(
#     "20221030_masterdata_chem_V2.csv",
#     "CAMREX_filled_template.csv",
#     "Chem_HYBAM.csv"
#   )
#
#   required_files <- c(file.path(input_dir, source_files), usgs_exclusion_file)
#   check_required_files(required_files, "required selection files")
#   check_work_folder(output_dir, input_dir, "source selection")
#   dir.create(output_dir, recursive = TRUE)
#
#   # ---- source selection helpers ----
#
#   read_source <- function(filename, directory = input_dir) {
#     read_source_csv(filename, directory)
#   }
#
#   id_signature <- function(data) {
#     if (anyNA(data[[1L]]) || anyDuplicated(data[[1L]]) > 0L) {
#       stop("the source row ids are missing or repeated")
#     }
#
#     signature_file <- tempfile()
#     on.exit(unlink(signature_file), add = TRUE)
#     writeLines(data[[1L]], signature_file, useBytes = TRUE)
#     unname(tools::md5sum(signature_file))
#   }
#
#   # this file check confirms that the same source rows stay in the same order
#   write_selected <- function(filename, data, expected_rows, expected_signature) {
#     assert_count(nrow(data), expected_rows, paste0(filename, " selected rows"))
#     observed_signature <- id_signature(data)
#     if (!identical(observed_signature, expected_signature)) {
#       stop(filename, " selected source rows changed")
#     }
#
#     write_prepared_csv(filename, data, output_dir)
#   }
#
#   select_site_rows <- function(filename, site_column, site_counts,
#                                full_rows, selected_rows, selected_signature) {
#     before <- read_source(filename)
#     assert_count(nrow(before), full_rows, paste0(filename, " full source rows"))
#
#     # remove only the named overlaps from this source file
#     remove_rows <- before[[site_column]] %in% names(site_counts)
#     observed_counts <- table(factor(
#       before[[site_column]][remove_rows],
#       levels = names(site_counts)
#     ))
#     if (!identical(as.integer(observed_counts), unname(site_counts))) {
#       stop(filename, " selected site counts changed")
#     }
#
#     after <- before[!remove_rows, , drop = FALSE]
#     rownames(after) <- NULL
#     write_selected(filename, after, selected_rows, selected_signature)
#   }
#
#   # ---- amazon source selections ----
#
#   # rebuild manacapuru from the full original downloads
#   # obidos is supplied by its reviewed combined file
#   select_site_rows(
#     filename = "CAMREX_filled_template.csv",
#     site_column = "Site.Stream.Name",
#     site_counts = c(Manacapuru = 12L, Obidos = 12L),
#     full_rows = 215L,
#     selected_rows = 191L,
#     selected_signature = "f8fd85fde2daf48c250646d2f0bd2794"
#   )
#
#   select_site_rows(
#     filename = "Chem_HYBAM.csv",
#     site_column = "nom",
#     site_counts = c(Manacapuru = 7690L, Obidos = 7674L),
#     full_rows = 117075L,
#     selected_rows = 101711L,
#     selected_signature = "0520fc06cff738577470e0d8cf12a224"
#   )
#
#   # ---- legacy source selection ----
#
#   filename <- "20221030_masterdata_chem_V2.csv"
#   before <- read_source(filename)
#   assert_count(nrow(before), 1033994L, "legacy full source rows")
#
#   # use data-frame column selection even after the amazon reader loads data.table
#   exclusions <- as.data.frame(readRDS(usgs_exclusion_file))
#   # these rows were confirmed as tds using official usgs records
#   required_columns <- c(
#     "source_file", "source_site", "date", "source_value"
#   )
#   missing_columns <- setdiff(required_columns, names(exclusions))
#   if (length(missing_columns) > 0L) {
#     stop(
#       "the USGS exclusion review lacks columns: ",
#       paste(missing_columns, collapse = ", ")
#     )
#   }
#
#   exclusions <- exclusions[
#     exclusions$source_file == filename,
#     required_columns,
#     drop = FALSE
#   ]
#   exclusion_keys <- unique(data.frame(
#     source_site = exclusions$source_site,
#     date = as.character(exclusions$date),
#     source_value = as.numeric(exclusions$source_value),
#     stringsAsFactors = FALSE
#   ))
#   assert_count(nrow(exclusion_keys), 8625L, "legacy TDS-as-TN evidence rows")
#
#   tn_scope <- before$LTER == "USGS" &
#     before$variable == "TN" &
#     before$units == "uM"
#   assert_count(sum(tn_scope), 22122L, "legacy USGS total-nitrogen rows")
#   candidates <- data.frame(
#     source_row = which(tn_scope),
#     source_site = before$Site.Stream.Name[tn_scope],
#     date = before$Sampling.Date[tn_scope],
#     source_value = suppressWarnings(as.numeric(before$value[tn_scope])),
#     stringsAsFactors = FALSE
#   )
#   reviewed_tds <- merge(
#     candidates,
#     exclusion_keys,
#     by = c("source_site", "date", "source_value"),
#     sort = FALSE
#   )
#   if (anyDuplicated(reviewed_tds$source_row) > 0L) {
#     stop("the legacy TDS-as-TN review matched a source row more than once")
#   }
#   assert_count(nrow(reviewed_tds), 8625L, "legacy TDS-as-TN rows")
#
#   remove_tds <- seq_len(nrow(before)) %in% reviewed_tds$source_row
#   # retain complete site records so unique variables remain available
#   # duplicate source series are resolved later by the master workflow
#   after <- before[!remove_tds, , drop = FALSE]
#   rownames(after) <- NULL
#   write_selected(
#     filename,
#     after,
#     1025369L,
#     "498d756e98403a078b7a9fcdfad68975"
#   )
# }
#
# # ---- collect prepared inputs ----
#
# assemble_inputs <- function(
#   raw_dir, selection_dir, qualifier_dir, direct_dir,
#   provider_dir, amazon_dir, key_file, output_dir
# ) {
#   required_paths <- c(
#     raw_dir,
#     selection_dir,
#     qualifier_dir,
#     direct_dir,
#     provider_dir,
#     amazon_dir,
#     key_file
#   )
#   check_required_files(required_paths, "run-input paths")
#
#   input_dirs <- c(
#     raw_dir,
#     selection_dir,
#     qualifier_dir,
#     direct_dir,
#     provider_dir,
#     amazon_dir
#   )
#   check_work_folder(output_dir, input_dirs, "assembled chemistry")
#   dir.create(output_dir, recursive = TRUE)
#
#   # ---- selected files ----
#
#   data_key <- read.csv(
#     key_file,
#     check.names = FALSE,
#     stringsAsFactors = FALSE
#   )
#   expected_files <- sort(unique(data_key$Raw_Filename[
#     data_key$Data_type == "chemistry" &
#       !is.na(data_key$Raw_Filename) &
#       data_key$Raw_Filename != ""
#   ]))
#
#   provider_files <- c(
#     "Canada_ECCC_AllSites_chemistry.csv",
#     "GEMS_AllSites_chemistry.csv",
#     "Danube_AllSites_chemistry.csv"
#   )
#
#   for (filename in expected_files) {
#     candidates <- c(
#       file.path(raw_dir, filename),
#       file.path(selection_dir, filename),
#       file.path(qualifier_dir, filename),
#       file.path(direct_dir, filename),
#       file.path(amazon_dir, filename)
#     )
#     if (filename %in% provider_files) {
#       candidates <- c(candidates, file.path(provider_dir, filename))
#     }
#     available <- candidates[file.exists(candidates)]
#     if (length(available) == 0L) {
#       stop("no run input is available for ", filename)
#     }
#
#     # use the latest prepared version and leave the downloaded source unchanged
#     selected <- available[[length(available)]]
#     copied <- file.copy(
#       selected,
#       file.path(output_dir, filename),
#       overwrite = TRUE,
#       copy.mode = FALSE,
#       copy.date = TRUE
#     )
#     if (!copied) {
#       stop("could not assemble ", filename)
#     }
#   }
#
#   # ---- final checks ----
#
#   assembled_files <- sort(list.files(output_dir, pattern = "[.]csv$"))
#   if (!identical(assembled_files, expected_files)) {
#     stop("the assembled chemistry file set does not match the data key")
#   }
#
#   for (filename in provider_files) {
#     provider_md5 <- unname(tools::md5sum(file.path(provider_dir, filename)))
#     assembled_md5 <- unname(tools::md5sum(file.path(output_dir, filename)))
#     if (!identical(provider_md5, assembled_md5)) {
#       stop("the exact provider output was not retained for ", filename)
#     }
#   }
#
#   amazon_file <- "Manacapuru_chemistry_combined.csv"
#   amazon_md5 <- unname(tools::md5sum(file.path(amazon_dir, amazon_file)))
#   assembled_amazon_md5 <- unname(tools::md5sum(file.path(output_dir, amazon_file)))
#   if (!identical(amazon_md5, assembled_amazon_md5)) {
#     stop("the generated manacapuru input was not retained")
#   }
# }
#
# # ---- gems silicon gap filling ----
#
# # remove overlaps before the master reads the combined inputs
# select_gems_si_gaps <- function(reactive, station_map, direct_keys, exclusions) {
#   daily_key <- c("LTER", "Stream_Name", "date")
#   if (anyDuplicated(station_map$gems_station)) {
#     stop("a GEMS silicon station matches more than one existing site")
#   }
#   reactive |>
#     dplyr::inner_join(station_map, by = "gems_station") |>
#     dplyr::anti_join(exclusions, by = c("gems_station", "date")) |>
#     dplyr::anti_join(direct_keys, by = daily_key)
# }
#
# prepare_gems_si_gaps <- function(input_dir, provider_dir, key_dir, return_sites = FALSE) {
#   archive <- file.path(provider_dir, "gems_gemstat", "GFQA_v3.zip")
#   stations <- read.csv(unz(archive, "GEMStat_station_metadata.csv"),
#     colClasses = "character", check.names = FALSE, fileEncoding = "latin1")
#   reference <- read.csv(file.path(key_dir, "Site_Reference_Table.csv"),
#     colClasses = "character", check.names = FALSE)
#   canada_sites <- read.csv(file.path(provider_dir, "canada_eccc",
#     "Water-Qual-Eau-Sites-National.csv"), colClasses = "character")
#
#   # match canada by the original station number, not the nearest coordinates
#   canada_map <- stations |>
#     dplyr::transmute(gems_station = `GEMS Station Number`,
#       SITE_NO = toupper(`Local Station Number`)) |>
#     dplyr::inner_join(canada_sites, by = "SITE_NO") |>
#     dplyr::transmute(gems_station, LTER = "Canada", Stream_Name = trimws(SITE_NAME)) |>
#     dplyr::semi_join(dplyr::filter(reference,
#       LTER == "Canada", Include_in_Master_Chem == "Yes"),
#       by = c("LTER", "Stream_Name"))
#   reactive <- read.csv(unz(archive, "Silicon.csv"), colClasses = "character",
#     check.names = FALSE)
#   reactive <- reactive |>
#     dplyr::filter(`Parameter Code` == "SiO2-Rea",
#       `Analysis Method Code` %in% gems_filtered_reactive_methods, Unit == "mg/l") |>
#     dplyr::transmute(gems_station = `GEMS Station Number`, date = `Sample Date`,
#       source_value = as.numeric(Value), qualifier = trimws(`Value Flags`))
#   canada_map <- dplyr::semi_join(canada_map, reactive, by = "gems_station")
#   check_reviewed_count(nrow(canada_map), 49L, "reviewed Canada/GEMS silicon matches")
#
#   # the us station numbers were checked against the gems station descriptions
#   usgs_map <- gems_usgs_si_sites |>
#     dplyr::inner_join(reference, by = c("usgs_station" = "USGSGageNumber")) |>
#     dplyr::filter(LTER == "USGS", Include_in_Master_Chem == "Yes") |>
#     dplyr::select(gems_station, LTER, Stream_Name)
#   check_reviewed_count(nrow(usgs_map), 5L, "reviewed USGS/GEMS silicon matches")
#   station_map <- dplyr::bind_rows(canada_map, usgs_map)
#   if (isTRUE(return_sites)) return(station_map)
#
#   # inspect prepared direct inputs, not a previous master output
#   canada <- as.data.frame(data.table::fread(
#     file.path(input_dir, "Canada_ECCC_AllSites_chemistry.csv"),
#     select = c("LTER", "Stream_Name", "date", "solute", "amount"),
#     colClasses = "character", showProgress = FALSE))
#   legacy <- as.data.frame(data.table::fread(
#     file.path(input_dir, "20221030_masterdata_chem_V2.csv"),
#     select = c("LTER", "site", "Sampling.Date", "variable", "value"),
#     colClasses = "character", showProgress = FALSE))
#   combined <- as.data.frame(data.table::fread(
#     file.path(input_dir, "Deduplicated_Sites_AllSites_chemistry.csv"),
#     select = c("LTER", "Stream_Name", "date", "solute", "amount"),
#     colClasses = "character", showProgress = FALSE))
#   direct <- dplyr::bind_rows(
#     dplyr::transmute(canada, LTER, Stream_Name, date, solute, amount),
#     dplyr::transmute(legacy, LTER, Stream_Name = site, date = Sampling.Date,
#       solute = variable, amount = value),
#     dplyr::transmute(combined, LTER, Stream_Name, date, solute, amount)
#   ) |>
#     dplyr::filter(tolower(solute) %in% c("dsi", "sio2_mgl")) |>
#     dplyr::mutate(numeric_value = suppressWarnings(as.numeric(
#       sub("^[<>]\\s*", "", trimws(amount))))) |>
#     dplyr::filter(is.finite(numeric_value)) |>
#     dplyr::distinct(LTER, Stream_Name, date)
#
#   matched <- dplyr::semi_join(reactive, station_map, by = "gems_station")
#   if (any(!is.finite(matched$source_value)) || anyNA(as.Date(matched$date)) ||
#       any(!matched$qualifier %in% c("", "<"))) {
#     stop("a reviewed GEMS reactive silicon result has an unsupported value, date, or qualifier")
#   }
#   gaps <- select_gems_si_gaps(matched, station_map, direct, gems_si_exclusions)
#   days <- dplyr::distinct(gaps, LTER, Stream_Name, date)
#   observed <- vapply(names(gems_si_expected_days), function(network) {
#     sum(days$LTER == network)
#   }, integer(1))
#   check_reviewed_counts(observed, gems_si_expected_days,
#     paste(names(gems_si_expected_days), "GEMS silicon gap days"))
#   check_reviewed_count(sum(gaps$qualifier == "<"), 1L,
#     "GEMS silicon gap records below the reported limit")
#
#   # keep source rows for the usual daily averaging and qualifier rules
#   additions <- gaps |>
#     dplyr::transmute(LTER, Stream_Name, date, solute = "DSi", solute_units = "uM",
#       amount = paste0(qualifier, format(source_value * 1000 / 60.0835,
#         digits = 15, scientific = FALSE, trim = TRUE)),
#       source_lab_flag = NA_character_, source_method_flag = gems_reactive_si_flag)
#   additions
# }
#
# # use the same unit and variable rules as the master when checking direct coverage
# read_gems_direct_keys <- function(input_dir, key_dir, station_map) {
#   data_key <- read.csv(file.path(key_dir, "SiSyn_Data_Key.csv"), na.strings = "") |>
#     dplyr::filter(Data_type == "chemistry") |>
#     dplyr::mutate(Raw_Column_Name = gsub(" |\\+|\\(|\\)|\\/|\\-", ".", Raw_Column_Name))
#   units_key <- read.csv(file.path(key_dir, "SiSyn_Chem_Units_Key.csv"), na.strings = "") |>
#     dplyr::filter(Data_type == "chemistry") |>
#     dplyr::mutate(Variable = tolower(Variable))
#   reference <- read.csv(file.path(key_dir, "Site_Reference_Table.csv"), na.strings = "")
#   files <- unique(c(data_key$Raw_Filename[data_key$Dataset %in% c("Canada", "USGS")],
#     "20221030_masterdata_chem_V2.csv", deduplicated_filename))
#   files <- files[files %in% list.files(input_dir)]
#   headers <- read_source_headers(input_dir, files)
#   names <- dplyr::bind_rows(
#     dplyr::transmute(reference, LTER, Stream_Name, source_name = Stream_Name),
#     dplyr::transmute(reference, LTER, Stream_Name, source_name = Alt_Stream_Name) |>
#       tidyr::separate_longer_delim(source_name, delim = ";"),
#     dplyr::transmute(reference, LTER, Stream_Name, source_name = Original_Stream_Name)
#   ) |>
#     dplyr::filter(!is.na(source_name), trimws(source_name) != "") |>
#     dplyr::mutate(name_key = stringr::str_to_lower(stringr::str_squish(source_name))) |>
#     dplyr::distinct(name_key, LTER, Stream_Name)
#   if (anyDuplicated(names$name_key)) stop("ambiguous direct-source site names")
#   keys <- vector("list", length(files))
#   for (row in seq_along(files)) {
#     direct <- harmonize_source(files[row], data_key, units_key, input_dir, headers) |>
#       standardize_source_data(data_key) |>
#       dplyr::filter(is.finite(value)) |>
#       dplyr::transmute(name_key = stringr::str_to_lower(stringr::str_squish(Stream_Name)),
#         date, variable, units) |>
#       dplyr::inner_join(names, by = "name_key") |>
#       dplyr::semi_join(station_map, by = c("LTER", "Stream_Name")) |>
#       dplyr::mutate(date = as.character(as.Date(date))) |>
#       dplyr::distinct(LTER, Stream_Name, date, variable, units)
#     if (anyNA(direct$date)) stop("a direct-source gap check has an unreadable date")
#     keys[[row]] <- direct
#   }
#   dplyr::distinct(dplyr::bind_rows(keys))
# }
#
# # average each gems day before changing the table shape so the lab notes survive
# average_gems_gap_days <- function(data) {
#   data <- dplyr::distinct(data)
#   parsed <- parse_qualified_numbers(data$amount)
#   data$sample_value <- parsed$numbers
#   data$sample_qualifier <- ifelse(parsed$is_below, "<", ifelse(parsed$is_above, ">", ""))
#   days <- data |>
#     dplyr::group_by(LTER, Stream_Name, date, solute, solute_units) |>
#     dplyr::summarise(
#       detected = sum(sample_qualifier == ""),
#       below = sum(sample_qualifier == "<"),
#       above = sum(sample_qualifier == ">"),
#       value = if (detected > 0L) mean(sample_value[sample_qualifier == ""]) else mean(sample_value),
#       qualifier = if (detected > 0L) NA_character_ else dplyr::first(sample_qualifier),
#       estimated = any(source_lab_flag == "estimated value", na.rm = TRUE),
#       source_lab_flag = reduce_daily_flag(
#         source_lab_flag[source_lab_flag != "estimated value"], "GEMS lab"),
#       source_method_flag = reduce_daily_flag(source_method_flag, "GEMS method"),
#       .groups = "drop"
#     )
#   calculated_flag <- daily_lab_flag(days$detected, days$below, days$above)
#   conflict <- !is.na(calculated_flag) & !is.na(days$source_lab_flag) &
#     calculated_flag != days$source_lab_flag
#   if (any(conflict)) stop("GEMS source and daily detection-limit flags conflict")
#   days$source_lab_flag <- add_estimated_lab_flag(
#     dplyr::coalesce(calculated_flag, days$source_lab_flag), days$estimated)
#   days |>
#     dplyr::mutate(amount = format_amount(value, qualifier)) |>
#     dplyr::select(LTER, Stream_Name, date, solute, solute_units, amount,
#       source_lab_flag, source_method_flag)
# }
#
# # fill only missing variables at the stations already reviewed for gems overlap
# prepare_gems_chemistry_gaps <- function(input_dir, provider_dir, key_dir) {
#   silicon <- prepare_gems_si_gaps(input_dir, provider_dir, key_dir)
#   sites <- prepare_gems_si_gaps(input_dir, provider_dir, key_dir, return_sites = TRUE)
#   direct <- read_gems_direct_keys(input_dir, key_dir, sites)
#   archive <- file.path(provider_dir, "gems_gemstat", "GFQA_v3.zip")
#   parameters <- gems_gap_parameters()
#   additions <- vector("list", nrow(parameters))
#   for (row in seq_len(nrow(parameters))) {
#     rule <- parameters[row, ]
#     source <- data.table::fread(cmd = sprintf("unzip -p %s %s",
#       shQuote(archive), shQuote(rule$source_entry)), encoding = "Latin-1",
#       colClasses = "character", na.strings = "", showProgress = FALSE)
#     # the master distinguishes oxygen concentration and saturation by their units
#     source <- tibble::as_tibble(source) |>
#       dplyr::filter(`GEMS Station Number` %in% sites$gems_station,
#         `Parameter Code` == rule$parameter_code, Unit == rule$provider_units) |>
#       dplyr::transmute(gems_station = `GEMS Station Number`, date = `Sample Date`,
#         variable = if (rule$variable == "do percent") "dissolved oxygen" else rule$variable,
#         units = rule$units, source_value = suppressWarnings(as.numeric(Value)),
#         qualifier = dplyr::coalesce(trimws(`Value Flags`), ""),
#         depth = suppressWarnings(as.numeric(Depth))) |>
#       dplyr::filter(is.finite(source_value), source_value != -9999,
#         !is.na(date), date != "") |>
#       dplyr::inner_join(sites, by = "gems_station") |>
#       dplyr::anti_join(direct, by = c("LTER", "Stream_Name", "date", "variable", "units"))
#     if (!nrow(source)) next
#     if (anyNA(as.Date(source$date))) {
#       stop("a GEMS gap result has an unsupported date: ", rule$parameter_code)
#     }
#     source <- source |>
#       dplyr::group_by(LTER, Stream_Name, date, variable, units) |>
#       dplyr::mutate(depth_order = dplyr::coalesce(depth, Inf),
#         multiple_depths = dplyr::n_distinct(depth[!is.na(depth)]) > 1L) |>
#       dplyr::filter(depth_order == min(depth_order)) |>
#       dplyr::ungroup()
#     # keep estimates and uncertain physical-variable flags separate from lab limits
#     flags <- interpret_gems_qualifiers(source$qualifier, rule$limit_is_clear)
#     source$qualifier <- flags$qualifier
#     source$lab_flag <- flags$lab_flag
#     additions[[row]] <- source |>
#       dplyr::transmute(LTER, Stream_Name, date, solute = rule$variable,
#         solute_units = rule$units,
#         amount = paste0(qualifier, format(source_value * rule$conversion,
#           digits = 15, scientific = FALSE, trim = TRUE)),
#         source_lab_flag = lab_flag,
#         source_method_flag = dplyr::if_else(multiple_depths,
#           "shallowest sample depth used", NA_character_))
#   }
#   other <- average_gems_gap_days(dplyr::bind_rows(additions))
#   keys <- other |>
#     dplyr::transmute(LTER, Stream_Name, date,
#       variable = dplyr::if_else(solute == "do percent", "dissolved oxygen", solute),
#       units = solute_units) |>
#     dplyr::distinct()
#   if (nrow(dplyr::inner_join(keys, direct,
#       by = c("LTER", "Stream_Name", "date", "variable", "units")))) {
#     stop("a GEMS addition overlaps a direct chemistry result")
#   }
#   dplyr::bind_rows(silicon, other)
# }
#
# # ---- run preparation ----
#
# prepare_chemistry(commandArgs(trailingOnly = TRUE))
# ---- end replacement: preparation-order ----

# ---- sidney: canada, gems, and danube records ----
# canada filtered icp-oes silicon uses the agreed si basis
# gems records explicitly labeled sio2 keep the sio2 conversion
# current file: chemistry-inputs/prepare_eccc_gems_danube.R
# ---- begin replacement: original-source-records ----
# # prepare the combined eccc, gems, and danube files from the original downloads
#
# # ---- packages ----
#
# suppressPackageStartupMessages({
#   library(data.table)
#   library(dplyr)
# })
#
# # ---- shared helpers ----
#
# helper_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
# if (length(helper_argument) != 1L) {
#   stop("the script folder could not be identified")
# }
# source(file.path(
#   dirname(dirname(normalizePath(sub("^--file=", "", helper_argument)))),
#   "chemistry_functions.R"
# ))
# rm(helper_argument)
#
# # ---- inputs ----
#
# args <- commandArgs(trailingOnly = TRUE)
#
# if (length(args) != 4L) {
#   stop(
#     paste(
#       "supply the source snapshot, provider folder, data key,",
#       "and output folder"
#     )
#   )
# }
# raw_dir <- normalizePath(args[[1]], mustWork = TRUE)
# provider_dir <- normalizePath(args[[2]], mustWork = TRUE)
# data_key_path <- normalizePath(args[[3]], mustWork = TRUE)
# site_reference_path <- file.path(
#   dirname(data_key_path),
#   "Site_Reference_Table.csv"
# )
# if (!file.exists(site_reference_path)) {
#   stop("Site_Reference_Table.csv must be beside the data key")
# }
# output_dir <- args[[4]]
#
# input_dirs <- c(
#   raw_dir,
#   provider_dir,
#   dirname(data_key_path)
# )
# check_work_folder(output_dir, input_dirs, "provider preparation")
#
# raw_output_dir <- file.path(output_dir, "chem_raw_subset")
# dir.create(raw_output_dir, recursive = TRUE)
#
# canada_file <- "Canada_ECCC_AllSites_chemistry.csv"
# gems_file <- "GEMS_AllSites_chemistry.csv"
# danube_file <- "Danube_AllSites_chemistry.csv"
#
# gems_pattern <- "^GEMS__GEMS__.*[.]csv$"
# danube_pattern <- "^Danube__Danube__.*[.]csv$"
#
# raw_files <- list.files(raw_dir)
# gems_files <- grep(gems_pattern, raw_files, value = TRUE)
# danube_files <- grep(danube_pattern, raw_files, value = TRUE)
#
# if (
#   canada_file %in% raw_files ||
#     length(gems_files) != 160L ||
#     length(danube_files) != 0L
# ) {
#   stop(
#     paste(
#       "the source snapshot must omit generated Canada and Danube files",
#       "and contain exactly 160 GEMS site files"
#     )
#   )
# }
#
# # ---- shared values ----
#
# Al_mw <- 26.9815385
# C_mw <- 12.011
# Ca_mw <- 40.078
# Cl_mw <- 35.453
# F_mw <- 18.998403163
# Fe_mw <- 55.845
# H_mw <- 1.00784
# K_mw <- 39.0983
# Mg_mw <- 24.305
# Mn_mw <- 54.938044
# N_mw <- 14.0067
# Na_mw <- 22.989769
# O_mw <- 15.999
# P_mw <- 30.973761998
# S_mw <- 32.065
# Si_mw <- 28.0855
# Zn_mw <- 65.38
#
# NH4_mw <- N_mw + 4 * H_mw
# NO3_mw <- N_mw + 3 * O_mw
# PO4_mw <- P_mw + 4 * O_mw
# SO4_mw <- S_mw + 4 * O_mw
# SiO2_mw <- Si_mw + 2 * O_mw
#
# read_long_files <- function(filenames) {
#   # read one file at a time, keeping the supplied file order
#   source_tables <- vector("list", length(filenames))
#   for (file_number in seq_along(filenames)) {
#     path <- file.path(raw_dir, filenames[file_number])
#     source_tables[[file_number]] <- data.table::fread(
#       path,
#       colClasses = "character",
#       na.strings = "",
#       showProgress = FALSE
#     )
#   }
#
#   # match columns by name and leave missing fields blank
#   combined <- data.table::rbindlist(source_tables, use.names = TRUE, fill = TRUE)
#   combined <- tibble::as_tibble(combined)
#   combined$date <- as.Date(combined$date)
#   combined
# }
#
# # combine method notes without dropping either source detail
# combine_method_flags <- function(primary, additional) {
#   primary <- dplyr::na_if(trimws(as.character(primary)), "")
#   additional <- dplyr::na_if(trimws(as.character(additional)), "")
#   dplyr::case_when(
#     is.na(primary) ~ additional,
#     is.na(additional) ~ primary,
#     TRUE ~ paste(primary, additional, sep = "; ")
#   )
# }
#
# # test one flag inside a semicolon-separated method field
# has_method_flag <- function(values, flag) {
#   flag_parts <- strsplit(dplyr::coalesce(as.character(values), ""), "; ", fixed = TRUE)
#   found <- logical(length(flag_parts))
#   names(found) <- names(flag_parts)
#   for (row in seq_along(flag_parts)) {
#     found[row] <- flag %in% flag_parts[[row]]
#   }
#   found
# }
#
# # ---- canada ----
#
# canada_provider_dir <- file.path(provider_dir, "canada_eccc")
# canada_sites_path <- file.path(
#   canada_provider_dir,
#   "Water-Qual-Eau-Sites-National.csv"
# )
# canada_data_paths <- list.files(
#   canada_provider_dir,
#   pattern = "2000-present[.]csv$",
#   full.names = TRUE
# )
#
# if (!file.exists(canada_sites_path) || length(canada_data_paths) != 17L) {
#   stop("the reviewed Canada provider files are unavailable")
# }
#
# canada_separate_sites <- c(
#   "ALSEK RIVER ABOVE BATES RIVER IN KLUANE NATIONAL PARK",
#   "BEAVER RIVER ABOVE HIGHWAY 1 IN GLACIER NATIONAL PARK",
#   "KICKING HORSE RIVER AT FIELD IN YOHO NATIONAL PARK",
#   "KOOTENAY RIVER ABOVE HIGHWAY 93 IN KOOTENAY NATIONAL PARK",
#   "LIARD RIVER AT UPPER CROSSING",
#   "SKEENA RIVER AT USK"
# )
#
# canada_site_names <- data.table::fread(
#   site_reference_path,
#   select = c("LTER", "Stream_Name"),
#   colClasses = "character",
#   showProgress = FALSE
# ) %>%
#   tibble::as_tibble() %>%
#   dplyr::filter(LTER == "Canada") %>%
#   dplyr::transmute(Stream_Name = trimws(Stream_Name)) %>%
#   dplyr::filter(!Stream_Name %in% canada_separate_sites) %>%
#   dplyr::distinct()
#
# if (
#   nrow(canada_site_names) != 123L ||
#     any(is.na(canada_site_names$Stream_Name)) ||
#     any(canada_site_names$Stream_Name == "")
# ) {
#   stop("the reviewed Canada site names changed")
# }
#
# canada_site_map <- data.table::fread(
#   canada_sites_path,
#   select = c("SITE_NO", "SITE_NAME"),
#   colClasses = "character",
#   showProgress = FALSE
# ) %>%
#   tibble::as_tibble() %>%
#   dplyr::mutate(Stream_Name = trimws(SITE_NAME)) %>%
#   dplyr::semi_join(canada_site_names, by = "Stream_Name") %>%
#   dplyr::distinct(SITE_NO, Stream_Name)
#
# if (
#   nrow(canada_site_map) != 123L ||
#     dplyr::n_distinct(canada_site_map$SITE_NO) != 123L ||
#     dplyr::n_distinct(canada_site_map$Stream_Name) != 123L
# ) {
#   stop("the reviewed Canada site map changed")
# }
#
# canada_source_columns <- c(
#   "SITE_NO",
#   "DATE_TIME_HEURE",
#   "FLAG_MARQUEUR",
#   "VALUE_VALEUR",
#   "SDL_LDE",
#   "MDL_LDM",
#   "VMV_CODE",
#   "UNIT_UNITÉ",
#   "VARIABLE",
#   "STATUS_STATUT",
#   "SAMPLE_ID_ÉCHANTILLON"
# )
#
# canada_provider <- data.table::rbindlist(
#   lapply(canada_data_paths, function(source_path) {
#     source_rows <- data.table::fread(
#       source_path,
#       select = canada_source_columns,
#       colClasses = "character",
#       showProgress = FALSE
#     )
#     source_rows[, source_file := basename(source_path)]
#     source_rows[SITE_NO %in% canada_site_map$SITE_NO]
#   }),
#   use.names = TRUE,
#   fill = TRUE
# ) %>%
#   tibble::as_tibble() %>%
#   dplyr::left_join(canada_site_map, by = "SITE_NO") %>%
#   dplyr::transmute(
#     site_no = SITE_NO,
#     Stream_Name,
#     source_datetime = DATE_TIME_HEURE,
#     date = as.Date(substr(DATE_TIME_HEURE, 1, 10)),
#     source_variable = trimws(VARIABLE),
#     source_units = trimws(UNIT_UNITÉ),
#     source_value = suppressWarnings(as.numeric(VALUE_VALEUR)),
#     value_qualifier = trimws(FLAG_MARQUEUR),
#     vmv_code = trimws(VMV_CODE),
#     status = trimws(STATUS_STATUT),
#     sample_id = SAMPLE_ID_ÉCHANTILLON,
#     source_file
#   )
#
# canada_cold_review_source <- tibble::tribble(
#   ~site_no, ~Stream_Name, ~source_datetime, ~sample_id,
#   ~source_file, ~expected_value,
#   "AL05BA0011", "BOW RIVER AT HIGHWAY 1 ABOVE LAKE LOUISE",
#   "2015-01-14 14:00", "2014PN140164",
#   "Water-Qual-Eau-S-Saskatchewan-2000-present.csv", -1.5,
#   "MA05OB0001", "PEMBINA RIVER AT WINDYGATES, MANITOBA",
#   "2015-12-15 11:00", "2015PN700147",
#   "Water-Qual-Eau-Assiniboine-Red-2000-present.csv", -2.5,
#   "MA05OB0001", "PEMBINA RIVER AT WINDYGATES, MANITOBA",
#   "2016-01-26 13:30", "2015PN700161",
#   "Water-Qual-Eau-Assiniboine-Red-2000-present.csv", -2.87,
#   "MA05PF0022", "WINNIPEG RIVER AT POINTE DU BOIS,",
#   "2015-12-17 11:00", "2015PN700145",
#   "Water-Qual-Eau-Winnipeg-2000-present.csv", -1.3
# )
#
# canada_cold_source_review <- canada_provider %>%
#   dplyr::filter(
#     source_variable == "TEMPERATURE WATER (FIELD)",
#     source_units == "DEG C",
#     vmv_code == "109028"
#   ) %>%
#   dplyr::inner_join(
#     canada_cold_review_source,
#     by = c(
#       "site_no",
#       "Stream_Name",
#       "source_datetime",
#       "sample_id",
#       "source_file"
#     )
#   )
#
# if (
#   nrow(canada_cold_source_review) != 4L ||
#     any(canada_cold_source_review$status != "P") ||
#     any(
#       !is.na(canada_cold_source_review$value_qualifier) &
#         canada_cold_source_review$value_qualifier != ""
#     ) ||
#     any(!dplyr::near(
#       canada_cold_source_review$source_value,
#       canada_cold_source_review$expected_value
#     ))
# ) {
#   stop("the reviewed Canada gross cold-temperature source rows changed")
# }
#
# canada_coppermine_source_review <- canada_provider %>%
#   dplyr::filter(
#     site_no == "NW10PC0019",
#     Stream_Name == "COPPERMINE RIVER ABOVE COPPER CREEK",
#     source_datetime == "2018-04-09 12:05",
#     source_variable == "TEMPERATURE WATER (FIELD)",
#     source_units == "DEG C",
#     vmv_code == "109028",
#     sample_id == "2018PN010001",
#     source_file == "Water-Qual-Eau-Arctic-2000-present.csv"
#   )
#
# if (
#   nrow(canada_coppermine_source_review) != 1L ||
#     !dplyr::near(canada_coppermine_source_review$source_value, -10) ||
#     canada_coppermine_source_review$status != "P" ||
#     (
#       !is.na(canada_coppermine_source_review$value_qualifier) &&
#         canada_coppermine_source_review$value_qualifier != ""
#     )
# ) {
#   stop("the reviewed Canada Coppermine source row changed")
# }
#
# # use the agreed elemental si basis for filtered soluble reactive silicon
# canada_map <- tibble::tribble(
#   ~source_variable, ~source_units, ~variable, ~units, ~conversion,
#   "ALKALINITY GRAN CACO3", "MG/L", "alkalinity", "ueq/L", 20,
#   "ALKALINITY TOTAL CACO3", "MG/L", "alkalinity", "ueq/L", 20,
#   "ALUMINUM DISSOLVED", "MG/L", "Al", "uM", 1000 / Al_mw,
#   "ALUMINUM DISSOLVED", "µG/L", "Al", "uM", 1 / Al_mw,
#   "CALCIUM", "MG/L", "Ca", "uM", 1000 / Ca_mw,
#   "CALCIUM DISSOLVED", "MG/L", "Ca", "uM", 1000 / Ca_mw,
#   "CHLOROPHYLL A", "MG/L", "chlorophyll a", "ug/L", 1000,
#   "CHLOROPHYLL A", "µG/L", "chlorophyll a", "ug/L", 1,
#   "CHLORIDE", "MG/L", "Cl", "uM", 1000 / Cl_mw,
#   "CHLORIDE DISSOLVED", "MG/L", "Cl", "uM", 1000 / Cl_mw,
#   "CARBON DISSOLVED ORGANIC", "MG/L", "dissolved org C", "uM", 1000 / C_mw,
#   "FLUORIDE", "MG/L", "F", "uM", 1000 / F_mw,
#   "FLUORIDE DISSOLVED", "MG/L", "F", "uM", 1000 / F_mw,
#   "IRON DISSOLVED", "MG/L", "Fe", "uM", 1000 / Fe_mw,
#   "IRON DISSOLVED", "µG/L", "Fe", "uM", 1 / Fe_mw,
#   "CONDUCTANCE (FIELD)", "µS/CM", "conductivity", "uS/cm", 1,
#   "SPECIFIC CONDUCTANCE (FIELD)", "µS/CM", "specific conductivity", "uS/cm", 1,
#   "PH (FIELD)", "PH", "pH", NA_character_, 1,
#   "POTASSIUM", "MG/L", "K", "uM", 1000 / K_mw,
#   "POTASSIUM DISSOLVED/FILTERED", "MG/L", "K", "uM", 1000 / K_mw,
#   "POTASSIUM FILTERED", "MG/L", "K", "uM", 1000 / K_mw,
#   "MAGNESIUM", "MG/L", "Mg", "uM", 1000 / Mg_mw,
#   "MAGNESIUM DISSOLVED", "MG/L", "Mg", "uM", 1000 / Mg_mw,
#   "MANGANESE DISSOLVED", "MG/L", "Mn", "uM", 1000 / Mn_mw,
#   "MANGANESE DISSOLVED", "µG/L", "Mn", "uM", 1 / Mn_mw,
#   "SODIUM", "MG/L", "Na", "uM", 1000 / Na_mw,
#   "SODIUM DISSOLVED/FILTERED", "MG/L", "Na", "uM", 1000 / Na_mw,
#   "SODIUM FILTERED", "MG/L", "Na", "uM", 1000 / Na_mw,
#   "AMMONIA DISSOLVED", "MG/L", "NHX", "uM", 1000 / N_mw,
#   "DISSOLVED NITROGEN NITRATE", "MG/L", "NO3", "uM", 1000 / N_mw,
#   "DISSOLVED NITROGEN NITRATE", "MG/L N", "NO3", "uM", 1000 / N_mw,
#   "NITRATE AND NITRITE (AS N)", "MG/L", "NOx", "uM", 1000 / N_mw,
#   "ORTHOPHOSPHORUS DISSOLVED", "MG/L", "PO4", "uM", 1000 / P_mw,
#   "PHOSPHORUS SOLUBLE REACTIVE", "MG/L", "SRP", "uM", 1000 / P_mw,
#   "SILICA REACTIVE", "MG/L", "DSi", "uM", 1000 / SiO2_mw,
#   "SILICA SOLUBLE REACTIVE", "MG/L", "DSi", "uM", 1000 / Si_mw,
#   "SULPHATE", "MG/L", "SO4", "uM", 1000 / SO4_mw,
#   "SULPHATE DISSOLVED", "MG/L", "SO4", "uM", 1000 / SO4_mw,
#   "NITROGEN TOTAL", "MG/L", "TN", "uM", 1000 / N_mw,
#   "NITROGEN TOTAL DISSOLVED", "MG/L", "tot dissolved N", "uM", 1000 / N_mw,
#   "TEMPERATURE (WATER)", "DEG C", "temp", "C", 1,
#   "TEMPERATURE WATER (FIELD)", "DEG C", "temp", "C", 1,
#   "PHOSPHORUS TOTAL", "MG/L", "TP", "uM", 1000 / P_mw,
#   "PHOSPHORUS TOTAL DISSOLVED", "MG/L", "TDP", "uM", 1000 / P_mw
# )
#
# # pass canada si onward in um so the units key does not convert it again
# canada_raw_map <- tibble::tribble(
#   ~variable, ~units, ~solute, ~solute_units, ~raw_per_final,
#   "alkalinity", "ueq/L", "alkalinity", "ueq/L", 1,
#   "Al", "uM", "al_ugl", "ug/L", Al_mw,
#   "Ca", "uM", "ca_mgl", "mg/L", Ca_mw / 1000,
#   "chlorophyll a", "ug/L", "chla_ugl", "ug/L", 1,
#   "Cl", "uM", "cl_mgl", "mg/L", Cl_mw / 1000,
#   "dissolved org C", "uM", "doc_mgl", "mg C/L", C_mw / 1000,
#   "F", "uM", "f_mgl", "mg/L", F_mw / 1000,
#   "Fe", "uM", "fe_ugl", "ug/L", Fe_mw,
#   "conductivity", "uS/cm", "conductivity", "uS/cm", 1,
#   "specific conductivity", "uS/cm", "specific conductivity", "uS/cm", 1,
#   "pH", NA_character_, "fldph", NA_character_, 1,
#   "K", "uM", "k_mgl", "mg/L", K_mw / 1000,
#   "Mg", "uM", "mg_mgl", "mg/L", Mg_mw / 1000,
#   "Mn", "uM", "mn_ugl", "ug/L", Mn_mw,
#   "Na", "uM", "na_mgl", "mg/L", Na_mw / 1000,
#   "NHX", "uM", "NHX", "mg NH4-N/L", N_mw / 1000,
#   "NO3", "uM", "NO3", "mg NO3-N/L", N_mw / 1000,
#   "NOx", "uM", "NOx", "mg NO3-N/L", N_mw / 1000,
#   "PO4", "uM", "PO4", "mg P/L", P_mw / 1000,
#   "SRP", "uM", "SRP", "mg P/L", P_mw / 1000,
#   "DSi", "uM", "DSi", "uM", 1,
#   "SO4", "uM", "so4_mgl", "mg SO4/L", SO4_mw / 1000,
#   "TN", "uM", "TN", "mg N/L", N_mw / 1000,
#   "tot dissolved N", "uM", "tdn_mgl", "mg N/L", N_mw / 1000,
#   "temp", "C", "temp_c", "C", 1,
#   "TP", "uM", "tp_ugl", "ug P/L", P_mw,
#   "TDP", "uM", "TDP", "mg P/L", P_mw / 1000
# )
#
# if (anyDuplicated(canada_map[c("source_variable", "source_units")])) {
#   stop("the Canada provider map contains a duplicate source mapping")
# }
#
# canada_source <- canada_provider %>%
#   dplyr::inner_join(
#     canada_map,
#     by = c("source_variable", "source_units")
#   ) %>%
#   dplyr::filter(is.finite(source_value), !is.na(date)) %>%
#   dplyr::mutate(
#     censored = !is.na(value_qualifier) & value_qualifier == "<",
#     detected = is.na(value_qualifier) | value_qualifier == "",
#     value = source_value * conversion,
#     source_method_flag = dplyr::case_when(
#       source_variable == "PH (FIELD)" ~ "field pH",
#       source_variable %in% c(
#         "CONDUCTANCE (FIELD)",
#         "SPECIFIC CONDUCTANCE (FIELD)"
#       ) ~ "field conductivity",
#       source_variable == "SILICA SOLUBLE REACTIVE" &
#         vmv_code == "109113" ~
#           "soluble reactive Si, filtered ICP-OES",
#       source_variable == "SILICA REACTIVE" &
#         vmv_code %in% c("106020", "61334") ~
#           "soluble reactive SiO2, ECCC",
#       TRUE ~ NA_character_
#     )
#   )
#
# if (
#   nrow(canada_source) != 258055L ||
#     sum(canada_source$censored) != 14077L ||
#     any(!canada_source$censored & !canada_source$detected)
# ) {
#   stop("the reviewed Canada provider rows changed")
# }
#
# # confirm four reported values kept for later outlier review
# canada_extreme_plan <- tibble::tribble(
#   ~site_no, ~source_datetime, ~source_variable, ~source_units,
#   ~vmv_code, ~sample_id, ~source_file, ~expected_value,
#   "NW10UF0003", "2016-08-17 16:46", "PH (FIELD)", "PH",
#   "109026", "2016PN010077",
#   "Water-Qual-Eau-Arctic-2000-present.csv", 63.50,
#   "NW10VK0002", "2015-07-15 19:05", "PH (FIELD)", "PH",
#   "109026", "2015PN010094",
#   "Water-Qual-Eau-Arctic-2000-present.csv", 59.00,
#   "NF02YL0011", "2020-10-23 11:45", "TEMPERATURE WATER (FIELD)",
#   "DEG C", "109028", "2020-AT0215-0235",
#   "Water-Qual-Eau-Newfoundland-TerreNeuve-Labrador-2000-present.csv", 87.47,
#   "NF03OE0035", "2023-07-05 11:45", "PH (FIELD)", "PH",
#   "109026", "2023-AT0215-0027",
#   "Water-Qual-Eau-Newfoundland-TerreNeuve-Labrador-2000-present.csv", 100.30
# )
# canada_extreme_keys <- names(canada_extreme_plan)[
#   names(canada_extreme_plan) != "expected_value"
# ]
# canada_extreme_review <- canada_source %>%
#   dplyr::inner_join(
#     canada_extreme_plan,
#     by = canada_extreme_keys
#   )
#
# if (
#   nrow(canada_extreme_review) != 4L ||
#     any(canada_extreme_review$status != "P") ||
#     any(
#       !is.na(canada_extreme_review$value_qualifier) &
#         canada_extreme_review$value_qualifier != ""
#     ) ||
#     any(!dplyr::near(
#       canada_extreme_review$source_value,
#       canada_extreme_review$expected_value
#     ))
# ) {
#   stop("the reviewed Canada extreme source values changed")
# }
#
# # keep filtered reactive si and exclude the method with no documented fraction
# canada_source_valid <- canada_source %>%
#   dplyr::filter(
#     is.na(source_method_flag) |
#       source_method_flag != "soluble reactive SiO2, ECCC"
#   )
#
# if (
#   nrow(canada_source_valid) != 258015L ||
#     any(canada_source_valid$source_method_flag ==
#       "soluble reactive SiO2, ECCC", na.rm = TRUE)
# ) {
#   stop("the reviewed Canada silica method selection changed")
# }
#
# canada_alkalinity_source <- canada_source_valid %>%
#   dplyr::filter(variable == "alkalinity") %>%
#   dplyr::group_by(Stream_Name, date, variable, units) %>%
#   dplyr::mutate(
#     has_total_detected = any(
#       source_variable == "ALKALINITY TOTAL CACO3" & detected
#     ),
#     has_gran_detected = any(
#       source_variable == "ALKALINITY GRAN CACO3" & detected
#     ),
#     has_total = any(source_variable == "ALKALINITY TOTAL CACO3"),
#     selected_method = dplyr::case_when(
#       has_total_detected ~ "ALKALINITY TOTAL CACO3",
#       has_gran_detected ~ "ALKALINITY GRAN CACO3",
#       has_total ~ "ALKALINITY TOTAL CACO3",
#       TRUE ~ "ALKALINITY GRAN CACO3"
#     )
#   ) %>%
#   dplyr::filter(source_variable == selected_method) %>%
#   dplyr::mutate(
#     source_method_flag = dplyr::case_when(
#       source_variable == "ALKALINITY TOTAL CACO3" ~
#         "total alkalinity, ECCC",
#       source_variable == "ALKALINITY GRAN CACO3" ~
#         "Gran alkalinity fallback, ECCC",
#       TRUE ~ source_method_flag
#     )
#   ) %>%
#   dplyr::ungroup()
#
# canada_alkalinity_overlap <- canada_source_valid %>%
#   dplyr::filter(variable == "alkalinity") %>%
#   dplyr::distinct(Stream_Name, date, source_variable) %>%
#   dplyr::count(Stream_Name, date, name = "methods") %>%
#   dplyr::filter(methods == 2L)
#
# canada_alkalinity_method_review <- canada_alkalinity_source %>%
#   dplyr::semi_join(
#     canada_alkalinity_overlap,
#     by = c("Stream_Name", "date")
#   ) %>%
#   dplyr::distinct(
#     Stream_Name,
#     date,
#     has_total_detected,
#     has_gran_detected,
#     selected_method
#   )
#
# if (
#   nrow(canada_alkalinity_overlap) != 2831L ||
#     dplyr::n_distinct(
#       canada_alkalinity_source$Stream_Name,
#       canada_alkalinity_source$date
#     ) != 13910L ||
#     any(canada_alkalinity_source$source_variable !=
#       canada_alkalinity_source$selected_method) ||
#     sum(
#       canada_alkalinity_method_review$has_total_detected &
#         canada_alkalinity_method_review$has_gran_detected
#     ) != 2157L ||
#     sum(
#       canada_alkalinity_method_review$has_total_detected &
#         !canada_alkalinity_method_review$has_gran_detected
#     ) != 1L ||
#     sum(
#       !canada_alkalinity_method_review$has_total_detected &
#         canada_alkalinity_method_review$has_gran_detected
#     ) != 622L ||
#     sum(
#       !canada_alkalinity_method_review$has_total_detected &
#         !canada_alkalinity_method_review$has_gran_detected
#     ) != 51L ||
#     any(
#       !canada_alkalinity_method_review$has_total_detected &
#         canada_alkalinity_method_review$has_gran_detected &
#         canada_alkalinity_method_review$selected_method !=
#           "ALKALINITY GRAN CACO3"
#     )
# ) {
#   stop("the reviewed Canada alkalinity method selection changed")
# }
#
# canada_selected_source <- canada_source_valid %>%
#   dplyr::filter(variable != "alkalinity") %>%
#   dplyr::bind_rows(canada_alkalinity_source)
#
# # average only detected values when some samples are below a limit
# # if all samples are below the limit, average the thresholds and keep the sign
# canada_daily <- canada_selected_source %>%
#   dplyr::group_by(Stream_Name, date, variable, units) %>%
#   dplyr::summarize(
#     any_censored = any(censored),
#     all_censored = all(censored),
#     value = if (all_censored) mean(value) else mean(value[detected]),
#     value_qualifier = if (all_censored) "<" else NA_character_,
#     source_method_flag = {
#       method_flags <- unique(source_method_flag[
#         !is.na(source_method_flag) & source_method_flag != ""
#       ])
#       if (length(method_flags) > 1L) {
#         stop("conflicting Canada method flags remain within a daily key")
#       }
#       if (length(method_flags) == 0L) NA_character_ else method_flags
#     },
#     .groups = "drop"
#   )
#
# canada_sensitive_variables <- c(
#   "alkalinity",
#   "conductivity",
#   "specific conductivity",
#   "NHX",
#   "NO3",
#   "NOx",
#   "PO4",
#   "SRP",
#   "DSi",
#   "TN",
#   "TDP"
# )
#
# canada_output <- canada_daily %>%
#   dplyr::left_join(canada_raw_map, by = c("variable", "units")) %>%
#   dplyr::transmute(
#     LTER = "Canada",
#     Stream_Name,
#     source_lab_flag = dplyr::case_when(
#       any_censored & !all_censored ~
#         "below-limit results excluded from daily mean",
#       all_censored ~ "below reported limit",
#       TRUE ~ NA_character_
#     ),
#     source_method_flag,
#     date,
#     solute,
#     solute_units,
#     amount = format_amount(value * raw_per_final, value_qualifier)
#   ) %>%
#   dplyr::arrange(Stream_Name, date, solute, solute_units)
#
# if (
#   anyNA(canada_output$solute) ||
#     any(canada_output$solute == "NH4") ||
#     nrow(canada_output) != 226083L ||
#     dplyr::n_distinct(
#       canada_output$Stream_Name,
#       canada_output$date,
#       canada_output$solute,
#       canada_output$solute_units
#     ) != 226083L
# ) {
#   stop("the reviewed Canada output rows changed")
# }
#
# canada_sensitive_output <- canada_output %>%
#   dplyr::semi_join(
#     canada_raw_map %>%
#       dplyr::filter(variable %in% canada_sensitive_variables) %>%
#       dplyr::select(solute, solute_units),
#     by = c("solute", "solute_units")
#   )
#
# canada_sensitive_review <- canada_sensitive_output %>%
#   dplyr::group_by(solute) %>%
#   dplyr::summarize(
#     rows = dplyr::n(),
#     less_than = sum(grepl("^<", amount)),
#     .groups = "drop"
#   )
#
# canada_sensitive_expected <- tibble::tribble(
#   ~solute, ~rows, ~less_than,
#   "alkalinity", 13910L, 52L,
#   "conductivity", 232L, 0L,
#   "specific conductivity", 11208L, 0L,
#   "NHX", 8312L, 1813L,
#   "NO3", 4585L, 2250L,
#   "NOx", 273L, 72L,
#   "PO4", 5320L, 1475L,
#   "SRP", 211L, 26L,
#   "DSi", 4366L, 0L,
#   "TN", 7268L, 24L,
#   "TDP", 8105L, 2018L
# )
#
# if (
#   !setequal(canada_sensitive_review, canada_sensitive_expected)
# ) {
#   stop(
#     "the reviewed Canada target corrections changed: ",
#     paste(
#       paste0(
#         canada_sensitive_review$solute,
#         "=",
#         canada_sensitive_review$rows,
#         "/",
#         canada_sensitive_review$less_than
#       ),
#       collapse = "; "
#     )
#   )
# }
#
# # keep the reported canada tdn value for later outlier review
# canada_lockhart_tdn <- canada_output %>%
#   dplyr::filter(
#     Stream_Name == "LOCKHART RIVER AT OUTLET OF ARTILLERY LAKE",
#     date == as.Date("2000-01-17"),
#     solute == "tdn_mgl",
#     solute_units == "mg N/L"
#   )
# canada_lockhart_tdn_um <- suppressWarnings(
#   as.numeric(canada_lockhart_tdn$amount) / (N_mw / 1000)
# )
#
# if (
#   nrow(canada_lockhart_tdn) != 1L ||
#     grepl("^\\s*[<>]", canada_lockhart_tdn$amount) ||
#     !dplyr::near(
#       suppressWarnings(as.numeric(canada_lockhart_tdn$amount)),
#       204
#     ) ||
#     !isTRUE(all.equal(
#       canada_lockhart_tdn_um,
#       14564.4584377476,
#       tolerance = 1e-12
#     ))
# ) {
#   stop("the reviewed Canada Lockhart TDN row changed")
# }
#
# canada_coppermine_output_review <- canada_output %>%
#   dplyr::filter(
#     Stream_Name == "COPPERMINE RIVER ABOVE COPPER CREEK",
#     date == as.Date("2018-04-09"),
#     solute == "temp_c",
#     solute_units == "C"
#   )
# if (
#   nrow(canada_coppermine_output_review) != 1L ||
#     !dplyr::near(
#       suppressWarnings(as.numeric(canada_coppermine_output_review$amount)),
#       -10
#     )
# ) {
#   stop("the reviewed Canada Coppermine output row changed")
# }
#
# canada_cold_review_keys <- canada_cold_review_source %>%
#   dplyr::transmute(
#     Stream_Name,
#     date = as.Date(substr(source_datetime, 1, 10)),
#     solute = "temp_c",
#     solute_units = "C",
#     expected_value
#   )
# canada_cold_output_review <- canada_output %>%
#   dplyr::inner_join(
#     canada_cold_review_keys,
#     by = c("Stream_Name", "date", "solute", "solute_units")
#   )
#
# if (
#   nrow(canada_cold_output_review) != 4L ||
#     any(grepl("^\\s*[<>]", canada_cold_output_review$amount)) ||
#     any(!dplyr::near(
#       suppressWarnings(as.numeric(canada_cold_output_review$amount)),
#       canada_cold_output_review$expected_value
#     ))
# ) {
#   stop("the reviewed Canada gross cold-temperature output rows changed")
# }
#
# canada_lockhart_tdn_remaining <- canada_output %>%
#   dplyr::filter(
#     Stream_Name == "LOCKHART RIVER AT OUTLET OF ARTILLERY LAKE",
#     date == as.Date("2000-01-17"),
#     solute == "tdn_mgl",
#     solute_units == "mg N/L"
#   )
# canada_low_temperature_rows <- canada_output %>%
#   dplyr::filter(solute == "temp_c", solute_units == "C") %>%
#   dplyr::mutate(
#     temperature = suppressWarnings(as.numeric(
#       sub("^\\s*[<>]\\s*", "", amount)
#     ))
#   ) %>%
#   dplyr::filter(temperature < -0.5)
#
# if (
#   nrow(canada_lockhart_tdn_remaining) != 1L ||
#     nrow(canada_low_temperature_rows) != 57L ||
#     !isTRUE(all.equal(
#       range(canada_low_temperature_rows$temperature),
#       c(-10, -0.51),
#       tolerance = 1e-12
#     )) ||
#     any(canada_output$solute == "NH4") ||
#     nrow(canada_output) != 226083L ||
#     dplyr::n_distinct(
#       canada_output$Stream_Name,
#       canada_output$date,
#       canada_output$solute,
#       canada_output$solute_units
#     ) != 226083L ||
#     sum(grepl("^<", canada_output$amount)) != 11000L ||
#     any(canada_output$source_method_flag ==
#       "soluble reactive SiO2, ECCC", na.rm = TRUE)
# ) {
#   stop("the prepared Canada input changed")
# }
#
# # keep all canada sites and dates until the final site check
# # the final master keeps 11295 total and 2131 gran fallback rows
# if (
#   nrow(canada_output) != 226083L ||
#     sum(canada_output$source_lab_flag ==
#       "below reported limit", na.rm = TRUE) != 11000L ||
#     sum(canada_output$source_lab_flag ==
#       "below-limit results excluded from daily mean", na.rm = TRUE) != 517L ||
#     sum(canada_output$source_method_flag ==
#       "soluble reactive Si, filtered ICP-OES", na.rm = TRUE) != 4366L ||
#     any(canada_output$source_method_flag ==
#       "soluble reactive SiO2, ECCC", na.rm = TRUE) ||
#     sum(canada_output$source_method_flag ==
#       "field pH", na.rm = TRUE) != 11574L ||
#     sum(canada_output$source_method_flag ==
#       "field conductivity", na.rm = TRUE) != 11440L ||
#     sum(canada_output$source_method_flag ==
#       "total alkalinity, ECCC", na.rm = TRUE) != 11668L ||
#     sum(canada_output$source_method_flag ==
#       "Gran alkalinity fallback, ECCC", na.rm = TRUE) != 2242L
# ) {
#   stop(
#     "the prepared Canada lab or method flags changed: rows=",
#     nrow(canada_output),
#     ", below=",
#     sum(canada_output$source_lab_flag == "below reported limit", na.rm = TRUE),
#     ", mixed=",
#     sum(
#       canada_output$source_lab_flag ==
#         "below-limit results excluded from daily mean",
#       na.rm = TRUE
#     ),
#     ", field_ph=",
#     sum(canada_output$source_method_flag == "field pH", na.rm = TRUE),
#     ", field_conductivity=",
#     sum(
#       canada_output$source_method_flag == "field conductivity",
#       na.rm = TRUE
#     ),
#     ", total_alkalinity=",
#     sum(
#       canada_output$source_method_flag == "total alkalinity, ECCC",
#       na.rm = TRUE
#     ),
#     ", gran_alkalinity=",
#     sum(
#       canada_output$source_method_flag == "Gran alkalinity fallback, ECCC",
#       na.rm = TRUE
#     )
#   )
# }
#
# # ---- gems ----
#
# gems_current <- read_long_files(gems_files) %>%
#   dplyr::mutate(solute = dplyr::if_else(solute == "NH4", "NHX", solute))
# gems_provider_path <- file.path(
#   provider_dir,
#   "gems_gemstat",
#   "GFQA_v3.zip"
# )
#
# if (!file.exists(gems_provider_path)) {
#   stop("the reviewed GEMS provider archive is unavailable")
# }
#
# gems_map <- tibble::tribble(
#   ~source_entry, ~parameter_code, ~provider_units, ~variable, ~units,
#   ~conversion, ~priority, ~safe,
#   "Aluminium.csv", "Al-Dis", "mg/l", "Al", "uM", 1000 / Al_mw, 1L, TRUE,
#   "Chloride.csv", "Cl-Dis", "mg/l", "Cl", "uM", 1000 / Cl_mw, 1L, TRUE,
#   "Pigment.csv", "Chl-a", "mg/l", "chlorophyll a", "ug/L", 1000, 1L, TRUE,
#   "Carbon.csv", "DOC", "mg/l", "dissolved org C", "uM", 1000 / C_mw, 1L, TRUE,
#   "Carbon.csv", "TOC", "mg/l", "tot org C", "uM", 1000 / C_mw, 1L, TRUE,
#   "Silicon.csv", "Si-Dis", "mg/l", "DSi", "uM", 1000 / Si_mw, 1L, TRUE,
#   "Silicon.csv", "SiO2-Dis", "mg/l", "DSi", "uM", 1000 / SiO2_mw, 1L, TRUE,
#   "Fluoride.csv", "F-Dis", "mg/l", "F", "uM", 1000 / F_mw, 1L, TRUE,
#   "Iron.csv", "Fe-Dis", "mg/l", "Fe", "uM", 1000 / Fe_mw, 1L, TRUE,
#   "Potassium.csv", "K-Dis", "mg/l", "K", "uM", 1000 / K_mw, 1L, TRUE,
#   "Manganese.csv", "Mn-Dis", "mg/l", "Mn", "uM", 1000 / Mn_mw, 1L, TRUE,
#   "Other_Nitrogen.csv", "NH4N", "mg/l", "NHX", "uM", 1000 / N_mw, 1L, TRUE,
#   "Other_Nitrogen.csv", "TN", "mg/l", "TN", "uM", 1000 / N_mw, 1L, TRUE,
#   "Other_Nitrogen.csv", "TKN", "mg/l", "tot kjeldahl N", "uM", 1000 / N_mw, 1L, TRUE,
#   "Oxidized_Nitrogen.csv", "NO2N", "mg/l", "NO2", "uM", 1000 / N_mw, 1L, TRUE,
#   "Oxidized_Nitrogen.csv", "NO3N", "mg/l", "NO3", "uM", 1000 / N_mw, 1L, TRUE,
#   "Phosphorus.csv", "TP", "mg/l", "TP", "uM", 1000 / P_mw, 1L, TRUE,
#   "Phosphorus.csv", "TDP", "mg/l", "tdp", "uM", 1000 / P_mw, 1L, TRUE,
#   "Water.csv", "TSS", "mg/l", "susp partic matter", "mg/L", 1, 1L, TRUE,
#   "Zinc.csv", "Zn-Dis", "mg/l", "Zn", "uM", 1000 / Zn_mw, 1L, TRUE,
#   "Dissolved_Gas.csv", "O2-Dis", "mg/l", "dissolved oxygen", "uM", 1000 / (2 * O_mw), 1L, FALSE,
#   "Dissolved_Gas.csv", "O2-Dis-Sat", "%", "do percent", "percent", 1, 1L, FALSE,
#   "Temperature.csv", "TEMP", "°C", "temp", "C", 1, 1L, FALSE,
#   "Electrical_Conductance.csv", "EC", "µS/cm", "conductivity", "uS/cm", 1, 1L, FALSE,
#   "pH.csv", "pH", "---", "pH", NA_character_, 1, 1L, FALSE
# )
# shallowest_depth_method_flag <- "shallowest sample depth used"
#
# gems_station_ids <- unique(gems_current$Stream_Name)
#
# gems_source <- lapply(unique(gems_map$source_entry), function(entry) {
#   one_map <- gems_map %>% dplyr::filter(source_entry == entry)
#   source_command <- sprintf(
#     "unzip -p %s %s",
#     shQuote(gems_provider_path),
#     shQuote(entry)
#   )
#   source_rows <- data.table::fread(
#     cmd = source_command,
#     select = c(
#       "GEMS Station Number",
#       "Sample Date",
#       "Sample Time",
#       "Depth",
#       "Parameter Code",
#       "Analysis Method Code",
#       "Value Flags",
#       "Value",
#       "Unit"
#     ),
#     na.strings = "",
#     encoding = "Latin-1",
#     showProgress = FALSE
#   )
#   source_rows[, provider_row_number := seq_len(.N)]
#   source_rows <- source_rows[
#     `GEMS Station Number` %in% gems_station_ids &
#       `Parameter Code` %in% one_map$parameter_code
#   ]
#   source_rows %>%
#     tibble::as_tibble() %>%
#     dplyr::transmute(
#       Stream_Name = `GEMS Station Number`,
#       date = as.Date(`Sample Date`),
#       depth = suppressWarnings(as.numeric(Depth)),
#       parameter_code = `Parameter Code`,
#       value_flag = trimws(`Value Flags`),
#       source_value = suppressWarnings(as.numeric(Value)),
#       source_units = trimws(Unit)
#     ) %>%
#     dplyr::inner_join(one_map, by = "parameter_code")
# }) %>%
#   dplyr::bind_rows() %>%
#   dplyr::filter(
#     source_units == provider_units,
#     is.finite(source_value),
#     !is.na(date)
#   ) %>%
#   dplyr::mutate(censored = !is.na(value_flag) & value_flag == "<")
#
# # the gemstat catalogue defines these as total fractions or different species
# # do not substitute them for the dissolved or standard variables used here
# # source: https://gemstat.org/data-gemstat/data-portal/custom-data-request/
# gems_unsafe_map <- tibble::tribble(
#   ~source_entry, ~parameter_code, ~provider_units, ~variable, ~units,
#   "Aluminium.csv", "Al-Tot", "mg/l", "Al", "uM",
#   "Chloride.csv", "Cl-Tot", "mg/l", "Cl", "uM",
#   "Silicon.csv", "SiO2-Tot", "mg/l", "DSi", "uM",
#   "Fluoride.csv", "F-Tot", "mg/l", "F", "uM",
#   "Iron.csv", "Fe-Tot", "mg/l", "Fe", "uM",
#   "Manganese.csv", "Mn-Tot", "mg/l", "Mn", "uM",
#   "Other_Nitrogen.csv", "TON", "mg/l", "TN", "uM",
#   "Other_Nitrogen.csv", "DKN", "mg/l", "tot kjeldahl N", "uM",
#   "Phosphorus.csv", "TIP", "mg/l", "TP", "uM",
#   "Zinc.csv", "Zn-Tot", "mg/l", "Zn", "uM"
# )
#
# gems_unsafe_source <- lapply(
#   unique(gems_unsafe_map$source_entry),
#   function(entry) {
#     one_map <- gems_unsafe_map %>% dplyr::filter(source_entry == entry)
#     source_rows <- data.table::fread(
#       cmd = sprintf(
#         "unzip -p %s %s",
#         shQuote(gems_provider_path),
#         shQuote(entry)
#       ),
#       select = c(
#         "GEMS Station Number",
#         "Sample Date",
#         "Parameter Code",
#         "Value",
#         "Unit"
#       ),
#       na.strings = "",
#       encoding = "Latin-1",
#       showProgress = FALSE
#     )
#     source_rows[
#       `GEMS Station Number` %in% gems_station_ids &
#         `Parameter Code` %in% one_map$parameter_code
#     ] %>%
#       tibble::as_tibble() %>%
#       dplyr::transmute(
#         Stream_Name = `GEMS Station Number`,
#         date = as.Date(`Sample Date`),
#         parameter_code = `Parameter Code`,
#         source_value = suppressWarnings(as.numeric(Value)),
#         source_units = trimws(Unit)
#       ) %>%
#       dplyr::inner_join(one_map, by = "parameter_code")
#   }
# ) %>%
#   dplyr::bind_rows() %>%
#   dplyr::filter(
#     source_units == provider_units,
#     is.finite(source_value),
#     !is.na(date)
#   )
#
# gems_safe_keys <- gems_source %>%
#   dplyr::filter(safe) %>%
#   dplyr::distinct(Stream_Name, date, variable)
#
# gems_unsafe_keys <- gems_unsafe_source %>%
#   dplyr::distinct(
#     Stream_Name,
#     date,
#     variable,
#     units,
#     parameter_code
#   ) %>%
#   dplyr::anti_join(
#     gems_safe_keys,
#     by = c("Stream_Name", "date", "variable")
#   )
#
# if (
#   nrow(gems_unsafe_keys) != 6637L ||
#     sum(gems_unsafe_keys$parameter_code == "TON") != 441L ||
#     sum(gems_unsafe_keys$parameter_code != "TON") != 6196L
# ) {
#   stop("the reviewed GEMS unsafe fallback keys changed")
# }
#
# gems_current_unsafe <- gems_current %>%
#   dplyr::semi_join(
#     gems_unsafe_keys,
#     by = c(
#       "Stream_Name",
#       "date",
#       "solute" = "variable",
#       "solute_units" = "units"
#     )
#   )
#
# if (nrow(gems_current_unsafe) != 0L) {
#   stop("unsafe GEMS fallback rows were found in the prepared input")
# }
#
# gems_current <- gems_current %>%
#   dplyr::anti_join(
#     gems_unsafe_keys,
#     by = c(
#       "Stream_Name",
#       "date",
#       "solute" = "variable",
#       "solute_units" = "units"
#     )
#   )
#
# if (
#   nrow(gems_source) != 601656L ||
#     sum(gems_source$censored) != 7443L ||
#     sum(gems_source$censored & gems_source$safe) != 4467L ||
#     sum(gems_source$censored & !gems_source$safe) != 2976L
# ) {
#   stop("the reviewed GEMS provider rows changed")
# }
#
# # keep the reported silicon form visible through daily averaging
# gems_silicon_code_conflicts <- gems_source %>%
#   dplyr::filter(safe, variable == "DSi") %>%
#   dplyr::group_by(Stream_Name, date) %>%
#   dplyr::summarize(
#     source_codes = dplyr::n_distinct(parameter_code),
#     .groups = "drop"
#   ) %>%
#   dplyr::filter(source_codes > 1L)
#
# if (nrow(gems_silicon_code_conflicts) > 0L) {
#   stop("more than one GEMS silicon form occurs on the same site-date")
# }
#
# # 2,976 flags occur on ordinary italian field values such as temperature and ph
# # keep their numeric values and record that the flag meaning still needs review
#
# # average only detected values when some samples are below a limit
# # if all samples are below the limit, average the thresholds and keep the sign
# gems_safe_depth_review <- gems_source %>%
#   dplyr::filter(safe) %>%
#   dplyr::group_by(Stream_Name, date, variable) %>%
#   dplyr::filter(priority == min(priority)) %>%
#   dplyr::mutate(
#     depth_order = dplyr::if_else(is.na(depth), Inf, depth),
#     selected_depth = min(depth_order),
#     multiple_sample_depths = dplyr::n_distinct(depth[!is.na(depth)]) > 1L
#   )
# gems_deeper_rows <- gems_safe_depth_review %>%
#   dplyr::filter(depth_order != selected_depth)
# gems_multiple_depth_groups <- gems_safe_depth_review %>%
#   dplyr::filter(multiple_sample_depths) %>%
#   dplyr::distinct(Stream_Name, date, variable)
#
# if (
#   nrow(gems_deeper_rows) != 748L ||
#     nrow(gems_multiple_depth_groups) != 165L
# ) {
#   stop("the reviewed GEMS sample-depth rows changed")
# }
#
# gems_daily <- gems_safe_depth_review %>%
#   dplyr::filter(depth_order == selected_depth) %>%
#   dplyr::mutate(
#     any_censored = any(censored),
#     all_censored = all(censored),
#     mixed = any_censored & !all_censored
#   ) %>%
#   dplyr::filter(all_censored | !censored) %>%
#   dplyr::ungroup() %>%
#   dplyr::group_by(
#     Stream_Name,
#     date,
#     variable,
#     units,
#     any_censored,
#     all_censored,
#     mixed
#   ) %>%
#   dplyr::summarize(
#     value = mean(source_value * conversion),
#     value_qualifier = if (dplyr::first(all_censored)) "<" else NA_character_,
#     source_method_flag = combine_method_flags(
#       dplyr::case_when(
#         dplyr::first(parameter_code) == "SiO2-Dis" ~
#           "reported as SiO2-Dis",
#         dplyr::first(parameter_code) == "Si-Dis" ~
#           "reported as Si-Dis",
#         TRUE ~ NA_character_
#       ),
#       dplyr::if_else(
#         dplyr::first(multiple_sample_depths),
#         shallowest_depth_method_flag,
#         NA_character_
#       )
#     ),
#     .groups = "drop"
#   )
#
# gems_replacements <- gems_daily %>%
#   dplyr::filter(any_censored) %>%
#   dplyr::transmute(
#     LTER = "GEMS",
#     Stream_Name,
#     date,
#     solute = variable,
#     solute_units = units,
#     amount = format_amount(value, value_qualifier),
#     all_censored,
#     mixed
#   )
#
# gems_review <- gems_replacements %>%
#   dplyr::left_join(
#     gems_current %>%
#       dplyr::mutate(current_value = suppressWarnings(as.numeric(amount))) %>%
#       dplyr::select(
#         Stream_Name,
#         date,
#         solute,
#         solute_units,
#         current_value
#     ),
#     by = c("Stream_Name", "date", "solute", "solute_units")
#   )
#
# if (
#   nrow(gems_replacements) != 4299L ||
#     sum(gems_replacements$all_censored) != 4290L ||
#     sum(gems_replacements$mixed) != 9L ||
#     sum(!is.na(gems_review$current_value[gems_review$all_censored])) != 56L ||
#     sum(is.na(gems_review$current_value[gems_review$all_censored])) != 4234L ||
#     any(is.na(gems_review$current_value[gems_review$mixed])) ||
#     any(!dplyr::near(
#       suppressWarnings(as.numeric(gems_review$amount[gems_review$mixed])),
#       gems_review$current_value[gems_review$mixed]
#     ))
# ) {
#   stop(
#     "the reviewed GEMS replacement values changed: replacements=",
#     nrow(gems_replacements),
#     ", all below=", sum(gems_replacements$all_censored),
#     ", mixed=", sum(gems_replacements$mixed),
#     ", matched old all-below=",
#     sum(!is.na(gems_review$current_value[gems_review$all_censored])),
#     ", missing old all-below=",
#     sum(is.na(gems_review$current_value[gems_review$all_censored])),
#     ", missing old mixed=",
#     sum(is.na(gems_review$current_value[gems_review$mixed])),
#     ", changed old mixed=",
#     sum(!dplyr::near(
#       suppressWarnings(as.numeric(gems_review$amount[gems_review$mixed])),
#       gems_review$current_value[gems_review$mixed]
#     ), na.rm = TRUE)
#   )
# }
#
# gems_safe_variables <- unique(gems_map$variable[gems_map$safe])
# gems_safe_output <- gems_daily %>%
#   dplyr::transmute(
#     LTER = "GEMS",
#     Stream_Name,
#     date,
#     solute = variable,
#     solute_units = units,
#     amount = format_amount(value, value_qualifier),
#     source_method_flag
#   )
# gems_non_safe_current <- gems_current %>%
#   dplyr::filter(!solute %in% gems_safe_variables)
# gems_physical_depth_review <- gems_source %>%
#   dplyr::filter(!safe) %>%
#   dplyr::group_by(
#     Stream_Name,
#     date,
#     variable,
#     units
#   ) %>%
#   dplyr::mutate(
#     depth_order = dplyr::if_else(is.na(depth), Inf, depth),
#     selected_depth = min(depth_order),
#     multiple_sample_depths = dplyr::n_distinct(depth[!is.na(depth)]) > 1L
#   )
# gems_physical_multiple_depth_groups <- gems_physical_depth_review %>%
#   dplyr::filter(multiple_sample_depths) %>%
#   dplyr::distinct(Stream_Name, date, variable, units)
# gems_physical_deeper_rows <- gems_physical_depth_review %>%
#   dplyr::filter(depth_order != selected_depth)
#
# if (
#   nrow(gems_physical_multiple_depth_groups) != 105L ||
#     nrow(gems_physical_deeper_rows) != 487L
# ) {
#   stop("the reviewed GEMS physical sample-depth rows changed")
# }
#
# gems_physical_flagged_output <- gems_physical_depth_review %>%
#   dplyr::filter(depth_order == selected_depth) %>%
#   dplyr::filter(any(censored)) %>%
#   dplyr::summarize(
#     amount = format_amount(mean(source_value * conversion)),
#     source_lab_flag = "provider < flag; meaning not confirmed",
#     source_method_flag = dplyr::if_else(
#       dplyr::first(multiple_sample_depths),
#       shallowest_depth_method_flag,
#       NA_character_
#     ),
#     .groups = "drop"
#   ) %>%
#   dplyr::transmute(
#     LTER = "GEMS",
#     Stream_Name,
#     date,
#     solute = variable,
#     solute_units = units,
#     amount,
#     source_lab_flag,
#     source_method_flag
#   )
# gems_physical_depth_flags <- gems_physical_multiple_depth_groups %>%
#   dplyr::transmute(
#     Stream_Name,
#     date,
#     solute = variable,
#     source_method_flag = shallowest_depth_method_flag
#   )
# if (anyDuplicated(gems_physical_depth_flags[c(
#   "Stream_Name", "date", "solute"
# )]) > 0L) {
#   stop("duplicate GEMS physical sample-depth flags remain")
# }
# gems_non_safe_current <- gems_non_safe_current %>%
#   dplyr::left_join(
#     gems_physical_depth_flags,
#     by = c("Stream_Name", "date", "solute")
#   )
# gems_non_safe_output <- gems_non_safe_current %>%
#   dplyr::anti_join(
#     gems_physical_flagged_output,
#     by = c("Stream_Name", "date", "solute", "solute_units")
#   ) %>%
#   dplyr::bind_rows(gems_physical_flagged_output) %>%
#   dplyr::arrange(Stream_Name, date, solute, solute_units)
#
# if (
#   nrow(gems_safe_output) != 334647L ||
#     nrow(gems_non_safe_current) != 527443L ||
#     nrow(gems_non_safe_output) != 530355L ||
#     sum(gems_non_safe_output$source_lab_flag ==
#       "provider < flag; meaning not confirmed", na.rm = TRUE) != 2912L ||
#     sum(has_method_flag(
#       gems_non_safe_output$source_method_flag,
#       shallowest_depth_method_flag
#     )) != 105L ||
#     dplyr::n_distinct(
#       gems_non_safe_output$Stream_Name,
#       gems_non_safe_output$date,
#       gems_non_safe_output$solute,
#       gems_non_safe_output$solute_units
#     ) != 530355L
# ) {
#   stop(
#     "the reviewed GEMS safe or retained physical rows changed: safe=",
#     nrow(gems_safe_output),
#     ", current physical=", nrow(gems_non_safe_current),
#     ", flagged physical=", nrow(gems_physical_flagged_output),
#     ", final physical=", nrow(gems_non_safe_output),
#     ", physical flags=", sum(
#       gems_non_safe_output$source_lab_flag ==
#         "provider < flag; meaning not confirmed",
#       na.rm = TRUE
#     ),
#     ", depth flags=", sum(has_method_flag(
#       gems_non_safe_output$source_method_flag,
#       shallowest_depth_method_flag
#     )),
#     ", final physical keys=", dplyr::n_distinct(
#       gems_non_safe_output$Stream_Name,
#       gems_non_safe_output$date,
#       gems_non_safe_output$solute,
#       gems_non_safe_output$solute_units
#     )
#   )
# }
#
# gems_output <- dplyr::bind_rows(
#   gems_non_safe_output,
#   gems_safe_output
# ) %>%
#   dplyr::arrange(Stream_Name, date, solute, solute_units)
#
# # check that the retained records match the original source values
# gems_suspect_rows <- tibble::tribble(
#   ~source_entry, ~provider_row_number, ~Stream_Name, ~date, ~parameter_code,
#   ~source_value, ~source_units, ~variable, ~units, ~conversion,
#   "Phosphorus.csv", 562348L, "IND02355", "1995-01-05", "TP",
#   90, "mg/l", "TP", "uM", 1000 / P_mw,
#   "Phosphorus.csv", 574788L, "IND02427", "2007-06-01", "TP",
#   9.48, "mg/l", "TP", "uM", 1000 / P_mw,
#   "Oxidized_Nitrogen.csv", 696171L, "IND02268", "2013-02-01", "NOxN",
#   355.014, "mg/l", "NOx", "uM", 1000 / N_mw,
#   "Phosphorus.csv", 527161L, "IND02160", "1999-02-01", "TP",
#   37, "mg/l", "TP", "uM", 1000 / P_mw,
#   "Iron.csv", 272048L, "IND02263", "1995-01-08", "Fe-Dis",
#   93, "mg/l", "Fe", "uM", 1000 / Fe_mw,
#   "Chloride.csv", 242417L, "IND02242", "1995-10-04", "Cl-Dis",
#   3501, "mg/l", "Cl", "uM", 1000 / Cl_mw,
#   "Potassium.csv", 181920L, "IND02268", "1980-06-02", "K-Dis",
#   262, "mg/l", "K", "uM", 1000 / K_mw,
#   "Other_Nitrogen.csv", 422115L, "IND02197", "1992-07-01", "NH4N",
#   14.04, "mg/l", "NHX", "uM", 1000 / N_mw,
#   "Fluoride.csv", 98048L, "IND02355", "1987-01-04", "F-Dis",
#   95, "mg/l", "F", "uM", 1000 / F_mw,
#   "Fluoride.csv", 73494L, "IND02208", "1992-03-02", "F-Dis",
#   48, "mg/l", "F", "uM", 1000 / F_mw,
#   "Fluoride.csv", 96669L, "IND02341", "1995-02-01", "F-Dis",
#   92, "mg/l", "F", "uM", 1000 / F_mw,
#   "Aluminium.csv", 151324L, "IND02295", "1999-06-21", "Al-Dis",
#   25, "mg/l", "Al", "uM", 1000 / Al_mw,
#   "Oxidized_Nitrogen.csv", 634953L, "IND02125", "2007-04-02", "NOxN",
#   322.511, "mg/l", "NOx", "uM", 1000 / N_mw,
#   "Oxidized_Nitrogen.csv", 732383L, "IND02355", "2011-02-05", "NOxN",
#   394, "mg/l", "NOx", "uM", 1000 / N_mw,
#   "Potassium.csv", 161840L, "IND02184", "1996-04-01", "K-Dis",
#   195.5, "mg/l", "K", "uM", 1000 / K_mw
# ) %>%
#   dplyr::mutate(date = as.Date(date))
#
# gems_retained_duplicates <- tibble::tribble(
#   ~source_entry, ~provider_row_number, ~Stream_Name, ~date, ~parameter_code,
#   ~source_value, ~source_units, ~variable, ~units, ~conversion,
#   "Phosphorus.csv", 562347L, "IND02355", "1995-01-05", "TP",
#   0.001, "mg/l", "TP", "uM", 1000 / P_mw,
#   "Fluoride.csv", 98049L, "IND02355", "1987-01-04", "F-Dis",
#   0.44, "mg/l", "F", "uM", 1000 / F_mw,
#   "Oxidized_Nitrogen.csv", 732382L, "IND02355", "2011-02-05", "NOxN",
#   1.958, "mg/l", "NOx", "uM", 1000 / N_mw
# ) %>%
#   dplyr::mutate(date = as.Date(date))
#
# gems_review_rows <- dplyr::bind_rows(
#   gems_suspect_rows %>%
#     dplyr::mutate(review_action = "retain_source_value"),
#   gems_retained_duplicates %>%
#     dplyr::mutate(review_action = "retain_source_value")
# )
#
# read_gems_review_rows <- function(entry) {
#   expected <- gems_review_rows %>% dplyr::filter(source_entry == entry)
#   source_rows <- data.table::fread(
#     cmd = sprintf(
#       "unzip -p %s %s",
#       shQuote(gems_provider_path),
#       shQuote(entry)
#     ),
#     select = c(
#       "GEMS Station Number",
#       "Sample Date",
#       "Parameter Code",
#       "Value",
#       "Unit"
#     ),
#     colClasses = "character",
#     na.strings = "",
#     encoding = "Latin-1",
#     showProgress = FALSE
#   )
#   source_rows[, provider_row_number := seq_len(.N)]
#   source_rows[provider_row_number %in% expected$provider_row_number] %>%
#     tibble::as_tibble() %>%
#     dplyr::transmute(
#       source_entry = entry,
#       provider_row_number,
#       Stream_Name = `GEMS Station Number`,
#       date = as.Date(`Sample Date`),
#       parameter_code = `Parameter Code`,
#       source_value = suppressWarnings(as.numeric(Value)),
#       source_units = trimws(Unit)
#     )
# }
#
# observed_gems_review_rows <- lapply(
#   unique(gems_review_rows$source_entry),
#   read_gems_review_rows
# ) %>%
#   dplyr::bind_rows()
#
# review_identity <- c(
#   "source_entry",
#   "provider_row_number",
#   "Stream_Name",
#   "date",
#   "parameter_code",
#   "source_units"
# )
# matched_gems_review_rows <- gems_review_rows %>%
#   dplyr::inner_join(observed_gems_review_rows, by = review_identity)
#
# if (
#   nrow(gems_suspect_rows) != 15L ||
#     nrow(gems_retained_duplicates) != 3L ||
#     nrow(observed_gems_review_rows) != 18L ||
#     nrow(matched_gems_review_rows) != 18L ||
#     any(!dplyr::near(
#       matched_gems_review_rows$source_value.x,
#       matched_gems_review_rows$source_value.y
#     ))
# ) {
#   stop("the reviewed GEMS suspect or retained provider rows changed")
# }
#
# gems_quality_keys <- gems_suspect_rows %>%
#   dplyr::transmute(
#     Stream_Name,
#     date,
#     solute = variable,
#     solute_units = units
#   )
#
# gems_quality_current <- gems_output %>%
#   dplyr::semi_join(
#     gems_quality_keys,
#     by = c("Stream_Name", "date", "solute", "solute_units")
#   )
#
# if (
#   nrow(gems_quality_current) != 15L ||
#     dplyr::n_distinct(
#       gems_quality_current$Stream_Name,
#       gems_quality_current$date,
#       gems_quality_current$solute,
#       gems_quality_current$solute_units
#     ) != 15L
# ) {
#   stop("the reviewed GEMS suspect daily rows changed")
# }
#
# gems_quality_expected <- dplyr::bind_rows(
#   gems_suspect_rows,
#   gems_retained_duplicates
# ) %>%
#   dplyr::group_by(Stream_Name, date, variable, units) %>%
#   dplyr::summarize(
#     expected_amount = mean(source_value * conversion),
#     .groups = "drop"
#   )
# gems_quality_retained <- gems_quality_current %>%
#   dplyr::inner_join(
#     gems_quality_expected,
#     by = c(
#       "Stream_Name",
#       "date",
#       "solute" = "variable",
#       "solute_units" = "units"
#     )
#   )
# gems_quality_mismatch <- !dplyr::near(
#   suppressWarnings(as.numeric(gems_quality_retained$amount)),
#   gems_quality_retained$expected_amount
# )
# if (
#   nrow(gems_quality_retained) != 15L ||
#     any(gems_quality_mismatch)
# ) {
#   mismatch_rows <- gems_quality_retained[gems_quality_mismatch, ]
#   stop(
#     "the retained GEMS reviewed daily values changed: ",
#     paste(
#       mismatch_rows$Stream_Name,
#       mismatch_rows$date,
#       mismatch_rows$solute,
#       mismatch_rows$amount,
#       mismatch_rows$expected_amount,
#       sep = "|",
#       collapse = ";"
#     )
#   )
# }
#
# if (
#   nrow(gems_output) != 865002L ||
#     dplyr::n_distinct(
#       gems_output$Stream_Name,
#       gems_output$date,
#       gems_output$solute,
#       gems_output$solute_units
#     ) != 865002L ||
#     sum(grepl("^<", gems_output$amount)) != 4290L ||
#     nrow(dplyr::filter(gems_output, solute == "NHX")) != 26668L ||
#     sum(grepl("^<", dplyr::filter(gems_output, solute == "NHX")$amount)) !=
#       494L ||
#     any(gems_output$solute == "NH4")
# ) {
#   stop(
#     "the prepared GEMS input changed: rows=", nrow(gems_output),
#     ", keys=", dplyr::n_distinct(
#       gems_output$Stream_Name,
#       gems_output$date,
#       gems_output$solute,
#       gems_output$solute_units
#     ),
#     ", less-than=", sum(grepl("^<", gems_output$amount)),
#     ", NHX=", nrow(dplyr::filter(gems_output, solute == "NHX")),
#     ", NHX less-than=", sum(grepl(
#       "^<",
#       dplyr::filter(gems_output, solute == "NHX")$amount
#     )),
#     ", NH4=", sum(gems_output$solute == "NH4")
#   )
# }
#
# gems_source_flags <- gems_daily %>%
#   dplyr::transmute(
#     Stream_Name,
#     date,
#     solute = variable,
#     solute_units = units,
#     source_safe_lab_flag = dplyr::case_when(
#       mixed ~ "below-limit results excluded from daily mean",
#       all_censored ~ "below reported limit",
#       TRUE ~ NA_character_
#     )
#   ) %>%
#   dplyr::distinct()
#
# gems_output <- gems_output %>%
#   dplyr::left_join(
#     gems_source_flags,
#     by = c("Stream_Name", "date", "solute", "solute_units")
#   ) %>%
#   dplyr::mutate(
#     source_lab_flag = dplyr::coalesce(
#       source_lab_flag,
#       source_safe_lab_flag
#     )
#   ) %>%
#   dplyr::select(-source_safe_lab_flag) %>%
#   dplyr::relocate(source_lab_flag, source_method_flag, .before = date)
#
# if (
#   nrow(gems_output) != 865002L ||
#     sum(gems_output$source_lab_flag ==
#       "below reported limit", na.rm = TRUE) != 4290L ||
#     sum(gems_output$source_lab_flag ==
#       "below-limit results excluded from daily mean", na.rm = TRUE) != 9L ||
#     sum(gems_output$source_lab_flag ==
#       "provider < flag; meaning not confirmed", na.rm = TRUE) != 2912L ||
#     sum(has_method_flag(
#       gems_output$source_method_flag,
#       "reported as SiO2-Dis"
#     )) != 39106L ||
#     sum(has_method_flag(
#       gems_output$source_method_flag,
#       "reported as Si-Dis"
#     )) != 161L ||
#     sum(has_method_flag(
#       gems_output$source_method_flag,
#       shallowest_depth_method_flag
#     )) != 270L
# ) {
#   stop("the prepared GEMS lab or method flags changed")
# }
#
# # ---- danube ----
#
# danube_provider_path <- file.path(
#   provider_dir,
#   "danube_icpdr",
#   "danube_wq_v1.csv"
# )
#
# if (!file.exists(danube_provider_path)) {
#   stop("the reviewed Danube provider file is unavailable")
# }
#
# danube_map <- tibble::tribble(
#   ~determinand_code, ~variable, ~conversion, ~priority,
#   "2.1.10", "NH4", 1000 / N_mw, 1L,
#   "2.1.20", "NO2", 1000 / N_mw, 1L,
#   "2.1.30", "NO3", 1000 / N_mw, 1L,
#   "2.1.40", "tot kjeldahl N", 1000 / N_mw, 1L,
#   "2.1.50", "TN", 1000 / N_mw, 1L,
#   "2.2.10", "PO4", 1000 / P_mw, 1L,
#   "2.2.20", "TP", 1000 / P_mw, 1L,
#   "2.2.21", "tdp", 1000 / P_mw, 1L,
#   # keep only the silica variable explicitly called dissolved in the original source
#   "2.3.11", "DSi", 1000 / SiO2_mw, 1L,
#   "3.1.10", "Na", 1000 / Na_mw, 1L,
#   "3.1.20", "K", 1000 / K_mw, 1L,
#   "3.1.30", "Ca", 1000 / Ca_mw, 1L,
#   "3.1.40", "Mg", 1000 / Mg_mw, 1L,
#   "3.2.10", "Cl", 1000 / Cl_mw, 1L,
#   "3.2.20", "SO4", 1000 / SO4_mw, 1L
# )
#
# danube_provider <- data.table::fread(
#   danube_provider_path,
#   select = c(
#     "id",
#     "value",
#     "value_calc",
#     "station_code",
#     "date_of_sampling",
#     "analytical_method_code",
#     "unit",
#     "detection_limit",
#     "determinand_code"
#   ),
#   na.strings = "",
#   showProgress = FALSE
# ) %>%
#   tibble::as_tibble() %>%
#   dplyr::transmute(
#     Stream_Name = station_code,
#     date = as.Date(substr(date_of_sampling, 1, 10)),
#     determinand_code,
#     source_value = suppressWarnings(as.numeric(value)),
#     value_calc = suppressWarnings(as.numeric(value_calc)),
#     detection_limit = suppressWarnings(as.numeric(detection_limit)),
#     source_units = trimws(unit)
#   )
#
# danube_source <- danube_provider %>%
#   dplyr::inner_join(danube_map, by = "determinand_code") %>%
#   dplyr::filter(
#     source_units == "mg/l",
#     is.finite(source_value),
#     is.finite(value_calc),
#     is.finite(detection_limit),
#     !is.na(date)
#   ) %>%
#   dplyr::mutate(censored = source_value < detection_limit) %>%
#   dplyr::group_by(Stream_Name, date, variable) %>%
#   dplyr::filter(priority == min(priority)) %>%
#   dplyr::ungroup()
#
# # average only detected values when some samples are below a limit
# # if all samples are below the limit, average the thresholds and keep the sign
# danube_daily <- danube_source %>%
#   dplyr::group_by(Stream_Name, date, variable) %>%
#   dplyr::summarize(
#     any_censored = any(censored),
#     all_censored = all(censored),
#     mixed = any_censored & !all_censored,
#     value = if (all_censored) {
#       mean(detection_limit * conversion)
#     } else {
#       mean(value_calc[!censored] * conversion[!censored])
#     },
#     value_qualifier = if (all_censored) "<" else NA_character_,
#     source_method_flag = NA_character_,
#     .groups = "drop"
#   )
#
# # build the prepared file from the original source download
# # do not rely on older generated files for individual sites
# danube_output <- danube_daily %>%
#   dplyr::transmute(
#     LTER = "Danube",
#     Stream_Name,
#     source_lab_flag = dplyr::case_when(
#       mixed ~ "below-limit results excluded from daily mean",
#       all_censored ~ "below reported limit",
#       TRUE ~ NA_character_
#     ),
#     source_method_flag,
#     date,
#     solute = variable,
#     solute_units = "uM",
#     amount = format_amount(value, value_qualifier)
#   ) %>%
#   dplyr::arrange(Stream_Name, date, solute, solute_units)
#
# if (
#   sum(danube_daily$all_censored) != 5496L ||
#     sum(danube_daily$mixed) != 757L ||
#     any(danube_daily$all_censored & danube_daily$mixed) ||
#     nrow(danube_output) != 239963L ||
#     dplyr::n_distinct(
#       danube_output$Stream_Name,
#       danube_output$date,
#       danube_output$solute,
#       danube_output$solute_units
#     ) != 239963L ||
#     sum(grepl("^<", danube_output$amount)) != 5496L ||
#     any(grepl("^>", danube_output$amount)) ||
#     sum(danube_output$source_lab_flag ==
#       "below-limit results excluded from daily mean", na.rm = TRUE) != 757L ||
#     sum(danube_output$source_lab_flag ==
#       "below reported limit", na.rm = TRUE) != 5496L ||
#     any(!is.na(danube_output$source_method_flag))
# ) {
#   stop("the reviewed Danube daily qualifier groups changed")
# }
#
# # ---- keys ----
#
# data_key <- read.csv(data_key_path, na.strings = "")
#
# provider_files <- c(canada_file, gems_file, danube_file)
# required_long_columns <- c(
#   "LTER", "Stream_Name", "date", "solute", "solute_units", "amount"
# )
#
# provider_key_counts <- data_key %>%
#   dplyr::filter(Raw_Filename %in% provider_files) %>%
#   dplyr::count(Raw_Filename, name = "rows")
#
# provider_key_expected <- tibble::tribble(
#   ~Raw_Filename, ~rows,
#   canada_file, 8L,
#   gems_file, 8L,
#   danube_file, 8L
# )
#
# if (
#   nrow(provider_key_counts) != 3L ||
#     !setequal(provider_key_counts, provider_key_expected) ||
#     any(grepl(gems_pattern, data_key$Raw_Filename)) ||
#     any(grepl(danube_pattern, data_key$Raw_Filename))
# ) {
#   stop("the reviewed provider data-key file set changed")
# }
#
# provider_key_columns <- data_key %>%
#   dplyr::filter(Raw_Filename %in% provider_files) %>%
#   dplyr::filter(!Standardized_Column_Name %in% c(
#     "source_lab_flag",
#     "source_method_flag"
#   )) %>%
#   dplyr::group_by(Raw_Filename) %>%
#   dplyr::summarize(
#     columns = paste(sort(Standardized_Column_Name), collapse = "|"),
#     .groups = "drop"
#   )
#
# if (any(provider_key_columns$columns !=
#   paste(sort(required_long_columns), collapse = "|"))) {
#   stop("the reviewed provider data-key columns changed")
# }
#
# unexpected_provider_columns <- data_key %>%
#   dplyr::filter(
#     Raw_Filename %in% provider_files,
#     !Standardized_Column_Name %in% c(
#       required_long_columns,
#       "source_lab_flag",
#       "source_method_flag"
#     )
#   )
#
# if (nrow(unexpected_provider_columns) > 0L) {
#   stop("an unexpected provider data-key column remains")
# }
#
# flag_key_rows <- data_key %>%
#   dplyr::filter(
#     Raw_Filename %in% provider_files,
#     Raw_Column_Name == "source_lab_flag",
#     Standardized_Column_Name == "source_lab_flag"
#   )
#
# method_flag_key_rows <- data_key %>%
#   dplyr::filter(
#     Raw_Filename %in% provider_files,
#     Raw_Column_Name == "source_method_flag",
#     Standardized_Column_Name == "source_method_flag"
#   )
#
# data_key_output <- data_key
#
# if (
#   nrow(flag_key_rows) != 3L ||
#     sum(data_key_output$Raw_Column_Name == "source_lab_flag") != 3L ||
#     nrow(method_flag_key_rows) != 3L ||
#     !setequal(
#       method_flag_key_rows$Raw_Filename,
#       provider_files
#     ) ||
#     sum(
#       data_key_output$Raw_Filename %in% provider_files &
#         data_key_output$Raw_Column_Name == "source_method_flag",
#       na.rm = TRUE
#     ) != 3L
# ) {
#   stop("the provider lab or method flag key rows changed")
# }
#
# # ---- prepared source files ----
#
# data.table::fwrite(
#   canada_output,
#   file.path(raw_output_dir, canada_file),
#   na = ""
# )
# data.table::fwrite(
#   gems_output,
#   file.path(raw_output_dir, gems_file),
#   na = ""
# )
# data.table::fwrite(
#   danube_output,
#   file.path(raw_output_dir, danube_file),
#   na = ""
# )
# ---- end replacement: original-source-records ----

# ---- sidney: reported limit signs ----
# keep documented limits and their signs instead of treating the thresholds as detections
# current file: chemistry-inputs/prepare_qualifier_inputs.R
# ---- begin replacement: reported-limits ----
# # ---- shared helpers ----
#
# helper_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
# if (length(helper_argument) != 1L) {
#   stop("the script folder could not be identified")
# }
# source(file.path(
#   dirname(dirname(normalizePath(sub("^--file=", "", helper_argument)))),
#   "chemistry_functions.R"
# ))
# rm(helper_argument)
#
# # ---- settings ----
#
# input_dir <- required_path("SISYN_CHEM_INPUT_DIR")
# selection_dir <- required_path("SISYN_CHEM_SELECTION_INPUT_DIR")
# output_dir <- required_path("SISYN_CHEM_QUALIFIER_OUTPUT_DIR")
# key_dir <- required_path("SISYN_CHEM_KEY_DIR")
#
# source_files <- c(
#   "Chem_HYBAM.csv",
#   "UK_nonSi_solutes.csv",
#   "UK_Si.csv",
#   "CAMELS_USGS_N_P.csv",
#   "Como_Creek_chem.csv",
#   "USGS_geogenic_solutes.csv",
#   "ElbeRiverChem.csv",
#   "WalkerBranch_Chem.csv",
#   "UMR_si_new_sites.csv",
#   "UMR_si_update_existing_sites.csv",
#   "Krycklan_NP.csv",
#   "WesternAus_AllSites_Filtered_v2.csv"
# )
#
# required_files <- c(
#   file.path(input_dir, source_files),
#   file.path(key_dir, "SiSyn_Data_Key.csv"),
#   file.path(key_dir, "SiSyn_Chem_Units_Key.csv")
# )
#
# check_required_files(required_files)
#
# read_dirs <- c(input_dir, selection_dir, key_dir)
# check_work_folder(output_dir, read_dirs, "qualifier preparation")
# dir.create(output_dir, recursive = TRUE)
#
# # ---- qualifier helpers ----
#
# read_source <- function(filename, directory = input_dir, clean_names = FALSE) {
#   preferred_dir <- if (identical(directory, input_dir)) selection_dir else NULL
#   read_source_csv(filename, directory, clean_names, preferred_dir)
# }
#
# assert_vector <- function(observed, expected, label) {
#   same_names <- identical(names(observed), names(expected))
#   same_values <- identical(as.integer(observed), as.integer(expected))
#
#   if (!same_names || !same_values) {
#     stop(label, " changed")
#   }
# }
#
# assert_unchanged_columns <- function(before, after, changed_columns, filename) {
#   if (!identical(dim(before), dim(after)) || !identical(names(before), names(after))) {
#     stop("row or column structure changed in '", filename, "'")
#   }
#
#   unchanged_columns <- setdiff(names(before), changed_columns)
#   for (column in unchanged_columns) {
#     if (!identical(before[[column]], after[[column]])) {
#       stop("an unapproved column changed in '", filename, "': ", column)
#     }
#   }
# }
#
# count_changed_cells <- function(before, after, columns) {
#   changed <- 0L
#   for (column in columns) {
#     old_values <- before[[column]]
#     new_values <- after[[column]]
#     both_missing <- is.na(old_values) & is.na(new_values)
#     both_present <- !is.na(old_values) & !is.na(new_values)
#     same_value <- both_present & old_values == new_values
#     unchanged <- both_missing | same_value
#     changed <- changed + sum(!unchanged)
#   }
#   changed
# }
#
# prefix_inequality <- function(value, qualifier) {
#   value <- as.character(value)
#   qualifier <- trimws(as.character(qualifier))
#   has_existing <- !is.na(value) & grepl("^\\s*[<>]", value)
#   existing <- rep(NA_character_, length(value))
#   existing[has_existing] <- sub(
#     "^\\s*([<>]).*$",
#     "\\1",
#     value[has_existing]
#   )
#   conflict <- qualifier %in% c("<", ">") &
#     !is.na(existing) & qualifier != existing
#
#   if (any(conflict, na.rm = TRUE)) {
#     stop("a separate qualifier conflicts with a value qualifier")
#   }
#
#   add_qualifier <- qualifier %in% c("<", ">") &
#     !is_blank(value) & !has_existing
#   value[add_qualifier] <- paste0(qualifier[add_qualifier], value[add_qualifier])
#   value
# }
#
# normalize_mdy_fixed50 <- function(x, label) {
#   x <- as.character(x)
#   nonblank <- !is_blank(x)
#   valid_shape <- grepl("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$", x)
#
#   if (any(nonblank & !valid_shape)) {
#     stop(label, " contains a non-mdy date")
#   }
#
#   parts <- strsplit(x[nonblank], "/", fixed = TRUE)
#   month <- integer(length(parts))
#   day <- integer(length(parts))
#   year_short <- integer(length(parts))
#   for (row in seq_along(parts)) {
#     month[row] <- as.integer(parts[[row]][1L])
#     day[row] <- as.integer(parts[[row]][2L])
#     year_short[row] <- as.integer(parts[[row]][3L])
#   }
#   # keep the reviewed cutoff: 50-99 means 1950-1999, 00-49 means 2000-2049
#   year <- ifelse(year_short >= 50L, 1900L + year_short, 2000L + year_short)
#   iso <- sprintf("%04d-%02d-%02d", year, month, day)
#   parsed <- as.Date(iso, format = "%Y-%m-%d")
#
#   if (any(is.na(parsed)) || any(format(parsed, "%Y-%m-%d") != iso)) {
#     stop(label, " contains an invalid calendar date")
#   }
#
#   output <- x
#   output[nonblank] <- iso
#   output
# }
#
# assert_iso_dates <- function(x, expected_count, expected_start, expected_end, label) {
#   nonblank <- !is_blank(x)
#   values <- x[nonblank]
#
#   assert_count(sum(nonblank), expected_count, paste0(label, " nonblank dates"))
#   assert_count(
#     sum(!grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", values)),
#     0L,
#     paste0(label, " non-ISO dates")
#   )
#   assert_count(
#     sum(grepl("/[0-9]{2}$", values)),
#     0L,
#     paste0(label, " two-digit-year dates")
#   )
#
#   if (!identical(min(values), expected_start) ||
#       !identical(max(values), expected_end)) {
#     stop(label, " date range changed")
#   }
# }
#
# write_prepared <- function(filename, before, after, changed_columns, changed_count) {
#   assert_unchanged_columns(before, after, changed_columns, filename)
#   assert_count(
#     count_changed_cells(before, after, changed_columns),
#     changed_count,
#     paste0(filename, " changed-cell count")
#   )
#
#   write_prepared_csv(filename, after, output_dir)
# }
#
# # ---- preserve reported limit signs ----
#
# prepare_hybam <- function() {
#   filename <- "Chem_HYBAM.csv"
#   before <- read_source(filename)
#   before$source_method_flag <- NA_character_
#   after <- before
#   suspect_chloride <-
#     before[[1L]] == "83980" &
#     before$id_station == "17730000" &
#     before$nom == "Itaituba" &
#     before$date == "3/10/14" &
#     before$valeur == "6061" &
#     before$code == "NA" &
#     before$solute == "Cl"
#   source_code <- trimws(after$code)
#   blank_value <- is_blank(after$valeur)
#   numeric_limit <- grepl(
#     "^<\\s*(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)$",
#     source_code
#   )
#   numeric_rows <- which(blank_value & numeric_limit)
#   special_rows <- which(
#     blank_value & source_code == "<50ppb" & after$solute == "NO3"
#   )
#   bare_rows <- which(
#     blank_value & source_code == "<" &
#       after$id_station == "14420000" &
#       after$nom == "Serrinha" &
#       after$date == "7/5/96" &
#       after$solute == "HCO3"
#   )
#
#   assert_count(length(numeric_rows), 3895L, "HYBAM numeric limits")
#   assert_count(length(special_rows), 1L, "HYBAM nitrate ppb limit")
#   assert_count(length(bare_rows), 1L, "HYBAM unresolved bare limit")
#   assert_count(sum(blank_value & source_code == "<", na.rm = TRUE), 1L,
#                "HYBAM bare-limit total")
#   assert_count(sum(grepl("^\\s*[<>]", before$valeur), na.rm = TRUE), 0L,
#                "HYBAM existing value qualifiers")
#   assert_count(sum(after$solute == "Si", na.rm = TRUE), 2364L,
#                "HYBAM silica labels")
#   assert_count(sum(after$solute == "DSi", na.rm = TRUE), 0L,
#                "HYBAM existing DSi labels")
#   assert_count(sum(suspect_chloride, na.rm = TRUE), 1L,
#                "HYBAM suspect chloride row")
#
#   # keep the reported value for later outlier review
#   if (!identical(after$valeur[suspect_chloride], "6061")) {
#     stop("HYBAM reviewed chloride value changed")
#   }
#   after$valeur[numeric_rows] <- source_code[numeric_rows]
#   after$valeur[special_rows] <- "<0.05"
#   # exclude the bare qualifier from the working input because no threshold is given
#   after$valeur[bare_rows] <- NA_character_
#   after$solute[after$solute == "Si"] <- "DSi"
#   # hybam filters geochemistry samples before major-element analysis
#   # the source reports si measured by colorimetry or icp-oes
#   after$source_method_flag[after$solute == "DSi"] <-
#     "dissolved Si, 0.22 um filtered"
#   after$date <- normalize_mdy_fixed50(after$date, "HYBAM")
#
#   assert_count(sum(grepl("^\\s*<", after$valeur), na.rm = TRUE), 3896L,
#                "HYBAM prepared limits")
#   assert_count(sum(after$solute == "DSi", na.rm = TRUE), 2364L,
#                "HYBAM prepared DSi labels")
#   assert_count(
#     sum(after$source_method_flag == "dissolved Si, 0.22 um filtered", na.rm = TRUE),
#     2364L,
#     "HYBAM dissolved-silicon method flags"
#   )
#   assert_iso_dates(
#     after$date,
#     101711L,
#     "1994-04-01",
#     "2020-02-10",
#     "HYBAM"
#   )
#   write_prepared(
#     filename,
#     before,
#     after,
#     c("date", "valeur", "solute", "source_method_flag"),
#     110336L
#   )
# }
#
# prepare_uk_nonsi <- function() {
#   filename <- "UK_nonSi_solutes.csv"
#   before <- read_source(filename)
#   after <- before
#   assert_count(sum(trimws(before$Qualifier) == "<", na.rm = TRUE), 8569L,
#                "UK non-silica less-than flags")
#   assert_count(sum(trimws(before$Qualifier) == ">", na.rm = TRUE), 15L,
#                "UK non-silica greater-than flags")
#   assert_count(sum(grepl("^\\s*[<>]", before$concentration), na.rm = TRUE), 0L,
#                "UK non-silica existing value qualifiers")
#
#   after$concentration <- prefix_inequality(
#     after$concentration,
#     after$Qualifier
#   )
#
#   sulfate <- after$solute == "SO4" & after$units == "mg/L"
#   assert_count(sum(sulfate, na.rm = TRUE), 65169L,
#                "UK non-silica sulfate unit rows")
#   after$units[sulfate] <- "mg SO4/L"
#
#   assert_count(sum(grepl("^\\s*<", after$concentration), na.rm = TRUE), 8569L,
#                "UK non-silica prepared less-than values")
#   assert_count(sum(grepl("^\\s*>", after$concentration), na.rm = TRUE), 15L,
#                "UK non-silica prepared greater-than values")
#
#   # choose which sites to include after the full chemistry table is built
#   write_prepared(
#     filename,
#     before,
#     after,
#     c("concentration", "units"),
#     73753L
#   )
# }
#
# prepare_uk_si <- function() {
#   filename <- "UK_Si.csv"
#   value_column <- "Silica..mg.SiO2.l."
#   before <- read_source(filename)
#   after <- before
#
#   assert_count(sum(trimws(before$Qualifier) == "<", na.rm = TRUE), 880L,
#                "UK silica less-than flags")
#   assert_count(sum(trimws(before$Qualifier) == ">", na.rm = TRUE), 0L,
#                "UK silica greater-than flags")
#   assert_count(sum(grepl("^\\s*[<>]", before[[value_column]]), na.rm = TRUE), 0L,
#                "UK silica existing value qualifiers")
#
#   after[[value_column]] <- prefix_inequality(
#     after[[value_column]],
#     after$Qualifier
#   )
#
#   assert_count(sum(grepl("^\\s*<", after[[value_column]]), na.rm = TRUE), 880L,
#                "UK silica prepared limits")
#   write_prepared(filename, before, after, value_column, 880L)
# }
#
# prepare_camels <- function() {
#   filename <- "CAMELS_USGS_N_P.csv"
#   before <- read_source(filename)
#   after <- before
#   result <- suppressWarnings(as.numeric(after$result_va))
#   qualifier <- trimws(after$remark_cd)
#   unknown <- which(qualifier == "<" & !is.na(result) & result <= 0)
#   expected_dates <- c(
#     "1982-11-02", "1983-03-24", "1983-05-17",
#     "1983-08-30", "1983-11-01", "1984-09-04"
#   )
#
#   assert_count(sum(qualifier == "<", na.rm = TRUE), 3963L,
#                "CAMELS less-than flags")
#   assert_count(length(unknown), 6L, "CAMELS unresolved limits")
#
#   if (
#     !all(after$stream[unknown] == "Wild River") ||
#       !all(after$variable[unknown] == "SRP") ||
#       !identical(sort(after$sample_dt[unknown]), sort(expected_dates))
#   ) {
#     stop("the reviewed CAMELS unresolved-limit rows changed")
#   }
#
#   # exclude qualifiers without positive thresholds from the working input
#   after$result_va[unknown] <- NA_character_
#   after$result_va <- prefix_inequality(after$result_va, after$remark_cd)
#
#   orthophosphate <- after$parm_cd == "00671" & after$variable == "SRP"
#   assert_count(sum(orthophosphate, na.rm = TRUE), 4895L,
#                "CAMELS orthophosphate rows")
#   after$variable[orthophosphate] <- "PO4"
#
#   assert_count(sum(grepl("^\\s*<", after$result_va), na.rm = TRUE), 3957L,
#                "CAMELS prepared limits")
#   write_prepared(
#     filename,
#     before,
#     after,
#     c("result_va", "variable"),
#     8858L
#   )
# }
#
# prepare_como <- function() {
#   filename <- "Como_Creek_chem.csv"
#   before <- read_source(filename)
#   after <- before
#   below_detection <- trimws(after$belowDetectionQF) == "BDL"
#   unit_map <- c(
#     "Ca" = "mg/L",
#     "Cl" = "mg/L",
#     "DIC" = "mg C/L",
#     "DOC" = "mg C/L",
#     "K" = "mg/L",
#     "Mg" = "mg/L",
#     "Na" = "mg/L",
#     "NH4 - N" = "mg NH4-N/L",
#     "NO3+NO2 - N" = "mg NO3-N/L",
#     "Ortho - P" = "mg PO4-P/L",
#     "Si" = "mg Si/L",
#     "SO4" = "mg SO4/L",
#     "specificConductance" = "uS/cm",
#     "TDS" = "mg/L",
#     "TN" = "mg N/L",
#     "TOC" = "mg C/L",
#     "TP" = "mg P/L"
#   )
#   expected_rows <- c(
#     "Ca" = 202L,
#     "Cl" = 205L,
#     "DIC" = 209L,
#     "DOC" = 209L,
#     "K" = 211L,
#     "Mg" = 180L,
#     "Na" = 202L,
#     "NH4 - N" = 215L,
#     "NO3+NO2 - N" = 207L,
#     "Ortho - P" = 218L,
#     "Si" = 210L,
#     "SO4" = 211L,
#     "specificConductance" = 107L,
#     "TDS" = 184L,
#     "TN" = 206L,
#     "TOC" = 209L,
#     "TP" = 227L
#   )
#   expected_blank_units <- c(
#     "Ca" = 0L,
#     "Cl" = 32L,
#     "DIC" = 0L,
#     "DOC" = 0L,
#     "K" = 0L,
#     "Mg" = 0L,
#     "Na" = 0L,
#     "NH4 - N" = 0L,
#     "NO3+NO2 - N" = 0L,
#     "Ortho - P" = 0L,
#     "Si" = 0L,
#     "SO4" = 32L,
#     "specificConductance" = 0L,
#     "TDS" = 3L,
#     "TN" = 0L,
#     "TOC" = 0L,
#     "TP" = 0L
#   )
#   source_unit_map <- stats::setNames(
#     rep("milligramsPerLiter", length(unit_map)),
#     names(unit_map)
#   )
#   source_unit_map[["specificConductance"]] <-
#     "microsiemensPerCentimeter"
#   expected_fill_rows <- c("Cl" = 32L, "TDS" = 3L)
#   expected_generic_fill_rows <- c("Cl" = 173L, "TDS" = 181L)
#   reviewed <- after$analyte %in% names(unit_map)
#   fill_units <- after$analyte %in% names(expected_fill_rows) &
#     is_blank(after$analyteUnits)
#   observed_rows <- vapply(
#     names(unit_map),
#     function(analyte) sum(after$analyte == analyte, na.rm = TRUE),
#     integer(1)
#   )
#   observed_blank_units <- vapply(
#     names(unit_map),
#     function(analyte) {
#       sum(after$analyte == analyte & is_blank(after$analyteUnits), na.rm = TRUE)
#     },
#     integer(1)
#   )
#   observed_generic_units <- vapply(
#     names(unit_map),
#     function(analyte) {
#       sum(
#         after$analyte == analyte &
#           after$analyteUnits == source_unit_map[[analyte]],
#         na.rm = TRUE
#       )
#     },
#     integer(1)
#   )
#
#   assert_count(sum(below_detection, na.rm = TRUE), 201L,
#                "Como Creek below-detection flags")
#   assert_count(sum(below_detection & is_blank(after$analyteConcentration)), 0L,
#                "Como Creek blank limits")
#   assert_vector(observed_rows, expected_rows,
#                 "Como Creek reviewed analyte rows")
#   assert_vector(observed_blank_units, expected_blank_units,
#                 "Como Creek blank reviewed units")
#   assert_vector(
#     observed_generic_units,
#     expected_rows - expected_blank_units,
#     "Como Creek generic reviewed units"
#   )
#   observed_fill_rows <- vapply(
#     names(expected_fill_rows),
#     function(analyte) {
#       sum(after$analyte == analyte & is_blank(after$analyteUnits), na.rm = TRUE)
#     },
#     integer(1)
#   )
#   observed_generic_fill_rows <- vapply(
#     names(expected_fill_rows),
#     function(analyte) {
#       sum(
#         after$analyte == analyte &
#           after$analyteUnits == "milligramsPerLiter",
#         na.rm = TRUE
#       )
#     },
#     integer(1)
#   )
#   assert_vector(observed_fill_rows, expected_fill_rows,
#                 "Como Creek blank chloride and TDS units")
#   assert_vector(
#     observed_generic_fill_rows,
#     expected_generic_fill_rows,
#     "Como Creek generic chloride and TDS units"
#   )
#
#   after$analyteConcentration <- prefix_inequality(
#     after$analyteConcentration,
#     ifelse(below_detection, "<", NA_character_)
#   )
#   after$analyteUnits[reviewed] <- unname(unit_map[after$analyte[reviewed]])
#   values_before_unit_fill <- after$analyteConcentration
#   after$analyteUnits[fill_units] <- "mg/L"
#
#   if (!identical(after$analyteConcentration, values_before_unit_fill)) {
#     stop("Como Creek unit filling changed measurement values")
#   }
#
#   assert_count(
#     sum(grepl("^\\s*<", after$analyteConcentration), na.rm = TRUE),
#     201L,
#     "Como Creek prepared limits"
#   )
#   prepared_unit_rows <- vapply(
#     names(unit_map),
#     function(analyte) {
#       sum(
#         after$analyte == analyte &
#           after$analyteUnits == unit_map[[analyte]],
#         na.rm = TRUE
#       )
#     },
#     integer(1)
#   )
#   assert_vector(prepared_unit_rows, expected_rows,
#                 "Como Creek prepared reviewed units")
#   prepared_fill_rows <- vapply(
#     names(expected_fill_rows),
#     function(analyte) {
#       sum(
#         after$analyte == analyte & after$analyteUnits == "mg/L",
#         na.rm = TRUE
#       )
#     },
#     integer(1)
#   )
#   assert_vector(
#     prepared_fill_rows,
#     expected_rows[names(expected_fill_rows)],
#                 "Como Creek filled chloride and TDS units")
#   assert_count(
#     sum(
#       after$analyte %in% names(expected_fill_rows) &
#         is_blank(after$analyteUnits),
#       na.rm = TRUE
#     ),
#     0L,
#     "Como Creek remaining blank chloride and TDS units"
#   )
#   write_prepared(
#     filename,
#     before,
#     after,
#     c("analyteConcentration", "analyteUnits"),
#     3613L
#   )
# }
#
# prepare_usgs_geogenic <- function() {
#   filename <- "USGS_geogenic_solutes.csv"
#   before <- read_source(filename)
#   after <- before
#   qualifier <- trimws(after$remark)
#   unresolved_r <- which(qualifier == "R")
#
#   assert_count(sum(qualifier == "<", na.rm = TRUE), 532L,
#                "USGS geogenic less-than flags")
#   assert_count(sum(qualifier == ">", na.rm = TRUE), 4L,
#                "USGS geogenic greater-than flags")
#   assert_count(length(unresolved_r), 1L,
#                "USGS geogenic unresolved R flag")
#   assert_count(sum(grepl("^\\s*[<>]", before$value), na.rm = TRUE), 0L,
#                "USGS geogenic existing value qualifiers")
#
#   reviewed_r <- before[unresolved_r, , drop = FALSE]
#   expected_r <- c(
#     USGSGageNumber = "7144100",
#     parameter_code = "00935",
#     date = "2011-09-22",
#     remark = "R",
#     value = "9",
#     solutes = "K"
#   )
#   observed_r <- unlist(reviewed_r[names(expected_r)], use.names = TRUE)
#
#   if (!identical(observed_r, expected_r)) {
#     stop("the reviewed USGS geogenic R-coded potassium row changed")
#   }
#
#   # the usgs samples record identifies this r-coded result as below its limit
#   # preserve the reported 9 mg/l threshold with a less-than qualifier
#   # source: https://api.waterdata.usgs.gov/samples-data/results/narrow
#   after$value[unresolved_r] <- paste0("<", trimws(after$value[unresolved_r]))
#   after$value <- prefix_inequality(after$value, after$remark)
#
#   assert_count(sum(grepl("^\\s*<", after$value), na.rm = TRUE), 533L,
#                "USGS geogenic prepared less-than values")
#   assert_count(sum(grepl("^\\s*>", after$value), na.rm = TRUE), 4L,
#                "USGS geogenic prepared greater-than values")
#   write_prepared(filename, before, after, "value", 537L)
# }
#
# prepare_elbe <- function() {
#   filename <- "ElbeRiverChem.csv"
#   before <- read_source(filename)
#   after <- before
#   measurement_columns <- c(
#     "SiO2", "TN", "NO3", "NO2_NO3", "NO2", "DNH4", "TP", "DIP",
#     "TOC", "DOC", "SO4", "Cl", "K", "Na", "Mg", "Ca", "Alkalinity",
#     "SPM", "SpecCond25C", "DO_mgL", "pH", "Temp_water", "DOSAT"
#   )
#   expected_less_than <- c(
#     SiO2 = 1L, TN = 0L, NO3 = 0L, NO2_NO3 = 0L, NO2 = 33L,
#     DNH4 = 71L, TP = 0L, DIP = 11L, TOC = 0L, DOC = 0L, SO4 = 0L,
#     Cl = 0L, K = 0L, Na = 0L, Mg = 0L, Ca = 0L, Alkalinity = 0L,
#     SPM = 1L, SpecCond25C = 0L, DO_mgL = 0L, pH = 0L,
#     Temp_water = 0L, DOSAT = 0L
#   )
#   observed_less_than <- vapply(
#     measurement_columns,
#     function(column) {
#       sum(trimws(after[[paste0(column, "_vrc")]]) == "<", na.rm = TRUE)
#     },
#     integer(1)
#   )
#
#   assert_vector(observed_less_than, expected_less_than,
#                 "Elbe qualifier fields")
#   assert_count(sum(observed_less_than), 117L, "Elbe qualifier total")
#   # glorich defines ton as total organic nitrogen rather than total nitrogen
#   # keep it unmapped unless the project adds a separate total organic n field
#   assert_count(sum(!is_blank(after$TON)), 13L, "Elbe excluded TON values")
#
#   # glorich stores half the detection limit when the qualifier is less than
#   # restore the full threshold before carrying the qualifier into the master
#   # source: https://doi.org/10.1594/pangaea.902360
#   for (column in measurement_columns) {
#     qualifier <- trimws(after[[paste0(column, "_vrc")]])
#     censored <- !is.na(qualifier) & qualifier == "<"
#     censored_value <- suppressWarnings(as.numeric(after[[column]][censored]))
#
#     if (any(is.na(censored_value))) {
#       stop("Elbe less-than values must be numeric")
#     }
#
#     after[[column]][censored] <- format(
#       censored_value * 2,
#       scientific = FALSE,
#       trim = TRUE
#     )
#     after[[column]] <- prefix_inequality(
#       after[[column]],
#       after[[paste0(column, "_vrc")]]
#     )
#   }
#
#   expected_thresholds <- c(
#     SiO2 = 3.204,
#     NO2 = 0.214,
#     DNH4 = 1.428,
#     DIP = 0.322,
#     SPM = 1
#   )
#   observed_minimums <- vapply(
#     names(expected_thresholds),
#     function(column) {
#       qualifier <- trimws(after[[paste0(column, "_vrc")]])
#       values <- after[[column]][!is.na(qualifier) & qualifier == "<"]
#       min(as.numeric(sub("^<", "", values)))
#     },
#     numeric(1)
#   )
#   if (!isTRUE(all.equal(observed_minimums, expected_thresholds))) {
#     stop("Elbe restored detection thresholds changed")
#   }
#
#   prepared_count <- sum(vapply(
#     measurement_columns,
#     function(column) sum(grepl("^\\s*<", after[[column]]), na.rm = TRUE),
#     integer(1)
#   ))
#   assert_count(prepared_count, 117L, "Elbe prepared limits")
#   write_prepared(filename, before, after, measurement_columns, 117L)
# }
#
# prepare_walker <- function() {
#   filename <- "WalkerBranch_Chem.csv"
#   before <- read_source(filename)
#   after <- before
#   flag_fields <- c(
#     AL_CONC = "AL_CONC_FL",
#     FE_CONC = "FE_CONC_FL",
#     K_CONC = "K_CONC_FL",
#     MN_CONC = "MN_CONC_FL",
#     SR_CONC = "SR_CONC_FL",
#     ZN_CONC = "ZN_CONC_FL"
#   )
#   expected_numeric <- c(
#     AL_CONC = 3161L, ALK = 3386L, FE_CONC = 3181L,
#     K_CONC = 3132L, MN_CONC = 3207L, PH = 3164L, SP_COND = 3479L,
#     SR_CONC = 2719L, TEMP = 3477L, ZN_CONC = 2872L
#   )
#   expected_unknown <- c(
#     AL_CONC = 328L, FE_CONC = 84L, K_CONC = 2L, MN_CONC = 21L,
#     SR_CONC = 0L, ZN_CONC = 386L
#   )
#   expected_v1_positive <- c(
#     AL_CONC = 206L, FE_CONC = 12L, K_CONC = 608L, MN_CONC = 11L,
#     SR_CONC = 34L, ZN_CONC = 74L
#   )
#   expected_v7_positive <- c(
#     AL_CONC = 1447L, FE_CONC = 1865L, K_CONC = 993L, MN_CONC = 1239L,
#     SR_CONC = 2L, ZN_CONC = 1367L
#   )
#   expected_m1 <- c(
#     AL_CONC = 282L, FE_CONC = 262L, K_CONC = 304L, MN_CONC = 236L,
#     SR_CONC = 540L, ZN_CONC = 438L
#   )
#   observed_numeric <- vapply(
#     names(expected_numeric),
#     function(column) sum(!is.na(suppressWarnings(as.numeric(after[[column]])))),
#     integer(1)
#   )
#   observed_unknown <- observed_v1_positive <- observed_v7_positive <-
#     observed_m1 <- observed_m1_numeric <- integer(length(flag_fields))
#   names(observed_unknown) <- names(observed_v1_positive) <-
#     names(observed_v7_positive) <- names(observed_m1) <-
#     names(observed_m1_numeric) <- names(flag_fields)
#
#   for (column in names(flag_fields)) {
#     measurement <- suppressWarnings(as.numeric(after[[column]]))
#     flag <- trimws(after[[flag_fields[[column]]]])
#     observed_unknown[column] <- sum(flag == "V1" & measurement <= 0,
#                                     na.rm = TRUE)
#     observed_v1_positive[column] <- sum(flag == "V1" & measurement > 0,
#                                         na.rm = TRUE)
#     observed_v7_positive[column] <- sum(flag == "V7" & measurement > 0,
#                                         na.rm = TRUE)
#     observed_m1[column] <- sum(flag == "M1", na.rm = TRUE)
#     observed_m1_numeric[column] <- sum(flag == "M1" & !is.na(measurement),
#                                        na.rm = TRUE)
#   }
#
#   assert_vector(observed_numeric, expected_numeric, "Walker numeric values")
#   assert_vector(observed_unknown, expected_unknown, "Walker unresolved limits")
#   assert_vector(observed_v1_positive, expected_v1_positive,
#                 "Walker positive V1 values")
#   assert_vector(observed_v7_positive, expected_v7_positive,
#                 "Walker V7 limits")
#   assert_vector(observed_m1, expected_m1, "Walker M1 flags")
#   assert_vector(
#     observed_m1_numeric,
#     setNames(integer(length(flag_fields)), names(flag_fields)),
#                 "Walker numeric M1 values")
#
#   k_measurement <- suppressWarnings(as.numeric(after$K_CONC))
#   k_flag <- trimws(after$K_CONC_FL)
#   k_date <- as.Date(after$DATE)
#   k_v7_one <- k_flag == "V7" & k_measurement == 1
#   k_v7_two <- k_flag == "V7" & k_measurement == 2
#   k_v7_one_allowed <-
#     (k_date >= as.Date("1997-01-07") & k_date <= as.Date("2009-12-29")) |
#     (after$site_name == "East Fork" &
#        k_date >= as.Date("2010-01-12") &
#        k_date <= as.Date("2010-08-17")) |
#     k_date >= as.Date("2010-08-24")
#   k_v7_two_allowed <-
#     (after$site_name == "West Fork" &
#        k_date >= as.Date("2010-01-05") &
#        k_date <= as.Date("2010-08-17")) |
#     (after$site_name == "East Fork" & k_date == as.Date("2010-01-05"))
#
#   assert_count(sum(k_v7_one, na.rm = TRUE), 960L,
#                "Walker potassium one mg/L limits")
#   assert_count(sum(k_v7_two, na.rm = TRUE), 33L,
#                "Walker potassium two mg/L limits")
#   assert_count(sum(k_flag == "V7" & !(k_v7_one | k_v7_two), na.rm = TRUE),
#                0L, "Walker unexpected potassium limits")
#   assert_count(sum(k_v7_one & !k_v7_one_allowed, na.rm = TRUE), 0L,
#                "Walker potassium one mg/L limit dates")
#   assert_count(sum(k_v7_two & !k_v7_two_allowed, na.rm = TRUE), 0L,
#                "Walker potassium two mg/L limit dates")
#
#   k_unresolved <- which(k_flag == "V1" & !is.na(k_measurement) &
#                           k_measurement <= 0)
#   k_unresolved_keys <- paste(after$site_name[k_unresolved],
#                              after$DATE[k_unresolved], sep = "|")
#   if (!identical(
#     sort(k_unresolved_keys),
#     c("East Fork|1996-06-18", "East Fork|1996-08-20")
#   )) {
#     stop("Walker unresolved potassium records changed")
#   }
#
#   walker_rows <- which(before$site_name == "WALK")
#   east_source_rows <- which(before$site_name == "East Fork")
#   west_source_rows <- which(before$site_name == "West Fork")
#
#   if (anyDuplicated(before$DATE[east_source_rows]) ||
#       anyDuplicated(before$DATE[west_source_rows])) {
#     stop("Walker branch dates are no longer unique")
#   }
#
#   east_rows <- east_source_rows[
#     match(before$DATE[walker_rows], before$DATE[east_source_rows])
#   ]
#   west_rows <- west_source_rows[
#     match(before$DATE[walker_rows], before$DATE[west_source_rows])
#   ]
#   east_discharge <- suppressWarnings(as.numeric(before$EF_DISCHARGE[east_rows]))
#   west_discharge <- suppressWarnings(as.numeric(before$WF_DISCHARGE[west_rows]))
#   expected_walker_numeric <- c(
#     AL_CONC = 1165L, FE_CONC = 1165L, K_CONC = 1158L,
#     MN_CONC = 1165L, SR_CONC = 981L, ZN_CONC = 1032L
#   )
#   expected_walker_limits <- c(
#     AL_CONC = 766L, FE_CONC = 1062L, K_CONC = 587L,
#     MN_CONC = 904L, SR_CONC = 2L, ZN_CONC = 712L
#   )
#   expected_walker_unresolved <- c(
#     AL_CONC = 253L, FE_CONC = 84L, K_CONC = 2L,
#     MN_CONC = 21L, SR_CONC = 0L, ZN_CONC = 265L
#   )
#   expected_walker_v1 <- c(
#     AL_CONC = 138L, FE_CONC = 9L, K_CONC = 446L,
#     MN_CONC = 5L, SR_CONC = 33L, ZN_CONC = 54L
#   )
#
#   for (column in names(flag_fields)) {
#     source_measurement <- suppressWarnings(as.numeric(before[[column]]))
#     walker_measurement <- source_measurement[walker_rows]
#     east_measurement <- source_measurement[east_rows]
#     west_measurement <- source_measurement[west_rows]
#     east_flag <- trimws(before[[flag_fields[[column]]]][east_rows])
#     west_flag <- trimws(before[[flag_fields[[column]]]][west_rows])
#     east_used <- !is.na(east_measurement) & !is.na(east_discharge) &
#       east_discharge > 0
#     west_used <- !is.na(west_measurement) & !is.na(west_discharge) &
#       west_discharge > 0
#     both_used <- east_used & west_used
#     east_only <- east_used & !west_used
#     west_only <- !east_used & west_used
#     derived_measurement <- rep(NA_real_, length(walker_rows))
#
#     derived_measurement[both_used] <-
#       (east_measurement[both_used] * east_discharge[both_used] +
#          west_measurement[both_used] * west_discharge[both_used]) /
#       (east_discharge[both_used] + west_discharge[both_used])
#     derived_measurement[east_only] <- east_measurement[east_only]
#     derived_measurement[west_only] <- west_measurement[west_only]
#
#     walker_numeric <- !is.na(walker_measurement)
#     assert_count(sum(walker_numeric), expected_walker_numeric[[column]],
#                  paste0("Walker derived ", column, " values"))
#     assert_count(sum(walker_numeric & is.na(derived_measurement)), 0L,
#                  paste0("Walker unmatched ", column, " values"))
#
#     mismatch <- walker_numeric &
#       abs(walker_measurement - derived_measurement) > 1e-10
#     assert_count(sum(mismatch, na.rm = TRUE), 0L,
#                  paste0("Walker recalculated ", column, " values"))
#
#     any_v7 <-
#       (east_used & east_flag == "V7") |
#       (west_used & west_flag == "V7")
#     unresolved_v1 <-
#       (east_used & east_flag == "V1" & east_measurement <= 0) |
#       (west_used & west_flag == "V1" & west_measurement <= 0)
#     positive_v1 <-
#       (east_used & east_flag == "V1" & east_measurement > 0) |
#       (west_used & west_flag == "V1" & west_measurement > 0)
#     walker_limit <- walker_numeric & any_v7 & !unresolved_v1
#     walker_unresolved <- walker_numeric & unresolved_v1
#     walker_v1 <- walker_numeric & positive_v1 & !walker_limit & !walker_unresolved
#
#     assert_count(sum(walker_limit, na.rm = TRUE),
#                  expected_walker_limits[[column]],
#                  paste0("Walker derived ", column, " limits"))
#     assert_count(sum(walker_unresolved, na.rm = TRUE),
#                  expected_walker_unresolved[[column]],
#                  paste0("Walker unresolved derived ", column, " values"))
#     assert_count(sum(walker_v1, na.rm = TRUE),
#                  expected_walker_v1[[column]],
#                  paste0("Walker derived V1 ", column, " values"))
#
#     # exclude v1 rows without positive thresholds from the working input
#     after[[column]][walker_rows[walker_unresolved]] <- NA_character_
#     after[[flag_fields[[column]]]][walker_rows[walker_v1]] <- "V1"
#     after[[column]][walker_rows] <- prefix_inequality(
#       after[[column]][walker_rows],
#       ifelse(walker_limit, "<", NA_character_)
#     )
#   }
#
#   for (column in names(flag_fields)) {
#     flag <- trimws(after[[flag_fields[[column]]]])
#     measurement <- suppressWarnings(as.numeric(after[[column]]))
#     unknown <- flag == "V1" & !is.na(measurement) & measurement <= 0
#     # leave unresolved and missing values blank rather than inventing thresholds
#     after[[column]][unknown | flag == "M1"] <- NA_character_
#     after[[column]] <- prefix_inequality(
#       after[[column]],
#       ifelse(flag == "V7", "<", NA_character_)
#     )
#   }
#
#   prepared_count <- sum(vapply(
#     names(flag_fields),
#     function(column) sum(grepl("^\\s*<", after[[column]]), na.rm = TRUE),
#     integer(1)
#   ))
#   assert_count(prepared_count, 10946L, "Walker prepared limits")
#   write_prepared(
#     filename,
#     before,
#     after,
#     c(names(flag_fields), unname(flag_fields)),
#     15139L
#   )
# }
#
# prepare_umr <- function(filename, expected_invalid, expected_limits,
#                         expected_changed) {
#   before <- read_source(filename)
#   after <- before
#   # official umesc qa metadata encodes below-mdl chemistry as a negative mdl
#   # https://www.umesc.usgs.gov/data_library/water_quality/wtr_qf_labflags.html
#   # limit conversion stays restricted to documented chemistry fields
#   # the absolute value is the positive threshold retained with a less-than sign
#   measurement_columns <- c(
#     "SS", "VSS", "SI", "TN", "NOX", "NHX", "TP", "SRP",
#     "CHLcal", "NA.", "K", "CA", "MG", "SO4", "CL"
#   )
#   chl <- suppressWarnings(as.numeric(after$CHLcal))
#   invalid_chl <- !is.na(chl) &
#     after$LOCATCD == "AL02.3M" &
#     after$DATE == "08/10/2022" &
#     after$TIME == "12:41" &
#     chl == -0.68747
#
#   assert_count(sum(invalid_chl, na.rm = TRUE), expected_invalid,
#                paste0(filename, " invalid chlorophyll values"))
#   # keep this unqualified calibration result for later outlier review
#   limit_count <- 0L
#
#   for (column in measurement_columns) {
#     measurement <- suppressWarnings(as.numeric(after[[column]]))
#     below_detection <- !is.na(measurement) & measurement < 0
#     if (column == "CHLcal") {
#       below_detection <- below_detection & !invalid_chl
#     }
#     limit_count <- limit_count + sum(below_detection)
#     after[[column]][below_detection] <- paste0(
#       "<",
#       abs(measurement[below_detection])
#     )
#   }
#
#   assert_count(limit_count, expected_limits,
#                paste0(filename, " documented limits"))
#
#   unresolved <- which(
#     filename == "UMR_si_new_sites.csv" &
#       after$LOCATCD == "BM00.7S" &
#       after$DATE == "12/16/1998" &
#       suppressWarnings(as.numeric(after$SO4)) == 0
#   )
#   expected_unresolved <- if (filename == "UMR_si_new_sites.csv") 1L else 0L
#   assert_count(length(unresolved), expected_unresolved,
#                paste0(filename, " unresolved sulfate values"))
#   # exclude zero sulfate from the working input because no threshold is given
#   after$SO4[unresolved] <- NA_character_
#
#   prepared_count <- sum(vapply(
#     measurement_columns,
#     function(column) sum(grepl("^\\s*<", after[[column]]), na.rm = TRUE),
#     integer(1)
#   ))
#   assert_count(prepared_count, expected_limits,
#                paste0(filename, " prepared limits"))
#   if (expected_invalid == 1L &&
#       !identical(after$CHLcal[invalid_chl], "-0.68747")) {
#     stop("UMR reviewed chlorophyll result changed")
#   }
#   write_prepared(
#     filename,
#     before,
#     after,
#     measurement_columns,
#     expected_changed
#   )
# }
#
# prepare_krycklan <- function() {
#   filename <- "Krycklan_NP.csv"
#   nox_column <- "NO2-N + NO3-N"
#   nh4_column <- "NH4-N"
#   before <- read_source(filename)
#   after <- before
#   normalized_date <- normalize_mdy_fixed50(after$SampleDate, "Krycklan")
#   sample_date <- as.Date(normalized_date, format = "%Y-%m-%d")
#   nh4_lod <- trimws(after[[nh4_column]]) == "<LOD"
#   nox_lod <- trimws(after[[nox_column]]) == "<LOD"
#   known_nh4 <- nh4_lod & !is.na(sample_date) & sample_date >= as.Date("2011-01-01")
#   known_nox <- nox_lod & !is.na(sample_date) & sample_date >= as.Date("2011-01-01")
#   unresolved_nh4 <- nh4_lod & !known_nh4
#   unresolved_nox <- nox_lod & !known_nox
#
#   assert_count(sum(nh4_lod, na.rm = TRUE), 79L, "Krycklan NH4 LOD values")
#   assert_count(sum(nox_lod, na.rm = TRUE), 7L, "Krycklan NOx LOD values")
#   assert_count(sum(known_nh4, na.rm = TRUE), 59L,
#                "Krycklan documented NH4 thresholds")
#   assert_count(sum(known_nox, na.rm = TRUE), 4L,
#                "Krycklan documented NOx thresholds")
#   assert_count(
#     sum(unresolved_nh4, na.rm = TRUE) + sum(unresolved_nox, na.rm = TRUE),
#     23L,
#     "Krycklan unresolved LOD values"
#   )
#
#   after[[nh4_column]][known_nh4] <- "<0.3"
#   after[[nox_column]][known_nox] <- "<0.4"
#   # exclude older lod tokens until their historical thresholds are documented
#   after[[nh4_column]][unresolved_nh4] <- NA_character_
#   after[[nox_column]][unresolved_nox] <- NA_character_
#   after$SampleDate <- normalized_date
#
#   assert_count(
#     sum(grepl("^\\s*<", after[[nh4_column]]), na.rm = TRUE) +
#       sum(grepl("^\\s*<", after[[nox_column]]), na.rm = TRUE),
#     63L,
#     "Krycklan prepared limits"
#   )
#   assert_count(
#     sum(after[[nh4_column]] == "<LOD", na.rm = TRUE) +
#       sum(after[[nox_column]] == "<LOD", na.rm = TRUE),
#     0L,
#     "Krycklan remaining LOD tokens"
#   )
#   assert_iso_dates(
#     after$SampleDate,
#     5925L,
#     "1992-10-22",
#     "2022-11-21",
#     "Krycklan"
#   )
#   write_prepared(
#     filename,
#     before,
#     after,
#     c("SampleDate", nox_column, nh4_column),
#     6011L
#   )
# }
#
# prepare_western_australia <- function() {
#   filename <- "WesternAus_AllSites_Filtered_v2.csv"
#   before <- read_source(filename)
#   before$source_method_flag <- NA_character_
#   after <- before
#   numeric_value <- suppressWarnings(as.numeric(after$Value.Num))
#   signed_rows <- after$Reading.Sign %in% c("<", ">")
#   bad_do <- which(
#     after$Site.Name == "Blackwood River - Darradup" &
#       after$Collect.Date == "2008-01-25" &
#       after$Determinand == "O2-{DO conc} (mg/L)" &
#       suppressWarnings(as.numeric(after$Value.Num)) == 163.7
#   )
#   salinity_conflict <- which(
#     after$Site.Name == "Collie River - Central Collie" &
#       after$Collect.Date == "2000-07-05" &
#       tolower(trimws(after$chemical)) == "salinity"
#   )
#   invalid_ph <- which(
#     tolower(trimws(after$chemical)) == "ph" &
#       !is.na(numeric_value) &
#       (numeric_value < 1 | numeric_value > 12)
#   )
#   high_do <- which(
#     after$Site.Name == "Donnelly River - Strickland" &
#       after$Collect.Date == "2005-09-20" &
#       after$Determinand == "O2-{DO conc} (mg/L)" &
#       numeric_value == 27.5
#   )
#   high_conductivity <- which(
#     after$Site.Name == "Frankland River - Mount Frankland" &
#       after$Collect.Date == "2007-12-04" &
#       after$Determinand == "Cond @ 25 deg C (uS/cm)" &
#       numeric_value == 86848
#   )
#   zero_conductivity <- which(
#     after$Site.Name == "Collie River - South Branch" &
#       after$Collect.Date == "2009-09-01" &
#       after$Determinand == "Cond @ 25 deg C (uS/cm)" &
#       numeric_value == 0
#   )
#   soluble_reactive_si <- grepl(
#     "^SiO2(-Si)? \\(sol react\\)",
#     after$Determinand
#   )
#
#   assert_count(sum(after$Reading.Sign == "<", na.rm = TRUE), 2074L,
#                "Western Australia less-than flags")
#   assert_count(sum(after$Reading.Sign == ">", na.rm = TRUE), 49L,
#                "Western Australia greater-than flags")
#   assert_count(length(bad_do), 1L, "Western Australia invalid oxygen value")
#   assert_count(length(salinity_conflict), 2L,
#                "Western Australia salinity conflict")
#   assert_count(length(invalid_ph), 8L,
#                "Western Australia reviewed pH values")
#   assert_count(length(high_do), 1L,
#                "Western Australia high oxygen value")
#   assert_count(length(high_conductivity), 1L,
#                "Western Australia high conductivity value")
#   assert_count(length(zero_conductivity), 1L,
#                "Western Australia zero conductivity value")
#   assert_count(sum(soluble_reactive_si, na.rm = TRUE), 1056L,
#                "Western Australia soluble-reactive silicon rows")
#
#   expected_ph <- c(
#     "Collie River - South Branch\r2002-08-06\rpH (no units)\r0.79",
#     "Kent River - Styx Junction\r2008-07-01\rpH (no units)\r12.74",
#     "Murray River - Baden Powell\r1987-06-15\rpH (no units)\r13",
#     "Murray River - Baden Powell\r1987-06-18\rpH (no units)\r12.5",
#     "Murray River - Baden Powell\r1987-06-20\rpH (no units)\r12.5",
#     "Murray River - Baden Powell\r1987-06-25\rpH (no units)\r13",
#     "Murray River - Baden Powell\r1987-06-28\rpH (no units)\r13",
#     "Murray River - Baden Powell\r1987-06-30\rpH (no units)\r13"
#   )
#   observed_ph <- do.call(
#     paste,
#     c(
#       after[invalid_ph, c(
#         "Site.Name", "Collect.Date", "Determinand", "Value.Num"
#       )],
#       sep = "\r"
#     )
#   )
#
#   if (!identical(sort(observed_ph), sort(expected_ph))) {
#     stop("the reviewed Western Australia pH rows changed")
#   }
#
#   expected_single_rows <- c(
#     "Donnelly River - Strickland\r2005-09-20\rO2-{DO conc} (mg/L)\r27.5",
#     "Frankland River - Mount Frankland\r2007-12-04\rCond @ 25 deg C (uS/cm)\r86848",
#     "Collie River - South Branch\r2009-09-01\rCond @ 25 deg C (uS/cm)\r0"
#   )
#   single_rows <- c(high_do, high_conductivity, zero_conductivity)
#   observed_single_rows <- do.call(
#     paste,
#     c(
#       after[single_rows, c(
#         "Site.Name", "Collect.Date", "Determinand", "Value.Num"
#       )],
#       sep = "\r"
#     )
#   )
#
#   if (!identical(observed_single_rows, expected_single_rows)) {
#     stop("the reviewed Western Australia oxygen or conductivity rows changed")
#   }
#
#   conflict_values <- sort(suppressWarnings(
#     as.numeric(after$Value.Num[salinity_conflict])
#   ))
#   if (!identical(conflict_values, c(0.5, 15.1))) {
#     stop("the Western Australia salinity conflict values changed")
#   }
#
#   after$Value.Num[signed_rows] <- after$Reading.Value[signed_rows]
#   after$source_method_flag[soluble_reactive_si] <- "soluble reactive Si"
#   # keep reported values for later outlier review
#   reviewed_invalid <- c(
#     invalid_ph,
#     high_do,
#     high_conductivity,
#     zero_conductivity
#   )
#   if (!identical(
#     after$Value.Num[c(bad_do, reviewed_invalid)],
#     after$Reading.Value[c(bad_do, reviewed_invalid)]
#   )) {
#     stop("Western Australia reviewed source values changed")
#   }
#
#   assert_count(sum(grepl("^\\s*<", after$Value.Num), na.rm = TRUE), 2074L,
#                "Western Australia prepared less-than values")
#   assert_count(sum(grepl("^\\s*>", after$Value.Num), na.rm = TRUE), 49L,
#                "Western Australia prepared greater-than values")
#   assert_count(sum(!is_blank(after$Value.Num[salinity_conflict])), 2L,
#                "Western Australia retained salinity values")
#   assert_count(sum(!is_blank(after$Value.Num[reviewed_invalid])), 11L,
#                "Western Australia retained reviewed source values")
#
#   # choose which sites to include after the full chemistry table is built
#   write_prepared(
#     filename,
#     before,
#     after,
#     c("Value.Num", "source_method_flag"),
#     3179L
#   )
# }
#
# # ---- generic parser check ----
#
# normalize_unit_label <- function(x) {
#   x <- trimws(as.character(x))
#   x <- gsub("µ|μ", "u", x)
#   lower <- tolower(x)
#   result <- x
#   result[is.na(x) | x == ""] <- x[is.na(x) | x == ""]
#   result[lower == "na"] <- NA_character_
#   result[x %in% c("-", "/")] <- ""
#   result[lower %in% c("mg/l", "milligrams/litre", "milligramsperliter")] <-
#     "mg/L"
#   result[lower == "mg(s)/l"] <- "mg S/L"
#   result[lower == "mg(so4)/l"] <- "mg SO4/L"
#   result[lower %in% c("ug/l", "micrograms/litre")] <- "ug/L"
#   result[lower %in% c(
#     "microsiemens/cm", "microsiemens cm", "microsiemenspercentimeter"
#   )] <- "uS/cm"
#   result[lower %in% c("deg c", "oc", "degrees celsius")] <- "C"
#   result[x == "%"] <- "percent"
#   result[lower == "pcu"] <- "PCU"
#   result[lower == "fnu"] <- "FNU"
#   result[lower == "ntu"] <- "NTU"
#   result[lower == "ueq/l"] <- "ueq/L"
#   result
# }
#
# mean_measurement <- function(x, missing_tokens) {
#   x <- as.character(x)
#   less_than <- grepl("^\\s*<\\s*(?:[0-9]|\\.[0-9])", x)
#   greater_than <- grepl("^\\s*>\\s*(?:[0-9]|\\.[0-9])", x)
#   number <- suppressWarnings(as.numeric(sub("^\\s*[<>]\\s*", "", x)))
#   unparsed <- !is.na(x) & !trimws(x) %in% missing_tokens & is.na(number)
#
#   if (any(unparsed)) {
#     stop("unparsed repeated chemistry values remain")
#   }
#
#   detected <- number[!is.na(number) & !less_than & !greater_than]
#   limited <- number[!is.na(number) & less_than]
#   lower_limited <- number[!is.na(number) & greater_than]
#
#   if (length(detected) > 0) {
#     return(as.character(mean(detected)))
#   }
#   if (length(limited) > 0) {
#     if (length(lower_limited) > 0) {
#       stop("mixed less-than and greater-than repeated measurements remain")
#     }
#     return(paste0("<", mean(limited)))
#   }
#   if (length(lower_limited) > 0) {
#     return(paste0(">", mean(lower_limited)))
#   }
#   NA_character_
# }
#
# parse_for_qualifier_count <- function(filename, data_key, units_key) {
#   raw <- read_source(filename, directory = output_dir, clean_names = TRUE)
#   key_rows <- data_key[
#     data_key$Raw_Filename == filename &
#       !is.na(data_key$Standardized_Column_Name) &
#       trimws(data_key$Standardized_Column_Name) != "",
#   ]
#   missing_columns <- setdiff(key_rows$Raw_Column_Name, names(raw))
#
#   if (length(missing_columns) > 0) {
#     stop("the lean data key expects missing prepared columns in '", filename, "'")
#   }
#
#   units <- normalize_unit_label(key_rows$Units)
#   units[!is.na(units) & nchar(units) == 0] <- NA_character_
#   unit_suffix <- gsub("/| |\\-", "_", units)
#   actual_names <- ifelse(
#     is.na(unit_suffix),
#     key_rows$Standardized_Column_Name,
#     paste0(key_rows$Standardized_Column_Name, "_", unit_suffix)
#   )
#   mapped <- raw[key_rows$Raw_Column_Name]
#   names(mapped) <- actual_names
#   mapped <- data.frame(
#     Dataset = key_rows$Dataset[[1]],
#     Raw_Filename = filename,
#     mapped,
#     check.names = FALSE,
#     stringsAsFactors = FALSE
#   )
#   mapped <- unique(mapped)
#
#   if (unique(key_rows$Dataset_orientation) == "wide") {
#     values <- unlist(mapped, use.names = FALSE)
#     return(c(
#       less_than = sum(grepl("^\\s*<", values), na.rm = TRUE),
#       greater_than = sum(grepl("^\\s*>", values), na.rm = TRUE)
#     ))
#   }
#
#   if (!requireNamespace("data.table", quietly = TRUE)) {
#     stop("the data.table package is required for the parser check")
#   }
#   source_has_row_units <- "solute_units" %in% names(mapped)
#   if (!source_has_row_units) {
#     mapped$solute_units <- NA_character_
#   }
#
#   mapped$solute <- tolower(gsub("/| |\\-", "_", mapped$solute))
#   mapped$solute_units <- normalize_unit_label(mapped$solute_units)
#   mapped$solute_units[mapped$solute_units == ""] <- NA_character_
#   mapped$solute_units <- gsub("/| |\\-", "_", mapped$solute_units)
#
#   missing_tokens <- unique(c(
#     data_key$NA_indicator,
#     "", "NA", "NaN", "N/A", "n/a", "<LOD", "wrong filtering",
#     "no filter", "nd"
#   ))
#   mapped_table <- data.table::as.data.table(mapped)
#   group_columns <- setdiff(names(mapped_table), "amount")
#   grouped <- mapped_table[
#     ,
#     .(amount = mean_measurement(amount, missing_tokens)),
#     by = group_columns
#   ]
#
#   unit_rows <- units_key[units_key$Raw_Filename == filename, ]
#   unit_rows$solute <- tolower(gsub("/| |\\-", "_", unit_rows$Variable))
#   unit_rows$key_unit <- normalize_unit_label(unit_rows$Units)
#   unit_rows$key_unit <- gsub("/| |\\-", "_", unit_rows$key_unit)
#   if (filename == "Como_Creek_chem.csv") {
#     unit_rows <- unit_rows[unit_rows$solute != "tss___dry_mass", ]
#   }
#
#   if (anyDuplicated(unit_rows$solute) > 0) {
#     stop("the parser check needs one units-key choice per target solute")
#   }
#
#   key_match <- match(grouped$solute, unit_rows$solute)
#   grouped$key_unit <- unit_rows$key_unit[key_match]
#   grouped <- grouped[!is.na(key_match)]
#   keep_unit <- if (source_has_row_units) {
#     (is.na(grouped$solute_units) & is.na(grouped$key_unit)) |
#       (!is.na(grouped$solute_units) & grouped$solute_units == grouped$key_unit)
#   } else {
#     rep(TRUE, nrow(grouped))
#   }
#
#   if (any(!keep_unit)) {
#     stop("row-level units did not resolve in '", filename, "'")
#   }
#
#   grouped <- grouped[keep_unit]
#   grouped$solute_actual <- ifelse(
#     is.na(grouped$key_unit) | grouped$key_unit == "",
#     grouped$solute,
#     paste0(grouped$solute, "_", grouped$key_unit)
#   )
#   final_groups <- setdiff(
#     names(grouped),
#     c("amount", "solute", "solute_units", "key_unit")
#   )
#   parsed <- grouped[
#     ,
#     .(amount = mean_measurement(amount, missing_tokens)),
#     by = final_groups
#   ]
#
#   c(
#     less_than = sum(grepl("^\\s*<", parsed$amount), na.rm = TRUE),
#     greater_than = sum(grepl("^\\s*>", parsed$amount), na.rm = TRUE)
#   )
# }
#
# # ---- prepare the files ----
#
# prepare_hybam()
# prepare_uk_nonsi()
# prepare_uk_si()
# prepare_camels()
# prepare_como()
# prepare_usgs_geogenic()
# prepare_elbe()
# prepare_walker()
# prepare_umr("UMR_si_update_existing_sites.csv", 0L, 27L, 27L)
# prepare_umr("UMR_si_new_sites.csv", 1L, 72L, 73L)
# prepare_krycklan()
# prepare_western_australia()
#
# # ---- validate parser results ----
#
# data_key <- read.csv(
#   file.path(key_dir, "SiSyn_Data_Key.csv"),
#   stringsAsFactors = FALSE,
#   na.strings = ""
# )
# data_key <- data_key[data_key$Data_type == "chemistry", ]
# data_key$Raw_Column_Name <- gsub(
#   " |\\+|\\(|\\)|/|\\-",
#   ".",
#   data_key$Raw_Column_Name
# )
# units_key <- read.csv(
#   file.path(key_dir, "SiSyn_Chem_Units_Key.csv"),
#   stringsAsFactors = FALSE,
#   na.strings = ""
# )
# units_key <- units_key[units_key$Data_type == "chemistry", ]
#
# expected_less_than <- c(
#   "Chem_HYBAM.csv" = 1751L,
#   "UK_nonSi_solutes.csv" = 8490L,
#   "UK_Si.csv" = 857L,
#   "CAMELS_USGS_N_P.csv" = 3689L,
#   "Como_Creek_chem.csv" = 188L,
#   "USGS_geogenic_solutes.csv" = 462L,
#   "ElbeRiverChem.csv" = 117L,
#   "WalkerBranch_Chem.csv" = 10946L,
#   "UMR_si_new_sites.csv" = 72L,
#   "UMR_si_update_existing_sites.csv" = 27L,
#   "Krycklan_NP.csv" = 63L,
#   "WesternAus_AllSites_Filtered_v2.csv" = 1996L
# )
# expected_greater_than <- c(
#   "Chem_HYBAM.csv" = 0L,
#   "UK_nonSi_solutes.csv" = 15L,
#   "UK_Si.csv" = 0L,
#   "CAMELS_USGS_N_P.csv" = 0L,
#   "Como_Creek_chem.csv" = 0L,
#   "USGS_geogenic_solutes.csv" = 2L,
#   "ElbeRiverChem.csv" = 0L,
#   "WalkerBranch_Chem.csv" = 0L,
#   "UMR_si_new_sites.csv" = 0L,
#   "UMR_si_update_existing_sites.csv" = 0L,
#   "Krycklan_NP.csv" = 0L,
#   "WesternAus_AllSites_Filtered_v2.csv" = 25L
# )
# observed_counts <- vapply(
#   names(expected_less_than),
#   parse_for_qualifier_count,
#   numeric(2),
#   data_key = data_key,
#   units_key = units_key
# )
#
# assert_vector(
#   observed_counts["less_than", ],
#   expected_less_than,
#   "generic parser less-than counts"
# )
# assert_vector(
#   observed_counts["greater_than", ],
#   expected_greater_than,
#   "generic parser greater-than counts"
# )
# ---- end replacement: reported-limits ----

# ---- sidney: units, dates, names, and method choices ----
# these sections handle the other datasets' units, dates, names, and methods
# the downloaded raw files stay unchanged and reported outliers remain for later review
# current file: chemistry-inputs/prepare_other_sources.R
# ---- begin replacement: individual-source-choices ----
# # prepare the other chemistry sources
# # read the chemistry files and original source downloads without changing them
# # prepare the files used to build the 77 master-chemistry inputs
# # each source has its own named section and preparation function below
# # the final section runs those functions in the reviewed order
#
# # ---- shared helpers ----
#
# helper_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
# if (length(helper_argument) != 1L) {
#   stop("the script folder could not be identified")
# }
# preparation_dir <- dirname(normalizePath(sub("^--file=", "", helper_argument)))
# source(file.path(dirname(preparation_dir), "chemistry_functions.R"))
# rm(helper_argument)
#
# # ---- settings ----
#
# input_dir <- required_path("SISYN_CHEM_INPUT_DIR")
# selection_dir <- required_path("SISYN_CHEM_SELECTION_INPUT_DIR")
# output_dir <- required_path("SISYN_CHEM_DIRECT_OUTPUT_DIR")
# qualifier_dir <- required_path("SISYN_CHEM_QUALIFIER_OUTPUT_DIR")
# key_dir <- required_path("SISYN_CHEM_KEY_DIR")
# canada_provider_dir <- required_path("SISYN_CHEM_CANADA_PROVIDER_DIR")
# cameroon_provider_zip <- required_path("SISYN_CAMEROON_PROVIDER_ZIP")
# check_file_versions(cameroon_provider_zip, "8b9238192be6fca67e70bbb3cc697168",
#   "Cameroon provider ZIP")
#
# canada_provider_files <- sort(list.files(
#   canada_provider_dir,
#   pattern = "2000-present[.]csv$",
#   full.names = TRUE
# ))
# canada_variable_file <- file.path(
#   canada_provider_dir,
#   "Water-Qual-Eau-VariableInfo.csv"
# )
#
# usgs_provider_dir <- required_path("SISYN_CHEM_USGS_PROVIDER_DIR")
# usgs_provider_files <- sort(list.files(
#   usgs_provider_dir,
#   pattern = "^samples_sites.*_pcodes(06|16).*\\.csv$",
#   full.names = TRUE
# ))
# usgs_clinton_file <- required_path("SISYN_CHEM_USGS_CLINTON_FILE")
# usgs_exclusion_file <- required_path("SISYN_CHEM_USGS_EXCLUSION_FILE")
# susquehanna_filename <-
#   "USGS__USGS__SUSQUEHANNA_RIVER__4db2531edf53.csv"
# reviewed_susquehanna_file <- required_path(
#   "SISYN_CHEM_REVIEWED_SUSQUEHANNA_FILE"
# )
# carey_file <- required_path("SISYN_CHEM_CAREY_FILE")
# nt_nsw_provider_file <- required_path("SISYN_CHEM_NT_NSW_PROVIDER_FILE")
# japan_provider_file <- required_path("SISYN_CHEM_JAPAN_PROVIDER_FILE")
# netherlands_provider_file <- required_path(
#   "SISYN_CHEM_NETHERLANDS_PROVIDER_FILE"
# )
# arc_provider_dir <- file.path(dirname(canada_provider_dir), "usa_arc")
# arc_provider_files <- file.path(
#   arc_provider_dir,
#   c(
#     "1988-2010_Kling_Akchem.csv",
#     "legacy_audit_ARC_2002-2024_Kling_AON_Imnavait_Chemistry.csv",
#     "legacy_audit_ARC_2012-2020_Kling_Akchem.02.csv"
#   )
# )
# arc_provider_metadata <- file.path(
#   arc_provider_dir,
#   "1988-2010_Kling_Akchem_eml.xml"
# )
# check_file_versions(nt_nsw_provider_file, "5832d9fb5276ed53e75a84c6e0f4bf89",
#   "NT/NSW silica provider file")
# check_file_versions(japan_provider_file, "69c4167a601ebd77cc890a79e3a13251",
#   "Japan provider file")
# check_file_versions(netherlands_provider_file, "dd92c6ab6756479e569bb11312185ac4",
#   "Netherlands provider file")
# expected_arc_provider_md5 <- c(
#   "a3db20ce1bb2a460a7ee9544a7d87980",
#   "9fcb805438e69755aa436d52d6a48d30",
#   "0d3f435c4154ac18d333732bd97f30dd",
#   "774af538a9b09bcb4ec50900584dd6d3"
# )
# check_file_versions(c(arc_provider_files, arc_provider_metadata),
#   expected_arc_provider_md5, "ARC provider files")
#
# canada_site_files <- c(
#   paste0(
#     "Canada__Canada__ALSEK_RIVER_ABOVE_BATES_RIVER_IN_",
#     "KLUANE_NATIONAL_PARK__b577690976a4.csv"
#   ),
#   paste0(
#     "Canada__Canada__BEAVER_RIVER_ABOVE_HIGHWAY_1_IN_GLACIER_",
#     "NATIONAL_PARK__8db5e916b53d.csv"
#   ),
#   paste0(
#     "Canada__Canada__KICKING_HORSE_RIVER_AT_FIELD_IN_YOHO_",
#     "NATIONAL_PARK__78eed20cf9d6.csv"
#   ),
#   paste0(
#     "Canada__Canada__KOOTENAY_RIVER_ABOVE_HIGHWAY_93_IN_",
#     "KOOTENAY_NATIONAL_PARK__7a1cbddf7523.csv"
#   ),
#   "Canada__Canada__LIARD_RIVER_AT_UPPER_CROSSING__47916feca4a8.csv",
#   "Canada__Canada__SKEENA_RIVER_AT_USK__6692e2a39888.csv"
# )
#
# if (length(canada_provider_files) != 17L) {
#   stop(
#     "the reviewed Canada provider file count changed: expected 17, found ",
#     length(canada_provider_files)
#   )
# }
#
# if (length(usgs_provider_files) != 40L) {
#   stop(
#     "the reviewed USGS provider file count changed: expected 40, found ",
#     length(usgs_provider_files)
#   )
# }
#
# guadeloupe_files <- c(
#   "guadeloupe_GRCBDMDF_chem.csv",
#   "guadeloupe_GRCBDPBD_chem.csv",
#   "guadeloupe_GRCBDQCK_chem.csv",
#   "guadeloupe_GRCCEDIG_chem.csv",
#   "guadeloupe_GRCVHBAR_chem.csv",
#   "guadeloupe_GRCVHSAV_chem.csv"
# )
#
# source_files <- c(
#   "Alpine_Clean.csv",
#   "Switzerland_CAMELS_CH_AllSites_chemistry.csv",
#   "EastRiverSFA_chem.csv",
#   "Krycklan_Cation_Clean.csv",
#   "Deduplicated_Sites_AllSites_chemistry.csv",
#   "Canada_WQ_dat.csv",
#   canada_site_files,
#   "CatalinaJemez_chemistry_2009-2019_V2.csv",
#   "Seine_baseGeo_Naides.csv",
#   "Cleaned_Seine_Data.csv",
#   "NT_NSW_Chem_Cleaned.csv",
#   "MCM_Chem_clean.csv",
#   "MurrayDarlingChem_Merged.csv",
#   "MD__MD__Merbein__9df18bcf9b24.csv",
#   "Chem_Cameroon.csv",
#   guadeloupe_files,
#   "CAMREX_filled_template.csv",
#   "Finnish_riverine_data_07032025.csv",
#   "Japan_MLIT_AllSites_chemistry.csv",
#   "Netherlands_RWS_AllSites_chemistry.csv",
#   "NIVA_geogenic_clean.csv",
#   "NIVA_Water_chemistry.csv",
#   "NigerRiver.csv",
#   "yzeron_301502401_chem.csv",
#   "yzeron_v301502402_chem.csv",
#   "yzeron_v3015810_chem.csv",
#   "20221030_masterdata_chem_V2.csv",
#   "USGS__USGS__Choptank_River__da9275d27028.csv",
#   "USGS__USGS__Frio_River__4e8764089639.csv",
#   "USGS__USGS__Mississippi_River_above_Vicksburg__d2360d259841.csv",
#   susquehanna_filename
# )
#
# required_files <- c(
#   file.path(input_dir, source_files),
#   canada_provider_files,
#   canada_variable_file,
#   cameroon_provider_zip,
#   usgs_provider_files,
#   usgs_clinton_file,
#   usgs_exclusion_file,
#   reviewed_susquehanna_file,
#   carey_file,
#   nt_nsw_provider_file,
#   japan_provider_file,
#   netherlands_provider_file,
#   arc_provider_files,
#   arc_provider_metadata,
#   file.path(qualifier_dir, "ElbeRiverChem.csv"),
#   file.path(key_dir, "SiSyn_Data_Key.csv"),
#   file.path(key_dir, "SiSyn_Chem_Units_Key.csv"),
#   file.path(key_dir, "Site_Reference_Table.csv")
# )
#
# check_required_files(required_files)
#
# read_dirs <- c(
#   input_dir,
#   selection_dir,
#   qualifier_dir,
#   key_dir,
#   canada_provider_dir,
#   dirname(cameroon_provider_zip),
#   usgs_provider_dir,
#   dirname(usgs_clinton_file),
#   dirname(usgs_exclusion_file),
#   dirname(reviewed_susquehanna_file),
#   dirname(carey_file),
#   dirname(nt_nsw_provider_file),
#   dirname(japan_provider_file),
#   dirname(netherlands_provider_file),
#   arc_provider_dir
# )
# check_work_folder(output_dir, read_dirs, "source preparation")
# dir.create(output_dir, recursive = TRUE)
#
# # ---- source preparation helpers ----
#
# read_source <- function(filename, directory = input_dir) {
#   preferred_dir <- if (identical(directory, input_dir)) selection_dir else NULL
#   read_source_csv(filename, directory, preferred_dir = preferred_dir)
# }
#
# as_number <- function(x) {
#   suppressWarnings(as.numeric(x))
# }
#
# assert_values <- function(observed, expected, label, tolerance = 1e-10) {
#   observed <- as.numeric(observed)
#   expected <- as.numeric(expected)
#
#   if (
#     length(observed) != length(expected) ||
#       any(is.na(observed) != is.na(expected)) ||
#       any(abs(observed - expected) > tolerance, na.rm = TRUE)
#   ) {
#     stop(label, " changed")
#   }
# }
#
# same_character <- function(x, y) {
#   identical(as.character(x), as.character(y)) &&
#     identical(is.na(x), is.na(y))
# }
#
# normalize_site_name <- function(x) {
#   tolower(gsub("[[:space:]]+", " ", trimws(as.character(x))))
# }
#
# find_tds_labeled_tn <- function(data, tolerance = 1e-9) {
#   # exact back-conversion identifies tn entries copied from paired tds values
#   amount <- as_number(data$amount)
#   sample_key <- paste(data$LTER, data$Stream_Name, data$date, sep = "\r")
#   tds <- data$solute == "tot dissolved solids" &
#     data$solute_units == "mg/L" &
#     !is.na(amount)
#   tn <- data$solute == "TN" &
#     data$solute_units == "uM" &
#     !is.na(amount)
#   tds_by_sample <- split(amount[tds], sample_key[tds])
#   matched <- rep(FALSE, nrow(data))
#
#   for (row_number in which(tn)) {
#     paired_tds <- tds_by_sample[[sample_key[row_number]]]
#     if (length(paired_tds) > 0L) {
#       matched[row_number] <- any(
#         abs(amount[row_number] * 14.0067 / 1000 - paired_tds) < tolerance
#       )
#     }
#   }
#
#   matched
# }
#
# normalize_reviewed_dates <- function(
#   x,
#   date_order,
#   expected_nonblank,
#   expected_two_digit,
#   expected_four_digit,
#   expected_unique,
#   expected_range,
#   label
# ) {
#   date_text <- trimws(as.character(x))
#   blank <- is_blank(date_text)
#   nonblank <- !blank
#   source_pattern <- paste0(
#     "^[0-9]{1,2}/[0-9]{1,2}/([0-9]{2}|[0-9]{4})",
#     "( [0-9]{1,2}:[0-9]{2})?$"
#   )
#
#   assert_count(sum(nonblank), expected_nonblank,
#                paste0(label, " nonblank dates"))
#   assert_count(sum(nonblank & !grepl(source_pattern, date_text)), 0L,
#                paste0(label, " unrecognized dates"))
#
#   date_only <- sub("[[:space:]].*$", "", date_text[nonblank])
#   time_suffix <- sub("^[^[:space:]]+", "", date_text[nonblank])
#   parts <- do.call(rbind, strsplit(date_only, "/", fixed = TRUE))
#   year_width <- nchar(parts[, 3])
#   assert_count(sum(year_width == 2L), expected_two_digit,
#                paste0(label, " two-digit source years"))
#   assert_count(sum(year_width == 4L), expected_four_digit,
#                paste0(label, " four-digit source years"))
#
#   first_part <- as.integer(parts[, 1])
#   second_part <- as.integer(parts[, 2])
#   year <- as.integer(parts[, 3])
#   # this source rule puts 00-50 in 2000-2050 and 51-99 in 1951-1999
#   # keep this reviewed cutoff separate from the hybam date rule
#   recent_two_digit <- year_width == 2L & year <= 50L
#   older_two_digit <- year_width == 2L & year > 50L
#   year[recent_two_digit] <- year[recent_two_digit] + 2000L
#   year[older_two_digit] <- year[older_two_digit] + 1900L
#
#   if (date_order == "mdy") {
#     month <- first_part
#     day <- second_part
#   } else if (date_order == "dmy") {
#     day <- first_part
#     month <- second_part
#   } else {
#     stop("unsupported reviewed date order for ", label)
#   }
#
#   parsed <- suppressWarnings(as.Date(sprintf(
#     "%04d-%02d-%02d",
#     year,
#     month,
#     day
#   )))
#   if (any(is.na(parsed))) {
#     stop(label, " dates did not parse")
#   }
#   if (!identical(range(parsed), as.Date(expected_range))) {
#     stop(label, " date range changed")
#   }
#   assert_count(length(unique(parsed)), expected_unique,
#                paste0(label, " unique dates"))
#
#   normalized <- as.character(x)
#   normalized[nonblank] <- paste0(format(parsed, "%Y-%m-%d"), time_suffix)
#   assert_count(
#     sum(nonblank & !grepl(
#       "^[0-9]{4}-[0-9]{2}-[0-9]{2}( [0-9]{1,2}:[0-9]{2})?$",
#       normalized
#     )),
#     0L,
#     paste0(label, " non-ISO outputs")
#   )
#   if (!identical(
#     sub("^[^[:space:]]+", "", normalized[nonblank]),
#     time_suffix
#   )) {
#     stop(label, " sampling times changed")
#   }
#   assert_count(
#     sum(nonblank & grepl(
#       "^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}( |$)",
#       normalized
#     )),
#     0L,
#     paste0(label, " two-digit-year outputs")
#   )
#   normalized
# }
#
# write_prepared <- function(filename, data) {
#   write_prepared_csv(filename, data, output_dir)
# }
#
# # ---- alpine field and laboratory methods ----
#
# prepare_alpine <- function() {
#   filename <- "Alpine_Clean.csv"
#   before <- read_source(filename)
#   after <- before
#   value_number <- as_number(before$value)
#   source_key <- paste(before$site, before$date, sep = "\r")
#   field_rows <- before$parameter == "fldph" & !is.na(value_number)
#   lab_rows <- before$parameter == "labph" & !is.na(value_number)
#   field_keys <- unique(source_key[field_rows])
#   lab_keys <- unique(source_key[lab_rows])
#   overlapping_keys <- intersect(field_keys, lab_keys)
#   overlapping_lab <- lab_rows & source_key %in% overlapping_keys
#   field_conductivity <- before$parameter == "fldcond_uscm" &
#     !is.na(value_number)
#   lab_conductivity <- before$parameter == "labcond_uscm" &
#     !is.na(value_number)
#   field_conductivity_keys <- unique(source_key[field_conductivity])
#   lab_conductivity_keys <- unique(source_key[lab_conductivity])
#   overlapping_conductivity_keys <- intersect(
#     field_conductivity_keys,
#     lab_conductivity_keys
#   )
#   overlapping_lab_conductivity <- lab_conductivity &
#     source_key %in% overlapping_conductivity_keys
#   low_temperature <- before$parameter == "temp_c" & value_number < -0.5
#   cold_temperature_ids <- c(
#     "29939", "36372", "36413", "36434", "36455", "36476", "36497",
#     "36518", "36539", "36560", "36581", "37406", "37447", "37469",
#     "37491", "37512", "37533", "37554", "37576", "37597", "37618",
#     "37640", "42743", "42763", "43792", "43812", "43832", "43852"
#   )
#   cold_temperature_dates <- c(
#     "1994-11-29", "2002-01-08", "2002-01-22", "2002-01-29",
#     "2002-02-05", "2002-02-12", "2002-02-19", "2002-02-26",
#     "2002-03-05", "2002-03-12", "2002-03-19", "2002-12-24",
#     "2003-01-07", "2003-01-14", "2003-01-21", "2003-01-28",
#     "2003-02-04", "2003-02-11", "2003-02-18", "2003-02-25",
#     "2003-03-04", "2003-03-11", "2008-01-01", "2008-01-08",
#     "2009-01-06", "2009-01-13", "2009-01-20", "2009-01-27"
#   )
#   cold_temperature_values <- c(
#     -2, rep(-1, 19L), -2, -1, -0.7, -0.6, -0.7, -0.7, -0.8, -0.8
#   )
#   cold_temperature_rows <- match(cold_temperature_ids, before[[1L]])
#   if (any(is.na(cold_temperature_rows))) {
#     stop("reviewed Alpine cold-temperature rows are missing")
#   }
#   if (
#     !all(before$site[cold_temperature_rows] == "loch") ||
#       !all(before$date[cold_temperature_rows] == cold_temperature_dates) ||
#       !all(before$parameter[cold_temperature_rows] == "temp_c") ||
#       !all(before$flag[cold_temperature_rows] == "0")
#   ) {
#     stop("reviewed Alpine cold-temperature row identity changed")
#   }
#   assert_values(
#     value_number[cold_temperature_rows],
#     cold_temperature_values,
#     "reviewed Alpine cold-temperature values"
#   )
#   if (!identical(which(low_temperature), sort(cold_temperature_rows))) {
#     stop("Alpine cold-temperature scope changed")
#   }
#   suspect_fluoride <-
#     before[[1L]] == "8363" &
#     before$site == "andrewscreek" &
#     before$date == "1986-09-09" &
#     before$parameter == "f_mgl" &
#     before$value == "6" &
#     before$flag == "1"
#
#   assert_count(sum(field_rows), 2075L, "Alpine field pH rows")
#   assert_count(length(field_keys), 1782L, "Alpine field pH keys")
#   assert_count(sum(lab_rows), 2899L, "Alpine lab pH rows")
#   assert_count(length(lab_keys), 2519L, "Alpine lab pH keys")
#   assert_count(length(overlapping_keys), 1589L, "Alpine overlapping pH keys")
#   assert_count(sum(overlapping_lab), 1832L, "Alpine overlapping lab pH rows")
#   assert_count(length(setdiff(lab_keys, field_keys)), 930L,
#                "Alpine lab-only pH keys")
#   assert_count(sum(field_conductivity), 2964L,
#                "Alpine field conductivity rows")
#   assert_count(length(field_conductivity_keys), 2557L,
#                "Alpine field conductivity keys")
#   assert_count(sum(lab_conductivity), 2961L,
#                "Alpine lab conductivity rows")
#   assert_count(length(lab_conductivity_keys), 2568L,
#                "Alpine lab conductivity keys")
#   assert_count(length(overlapping_conductivity_keys), 2421L,
#                "Alpine overlapping conductivity keys")
#   assert_count(sum(overlapping_lab_conductivity), 2785L,
#                "Alpine overlapping lab conductivity rows")
#   assert_count(
#     length(setdiff(lab_conductivity_keys, field_conductivity_keys)),
#     147L,
#     "Alpine lab-only conductivity keys"
#   )
#   assert_count(sum(low_temperature, na.rm = TRUE), 28L,
#                "Alpine low-temperature rows")
#   assert_count(sum(suspect_fluoride, na.rm = TRUE), 1L,
#                "Alpine suspect fluoride row")
#
#   # keep field ph and conductivity when field and lab share a site-day
#   after$source_method_flag <- NA_character_
#   after$source_method_flag[field_rows] <- "field pH"
#   after$source_method_flag[lab_rows & !overlapping_lab] <-
#     "laboratory pH fallback"
#   after$source_method_flag[field_conductivity] <- "field conductivity"
#   after$source_method_flag[
#     lab_conductivity & !overlapping_lab_conductivity
#   ] <- "laboratory conductivity fallback"
#   after$value[overlapping_lab] <- NA_character_
#   after$value[overlapping_lab_conductivity] <- NA_character_
#   # keep reported fluoride and cold temperatures for later outlier review
#   assert_values(
#     as_number(after$value[suspect_fluoride]),
#     6,
#     "prepared Alpine reviewed fluoride value"
#   )
#   assert_values(
#     as_number(after$value[cold_temperature_rows]),
#     cold_temperature_values,
#     "prepared Alpine reviewed cold-temperature values"
#   )
#
#   assert_count(
#     sum(after$parameter == "labph" & !is.na(as_number(after$value))),
#     1067L,
#     "Alpine retained lab pH rows"
#   )
#   assert_count(sum(after$source_method_flag == "field pH", na.rm = TRUE),
#                2075L, "Alpine field pH method flags")
#   assert_count(
#     sum(after$source_method_flag == "laboratory pH fallback", na.rm = TRUE),
#     1067L,
#     "Alpine laboratory pH fallback method flags"
#   )
#   assert_count(
#     sum(after$parameter == "labcond_uscm" & !is.na(as_number(after$value))),
#     176L,
#     "Alpine retained lab conductivity rows"
#   )
#   assert_count(
#     sum(after$source_method_flag == "field conductivity", na.rm = TRUE),
#     2964L,
#     "Alpine field conductivity method flags"
#   )
#   assert_count(
#     sum(
#       after$source_method_flag == "laboratory conductivity fallback",
#       na.rm = TRUE
#     ),
#     176L,
#     "Alpine laboratory conductivity fallback method flags"
#   )
#   assert_count(
#     length(unique(source_key[
#       after$source_method_flag == "field conductivity" &
#         !is.na(after$source_method_flag)
#     ])),
#     2557L,
#     "Alpine prepared field conductivity keys"
#   )
#   assert_count(
#     length(unique(source_key[
#       after$source_method_flag == "laboratory conductivity fallback" &
#         !is.na(after$source_method_flag)
#     ])),
#     147L,
#     "Alpine prepared lab conductivity keys"
#   )
#   assert_count(
#     sum(!is.na(as_number(after$value[cold_temperature_rows]))),
#     28L,
#     "prepared Alpine retained cold-temperature rows"
#   )
#
#   write_prepared(filename, after)
# }
#
# # ---- switzerland methods and units ----
#
# prepare_switzerland <- function() {
#   filename <- "Switzerland_CAMELS_CH_AllSites_chemistry.csv"
#   before <- read_source(filename)
#   after <- before
#   amount_number <- as_number(before$amount)
#   source_key <- paste(before$LTER, before$Stream_Name, before$date, sep = "\r")
#   lab_ph <- before$solute == "labph" & !is.na(amount_number)
#   field_conductivity <- before$solute == "fldcond_uscm" &
#     !is.na(amount_number)
#   lab_conductivity <- before$solute == "labcond_uscm" &
#     !is.na(amount_number)
#   field_conductivity_keys <- unique(source_key[field_conductivity])
#   lab_conductivity_keys <- unique(source_key[lab_conductivity])
#
#   assert_count(nrow(before), 186696L, "Switzerland source rows")
#   assert_count(sum(lab_ph), 20085L, "Switzerland lab pH rows")
#   assert_count(
#     length(unique(source_key[lab_ph])),
#     11316L,
#     "Switzerland lab pH keys"
#   )
#   assert_count(
#     sum(field_conductivity),
#     14400L,
#     "Switzerland field conductivity rows"
#   )
#   assert_count(
#     length(field_conductivity_keys),
#     7659L,
#     "Switzerland field conductivity keys"
#   )
#   assert_count(
#     sum(lab_conductivity),
#     5954L,
#     "Switzerland lab conductivity rows"
#   )
#   assert_count(
#     length(lab_conductivity_keys),
#     3657L,
#     "Switzerland lab conductivity keys"
#   )
#   assert_count(
#     length(intersect(field_conductivity_keys, lab_conductivity_keys)),
#     0L,
#     "Switzerland overlapping conductivity keys"
#   )
#
#   after$source_method_flag <- NA_character_
#   after$source_method_flag[lab_ph] <- "laboratory pH fallback"
#   after$source_method_flag[field_conductivity] <- "field conductivity"
#   after$source_method_flag[lab_conductivity] <-
#     "laboratory conductivity fallback"
#
#   assert_count(
#     length(unique(source_key[
#       after$source_method_flag == "laboratory pH fallback" &
#         !is.na(after$source_method_flag)
#     ])),
#     11316L,
#     "Switzerland prepared lab pH keys"
#   )
#   assert_count(
#     length(unique(source_key[
#       after$source_method_flag == "field conductivity" &
#         !is.na(after$source_method_flag)
#     ])),
#     7659L,
#     "Switzerland prepared field conductivity keys"
#   )
#   assert_count(
#     length(unique(source_key[
#       after$source_method_flag == "laboratory conductivity fallback" &
#         !is.na(after$source_method_flag)
#     ])),
#     3657L,
#     "Switzerland prepared lab conductivity keys"
#   )
#   assert_count(
#     sum(!is.na(after$source_method_flag)),
#     40439L,
#     "Switzerland prepared method flags"
#   )
#
#   write_prepared(filename, after)
# }
#
# # ---- east river reporting limits ----
#
# prepare_east_river <- function() {
#   filename <- "EastRiverSFA_chem.csv"
#   before <- read_source(filename)
#   after <- before
#   anion_limits <- c(nitrate = 0.1, phosphate = 0.1, sulfate = 0.1)
#   expected_anion_nd <- c(nitrate = 310L, phosphate = 3115L, sulfate = 1L)
#   anion_nd <- before$solute %in% names(anion_limits) &
#     as_number(before$value) == 0
#   observed_anion_nd <- table(before$solute[anion_nd])
#   npoc <- before$solute == "npoc"
#   assert_count(nrow(before), 33142L, "East River rows")
#   assert_count(sum(npoc, na.rm = TRUE), 1420L, "East River NPOC rows")
#   if (
#     !identical(names(observed_anion_nd), names(expected_anion_nd)) ||
#       !identical(as.integer(observed_anion_nd), unname(expected_anion_nd))
#   ) {
#     stop("the reviewed East River not-detectable scope changed")
#   }
#   # the original source defines zero anion results as not detectable
#   # the reported as11 method has a 0.1 micromolar lower concentration limit
#   for (solute_name in names(anion_limits)) {
#     target <- anion_nd & before$solute == solute_name
#     after$value[target] <- paste0("<", anion_limits[[solute_name]])
#   }
#
#   # keep the source label in the saved file and map npoc in the working copy
#   after$source_method_flag <- NA_character_
#   after$solute[npoc] <- "doc"
#   after$source_method_flag[npoc] <- "reported as NPOC"
#
#   write_prepared(filename, after)
# }
#
# # ---- japan source values ----
#
# read_japan_provider_values <- function(header_label) {
#   if (!requireNamespace("jsonlite", quietly = TRUE)) {
#     stop("the jsonlite package is required for the Japan provider file")
#   }
#
#   tables <- jsonlite::fromJSON(japan_provider_file, simplifyVector = FALSE)
#   values <- list()
#   value_row <- 0L
#
#   for (table in tables) {
#     if (length(table$rows) < 2L || length(table$rows[[1L]]) == 0L) {
#       next
#     }
#
#     header <- unlist(table$rows[[1L]], use.names = FALSE)
#     column <- which(sub("\\n.*$", "", header) == header_label)
#     if (length(column) == 0L) {
#       next
#     }
#     if (length(column) != 1L) {
#       stop("a Japan provider table has repeated target columns")
#     }
#
#     for (row in table$rows[-1L]) {
#       row <- unlist(row, use.names = FALSE)
#       if (length(row) < column || is_blank(row[1L]) || is_blank(row[column])) {
#         next
#       }
#       value_row <- value_row + 1L
#       values[[value_row]] <- data.frame(
#         date = sub(
#           "日$",
#           "",
#           sub("月", "-", sub("年", "-", row[1L], fixed = TRUE), fixed = TRUE)
#         ),
#         amount = as_number(sub("^<", "", trimws(row[column]))),
#         qualifier = if (startsWith(trimws(row[column]), "<")) "<" else NA_character_,
#         stringsAsFactors = FALSE
#       )
#     }
#   }
#
#   if (length(values) == 0L) {
#     stop("the requested Japan provider column was not found")
#   }
#   do.call(rbind, values)
# }
#
# prepare_japan <- function() {
#   filename <- "Japan_MLIT_AllSites_chemistry.csv"
#   before <- read_source(filename)
#   after <- before
#   nh4_factor <- (14.0067 + 4 * 1.00784) / 14.0067
#   po4_factor <- (30.973762 + 4 * 15.999) / 30.973762
#   nh4_provider <- read_japan_provider_values("アンモニウム態窒素")
#   po4_provider <- read_japan_provider_values("溶解性オルトリン酸態リン")
#   nh4_rows <- before$solute == "nh4_mgl"
#   po4_rows <- which(before$solute == "po4_mgl")
#
#   assert_count(nrow(before), 346L, "Japan source rows")
#   assert_count(sum(nh4_rows, na.rm = TRUE), 68L, "Japan ammonium rows")
#   assert_count(nrow(nh4_provider), 68L, "Japan provider ammonium rows")
#   assert_count(sum(nh4_provider$qualifier == "<", na.rm = TRUE), 27L,
#                "Japan provider ammonium limits")
#   assert_count(length(po4_rows), 74L, "Japan source phosphate rows")
#   assert_count(nrow(po4_provider), 15L,
#                "Japan provider dissolved phosphate rows")
#   assert_count(sum(po4_provider$qualifier == "<", na.rm = TRUE), 1L,
#                "Japan provider dissolved phosphate limits")
#
#   nh4_order <- match(before$date[nh4_rows], nh4_provider$date)
#   if (anyNA(nh4_order)) {
#     stop("Japan ammonium dates no longer match the provider")
#   }
#   assert_values(
#     as_number(before$amount[nh4_rows]),
#     nh4_provider$amount[nh4_order] * nh4_factor,
#     "Japan provider ammonium values",
#     tolerance = 1e-8
#   )
#   nh4_qualifier <- nh4_provider$qualifier[nh4_order]
#   after$amount[nh4_rows] <- format_amount(
#     as_number(before$amount[nh4_rows]),
#     nh4_qualifier
#   )
#   after$solute_units[nh4_rows] <- "mg NH4/L"
#
#   kept_po4 <- integer(nrow(po4_provider))
#   available_po4 <- po4_rows
#   for (provider_row in seq_len(nrow(po4_provider))) {
#     expected_amount <- po4_provider$amount[provider_row] * po4_factor
#     candidates <- available_po4[
#       before$date[available_po4] == po4_provider$date[provider_row] &
#         abs(as_number(before$amount[available_po4]) - expected_amount) < 1e-8
#     ]
#     if (length(candidates) == 0L) {
#       stop("a dissolved Japan phosphate result no longer matches the source")
#     }
#     kept_po4[provider_row] <- candidates[1L]
#     available_po4 <- setdiff(available_po4, candidates[1L])
#   }
#
#   after$source_method_flag <- NA_character_
#   after$source_method_flag[kept_po4] <-
#     "dissolved orthophosphate as P, MLIT"
#   after$amount[kept_po4] <- format_amount(
#     as_number(before$amount[kept_po4]),
#     po4_provider$qualifier
#   )
#   keep <- before$solute != "po4_mgl" | seq_len(nrow(before)) %in% kept_po4
#   after <- after[keep, , drop = FALSE]
#   rownames(after) <- NULL
#
#   assert_count(nrow(after), 287L, "prepared Japan rows")
#   assert_count(sum(after$solute == "po4_mgl"), 15L,
#                "prepared Japan dissolved phosphate rows")
#   assert_count(sum(grepl("^<", after$amount)), 28L,
#                "prepared Japan qualified rows")
#   write_prepared(filename, after)
# }
#
# # ---- match netherlands source records ----
#
# netherlands_signature <- function(site, date, value) {
#   paste(site, date, sprintf("%.12g", as_number(value)), sep = "\r")
# }
#
# prepare_netherlands <- function() {
#   filename <- "Netherlands_RWS_AllSites_chemistry.csv"
#   before <- read_source(filename)
#   after <- before
#   provider <- readRDS(netherlands_provider_file)
#   if (!isTRUE(provider$complete) || nrow(provider$failures) != 0L) {
#     stop("the reviewed Netherlands provider download is incomplete")
#   }
#
#   station_names <- c(
#     "borgharen.boven" = "Meuse at Borgharen Boven",
#     "driel.boven" = "Nederrijn at Driel Boven",
#     "eijsden" = "Meuse at Eijsden",
#     "hagestein" = "Lek at Hagestein",
#     "lith.boven" = "Meuse at Lith Boven",
#     "lobith.ponton" = "Rhine at Lobith Ponton",
#     "maasbracht" = "Meuse at Maasbracht"
#   )
#   plan <- data.frame(
#     solute = c("nh4_mgl", "no3_mgl", "po4_mgl", "sio2_mgl"),
#     target = c(
#       "ammonium_as_n_dissolved",
#       "nitrate_as_n_dissolved",
#       "phosphate_as_p_dissolved",
#       "dsi_as_si"
#     ),
#     factor = c(
#       (14.0067 + 4 * 1.00784) / 14.0067,
#       (14.0067 + 3 * 15.999) / 14.0067,
#       (30.973762 + 4 * 15.999) / 30.973762,
#       (28.0855 + 2 * 15.999) / 28.0855
#     ),
#     source_rows = c(155636L, 16754L, 18815L, 4010L),
#     matched_rows = c(4302L, 1767L, 4305L, 4010L),
#     excluded_rows = c(151334L, 14987L, 14510L, 0L),
#     unmatched_only_days = c(7362L, 1022L, 1012L, 0L),
#     limit_rows = c(43L, 1L, 11L, 1L),
#     stringsAsFactors = FALSE
#   )
#
#   assert_count(nrow(before), 385343L, "Netherlands source rows")
#   source_value <- as_number(before$amount)
#   source_key <- netherlands_signature(
#     before$Stream_Name,
#     before$date,
#     source_value
#   )
#   source_site_day <- paste(before$Stream_Name, before$date, sep = "\r")
#   keep <- rep(TRUE, nrow(before))
#
#   for (plan_row in seq_len(nrow(plan))) {
#     rule <- plan[plan_row, , drop = FALSE]
#     source_target <- before$solute == rule$solute
#     assert_count(sum(source_target, na.rm = TRUE), rule$source_rows,
#                  paste0("Netherlands ", rule$solute, " source rows"))
#
#     observations <- provider$observations[
#       provider$observations$target_name == rule$target &
#         is.finite(provider$observations$value_numeric) &
#         abs(provider$observations$value_numeric) < 1e10,
#       ,
#       drop = FALSE
#     ]
#     provider_site <- unname(station_names[observations$station_id])
#     if (anyNA(provider_site)) {
#       stop("a Netherlands provider station is not mapped")
#     }
#     provider_date <- substr(observations$source_time, 1L, 10L)
#     provider_key <- netherlands_signature(
#       provider_site,
#       provider_date,
#       observations$value_numeric
#     )
#     matched <- source_target & source_key %in% unique(provider_key)
#     limit_key <- unique(provider_key[observations$limit_symbol == "<" &
#       !is.na(observations$limit_symbol)])
#     limited <- source_target & source_key %in% limit_key
#
#     assert_count(sum(matched, na.rm = TRUE), rule$matched_rows,
#                  paste0("Netherlands ", rule$solute, " provider matches"))
#     assert_count(sum(source_target & !matched, na.rm = TRUE),
#                  rule$excluded_rows,
#                  paste0("Netherlands ", rule$solute, " excluded rows"))
#     assert_count(sum(limited, na.rm = TRUE), rule$limit_rows,
#                  paste0("Netherlands ", rule$solute, " limit rows"))
#     unmatched_only <- source_target & !matched &
#       !source_site_day %in% unique(source_site_day[matched])
#     assert_count(length(unique(source_site_day[unmatched_only])),
#                  rule$unmatched_only_days,
#                  paste0("Netherlands ", rule$solute, " unmatched-only days"))
#     if (!identical(
#       sort(unique(before$Stream_Name[source_target])),
#       sort(unique(before$Stream_Name[matched]))
#     )) {
#       stop("a Netherlands solute would lose a source site")
#     }
#
#     convert <- matched
#     qualifier <- rep(NA_character_, sum(convert))
#     qualifier[limited[convert]] <- "<"
#     after$amount[convert] <- format_amount(
#       source_value[convert] * rule$factor,
#       qualifier
#     )
#     keep[source_target & !matched] <- FALSE
#   }
#
#   after <- after[keep, , drop = FALSE]
#   rownames(after) <- NULL
#   after$solute_units[after$solute == "nh4_mgl"] <- "mg NH4/L"
#   assert_count(nrow(after), 204512L, "prepared Netherlands rows")
#   assert_count(sum(grepl("^<", after$amount)), 56L,
#                "prepared Netherlands qualified rows")
#   write_prepared(filename, after)
# }
#
#
#
# # ---- individual canada source records ----
#
# prepare_canada_sites <- function() {
#   site_plan <- data.frame(
#     filename = canada_site_files,
#     site_no = c(
#       "YT08AB0009", "BC08NB0002", "BC08NA0033",
#       "BC08NF0001", "YT10AA0001", "BC08EF0001"
#     ),
#     stream_name = c(
#       "ALSEK RIVER ABOVE BATES RIVER IN KLUANE NATIONAL PARK",
#       "BEAVER RIVER ABOVE HIGHWAY 1 IN GLACIER NATIONAL PARK",
#       "KICKING HORSE RIVER AT FIELD IN YOHO NATIONAL PARK",
#       "KOOTENAY RIVER ABOVE HIGHWAY 93 IN KOOTENAY NATIONAL PARK",
#       "LIARD RIVER AT UPPER CROSSING",
#       "SKEENA RIVER AT USK"
#     ),
#     matched_rows = c(2445L, 7151L, 8646L, 8752L, 5450L, 8560L),
#     selected_rows = c(2445L, 7149L, 8638L, 8750L, 5448L, 8560L),
#     provider_days = c(2445L, 7147L, 8638L, 8750L, 5448L, 8559L),
#     qualified_days = c(409L, 1375L, 1906L, 2240L, 880L, 1283L),
#     dissolved_si_days = c(48L, 119L, 111L, 153L, 78L, 91L),
#     legacy_rows = c(3L, 0L, 0L, 0L, 5L, 1L),
#     final_rows = c(2448L, 7147L, 8638L, 8750L, 5453L, 8560L),
#     source_rows = c(3041L, 8966L, 11413L, 10612L, 7201L, 11886L),
#     stringsAsFactors = FALSE
#   )
#
#   mapping_rows <- function(
#     codes,
#     variables,
#     provider_units,
#     solute,
#     solute_units,
#     factor
#   ) {
#     row_count <- length(codes)
#     data.frame(
#       VMV_CODE = as.character(codes),
#       Provider_Variable = rep(variables, length.out = row_count),
#       Provider_Units = rep(provider_units, length.out = row_count),
#       solute = rep(solute, length.out = row_count),
#       solute_units = rep(solute_units, length.out = row_count),
#       factor = rep(as.numeric(factor), length.out = row_count),
#       stringsAsFactors = FALSE
#     )
#   }
#
#   mg_to_um <- function(mass) 1000 / mass
#   ug_to_um <- function(mass) 1 / mass
#
#   mapping <- do.call(rbind, list(
#     mapping_rows(c("107941", "6207"), "ALUMINUM DISSOLVED", "µG/L",
#                  "Al", "uM", ug_to_um(26.9815385)),
#     mapping_rows("13788", "ALUMINUM DISSOLVED", "MG/L",
#                  "Al", "uM", mg_to_um(26.9815385)),
#     mapping_rows(c("101535", "8264"), "BROMIDE DISSOLVED", "MG/L",
#                  "Br", "uM", mg_to_um(79.904)),
#     mapping_rows(c("1180", "13087", "1978"), "CALCIUM DISSOLVED", "MG/L",
#                  "Ca", "uM", mg_to_um(40.078)),
#     mapping_rows(
#       c("108311", "17209", "8262", "1958"),
#       c(rep("CHLORIDE DISSOLVED", 3L), "CHLORIDE"),
#       "MG/L", "Cl", "uM", mg_to_um(35.45)
#     ),
#     mapping_rows(
#       c("108310", "201836", "8261", "9105"),
#       "FLUORIDE DISSOLVED", "MG/L", "F", "uM",
#       mg_to_um(18.998403163)
#     ),
#     mapping_rows(c("107953", "6219"), "IRON DISSOLVED", "µG/L",
#                  "Fe", "uM", ug_to_um(55.845)),
#     mapping_rows("13801", "IRON DISSOLVED", "MG/L",
#                  "Fe", "uM", mg_to_um(55.845)),
#     mapping_rows(
#       c("1181", "13095", "1980"),
#       "POTASSIUM DISSOLVED/FILTERED", "MG/L", "K", "uM",
#       mg_to_um(39.0983)
#     ),
#     mapping_rows(c("107956", "6222"), "LITHIUM DISSOLVED", "µG/L",
#                  "Li", "uM", ug_to_um(6.94)),
#     mapping_rows("13803", "LITHIUM DISSOLVED", "MG/L",
#                  "Li", "uM", mg_to_um(6.94)),
#     mapping_rows(c("1179", "13093", "1979"), "MAGNESIUM DISSOLVED", "MG/L",
#                  "Mg", "uM", mg_to_um(24.305)),
#     mapping_rows(c("107957", "6223"), "MANGANESE DISSOLVED", "µG/L",
#                  "Mn", "uM", ug_to_um(54.938044)),
#     mapping_rows("13805", "MANGANESE DISSOLVED", "MG/L",
#                  "Mn", "uM", mg_to_um(54.938044)),
#     mapping_rows(
#       c("1178", "13097", "1981"),
#       "SODIUM DISSOLVED/FILTERED", "MG/L", "Na", "uM",
#       mg_to_um(22.98976928)
#     ),
#     mapping_rows(c("107968", "6229"), "STRONTIUM DISSOLVED", "µG/L",
#                  "Sr", "uM", ug_to_um(87.62)),
#     mapping_rows("13815", "STRONTIUM DISSOLVED", "MG/L",
#                  "Sr", "uM", mg_to_um(87.62)),
#     # keep dissolved silicon reported on a si mass basis
#     mapping_rows(c("1175", "13812"), "SILICON DISSOLVED", "MG/L",
#                  "DSi", "uM", mg_to_um(28.0855)),
#     mapping_rows(c("107975", "6235"), "ZINC DISSOLVED", "µG/L",
#                  "Zn", "uM", ug_to_um(65.38)),
#     mapping_rows("13825", "ZINC DISSOLVED", "MG/L",
#                  "Zn", "uM", mg_to_um(65.38)),
#     mapping_rows("7557", "AMMONIA DISSOLVED", "MG/L",
#                  "NHX", "uM", mg_to_um(14.0067)),
#     mapping_rows(
#       c("1099", "1970", "444", "8263"),
#       c(rep("NITROGEN NITRITE", 3L), "NITROGEN DISSOLVED NITRITE"),
#       "MG/L", "NO2", "uM", mg_to_um(14.0067)
#     ),
#     mapping_rows(c("7315", "8266"), "DISSOLVED NITROGEN NITRATE", "MG/L",
#                  "NO3", "uM", mg_to_um(14.0067)),
#     mapping_rows(
#       c("1110", "2233"),
#       c("DISSOLVED NITRITE/NITRATE", "NITRATE AND NITRITE (AS N)"),
#       "MG/L", "NOx", "uM", mg_to_um(14.0067)
#     ),
#     mapping_rows(
#       c("1072", "15253"),
#       c("ORTHOPHOSPHORUS DISSOLVED", "ORTHOPHOSPHATE DISSOLVED"),
#       "MG/L", "PO4", "uM", mg_to_um(30.973761998)
#     ),
#     mapping_rows(
#       c("108312", "16309", "8265", "1972"),
#       c(rep("SULPHATE DISSOLVED", 3L), "SULPHATE"),
#       "MG/L", "SO4", "uM", mg_to_um(96.060)
#     ),
#     mapping_rows(c("108768", "1130", "1967", "9326"),
#                  "NITROGEN TOTAL", "MG/L", "TN", "uM",
#                  mg_to_um(14.0067)),
#     mapping_rows(c("109108", "15406", "2013", "25359"),
#                  "PHOSPHORUS TOTAL", "MG/L", "TP", "uM",
#                  mg_to_um(30.973761998)),
#     mapping_rows(c("109107", "1204", "15102", "2010"),
#                  "PHOSPHORUS TOTAL DISSOLVED", "MG/L", "tdp", "uM",
#                  mg_to_um(30.973761998)),
#     mapping_rows(c("108767", "1968", "7657"),
#                  "NITROGEN TOTAL DISSOLVED", "MG/L",
#                  "tot dissolved N", "uM", mg_to_um(14.0067)),
#     mapping_rows(c("10101", "10111", "1131", "9134"),
#                  "ALKALINITY TOTAL CACO3", "MG/L",
#                  "alkalinity", "ueq/L", 1000 / 50.04345),
#     mapping_rows("1963", "COLOUR TRUE", "CTU",
#                  "colour PCU", "PCU", 1),
#     mapping_rows(c("100300", "101252", "1974", "201144"),
#                  "CARBON DISSOLVED INORGANIC", "MG/L",
#                  "dissolved inorg C", "uM", mg_to_um(12.011)),
#     mapping_rows(c("106008", "201145", "6104"),
#                  "CARBON DISSOLVED ORGANIC", "MG/L",
#                  "dissolved org C", "uM", mg_to_um(12.011)),
#     mapping_rows(c("1064", "110002"), "OXYGEN DISSOLVED", "MG/L",
#                  "dissolved oxygen", "uM", mg_to_um(31.998)),
#     mapping_rows(c("2235", "6009"), "CARBON TOTAL ORGANIC", "MG/L",
#                  "tot org C", "uM", mg_to_um(12.011)),
#     mapping_rows(c("10401", "1145", "1961"), "RESIDUE NONFILTERABLE", "MG/L",
#                  "tss   dry mass", "mg/L", 1),
#     mapping_rows(c("157", "2074"), "TURBIDITY", "NTU",
#                  "turbidity", "NTU", 1),
#     mapping_rows(c("1125", "15544"), "TEMPERATURE (WATER)", "DEG C",
#                  "temp", "C", 1),
#     mapping_rows(c("109026", "10301", "1075", "1965"),
#                  c("PH (FIELD)", rep("PH", 3L)), "PH",
#                  "pH", NA_character_, 1),
#     mapping_rows(c("109027", "2", "2041"),
#                  c("SPECIFIC CONDUCTANCE (FIELD)",
#                    rep("SPECIFIC CONDUCTANCE", 2L)),
#                  "µS/CM", "specific conductivity", "uS/cm", 1)
#   ))
#
#   expected_solutes <- c(
#     "Al", "Br", "Ca", "Cl", "F", "Fe", "K", "Li", "Mg", "Mn",
#     "DSi", "NHX", "NO2", "NO3", "NOx", "Na", "PO4", "SO4", "Sr", "TN",
#     "TP", "Zn", "alkalinity", "colour PCU", "dissolved inorg C",
#     "dissolved org C", "dissolved oxygen", "pH", "specific conductivity",
#     "tdp", "temp", "tot dissolved N", "tot org C", "tss   dry mass",
#     "turbidity"
#   )
#   if (
#     anyDuplicated(mapping$VMV_CODE) > 0L ||
#       !setequal(unique(mapping$solute), expected_solutes) ||
#       length(unique(mapping$solute)) != 35L
#   ) {
#     stop("the reviewed Canada provider mapping changed")
#   }
#
#   variable_info <- read.csv(
#     canada_variable_file,
#     check.names = FALSE,
#     stringsAsFactors = FALSE,
#     na.strings = "",
#     colClasses = "character",
#     comment.char = ""
#   )
#   mapping_key <- paste(
#     mapping$VMV_CODE,
#     mapping$Provider_Variable,
#     mapping$Provider_Units,
#     sep = "\r"
#   )
#   variable_info_key <- paste(
#     variable_info$VMV_CODE,
#     variable_info$VARIABLE,
#     variable_info[["UNIT_UNITÉ"]],
#     sep = "\r"
#   )
#   mapping_info <- match(mapping_key, variable_info_key)
#   method_counts <- vapply(mapping_key, function(key) {
#     length(unique(variable_info$METHOD_CODE[variable_info_key == key]))
#   }, integer(1))
#   bad_info <- is.na(mapping_info)
#   bad_method <- method_counts != 1L
#   bad_variable <- rep(FALSE, nrow(mapping))
#   bad_units <- rep(FALSE, nrow(mapping))
#   available_info <- !bad_info
#   bad_variable[available_info] <-
#     mapping$Provider_Variable[available_info] !=
#     variable_info$VARIABLE[mapping_info[available_info]]
#   bad_units[available_info] <-
#     mapping$Provider_Units[available_info] !=
#     variable_info[["UNIT_UNITÉ"]][mapping_info[available_info]]
#   allowed_missing_info <- mapping$VMV_CODE == "1125"
#   if (
#     !identical(mapping$VMV_CODE[bad_info], "1125") ||
#       any(bad_method & !allowed_missing_info) ||
#       any(bad_variable) ||
#       any(bad_units)
#   ) {
#     bad <- (bad_info & !allowed_missing_info) |
#       (bad_method & !allowed_missing_info) | bad_variable | bad_units
#     stop(
#       "the reviewed Canada provider variable information changed for: ",
#       paste(mapping$VMV_CODE[bad], collapse = ", ")
#     )
#   }
#   mapping$Provider_Method_Code <- NA_character_
#   mapping$Provider_Method_Code[available_info] <-
#     variable_info$METHOD_CODE[mapping_info[available_info]]
#
#   required_columns <- c(
#     "SITE_NO", "DATE_TIME_HEURE", "FLAG_MARQUEUR", "VALUE_VALEUR",
#     "SDL_LDE", "MDL_LDM", "VMV_CODE", "UNIT_UNITÉ", "VARIABLE",
#     "STATUS_STATUT", "SAMPLE_ID_ÉCHANTILLON"
#   )
#   provider_parts <- vector("list", length(canada_provider_files))
#   for (file_index in seq_along(canada_provider_files)) {
#     provider_file <- canada_provider_files[[file_index]]
#     provider_part <- read.csv(
#       provider_file,
#       check.names = FALSE,
#       stringsAsFactors = FALSE,
#       na.strings = "",
#       colClasses = "character",
#       comment.char = ""
#     )
#     if (!all(required_columns %in% names(provider_part))) {
#       stop("the reviewed Canada provider columns changed in ",
#            basename(provider_file))
#     }
#     provider_part <- provider_part[
#       provider_part$SITE_NO %in% site_plan$site_no,
#       required_columns,
#       drop = FALSE
#     ]
#     provider_part$Provider_File <- rep(basename(provider_file), nrow(provider_part))
#     provider_parts[[file_index]] <- provider_part
#   }
#   provider <- do.call(rbind, provider_parts)
#   rownames(provider) <- NULL
#
#   assert_count(nrow(provider), 128469L,
#                "six-site Canada provider source rows")
#   assert_count(
#     sum(provider$VMV_CODE == "2021" &
#           provider$VARIABLE == "COLOUR TRUE" &
#           provider[["UNIT_UNITÉ"]] == "REL UNIT"),
#     1573L,
#     "excluded Canada relative-unit colour rows"
#   )
#
#   mapping_index <- match(provider$VMV_CODE, mapping$VMV_CODE)
#   mapped <- provider[!is.na(mapping_index), , drop = FALSE]
#   mapped_mapping <- mapping[mapping_index[!is.na(mapping_index)], , drop = FALSE]
#   if (
#     !same_character(mapped$VARIABLE, mapped_mapping$Provider_Variable) ||
#       !same_character(mapped[["UNIT_UNITÉ"]], mapped_mapping$Provider_Units)
#   ) {
#     stop("a reviewed Canada provider code changed variable or unit")
#   }
#
#   mapped$date <- substr(mapped$DATE_TIME_HEURE, 1L, 10L)
#   mapped$solute <- mapped_mapping$solute
#   mapped$solute_units <- mapped_mapping$solute_units
#   mapped$Provider_Method_Code <- mapped_mapping$Provider_Method_Code
#   mapped$numeric_value <- as_number(mapped$VALUE_VALEUR)
#   mapped$converted_value <- mapped$numeric_value * mapped_mapping$factor
#   mapped$censored <- !is_blank(mapped$FLAG_MARQUEUR) &
#     trimws(mapped$FLAG_MARQUEUR) == "<"
#
#   if (
#     any(!grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", mapped$date)) ||
#       any(is.na(as.Date(mapped$date))) ||
#       any(is.na(mapped$numeric_value)) ||
#       any(!is.finite(mapped$converted_value)) ||
#       any(!is_blank(mapped$FLAG_MARQUEUR) &
#             trimws(mapped$FLAG_MARQUEUR) != "<") ||
#       any(mapped$censored & mapped$converted_value <= 0)
#   ) {
#     stop("the reviewed Canada provider values, dates, or flags changed")
#   }
#
#   assert_count(nrow(mapped), 41004L,
#                "mapped Canada provider rows before method precedence")
#   assert_count(sum(mapped$censored), 8095L,
#                "mapped Canada provider censored rows")
#   assert_count(sum(mapped$solute == "DSi"), 600L,
#                "mapped Canada dissolved-silicon rows")
#   for (site_row in seq_len(nrow(site_plan))) {
#     assert_count(
#       sum(mapped$SITE_NO == site_plan$site_no[site_row]),
#       site_plan$matched_rows[site_row],
#       paste0(site_plan$site_no[site_row], " mapped Canada rows")
#     )
#     assert_count(
#       sum(
#         mapped$SITE_NO == site_plan$site_no[site_row] &
#           mapped$solute == "DSi"
#       ),
#       site_plan$dissolved_si_days[site_row],
#       paste0(site_plan$site_no[site_row], " dissolved-silicon rows")
#     )
#   }
#
#   cold_review_plan <- data.frame(
#     site_no = c("BC08NA0033", "BC08NA0033", "BC08NF0001", "BC08NF0001"),
#     date_time = c(
#       "2004-12-29 09:45", "2007-01-02 08:45",
#       "2001-02-12 11:45", "2002-12-30 09:30"
#     ),
#     sample_id = c("04PY002644", "07PY000426", "01PY000684", "03PY000001"),
#     value = c(-2.8, -5, -4, -9),
#     stringsAsFactors = FALSE
#   )
#   cold_review <- rep(FALSE, nrow(mapped))
#   for (review_row in seq_len(nrow(cold_review_plan))) {
#     plan <- cold_review_plan[review_row, , drop = FALSE]
#     target <- mapped$SITE_NO == plan$site_no &
#       mapped$DATE_TIME_HEURE == plan$date_time &
#       mapped[["SAMPLE_ID_ÉCHANTILLON"]] == plan$sample_id &
#       mapped$VMV_CODE == "15544" &
#       mapped$VARIABLE == "TEMPERATURE (WATER)" &
#       mapped[["UNIT_UNITÉ"]] == "DEG C" &
#       mapped$Provider_Method_Code == "4456" &
#       mapped$Provider_File == "Water-Qual-Eau-Columbia-2000-present.csv" &
#       mapped$STATUS_STATUT == "P" &
#       is_blank(mapped$FLAG_MARQUEUR) &
#       is_blank(mapped$SDL_LDE) &
#       is_blank(mapped$MDL_LDM) &
#       abs(mapped$numeric_value - plan$value) < 1e-12
#     target[is.na(target)] <- FALSE
#     assert_count(sum(target), 1L,
#                  paste0("reviewed Canada cold value ", plan$sample_id))
#     cold_review <- cold_review | target
#   }
#   assert_count(sum(cold_review), 4L, "reviewed Canada cold rows")
#   # keep reported cold values for later outlier review
#   assert_values(
#     mapped$numeric_value[cold_review],
#     cold_review_plan$value,
#     "prepared Canada reviewed cold-temperature values"
#   )
#   assert_count(nrow(mapped), 41004L,
#                "mapped Canada provider rows after cold-value review")
#
#   # use field ph and conductance first when lab values occur on the same day
#   site_day <- paste(mapped$SITE_NO, mapped$date, sep = "\r")
#   field_ph_days <- unique(site_day[mapped$VMV_CODE == "109026"])
#   field_conductance_days <- unique(site_day[mapped$VMV_CODE == "109027"])
#   drop_lab_ph <- mapped$VMV_CODE %in% c("10301", "1075", "1965") &
#     site_day %in% field_ph_days
#   drop_lab_conductance <- mapped$VMV_CODE %in% c("2", "2041") &
#     site_day %in% field_conductance_days
#   precedence_drop <- drop_lab_ph | drop_lab_conductance
#
#   assert_count(sum(drop_lab_ph), 7L,
#                "Canada lab pH rows superseded by field pH")
#   assert_count(sum(drop_lab_conductance), 7L,
#                "Canada lab conductance rows superseded by field conductance")
#   assert_count(sum(precedence_drop & mapped$censored), 0L,
#                "censored Canada rows removed by method precedence")
#
#   selected <- mapped[!precedence_drop, , drop = FALSE]
#   rownames(selected) <- NULL
#   assert_count(nrow(selected), 40990L,
#                "selected Canada provider rows before daily aggregation")
#   assert_count(sum(selected$censored), 8095L,
#                "selected Canada provider censored rows")
#   for (site_row in seq_len(nrow(site_plan))) {
#     assert_count(
#       sum(selected$SITE_NO == site_plan$site_no[site_row]),
#       site_plan$selected_rows[site_row],
#       paste0(site_plan$site_no[site_row], " selected Canada rows")
#     )
#   }
#
#   unit_key <- ifelse(is.na(selected$solute_units), "<blank>",
#                      selected$solute_units)
#   daily_key <- paste(
#     selected$SITE_NO,
#     selected$date,
#     selected$solute,
#     unit_key,
#     sep = "\r"
#   )
#   daily_groups <- split(seq_len(nrow(selected)), daily_key)
#   group_sizes <- lengths(daily_groups)
#   assert_count(sum(group_sizes > 1L), 3L,
#                "Canada provider repeated daily keys")
#   assert_count(sum(group_sizes - 1L), 3L,
#                "Canada provider repeated daily rows")
#
#   collapse_provenance <- function(x) {
#     values <- as.character(x)
#     values[is.na(values)] <- ""
#     collapsed <- paste(sort(unique(values)), collapse = ";")
#     if (collapsed == "") NA_character_ else collapsed
#   }
#
#   method_flag_by_code <- c(
#     "1175" = "dissolved Si, ECCC",
#     "13812" = "dissolved Si, ECCC",
#     "109026" = "field pH",
#     "10301" = "laboratory pH fallback",
#     "1075" = "laboratory pH fallback",
#     "1965" = "laboratory pH fallback",
#     "109027" = "field conductivity",
#     "2" = "laboratory conductivity fallback",
#     "2041" = "laboratory conductivity fallback"
#   )
#
#   # average only detected values when some canada samples are below a limit
#   # if all samples are below the limit, average the thresholds and keep the sign
#   daily_rows <- vector("list", length(daily_groups))
#   names(daily_rows) <- names(daily_groups)
#   for (group_number in seq_along(daily_groups)) {
#     row_index <- daily_groups[[group_number]]
#     detected <- !selected$censored[row_index]
#     has_detections <- any(detected)
#     has_limits <- any(!detected)
#
#     # detected measurements alone define the mean on mixed days
#     used_rows <- row_index
#     source_lab_flag <- NA_character_
#     if (has_detections) {
#       used_rows <- row_index[detected]
#       if (has_limits) {
#         source_lab_flag <- "below-limit results excluded from daily mean"
#       }
#     } else {
#       source_lab_flag <- "below reported limit"
#     }
#     source_method_values <- unique(unname(
#       method_flag_by_code[selected$VMV_CODE[row_index]]
#     ))
#     source_method_values <- source_method_values[!is.na(source_method_values)]
#     if (length(source_method_values) > 1L) {
#       stop("multiple retained Canada methods remain within one daily key")
#     }
#     source_method_flag <- if (length(source_method_values) == 1L) {
#       source_method_values
#     } else {
#       NA_character_
#     }
#     daily_value <- mean(selected$converted_value[used_rows])
#     amount <- sprintf("%.17g", daily_value)
#     if (!has_detections) {
#       amount <- paste0("<", amount)
#     }
#
#     # keep original source details from all samples, even those left out of the average
#     daily_rows[[group_number]] <- data.frame(
#       LTER = "Canada",
#       Stream_Name = site_plan$stream_name[
#         match(selected$SITE_NO[row_index[1L]], site_plan$site_no)
#       ],
#       date = selected$date[row_index[1L]],
#       solute = selected$solute[row_index[1L]],
#       solute_units = selected$solute_units[row_index[1L]],
#       amount = amount,
#       Source_Lab_Flag = source_lab_flag,
#       Source_Method_Flag = source_method_flag,
#       Provider_Record_Source = "ECCC current provider",
#       Provider_File = collapse_provenance(selected$Provider_File[row_index]),
#       Provider_VMV_Code = collapse_provenance(selected$VMV_CODE[row_index]),
#       Provider_Method_Code = collapse_provenance(
#         selected$Provider_Method_Code[row_index]
#       ),
#       Provider_Sample_ID = collapse_provenance(
#         selected[["SAMPLE_ID_ÉCHANTILLON"]][row_index]
#       ),
#       Provider_Date_Time = collapse_provenance(
#         selected$DATE_TIME_HEURE[row_index]
#       ),
#       Provider_Variable = collapse_provenance(selected$VARIABLE[row_index]),
#       Provider_Units = collapse_provenance(
#         selected[["UNIT_UNITÉ"]][row_index]
#       ),
#       Provider_Value = collapse_provenance(selected$VALUE_VALEUR[row_index]),
#       Provider_Flag = collapse_provenance(selected$FLAG_MARQUEUR[row_index]),
#       Provider_SDL = collapse_provenance(selected$SDL_LDE[row_index]),
#       Provider_MDL = collapse_provenance(selected$MDL_LDM[row_index]),
#       Provider_Status = collapse_provenance(selected$STATUS_STATUT[row_index]),
#       .site_no = selected$SITE_NO[row_index[1L]],
#       stringsAsFactors = FALSE
#     )
#   }
#   daily <- do.call(rbind, daily_rows)
#   rownames(daily) <- NULL
#
#   assert_count(nrow(daily), 40987L, "official Canada provider daily keys")
#   assert_count(sum(grepl("^<", daily$amount)), 8093L,
#                "official Canada provider qualified daily keys")
#   assert_count(sum(daily$solute == "NO3"), 2139L,
#                "official Canada provider nitrate days")
#   assert_count(sum(daily$solute == "DSi"), 600L,
#                "official Canada dissolved-silicon days")
#   assert_count(
#     sum(daily$Source_Method_Flag == "dissolved Si, ECCC", na.rm = TRUE),
#     600L,
#     "official Canada dissolved-silicon method flags"
#   )
#   method_variables <- daily$solute %in% c(
#     "DSi", "pH", "specific conductivity"
#   )
#   if (
#     any(is.na(daily$Source_Method_Flag[method_variables])) ||
#       any(!method_variables & !is.na(daily$Source_Method_Flag))
#   ) {
#     stop("prepared Canada method flags changed")
#   }
#   for (site_row in seq_len(nrow(site_plan))) {
#     site_daily <- daily$.site_no == site_plan$site_no[site_row]
#     assert_count(
#       sum(site_daily),
#       site_plan$provider_days[site_row],
#       paste0(site_plan$site_no[site_row], " Canada provider daily keys")
#     )
#     assert_count(
#       sum(site_daily & grepl("^<", daily$amount)),
#       site_plan$qualified_days[site_row],
#       paste0(site_plan$site_no[site_row], " qualified Canada provider days")
#     )
#   }
#
#   expand_provenance <- function(x) {
#     x <- x[!is.na(x) & x != ""]
#     values <- unlist(strsplit(x, ";", fixed = TRUE))
#     sort(unique(values[values != ""]))
#   }
#   provenance_pairs <- list(
#     VMV_CODE = c("VMV_CODE", "Provider_VMV_Code"),
#     Provider_Method_Code = c(
#       "Provider_Method_Code", "Provider_Method_Code"
#     ),
#     Sample_ID = c("SAMPLE_ID_ÉCHANTILLON", "Provider_Sample_ID"),
#     SDL = c("SDL_LDE", "Provider_SDL"),
#     MDL = c("MDL_LDM", "Provider_MDL")
#   )
#   for (pair_name in names(provenance_pairs)) {
#     source_column <- provenance_pairs[[pair_name]][1L]
#     output_column <- provenance_pairs[[pair_name]][2L]
#     source_values <- expand_provenance(selected[[source_column]])
#     output_values <- expand_provenance(daily[[output_column]])
#     if (
#       length(source_values) != length(output_values) ||
#         !setequal(source_values, output_values)
#     ) {
#       stop(
#         "Canada provider ", pair_name, " provenance changed; missing: ",
#         paste(setdiff(source_values, output_values), collapse = ","),
#         "; added: ",
#         paste(setdiff(output_values, source_values), collapse = ",")
#       )
#     }
#   }
#
#   legacy_plan <- data.frame(
#     site_no = c(
#       "BC08EF0001", "YT08AB0009", "YT08AB0009", "YT08AB0009",
#       rep("YT10AA0001", 5L)
#     ),
#     date = c(
#       "2005-06-05", "2017-02-09", "2023-01-27", "2023-01-27",
#       rep("2025-07-29", 5L)
#     ),
#     solute = c(
#       "TP", "dissolved org C", "TN", "tot dissolved N",
#       "Al", "Fe", "Mn", "Sr", "Zn"
#     ),
#     solute_units = "uM",
#     expected_amount = c(
#       3.29310982631041, 99.9084172841562, 6.42549636959455,
#       8.56732849279273, 4.61426615832155, 4.57516339869281,
#       0.240270658343788, 1.38096325039945, 0.0252370755582747
#     ),
#     stringsAsFactors = FALSE
#   )
#   legacy_rows <- vector("list", nrow(legacy_plan))
#   source_by_file <- setNames(
#     lapply(site_plan$filename, read_source),
#     site_plan$filename
#   )
#   for (site_row in seq_len(nrow(site_plan))) {
#     plan <- site_plan[site_row, , drop = FALSE]
#     assert_count(
#       nrow(source_by_file[[plan$filename]]),
#       plan$source_rows,
#       paste0(plan$filename, " source rows")
#     )
#   }
#
#   for (legacy_index in seq_len(nrow(legacy_plan))) {
#     plan <- legacy_plan[legacy_index, , drop = FALSE]
#     filename <- site_plan$filename[match(plan$site_no, site_plan$site_no)]
#     source_data <- source_by_file[[filename]]
#     keep <- source_data$date == plan$date &
#       source_data$solute == plan$solute &
#       source_data$solute_units == plan$solute_units
#     keep[is.na(keep)] <- FALSE
#     assert_count(sum(keep), 1L,
#                  paste0("legacy-only Canada row ", legacy_index))
#     source_row <- source_data[keep, , drop = FALSE]
#     assert_values(as_number(source_row$amount), plan$expected_amount,
#                   paste0("legacy-only Canada amount ", legacy_index))
#     if (grepl("^[<>]", source_row$amount)) {
#       stop("a legacy-only Canada row unexpectedly has a qualifier")
#     }
#
#     legacy_rows[[legacy_index]] <- data.frame(
#       source_row[, c(
#         "LTER", "Stream_Name", "date", "solute", "solute_units", "amount"
#       )],
#       Source_Lab_Flag = NA_character_,
#       Source_Method_Flag = NA_character_,
#       Provider_Record_Source = "legacy-only whitelist",
#       Provider_File = NA_character_,
#       Provider_VMV_Code = NA_character_,
#       Provider_Method_Code = NA_character_,
#       Provider_Sample_ID = NA_character_,
#       Provider_Date_Time = NA_character_,
#       Provider_Variable = NA_character_,
#       Provider_Units = NA_character_,
#       Provider_Value = NA_character_,
#       Provider_Flag = NA_character_,
#       Provider_SDL = NA_character_,
#       Provider_MDL = NA_character_,
#       Provider_Status = NA_character_,
#       .site_no = plan$site_no,
#       stringsAsFactors = FALSE
#     )
#   }
#   legacy <- do.call(rbind, legacy_rows)
#   rownames(legacy) <- NULL
#
#   excluded_source <- source_by_file[[site_plan$filename[
#     site_plan$site_no == "BC08NF0001"
#   ]]]
#   excluded_nitrate <- excluded_source$date == "2022-12-05" &
#     excluded_source$solute == "NO3" &
#     excluded_source$solute_units == "uM"
#   excluded_nitrate[is.na(excluded_nitrate)] <- FALSE
#   assert_count(sum(excluded_nitrate), 1L,
#                "withdrawn legacy Kootenay nitrate row")
#   assert_values(
#     as_number(excluded_source$amount[excluded_nitrate]),
#     2.54823502468401,
#     "withdrawn legacy Kootenay nitrate value"
#   )
#
#   provider_key <- paste(
#     daily$.site_no, daily$date, daily$solute,
#     ifelse(is.na(daily$solute_units), "<blank>", daily$solute_units),
#     sep = "\r"
#   )
#   legacy_key <- paste(
#     legacy$.site_no, legacy$date, legacy$solute,
#     ifelse(is.na(legacy$solute_units), "<blank>", legacy$solute_units),
#     sep = "\r"
#   )
#   if (anyDuplicated(legacy_key) > 0L || any(legacy_key %in% provider_key)) {
#     stop("the reviewed legacy-only Canada keys changed")
#   }
#
#   combined <- rbind(daily, legacy)
#   assert_count(nrow(combined), 40996L,
#                "provider and legacy Canada output rows")
#   assert_count(sum(grepl("^<", combined$amount)), 8093L,
#                "qualified provider and legacy Canada rows")
#   assert_count(length(unique(combined$solute)), 35L,
#                "prepared Canada solutes")
#   assert_count(sum(combined$solute == "NH4"), 0L,
#                "prepared Canada NH4 rows")
#
#   for (site_row in seq_len(nrow(site_plan))) {
#     plan <- site_plan[site_row, , drop = FALSE]
#     filename <- plan$filename
#     before <- source_by_file[[filename]]
#     after <- combined[combined$.site_no == plan$site_no, , drop = FALSE]
#     after$.site_no <- NULL
#     after <- after[
#       order(after$date, after$solute, after$solute_units, after$amount),
#       ,
#       drop = FALSE
#     ]
#     rownames(after) <- NULL
#
#     assert_count(nrow(after), plan$final_rows,
#                  paste0(filename, " prepared rows"))
#     assert_count(
#       sum(after$Provider_Record_Source == "legacy-only whitelist"),
#       plan$legacy_rows,
#       paste0(filename, " legacy-only rows")
#     )
#     assert_count(sum(after$solute == "NH4"), 0L,
#                  paste0(filename, " prepared NH4 rows"))
#     assert_count(
#       sum(after$solute == "DSi"),
#       plan$dissolved_si_days,
#       paste0(filename, " prepared dissolved-silicon rows")
#     )
#     assert_count(
#       sum(
#         after$solute == "DSi" &
#           after$Source_Method_Flag == "dissolved Si, ECCC",
#         na.rm = TRUE
#       ),
#       plan$dissolved_si_days,
#       paste0(filename, " dissolved-silicon method flags")
#     )
#     output_key <- paste(
#       after$date, after$solute,
#       ifelse(is.na(after$solute_units), "<blank>", after$solute_units),
#       sep = "\r"
#     )
#     if (anyDuplicated(output_key) > 0L) {
#       stop("duplicate prepared Canada daily keys remain in ", filename)
#     }
#
#     write_prepared(filename, after)
#   }
# }
#
# # ---- seine reporting units ----
#
# prepare_seine_base <- function() {
#   filename <- "Seine_baseGeo_Naides.csv"
#   before <- read_source(filename)
#   after <- before
#   sediment_calcium <- after$chemical == "Calcium" &
#     after$units == "mg(Ca)/kg"
#   isotope <- after$units == "‰ vs SMOW"
#   sulfate_counts <- table(after$units[after$chemical == "Sulfates"])
#   expected_sulfate_counts <- c(
#     "mg(S)/L" = 8930L,
#     "mg(SO4)/L" = 3380L,
#     "mg/L" = 405L,
#     "‰ vs SMOW" = 1L
#   )
#
#   assert_count(sum(sediment_calcium, na.rm = TRUE), 69L,
#                "Seine sediment-calcium rows")
#   assert_count(sum(isotope, na.rm = TRUE), 1L, "Seine isotope rows")
#   if (!identical(as.integer(sulfate_counts[names(expected_sulfate_counts)]),
#                  as.integer(expected_sulfate_counts))) {
#     stop("Seine sulfate-unit counts changed")
#   }
#
#   plain_sulfate <- after$chemical == "Sulfates" & after$units == "mg/L"
#   assert_count(sum(!is.na(as_number(after$value[plain_sulfate]))), 357L,
#                "Seine numeric plain-unit sulfate rows")
#   plain_keys <- unique(paste(after$Stream_Name[plain_sulfate &
#     !is.na(as_number(after$value))], after$date[plain_sulfate &
#     !is.na(as_number(after$value))], sep = "\r"))
#   assert_count(length(plain_keys), 347L,
#                "Seine distinct plain-unit sulfate keys")
#
#   change_masks <- list(
#     calcium = after$chemical == "Calcium" & after$units == "mg(Ca)/L",
#     magnesium = after$chemical == "Magnesium" & after$units == "mg(Mg)/L",
#     potassium = after$chemical == "Potassium" & after$units == "mg(K)/L",
#     sulfate_s = after$chemical == "Sulfates" & after$units == "mg(S)/L",
#     sulfate_so4 = after$chemical == "Sulfates" &
#       after$units %in% c("mg(SO4)/L", "mg/L")
#   )
#   expected_changes <- c(
#     calcium = 9555L,
#     magnesium = 9548L,
#     potassium = 9349L,
#     sulfate_s = 8930L,
#     sulfate_so4 = 3785L
#   )
#   observed_changes <- vapply(change_masks, sum, integer(1), na.rm = TRUE)
#   if (!identical(observed_changes, expected_changes)) {
#     stop("Seine reviewed unit-change counts changed")
#   }
#
#   # retain the source molecular basis so sulfur and sulfate are not mixed
#   after$units[change_masks$calcium] <- "mg/L"
#   after$units[change_masks$magnesium] <- "mg/L"
#   after$units[change_masks$potassium] <- "mg/L"
#   after$units[change_masks$sulfate_s] <- "mg S/L"
#   after$units[change_masks$sulfate_so4] <- "mg SO4/L"
#   # exclude sediment and isotope rows from the water-concentration master
#   after <- after[!(sediment_calcium | isotope), , drop = FALSE]
#   rownames(after) <- NULL
#   sulfate_labels <- after$chemical == "Sulfates"
#   assert_count(sum(sulfate_labels, na.rm = TRUE), 12715L,
#                "Seine sulfate labels")
#   after$chemical[sulfate_labels] <- "SO4"
#
#   assert_count(nrow(after), 43639L, "prepared Seine base rows")
#   assert_count(sum(after$units %in% c(
#     "mg(Ca)/kg", "‰ vs SMOW", "mg(Ca)/L", "mg(Mg)/L", "mg(K)/L",
#     "mg(S)/L", "mg(SO4)/L"
#   )), 0L, "prepared Seine old unit labels")
#   assert_count(sum(after$chemical == "Sulfates", na.rm = TRUE), 0L,
#                "prepared Seine old sulfate labels")
#   assert_count(sum(after$chemical == "SO4" & after$units == "mg S/L"),
#                8930L, "prepared Seine sulfate-as-sulfur rows")
#   assert_count(sum(after$chemical == "SO4" & after$units == "mg SO4/L"),
#                3785L, "prepared Seine sulfate-as-sulfate rows")
#
#   write_prepared(filename, after)
# }
#
# # ---- seine source dates ----
#
# prepare_cleaned_seine <- function() {
#   filename <- "Cleaned_Seine_Data.csv"
#   before <- read_source(filename)
#   after <- before
#   invalid_do <- after$station_name == "IVRY-SUR-SEINE" &
#     after$date == "2/10/04" & as_number(after$DO_mgl) == 95
#   invalid_nh3 <- after$station_name == "HEURTEAUVILLE 1" &
#     after$date == "7/11/05" & as_number(after$NH3..mg.l.) == -0.002
#
#   assert_count(sum(invalid_do, na.rm = TRUE), 1L, "Seine invalid oxygen row")
#   assert_count(sum(invalid_nh3, na.rm = TRUE), 1L, "Seine invalid ammonia row")
#   # keep the reported oxygen and ammonia values for later outlier review
#   assert_values(as_number(after$DO_mgl[invalid_do]), 95,
#                 "prepared Seine reviewed oxygen value")
#   assert_values(as_number(after$NH3..mg.l.[invalid_nh3]), -0.002,
#                 "prepared Seine reviewed ammonia value")
#   after$date <- normalize_reviewed_dates(
#     after$date,
#     date_order = "mdy",
#     expected_nonblank = 18411L,
#     expected_two_digit = 18411L,
#     expected_four_digit = 0L,
#     expected_unique = 3155L,
#     expected_range = c("1955-12-05", "2013-12-20"),
#     label = "Seine"
#   )
#
#   write_prepared(filename, after)
# }
#
#
# # ---- murray-darling units and methods ----
#
# prepare_murray <- function() {
#   filename <- "MurrayDarlingChem_Merged.csv"
#   before <- read_source(filename)
#   after <- before
#   suspect_si <- c(
#     "436125" = 110,
#     "436221" = 520,
#     "436222" = 350.1,
#     "436227" = 90.1,
#     "436228" = 860,
#     "436229" = 70,
#     "436230" = 760,
#     "436234" = 430
#   )
#   suspect_rows <- match(names(suspect_si), after[[1L]])
#
#   if (any(is.na(suspect_rows))) {
#     stop("reviewed Murray-Darling suspect silica rows are missing")
#   }
#   assert_count(
#     sum(after[[1L]] %in% names(suspect_si)),
#     8L,
#     "Murray-Darling suspect silica rows"
#   )
#   if (
#     !all(after$Station[suspect_rows] == "414206") ||
#       !all(after$Site[suspect_rows] == "Merbein") ||
#       !all(after$shortname[suspect_rows] == "Si")
#   ) {
#     stop("reviewed Murray-Darling suspect silica identity changed")
#   }
#   assert_values(
#     as_number(after$Value[suspect_rows]),
#     unname(suspect_si),
#     "Murray-Darling suspect silica values"
#   )
#   # keep reported silica values for later review
#   value <- as_number(after$Value)
#   source_key <- paste(after$Site, after$Datetime, sep = "\r")
#   primary_alkalinity <- after$shortname == "Alk_CaCO3" & !is.na(value)
#   fallback_alkalinity <- after$shortname == "BiCarbAlk_CaCO3" &
#     !is.na(value)
#   primary_keys <- unique(source_key[primary_alkalinity])
#   fallback_keys <- unique(source_key[fallback_alkalinity])
#   overlap_keys <- intersect(primary_keys, fallback_keys)
#   fallback_overlap <- fallback_alkalinity & source_key %in% overlap_keys
#
#   assert_count(sum(primary_alkalinity), 2063L,
#                "Murray-Darling primary alkalinity rows")
#   assert_count(length(primary_keys), 2063L,
#                "Murray-Darling primary alkalinity keys")
#   assert_count(sum(fallback_alkalinity), 121L,
#                "Murray-Darling fallback alkalinity rows")
#   assert_count(length(fallback_keys), 121L,
#                "Murray-Darling fallback alkalinity keys")
#   assert_count(length(overlap_keys), 94L,
#                "Murray-Darling alkalinity overlap keys")
#   assert_count(sum(fallback_overlap), 94L,
#                "Murray-Darling superseded fallback alkalinity rows")
#   assert_count(length(setdiff(fallback_keys, primary_keys)), 27L,
#                "Murray-Darling fallback-only alkalinity keys")
#
#   primary_values <- stats::setNames(
#     value[primary_alkalinity],
#     source_key[primary_alkalinity]
#   )
#   fallback_values <- stats::setNames(
#     value[fallback_alkalinity],
#     source_key[fallback_alkalinity]
#   )
#   disagreement_keys <- overlap_keys[
#     primary_values[overlap_keys] != fallback_values[overlap_keys]
#   ]
#   expected_disagreement <- paste("Swan Hill", "2008-08-26", sep = "\r")
#   if (
#     !identical(disagreement_keys, expected_disagreement) ||
#       primary_values[expected_disagreement] != 20 ||
#       fallback_values[expected_disagreement] != 19
#   ) {
#     stop("the reviewed Murray-Darling alkalinity disagreement changed")
#   }
#
#   # prefer total alkalinity and use bicarbonate alkalinity as a fallback
#   after$source_method_flag <- NA_character_
#   after$source_method_flag[primary_alkalinity] <- "total alkalinity"
#   after$source_method_flag[fallback_alkalinity & !fallback_overlap] <-
#     "bicarbonate alkalinity fallback"
#   after$Value[fallback_overlap] <- NA_character_
#   alkalinity_labels <- after$shortname %in%
#     c("Alk_CaCO3", "BiCarbAlk_CaCO3")
#   water_temperature_labels <- after$shortname == "Water_Temp"
#   other_temperature_labels <- after$shortname == "Temp"
#   assert_count(sum(alkalinity_labels), 2184L,
#                "Murray-Darling alkalinity labels")
#   assert_count(sum(water_temperature_labels), 15141L,
#                "Murray-Darling water-temperature labels")
#   assert_count(sum(other_temperature_labels), 18882L,
#                "Murray-Darling unmapped temperature labels")
#   after$shortname[alkalinity_labels] <- "alkalinity"
#
#   assert_count(sum(after$shortname %in%
#     c("Alk_CaCO3", "BiCarbAlk_CaCO3")), 0L,
#     "prepared Murray-Darling old alkalinity labels")
#   assert_count(sum(after$shortname == "alkalinity"), 2184L,
#                "prepared Murray-Darling alkalinity labels")
#   assert_count(sum(after$shortname == "alkalinity" &
#     !is.na(as_number(after$Value))), 2090L,
#     "prepared Murray-Darling retained alkalinity rows")
#   assert_count(
#     sum(after$source_method_flag == "total alkalinity", na.rm = TRUE),
#     2063L,
#     "Murray-Darling total alkalinity method flags"
#   )
#   assert_count(
#     sum(
#       after$source_method_flag == "bicarbonate alkalinity fallback",
#       na.rm = TRUE
#     ),
#     27L,
#     "Murray-Darling bicarbonate fallback method flags"
#   )
#   assert_count(sum(after$shortname == "Water_Temp"), 15141L,
#                "prepared Murray-Darling water-temperature labels")
#   assert_count(sum(after$shortname == "Temp"), 18882L,
#                "prepared Murray-Darling unmapped temperature labels")
#   temperature_rows <- before$shortname %in% c("Temp", "Water_Temp")
#   if (
#     !same_character(
#       before$shortname[temperature_rows],
#       after$shortname[temperature_rows]
#     ) ||
#       !same_character(before$Value[temperature_rows], after$Value[temperature_rows]) ||
#       !same_character(before$Varnam[temperature_rows], after$Varnam[temperature_rows])
#   ) {
#     stop("Murray-Darling temperature labels, values, or descriptions changed")
#   }
#
#   site_map <- c(
#     "Euston" = "Euston Weir",
#     "Mitta Mitta at Talandoon" = "Mitta Mitta at Tallandoon",
#     "Murrimbidgee River @ Balranald" = "Murrumbidgee River at Balranald",
#     "Rufus River Junct" = "Rufus River Junction"
#   )
#   expected_names <- c(
#     "Euston" = 16434L,
#     "Mitta Mitta at Talandoon" = 27781L,
#     "Murrimbidgee River @ Balranald" = 15892L,
#     "Rufus River Junct" = 12493L
#   )
#   observed_names <- vapply(
#     names(site_map),
#     function(name) sum(after$Site == name, na.rm = TRUE),
#     integer(1)
#   )
#   if (!identical(observed_names, expected_names)) {
#     stop("Murray-Darling reviewed source-name counts changed")
#   }
#   for (old_name in names(site_map)) {
#     after$Site[after$Site == old_name] <- unname(site_map[[old_name]])
#   }
#
#   value <- as_number(after$Value)
#   zero_conductivity <- after$shortname == "Conduct" & value == 0
#   zero_field_ec <- after$shortname == "Field_EC" & value == 0
#   assert_count(sum(zero_conductivity, na.rm = TRUE), 30L,
#                "Murray-Darling zero-conductivity rows")
#   assert_count(sum(zero_field_ec, na.rm = TRUE), 38L,
#                "Murray-Darling zero field-EC rows")
#   # keep reported zero conductivity for later outlier review
#
#   if (!same_character(before$Value[zero_field_ec], after$Value[zero_field_ec])) {
#     stop("Murray-Darling unkeyed field EC changed")
#   }
#   assert_count(sum(after$Site %in% names(site_map)), 0L,
#                "prepared Murray-Darling old names")
#   write_prepared(filename, after)
# }
#
# # ---- merbein source checks ----
#
# prepare_md_merbein <- function() {
#   filename <- "MD__MD__Merbein__9df18bcf9b24.csv"
#   before <- read_source(filename)
#   after <- before
#   suspect_values <- c(
#     "1987-09-08\rDSi" = 1830.78549019281,
#     "1989-08-22\rDSi" = 1499.57975151248,
#     "1989-09-05\rDSi" = 1165.04531194088,
#     "1989-12-05\rZn" = 1000.30590394616,
#     "1991-01-29\rZn" = 1300.09177118385
#   )
#   source_key <- paste(after$date, after$solute, sep = "\r")
#   suspect_rows <- match(names(suspect_values), source_key)
#
#   if (any(is.na(suspect_rows))) {
#     stop("reviewed Merbein suspect rows are missing")
#   }
#   assert_count(
#     sum(source_key %in% names(suspect_values)),
#     5L,
#     "Merbein suspect rows"
#   )
#   if (
#     !all(after$LTER[suspect_rows] == "MD") ||
#       !all(after$Stream_Name[suspect_rows] == "Merbein") ||
#       !all(after$solute_units[suspect_rows] == "uM")
#   ) {
#     stop("reviewed Merbein suspect row identity changed")
#   }
#   assert_values(
#     as_number(after$amount[suspect_rows]),
#     unname(suspect_values),
#     "Merbein suspect row values"
#   )
#   # keep these five reported values for later outlier review
#   assert_count(
#     sum(!is.na(as_number(after$amount[suspect_rows]))),
#     5L,
#     "prepared Merbein retained reviewed rows"
#   )
#
#   write_prepared(filename, after)
# }
#
# # ---- cameroon source structure and dates ----
#
# prepare_cameroon <- function() {
#   filename <- "Chem_Cameroon.csv"
#   before <- read_source(filename)
#   after <- before
#   # the data key now records alkalinity as ueq/l instead of the old um label
#   sample_date <- as.Date(after$Date, format = "%m/%d/%y")
#
#   assert_count(nrow(after), 3088L, "Cameroon rows")
#   # these are the first, legitimate june 16 records in the 2021 sequences
#   assert_count(sum(after$Date == "06/16/21", na.rm = TRUE), 6L,
#                "Cameroon retained June 2021 rows")
#   assert_count(sum(after$Date == "06/16/22", na.rm = TRUE), 0L,
#                "Cameroon absent June 2022 block")
#   if (!identical(range(sample_date), as.Date(c("1994-08-11", "2022-11-04")))) {
#     stop("Cameroon date range changed")
#   }
#
#   if (!requireNamespace("readxl", quietly = TRUE)) {
#     stop("the Cameroon date repair requires the readxl package")
#   }
#
#   provider_plan <- data.frame(
#     site = c(
#       "Mbalmayo", "Messam", "Nsimi_outlet", "Nsimi_spring", "Olama",
#       "Pont_So'o"
#     ),
#     workbook = c(
#       "Mbalmayo_hydrochemistry.xlsx",
#       "Messam_hydrochemistry.xlsx",
#       "Nsimi_outlet_hydrochemistry.xlsx",
#       "Nsimi_spring_hydrochemistry.xlsx",
#       "Olama_hydrochemistry.xlsx",
#       "Pont_So'o_hydrochemistry.xlsx"
#     ),
#     first_excel_row = c(494L, 475L, 495L, 527L, 491L, 520L),
#     second_excel_row = c(514L, 495L, 515L, 547L, 511L, 540L),
#     stringsAsFactors = FALSE
#   )
#   provider_columns <- c(
#     "Date", "WT", "pH", "EC", "Na+", "K+", "Mg2+", "Ca2+", "F-",
#     "Cl-", "NO3-", "SO42-", "Alk", "HCO3-", "Si", "DOC", "SPM"
#   )
#   provider_measurements <- c(
#     "pH", "EC", "Na+", "K+", "Mg2+", "Ca2+", "F-", "Cl-", "NO3-",
#     "SO42-", "Alk", "HCO3-", "Si", "DOC", "SPM"
#   )
#   prepared_measurements <- c(
#     "pH", "EC", "Na", "K", "Mg", "Ca", "F", "Cl", "NO3", "SO4",
#     "Alk", "HCO3", "Si", "DOC", "SPM"
#   )
#
#   extract_dir <- tempfile("cameroon-provider-")
#   dir.create(extract_dir)
#   on.exit(unlink(extract_dir, recursive = TRUE, force = TRUE), add = TRUE)
#   utils::unzip(cameroon_provider_zip, exdir = extract_dir)
#   provider_files <- list.files(
#     extract_dir,
#     pattern = "[.]xlsx$",
#     recursive = TRUE,
#     full.names = TRUE
#   )
#
#   parse_provider_dates <- function(x) {
#     if (inherits(x, "POSIXt") || inherits(x, "Date")) {
#       return(as.Date(x))
#     }
#     text <- trimws(as.character(x))
#     number <- as_number(text)
#     parsed <- rep(as.Date(NA), length(text))
#     numeric_rows <- !is.na(number)
#     parsed[numeric_rows] <- as.Date(
#       number[numeric_rows],
#       origin = "1899-12-30"
#     )
#     iso_rows <- is.na(parsed) & grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}", text)
#     parsed[iso_rows] <- as.Date(
#       substr(text[iso_rows], 1L, 10L),
#       format = "%Y-%m-%d"
#     )
#     parsed
#   }
#
#   restored_rows <- vector("list", nrow(provider_plan))
#   for (site_index in seq_len(nrow(provider_plan))) {
#     plan <- provider_plan[site_index, , drop = FALSE]
#     provider_file <- provider_files[
#       basename(provider_files) == plan$workbook
#     ]
#     assert_count(length(provider_file), 1L,
#                  paste0(plan$site, " Cameroon provider workbook"))
#     provider <- readxl::read_excel(provider_file)
#     expected_provider_columns <- if (plan$site == "Nsimi_outlet") {
#       c(provider_columns, "POC")
#     } else {
#       provider_columns
#     }
#     if (!identical(names(provider), expected_provider_columns)) {
#       stop("the reviewed Cameroon provider columns changed for ", plan$site)
#     }
#
#     provider_date <- parse_provider_dates(provider$Date)
#     june_rows <- which(provider_date == as.Date("2021-06-16")) + 1L
#     expected_june_rows <- c(plan$first_excel_row, plan$second_excel_row)
#     if (!identical(june_rows, expected_june_rows)) {
#       stop("the reviewed Cameroon June rows changed for ", plan$site)
#     }
#     second_index <- plan$second_excel_row - 1L
#     if (
#       provider_date[second_index - 1L] != as.Date("2022-05-31") ||
#         provider_date[second_index + 1L] != as.Date("2022-07-06")
#     ) {
#       stop("the reviewed Cameroon 2022 sequence changed for ", plan$site)
#     }
#
#     existing <- before$Site == plan$site & before$Date == "06/16/21"
#     assert_count(sum(existing, na.rm = TRUE), 1L,
#                  paste0(plan$site, " retained Cameroon 2021 row"))
#     first_index <- plan$first_excel_row - 1L
#     for (measurement_index in seq_along(prepared_measurements)) {
#       prepared_name <- prepared_measurements[measurement_index]
#       provider_name <- provider_measurements[measurement_index]
#       prepared_value <- as_number(before[[prepared_name]][existing])
#       provider_value <- as_number(provider[[provider_name]][first_index])
#       if (
#         xor(is.na(prepared_value), is.na(provider_value)) ||
#           (!is.na(prepared_value) &&
#              abs(prepared_value - provider_value) > 1e-10)
#       ) {
#         stop(
#           "the retained Cameroon 2021 value changed for ", plan$site,
#           " and ", prepared_name
#         )
#       }
#     }
#
#     restored <- as.data.frame(
#       stats::setNames(
#         replicate(ncol(after), NA_character_, simplify = FALSE),
#         names(after)
#       ),
#       stringsAsFactors = FALSE
#     )
#     for (measurement_index in seq_along(prepared_measurements)) {
#       prepared_name <- prepared_measurements[measurement_index]
#       provider_name <- provider_measurements[measurement_index]
#       provider_value <- provider[[provider_name]][second_index]
#       restored[[prepared_name]] <- if (is.na(provider_value)) {
#         NA_character_
#       } else {
#         as.character(provider_value)
#       }
#     }
#     restored$Date <- "06/16/22"
#     restored$Site <- plan$site
#     assert_count(
#       sum(!is.na(as_number(unlist(
#         restored[prepared_measurements],
#         use.names = FALSE
#       )))),
#       11L,
#       paste0(plan$site, " restored Cameroon measurements")
#     )
#     restored_rows[[site_index]] <- restored
#   }
#
#   after <- rbind(after, do.call(rbind, restored_rows))
#   rownames(after) <- NULL
#   assert_count(nrow(after), 3094L, "prepared Cameroon rows")
#   assert_count(sum(after$Date == "06/16/21", na.rm = TRUE), 6L,
#                "prepared Cameroon June 2021 rows")
#   assert_count(sum(after$Date == "06/16/22", na.rm = TRUE), 6L,
#                "restored Cameroon June 2022 rows")
#
#   unresolved_sulfate <- after$Site == "Olama" &
#     after$Date == "12/15/03" & tolower(trimws(after$SO4)) == "nd"
#   assert_count(sum(unresolved_sulfate, na.rm = TRUE), 1L,
#                "Cameroon unresolved sulfate row")
#   assert_count(sum(tolower(trimws(after$SO4)) == "nd", na.rm = TRUE), 1L,
#                "Cameroon unresolved sulfate total")
#   # exclude nd sulfate from the working input because no threshold is given
#   after$SO4[unresolved_sulfate] <- NA_character_
#   after$Date <- normalize_reviewed_dates(
#     after$Date,
#     date_order = "mdy",
#     expected_nonblank = 3094L,
#     expected_two_digit = 3094L,
#     expected_four_digit = 0L,
#     expected_unique = 768L,
#     expected_range = c("1994-08-11", "2022-11-04"),
#     label = "Cameroon"
#   )
#
#   write_prepared(filename, after)
# }
#
# # ---- guadeloupe source checks ----
#
# validate_guadeloupe_sources <- function() {
#   expected_rows <- c(
#     guadeloupe_GRCBDMDF_chem.csv = 4736L,
#     guadeloupe_GRCBDPBD_chem.csv = 1856L,
#     guadeloupe_GRCBDQCK_chem.csv = 3184L,
#     guadeloupe_GRCCEDIG_chem.csv = 26624L,
#     guadeloupe_GRCVHBAR_chem.csv = 464L,
#     guadeloupe_GRCVHSAV_chem.csv = 304L
#   )
#   expected_dates <- c(
#     guadeloupe_GRCBDMDF_chem.csv = 289L,
#     guadeloupe_GRCBDPBD_chem.csv = 115L,
#     guadeloupe_GRCBDQCK_chem.csv = 196L,
#     guadeloupe_GRCCEDIG_chem.csv = 516L,
#     guadeloupe_GRCVHBAR_chem.csv = 29L,
#     guadeloupe_GRCVHSAV_chem.csv = 19L
#   )
#   expected_columns <- c(
#     "Date", "Hour", "Code Site", "Site", "Level", "Type",
#     "Flask", "Comment", "Validate", "Site Name", "parameter", "value"
#   )
#
#   for (filename in guadeloupe_files) {
#     data <- read_source(filename)
#     if (!identical(names(data), expected_columns)) {
#       stop(filename, " no longer has the original source columns")
#     }
#     assert_count(nrow(data), expected_rows[[filename]],
#                  paste0(filename, " source rows"))
#
#     source_date <- trimws(data$Date)
#     assert_count(sum(is_blank(source_date)), 0L,
#                  paste0(filename, " blank source dates"))
#     parsed_date <- as.Date(source_date, format = "%Y-%m-%d")
#     if (anyNA(parsed_date) || any(format(parsed_date, "%Y-%m-%d") != source_date)) {
#       stop(filename, " has an invalid source date")
#     }
#     assert_count(length(unique(parsed_date)), expected_dates[[filename]],
#                  paste0(filename, " source dates"))
#
#     source_hour <- trimws(data$Hour)
#     invalid_hour <- !is_blank(source_hour) &
#       !grepl("^[0-9]{2}:[0-9]{2}$", source_hour)
#     assert_count(sum(invalid_hour), 0L,
#                  paste0(filename, " invalid source times"))
#   }
#
#   invisible(TRUE)
# }
#
# # ---- camrex source dates ----
#
# prepare_camrex <- function() {
#   filename <- "CAMREX_filled_template.csv"
#   before <- read_source(filename)
#   undated <- is_blank(before$Sampling.Date)
#
#   assert_count(nrow(before), 191L, "CAMREX rows")
#   assert_count(sum(undated), 21L, "CAMREX undated rows")
#   # keep undated rows out of the master rather than assigning unsupported dates
#   after <- before[!undated, , drop = FALSE]
#   rownames(after) <- NULL
#
#   sample_date <- as.Date(after$Sampling.Date, format = "%m/%d/%Y")
#   if (
#     any(is.na(sample_date)) ||
#       !identical(range(sample_date), as.Date(c("1982-05-02", "1991-09-03")))
#   ) {
#     stop("CAMREX source dates changed")
#   }
#   # the original guide defines julian day 1 as january 1, 1982
#   # the earlier conversion added one day by using that date as a zero-day origin
#   after$Sampling.Date <- format(sample_date - 1, "%Y-%m-%d")
#   if (!identical(
#     range(as.Date(after$Sampling.Date)),
#     as.Date(c("1982-05-01", "1991-09-02"))
#   )) {
#     stop("CAMREX corrected date range changed")
#   }
#
#   site_map <- c(
#     "Santo Antonio do Ica" = "Amazon River at Santo Antonio do Ica",
#     "Itapeua" = "Amazon River at Itapeua",
#     "Vargem Grande" = "Amazon River at Vargem Grande"
#   )
#   expected_names <- c(
#     "Santo Antonio do Ica" = 9L,
#     "Itapeua" = 12L,
#     "Vargem Grande" = 11L
#   )
#   observed_names <- vapply(
#     names(site_map),
#     function(name) sum(after$Site.Stream.Name == name, na.rm = TRUE),
#     integer(1)
#   )
#   if (!identical(observed_names, expected_names)) {
#     stop("CAMREX reviewed source-name counts changed")
#   }
#   for (old_name in names(site_map)) {
#     after$Site.Stream.Name[after$Site.Stream.Name == old_name] <-
#       unname(site_map[[old_name]])
#   }
#
#   assert_count(nrow(after), 170L, "prepared CAMREX rows")
#   assert_count(sum(after$Site.Stream.Name %in% names(site_map)), 0L,
#                "prepared CAMREX old names")
#   write_prepared(filename, after)
# }
#
#
# # ---- niva source dates and labels ----
#
# prepare_niva <- function() {
#   filename <- "NIVA_Water_chemistry.csv"
#   before <- read_source(filename)
#   after <- before
#   osteglo <- before$station_code == "ØSTEGLO"
#   suspect_conductivity <-
#     before$station_id == "29782" &
#     before$station_code == "NOREVEF" &
#     (
#       (before$sample_date == "1/6/04 0:00" & before$`COND_mS/m` == "506") |
#       (before$sample_date == "2/5/04 0:00" & before$`COND_mS/m` == "800") |
#       (before$sample_date == "3/8/04 0:00" & before$`COND_mS/m` == "86.7")
#     )
#
#   assert_count(nrow(before), 3677L, "NIVA rows")
#   assert_count(sum(osteglo, na.rm = TRUE), 538L, "NIVA ØSTEGLO rows")
#   assert_count(sum(suspect_conductivity, na.rm = TRUE), 3L,
#                "NIVA suspect conductivity rows")
#   # keep reported conductivity values for later outlier review
#   assert_values(
#     as_number(after$`COND_mS/m`[suspect_conductivity]),
#     c(506, 800, 86.7),
#     "prepared NIVA reviewed conductivity values"
#   )
#   after$station_code[osteglo] <- "OSTEGLO"
#   after$sample_date <- normalize_reviewed_dates(
#     after$sample_date,
#     date_order = "mdy",
#     expected_nonblank = 3677L,
#     expected_two_digit = 3677L,
#     expected_four_digit = 0L,
#     expected_unique = 2118L,
#     expected_range = c("1990-01-09", "2020-12-07"),
#     label = "NIVA"
#   )
#
#   write_prepared(filename, after)
# }
#
# # ---- niva geogenic site names ----
#
# prepare_niva_geogenic <- function() {
#   filename <- "NIVA_geogenic_clean.csv"
#   before <- read_source(filename)
#   after <- before
#   osteglo <- before$Stream_Name == "ØSTEGLO"
#   sulfate <- before$Parameter_id == "SO4" & before$unit == "mg/l"
#
#   assert_count(nrow(before), 6434L, "NIVA geogenic rows")
#   assert_count(sum(osteglo, na.rm = TRUE), 496L,
#                "NIVA geogenic ØSTEGLO rows")
#   assert_count(sum(sulfate, na.rm = TRUE), 1160L,
#                "NIVA geogenic sulfate rows")
#
#   after$Stream_Name[osteglo] <- "OSTEGLO"
#   after$unit[sulfate] <- "mg SO4/L"
#
#   write_prepared(filename, after)
# }
#
# # ---- niger source checks ----
#
# prepare_niger <- function() {
#   filename <- "NigerRiver.csv"
#   before <- read_source(filename)
#
#   assert_count(nrow(before), 85L, "Niger rows")
#   assert_count(sum(before$Stream_Name == "Niger", na.rm = TRUE), 85L,
#                "Niger normalized source names")
#   assert_count(sum(is_blank(before$Stream_Name)), 0L, "Niger blank names")
#
#   write_prepared(filename, before)
# }
#
# # ---- yzeron source labels ----
#
# prepare_yzeron <- function(filename, station_id, expected_rows) {
#   before <- read_source(filename)
#   after <- before
#   expected_columns <- c("date", "parameter", "value", "flag")
#
#   assert_count(nrow(before), expected_rows, paste0(filename, " rows"))
#   if (!identical(names(before), expected_columns)) {
#     stop(filename, " source columns changed")
#   }
#
#   # site fields come from the file identity and belong only in the working copy
#   after$site <- "Yzeron"
#   after$`station id` <- station_id
#
#   write_prepared(filename, after)
# }
#
#
# # ---- elbe source name ----
#
# validate_elbe_name <- function() {
#   filename <- "ElbeRiverChem.csv"
#   data <- read_source(filename, directory = qualifier_dir)
#   assert_count(nrow(data), 233L, "Elbe rows")
#   assert_count(sum(data$Stream_Name == "Elbe", na.rm = TRUE), 233L,
#                "Elbe normalized source names")
#   assert_count(sum(is_blank(data$Stream_Name)), 0L, "Elbe blank source names")
# }
#
# # ---- usgs and older master repairs ----
# # load these repairs here so their setup and later calls keep the same order
# source(file.path(preparation_dir, "prepare_usgs_legacy.R"), local = TRUE)
#
# # ---- krycklan source checks ----
#
# prepare_krycklan_cations <- function() {
#   filename <- "Krycklan_Cation_Clean.csv"
#   before <- read_source(filename)
#   suspect <- before[[1L]] == "5673" &
#     before$Date == "4/30/2008" &
#     before$SiteID == "15" &
#     before$Stream_Name == "Site 15" &
#     before$K.ug.l == "52438"
#
#   assert_count(nrow(before), 7930L, "Krycklan cation rows")
#   assert_count(sum(suspect, na.rm = TRUE), 1L,
#                "Krycklan reviewed potassium row")
#   write_prepared(filename, before)
# }
#
# # ---- legacy canada reporting units ----
#
# prepare_canada <- function() {
#   filename <- "Canada_WQ_dat.csv"
#   before <- read_source(filename)
#   after <- before
#   withdrawn_nitrate <-
#     before$SITE_NO == "BC08NF0001" &
#     before$DATE_TIME_HEURE == "2022-12-05 09:48:00" &
#     before$VARIABLE == "DISSOLVED NITROGEN NITRATE" &
#     before$VALUE_VALEUR == "0.158" &
#     before$UNIT_UNITE == "MG/L"
#   withdrawn_nitrate[is.na(withdrawn_nitrate)] <- FALSE
#   assert_count(sum(withdrawn_nitrate), 1L,
#                "withdrawn legacy Kootenay nitrate row")
#   # the current original source download no longer contains this older result
#   after <- after[!withdrawn_nitrate, , drop = FALSE]
#   rownames(after) <- NULL
#   extractable_silicon <- before$VARIABLE == "SILICON EXTRACTABLE"
#   unit_map <- c(
#     "CARBON DISSOLVED ORGANIC" = "mg C/L",
#     "DISSOLVED NITROGEN NITRATE" = "mg NO3-N/L",
#     "NITROGEN TOTAL" = "mg N/L",
#     "NITROGEN TOTAL DISSOLVED" = "mg N/L",
#     "PHOSPHORUS TOTAL" = "mg P/L",
#     "PHOSPHORUS TOTAL DISSOLVED" = "mg P/L",
#     "AMMONIA DISSOLVED" = "mg N/L",
#     "SILICON EXTRACTABLE" = "mg Si/L"
#   )
#   expected <- c(
#     "CARBON DISSOLVED ORGANIC" = 1988L,
#     "DISSOLVED NITROGEN NITRATE" = 1973L,
#     "NITROGEN TOTAL" = 1097L,
#     "NITROGEN TOTAL DISSOLVED" = 1966L,
#     "PHOSPHORUS TOTAL" = 1988L,
#     "PHOSPHORUS TOTAL DISSOLVED" = 997L,
#     "AMMONIA DISSOLVED" = 189L,
#     "SILICON EXTRACTABLE" = 1549L
#   )
#
#   assert_count(nrow(before), 11748L, "legacy Canada rows")
#   original_values <- after$VALUE_VALEUR
#   for (variable in names(unit_map)) {
#     target <- after$VARIABLE == variable & after$UNIT_UNITE == "MG/L"
#     assert_count(sum(target, na.rm = TRUE), expected[[variable]],
#                  paste0("legacy Canada ", variable, " rows"))
#     after$UNIT_UNITE[target] <- unit_map[[variable]]
#   }
#   if (!same_character(original_values, after$VALUE_VALEUR)) {
#     stop("legacy Canada values changed while labeling reporting bases")
#   }
#
#   # keep extractable silicon in the prepared input for source transparency
#   # the master workflow excludes it because method f055 confirms no filtration
#   assert_count(sum(extractable_silicon, na.rm = TRUE), 1549L,
#                "legacy Canada extractable silicon rows")
#   assert_count(nrow(after), 11747L, "prepared legacy Canada rows")
#
#   write_prepared(filename, after)
# }
#
# # ---- catalina-jemez units and source checks ----
#
# prepare_catalina <- function() {
#   filename <- "CatalinaJemez_chemistry_2009-2019_V2.csv"
#   before <- read_source(filename)
#   after <- before
#   inferred <- after$SiteCode == "MG_WEIR" &
#     after$SampleCode == "AZ_S1793" & is_blank(after$DateTime)
#
#   assert_count(sum(inferred, na.rm = TRUE), 1L,
#                "Catalina inferred-date sample")
#   after$DateTime[inferred] <- "2018-04-19"
#
#   date_text <- trimws(after$DateTime)
#   mdy <- grepl(
#     "^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/(09|1[0-9])$",
#     date_text
#   )
#   iso <- grepl(
#     "^20(09|1[0-9])-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$",
#     date_text
#   )
#   assert_count(sum(mdy), 2234L, "Catalina month-day-year dates")
#   assert_count(sum(iso), 1L, "Catalina inferred ISO date")
#   assert_count(sum(!mdy & !iso), 0L, "Catalina unrecognized dates")
#
#   parsed <- as.Date(rep(NA_character_, nrow(after)))
#   parsed[mdy] <- as.Date(date_text[mdy], format = "%m/%d/%y")
#   parsed[iso] <- as.Date(date_text[iso], format = "%Y-%m-%d")
#   if (anyNA(parsed)) {
#     stop("Catalina dates did not parse")
#   }
#   after$DateTime <- format(parsed, "%Y-%m-%d")
#
#   source_names <- c(MG_WEIR = "MGWEIR", OR_low = "ORlow")
#   for (source_name in names(source_names)) {
#     after$SiteCode[after$SiteCode == source_name] <- source_names[[source_name]]
#   }
#
#   # do not alter the reported ph, oxygen, or temperature values here
#   if (!same_character(before$pH, after$pH) ||
#       !same_character(before$DO, after$DO) ||
#       !same_character(before$Temp, after$Temp)) {
#     stop("Catalina measurement values changed during date preparation")
#   }
#
#   write_prepared(filename, after)
# }
#
# # ---- nt and nsw source silica and reporting units ----
#
# prepare_nt_nsw <- function() {
#   filename <- "NT_NSW_Chem_Cleaned.csv"
#   before <- read_source(filename)
#   after <- before
#   row_id <- as.character(after[[1L]])
#   portal_duplicate <- row_id %in% c("195038", "195039")
#   expected_signature <- data.frame(
#     id = c("195038", "195039"),
#     Site_Code = c("425007", "425007"),
#     Parameter = c("DO", "DO_pct"),
#     Date = c("12/1/09 17:41", "12/1/09 17:41"),
#     Value = c("69.2", "5.53"),
#     Unit = c("mg/L", "%"),
#     Name = c(
#       "DARLING RIVER AT BURTUNDY",
#       "DARLING RIVER AT BURTUNDY"
#     ),
#     stringsAsFactors = FALSE
#   )
#   observed_signature <- data.frame(
#     id = row_id[portal_duplicate],
#     after[portal_duplicate, c(
#       "Site_Code", "Parameter", "Date", "Value", "Unit", "Name"
#     )],
#     stringsAsFactors = FALSE
#   )
#   rownames(observed_signature) <- NULL
#   if (!identical(observed_signature, expected_signature)) {
#     stop("the two provider-confirmed NT/NSW duplicate rows changed")
#   }
#
#   # the official portal contains only the correctly labeled paired rows
#   # remove the two extra swapped duplicates from the prepared input
#   after <- after[!portal_duplicate, , drop = FALSE]
#   rownames(after) <- NULL
#
#   zinc_source <- as.character(after[[1L]]) == "244833" &
#     after$Site_Code == "G8140159" &
#     after$Parameter == "Zn" &
#     after$Date == "27/09/1995" &
#     after$Value == "4.6" &
#     after$Unit == "micrograms/litre"
#   assert_count(sum(zinc_source, na.rm = TRUE), 1L,
#                "NT/NSW source zinc row")
#   # the portal reports this result in micrograms per litre
#   # convert it only in the working copy used by the chemistry workflow
#   after$Value[zinc_source] <- "0.0046"
#   after$Unit[zinc_source] <- "mg/L"
#
#   unit_pairs <- data.frame(
#     parameter = c(
#       "DOC", "NH4", "NO2", "NO2", "NO3", "NO3", "NO3", "NOx", "NOx",
#       "SO4", "SO4", "SRP", "SRP", "TKN",
#       "TN", "TN", "TP", "TP"
#     ),
#     source_unit = c(
#       "mg/L", "mg/L", "mg/L", "Milligrams/Litre", "mg/L", "milligrams/litre",
#       "Milligrams/Litre", "mg/L", "milligrams/litre", "mg/L",
#       "milligrams/litre", "mg/L", "milligrams/litre", "mg/L",
#       "mg/L", "Milligrams/Litre", "mg/L", "Milligrams/Litre"
#     ),
#     target_unit = c(
#       "mg C/L", "mg NH4-N/L", "mg NO2-N/L", "mg NO2-N/L", "mg NO3-N/L",
#       "mg NO3-N/L", "mg NO3-N/L", "mg NO3-N/L", "mg NO3-N/L",
#       "mg SO4/L",
#       "mg SO4/L", "mg P/L", "mg P/L", "mg N/L", "mg N/L",
#       "mg N/L", "mg P/L", "mg P/L"
#     ),
#     expected_rows = c(
#       2172L, 1659L, 72L, 44L, 3894L, 140L, 47L, 5441L, 9L,
#       5566L, 138L, 4361L, 95L, 2761L, 9580L, 134L,
#       15559L, 137L
#     ),
#     stringsAsFactors = FALSE
#   )
#   source_values <- after$Value
#   for (pair_row in seq_len(nrow(unit_pairs))) {
#     target <- after$Parameter == unit_pairs$parameter[pair_row] &
#       after$Unit == unit_pairs$source_unit[pair_row]
#     assert_count(sum(target, na.rm = TRUE), unit_pairs$expected_rows[pair_row],
#                  "NT/NSW molecular-basis unit rows")
#     after$Unit[target] <- unit_pairs$target_unit[pair_row]
#   }
#   ph_unit <- after$Parameter == "pH" & after$Unit == "/"
#   assert_count(sum(ph_unit, na.rm = TRUE), 27308L, "NT/NSW pH unit rows")
#   after$Unit[ph_unit] <- NA_character_
#   if (!same_character(source_values, after$Value)) {
#     stop("NT/NSW measurement values changed during working preparation")
#   }
#
#   after$Date <- normalize_reviewed_dates(
#     after$Date,
#     date_order = "dmy",
#     expected_nonblank = 244962L,
#     expected_two_digit = 103972L,
#     expected_four_digit = 140990L,
#     expected_unique = 9338L,
#     expected_range = c("1962-01-11", "2019-08-14"),
#     label = "NT/NSW"
#   )
#
#   # the combined file does not identify the si fraction or reporting basis
#   legacy_si <- after$Parameter == "Si"
#   assert_count(sum(legacy_si, na.rm = TRUE), 7371L, "NT/NSW generic Si rows")
#   assert_count(
#     length(unique(after$Site_Code[legacy_si])),
#     31L,
#     "NT/NSW generic Si sites"
#   )
#
#   provider <- read_source_csv(
#     basename(nt_nsw_provider_file),
#     dirname(nt_nsw_provider_file)
#   )
#   provider_columns <- c(
#     "Station No.", "Station Name", "Parameter", "Date & Time", "Value", "Unit"
#   )
#   if (!all(provider_columns %in% names(provider))) {
#     stop("the NT/NSW silica provider columns changed")
#   }
#
#   provider$provider_date <- substr(provider[["Date & Time"]], 1L, 10L)
#   valid_provider_date <- grepl(
#     "^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
#     provider$provider_date
#   )
#   provider_station <- provider[["Station No."]] %in% c("409025", "425007")
#   provider_soluble <- provider$Parameter == "Silica - Soluble"
#   provider_unit <- tolower(trimws(provider$Unit)) == "mg/l"
#   reviewed_provider <- provider[
#     provider_station & provider_soluble & provider_unit & valid_provider_date,
#     ,
#     drop = FALSE
#   ]
#
#   legacy_target <- legacy_si & after$Site_Code %in% c("409025", "425007")
#   legacy_keys <- unique(paste(
#     after$Site_Code[legacy_target],
#     substr(after$Date[legacy_target], 1L, 10L),
#     sep = "\r"
#   ))
#   provider_keys <- paste(
#     reviewed_provider[["Station No."]],
#     reviewed_provider$provider_date,
#     sep = "\r"
#   )
#   reviewed_provider <- reviewed_provider[
#     provider_keys %in% legacy_keys,
#     ,
#     drop = FALSE
#   ]
#   provider_keys <- provider_keys[provider_keys %in% legacy_keys]
#
#   assert_count(length(unique(legacy_keys)), 3165L,
#                "NT/NSW project silica site-days")
#   assert_count(nrow(reviewed_provider), 1752L,
#                "NT/NSW provider-confirmed soluble silica rows")
#   assert_count(length(unique(provider_keys)), 1752L,
#                "NT/NSW provider-confirmed soluble silica site-days")
#   assert_count(
#     sum(reviewed_provider[["Station No."]] == "409025"),
#     878L,
#     "NT/NSW Yarrawonga soluble silica rows"
#   )
#   assert_count(
#     sum(reviewed_provider[["Station No."]] == "425007"),
#     874L,
#     "NT/NSW Burtundy soluble silica rows"
#   )
#   if (any(is_blank(reviewed_provider$Value))) {
#     stop("the NT/NSW provider-confirmed silica contains blank values")
#   }
#
#   after$Source_Method_Flag <- NA_character_
#   si_rows <- after[rep(1L, nrow(reviewed_provider)), , drop = FALSE]
#   si_rows[[1L]] <- paste0(
#     "MDBA-",
#     reviewed_provider[["Station No."]],
#     "-",
#     gsub("-", "", reviewed_provider$provider_date)
#   )
#   si_rows$Site_Code <- reviewed_provider[["Station No."]]
#   si_rows$Parameter <- "Si"
#   si_rows$Date <- reviewed_provider$provider_date
#   si_rows$Value <- reviewed_provider$Value
#   si_rows$Unit <- "mg SiO2/L"
#   si_rows$Name <- reviewed_provider[["Station Name"]]
#   si_rows$Source_Method_Flag <- "reported as Silica - Soluble (RMWQMP)"
#
#   after <- rbind(after[!legacy_si, , drop = FALSE], si_rows)
#   rownames(after) <- NULL
#
#   write_prepared(filename, after)
# }
#
# # ---- mcmurdo alkalinity units ----
#
# prepare_mcm <- function() {
#   filename <- "MCM_Chem_clean.csv"
#   before <- read_source(filename)
#   after <- before
#   andersen_h1 <- after$location == "Andersen Creek at F5"
#   aiken_f5 <- after$location == "Aiken Creek at f5"
#
#   assert_count(sum(andersen_h1, na.rm = TRUE), 11L,
#                "McMurdo Andersen H1 rows")
#   if (!identical(unique(after$strmgageid[andersen_h1]), "andrsn_h1")) {
#     stop("McMurdo Andersen gage identity changed")
#   }
#   assert_count(sum(aiken_f5, na.rm = TRUE), 13L, "McMurdo Aiken F5 rows")
#   assert_count(sum(!is.na(as_number(after$alkalinity_meql))), 1949L,
#                "McMurdo raw alkalinity rows")
#
#   after$location[andersen_h1] <- "Andersen Creek at H1"
#   after$location[aiken_f5] <- "Aiken Creek at F5"
#   if (!same_character(before$alkalinity_meql, after$alkalinity_meql)) {
#     stop("McMurdo raw alkalinity values changed during source preparation")
#   }
#
#   write_prepared(filename, after)
# }
#
# # ---- finnish source dates and labels ----
#
# prepare_finnish <- function() {
#   filename <- "Finnish_riverine_data_07032025.csv"
#   before <- read_source(filename)
#   after <- before
#   name_map <- c(
#     "Koskenkylänjoki 6030" = "Koskenkylanjoki 6030",
#     "Närpiönjoki mts 6761" = "Narpionjoki mts 6761",
#     "Pyhäjoki Hourunk 11400" = "Pyhajoki Hourunk 11400",
#     "SIMOJOKI AS. 13500      " = "SIMOJOKI AS. 13500",
#     "TORNIONJ KUKKOLA 14310  " = "TORNIONJ KUKKOLA 14310"
#   )
#   for (source_name in names(name_map)) {
#     after$`Station name`[after$`Station name` == source_name] <-
#       name_map[[source_name]]
#   }
#   sodium <- trimws(after$Parameter) == "NA"
#   assert_count(sum(sodium, na.rm = TRUE), 9638L, "Finnish sodium rows")
#   after$Parameter[sodium] <- "Na"
#   sulfate <- trimws(after$Parameter) == "SO4" & after$Unit == "mg/l"
#   assert_count(sum(sulfate, na.rm = TRUE), 9863L, "Finnish sulfate rows")
#   after$Unit[sulfate] <- "mg SO4/L"
#   source_values <- after$Value
#   after$Date <- normalize_reviewed_dates(
#     after$Date,
#     date_order = "mdy",
#     expected_nonblank = 58307L,
#     expected_two_digit = 58307L,
#     expected_four_digit = 0L,
#     expected_unique = 2523L,
#     expected_range = c("2000-01-03", "2022-12-15"),
#     label = "Finnish"
#   )
#   if (!same_character(source_values, after$Value)) {
#     stop("Finnish measurement values changed during working preparation")
#   }
#   write_prepared(filename, after)
# }
#
# # ---- combined source checks ----
#
# prepare_deduplicated_sites <- function() {
#   filename <- "Deduplicated_Sites_AllSites_chemistry.csv"
#   before <- read_source(filename)
#   assert_count(nrow(before), 39129L, "deduplicated chemistry rows")
#   assert_count(sum(before$solute %in% c("TN", "TDS", "tot dissolved solids"),
#                    na.rm = TRUE), 0L,
#                "deduplicated uppercase TN or TDS rows")
#   write_prepared(filename, before)
# }
#
# # ---- units-key coverage ----
#
# normalize_solute <- function(x) {
#   tolower(gsub("/| |\\-", "_", as.character(x)))
# }
#
# normalize_unit <- function(x) {
#   value <- trimws(as.character(x))
#   value <- gsub("µ|μ", "u", value)
#   lower_value <- tolower(value)
#   value[is.na(x) | value == "" | lower_value == "na"] <- NA_character_
#   value[lower_value %in% c("-", "/")] <- NA_character_
#   value[lower_value %in% c(
#     "mg/l", "milligrams/litre", "milligramsperliter"
#   )] <- "mg/L"
#   value[lower_value == "mg(s)/l"] <- "mg S/L"
#   value[lower_value == "mg(so4)/l"] <- "mg SO4/L"
#   value[lower_value %in% c("ug/l", "micrograms/litre")] <- "ug/L"
#   value[lower_value %in% c(
#     "microsiemens/cm", "microsiemens cm", "microsiemenspercentimeter"
#   )] <- "uS/cm"
#   value[lower_value %in% c("deg c", "oc", "degrees celsius")] <- "C"
#   value[value == "%"] <- "percent"
#   value[lower_value == "pcu"] <- "PCU"
#   value[lower_value == "fnu"] <- "FNU"
#   value[lower_value == "ntu"] <- "NTU"
#   value[lower_value == "ueq/l"] <- "ueq/L"
#   gsub("/| |\\-", "_", value)
# }
#
# validate_long_units_key <- function() {
#   data_key <- read_source("SiSyn_Data_Key.csv", directory = key_dir)
#   units_key <- read_source("SiSyn_Chem_Units_Key.csv", directory = key_dir)
#   canada_alkalinity <- units_key$Raw_Filename %in% canada_site_files &
#     units_key$Variable == "alkalinity" & units_key$Units == "uM"
#   canada_alkalinity_fixed <- units_key$Raw_Filename %in% canada_site_files &
#     units_key$Variable == "alkalinity" & units_key$Units == "ueq/L"
#   canada_nh4 <- units_key$Raw_Filename %in% canada_site_files &
#     units_key$Variable == "NH4" & units_key$Units == "uM"
#
#   assert_count(sum(canada_alkalinity), 0L,
#                "old six-site Canada alkalinity key rows")
#   assert_count(sum(canada_alkalinity_fixed), 6L,
#                "fixed six-site Canada alkalinity key rows")
#   assert_count(sum(canada_nh4), 0L,
#                "old six-site Canada NH4 key rows")
#   for (filename in canada_site_files[2:4]) {
#     assert_count(
#       sum(
#         units_key$Raw_Filename == filename &
#           units_key$Variable == "NHX" & units_key$Units == "uM"
#       ),
#       1L,
#       paste0(filename, " NHX key rows")
#     )
#   }
#   for (filename in canada_site_files[4:5]) {
#     assert_count(
#       sum(
#         units_key$Raw_Filename == filename &
#           units_key$Variable == "PO4" & units_key$Units == "uM"
#       ),
#       1L,
#       paste0(filename, " PO4 key rows")
#     )
#   }
#   murray_alkalinity <- units_key$Raw_Filename ==
#     "MurrayDarlingChem_Merged.csv" & units_key$Variable == "alkalinity" &
#     units_key$Units == "mg CaCO3/L"
#   murray_old_alkalinity <- units_key$Raw_Filename ==
#     "MurrayDarlingChem_Merged.csv" & units_key$Variable %in%
#     c("Alk_CaCO3", "BiCarbAlk_CaCO3")
#   murray_temperature <- units_key$Raw_Filename ==
#     "MurrayDarlingChem_Merged.csv" & units_key$Variable == "Water_Temp" &
#     units_key$Units == "C"
#   murray_unmapped_temperature <- units_key$Raw_Filename ==
#     "MurrayDarlingChem_Merged.csv" & units_key$Variable == "Temp"
#   seine_sulfate <- units_key$Raw_Filename == "Seine_baseGeo_Naides.csv" &
#     units_key$Variable == "SO4" &
#     units_key$Units %in% c("mg S/L", "mg SO4/L")
#   seine_old_sulfate <- units_key$Raw_Filename ==
#     "Seine_baseGeo_Naides.csv" & units_key$Variable == "Sulfates"
#
#   assert_count(sum(murray_alkalinity), 1L,
#                "Murray-Darling alkalinity key rows")
#   assert_count(sum(murray_old_alkalinity), 0L,
#                "Murray-Darling old alkalinity key rows")
#   assert_count(sum(murray_temperature), 1L,
#                "Murray-Darling temperature key rows")
#   assert_count(sum(murray_unmapped_temperature), 0L,
#                "Murray-Darling unmapped temperature key rows")
#   assert_count(sum(seine_sulfate), 2L, "Seine sulfate key rows")
#   assert_count(sum(seine_old_sulfate), 0L, "Seine old sulfate key rows")
#
#   # check units-key coverage against the working files written above
#   prepared_files <- list.files(output_dir, pattern = "[.]csv$")
#   long_files <- unique(data_key$Raw_Filename[
#     data_key$Dataset_orientation == "long" &
#       data_key$Raw_Filename %in% prepared_files
#   ])
#   workflow_aliases <- list(
#     "Deduplicated_Sites_AllSites_chemistry.csv" = c(
#       "ca_mgl", "cl_mgl", "sio2_mgl", "k_mgl", "mg_mgl",
#       "nh4_mgl", "na_mgl", "po4_mgl", "so4_mgl", "tp_ugl",
#       "doc_mgl", "no3_mgl", "al_ugl", "f_mgl", "fe_ugl",
#       "mn_ugl", "anc_ueql", "temp_c", "tdn_mgl"
#     )
#   )
#   approved_unkeyed <- list(
#     "Como_Creek_chem.csv" = "tss___dry_mass",
#     "MurrayDarlingChem_Merged.csv" = c(
#       "alk_ph", "depth", "field_ec", "flow", "sample_temp", "temp"
#     ),
#     "NT_NSW_Chem_Cleaned.csv" = c("cu", "q"),
#     "guadeloupe_GRCCEDIG_chem.csv" = "nicb_(%)",
#     "guadeloupe_GRCBDPBD_chem.csv" = "nicb_(%)",
#     "guadeloupe_GRCVHSAV_chem.csv" = "nicb_(%)"
#   )
#   missing_rows <- data.frame(
#     Raw_Filename = character(),
#     Variable = character(),
#     Units = character(),
#     stringsAsFactors = FALSE
#   )
#
#   for (filename in long_files) {
#     data <- read_source(filename, directory = output_dir)
#     key_rows <- data_key[data_key$Raw_Filename == filename, , drop = FALSE]
#     solute_column <- key_rows$Raw_Column_Name[
#       !is.na(key_rows$Standardized_Column_Name) &
#         key_rows$Standardized_Column_Name == "solute"
#     ]
#     amount_column <- key_rows$Raw_Column_Name[
#       !is.na(key_rows$Standardized_Column_Name) &
#         key_rows$Standardized_Column_Name == "amount"
#     ]
#     unit_column <- key_rows$Raw_Column_Name[
#       !is.na(key_rows$Standardized_Column_Name) &
#         key_rows$Standardized_Column_Name == "solute_units"
#     ]
#
#     if (length(solute_column) != 1L || length(amount_column) != 1L) {
#       stop("long-file key fields changed for '", filename, "'")
#     }
#
#     observed_solute <- normalize_solute(data[[solute_column]])
#     observed_unit <- if (length(unit_column) == 1L) {
#       normalize_unit(data[[unit_column]])
#     } else {
#       rep(NA_character_, nrow(data))
#     }
#     observed_pairs <- unique(data.frame(
#       solute = observed_solute,
#       source_units = observed_unit,
#       stringsAsFactors = FALSE
#     ))
#     observed_pairs <- observed_pairs[!is_blank(observed_pairs$solute), , drop = FALSE]
#     key_rows_for_file <- units_key[
#       units_key$Raw_Filename == filename,
#       c("Variable", "Units"),
#       drop = FALSE
#     ]
#     key_pairs <- unique(data.frame(
#       solute = normalize_solute(key_rows_for_file$Variable),
#       key_units = normalize_unit(key_rows_for_file$Units),
#       stringsAsFactors = FALSE
#     ))
#     keyed <- unique(key_pairs$solute)
#     observed <- unique(observed_pairs$solute)
#     missing <- setdiff(
#       observed,
#       c(
#         keyed,
#         workflow_aliases[[filename]],
#         approved_unkeyed[[filename]]
#       )
#     )
#
#     if (length(missing) > 0L) {
#       for (solute in missing) {
#         source_units <- unique(observed_pairs$source_units[
#           observed_pairs$solute == solute & !is.na(observed_pairs$source_units)
#         ])
#         if (length(source_units) != 1L) {
#           stop("missing units-key row has unclear units in '", filename, "'")
#         }
#         missing_rows <- rbind(
#           missing_rows,
#           data.frame(
#             Raw_Filename = filename,
#             Variable = toupper(solute),
#             Units = source_units,
#             stringsAsFactors = FALSE
#           )
#         )
#       }
#     }
#
#     covered_pairs <- observed_pairs[
#       observed_pairs$solute %in% keyed,
#       ,
#       drop = FALSE
#     ]
#     for (pair_row in seq_len(nrow(covered_pairs))) {
#       solute <- covered_pairs$solute[pair_row]
#       source_unit <- covered_pairs$source_units[pair_row]
#       choices <- key_pairs[key_pairs$solute == solute, , drop = FALSE]
#       exact_match <- if (is.na(source_unit)) {
#         nrow(choices) == 1L
#       } else {
#         sum(!is.na(choices$key_units) & choices$key_units == source_unit) == 1L
#       }
#       if (!exact_match) {
#         stop(
#           "a prepared variable/unit pair does not resolve in '",
#           filename,
#           "': ", solute, " / ", source_unit
#         )
#       }
#     }
#   }
#
#   missing_rows <- missing_rows[
#     order(missing_rows$Raw_Filename, missing_rows$Variable, missing_rows$Units),
#     ,
#     drop = FALSE
#   ]
#   rownames(missing_rows) <- NULL
#   if (nrow(missing_rows) > 0L) {
#     observed_text <- apply(missing_rows, 1L, paste, collapse = " | ")
#     stop(
#       "prepared long-file rows are missing from the Units Key: ",
#       paste(observed_text, collapse = "; ")
#     )
#   }
#   invisible(TRUE)
# }
#
# # ---- run ----
#
# prepare_alpine()
# prepare_switzerland()
# prepare_east_river()
# prepare_krycklan_cations()
# prepare_canada()
# prepare_canada_sites()
# prepare_catalina()
# prepare_seine_base()
# prepare_cleaned_seine()
# prepare_nt_nsw()
# prepare_mcm()
# prepare_murray()
# prepare_md_merbein()
# prepare_cameroon()
# validate_guadeloupe_sources()
# prepare_camrex()
# prepare_finnish()
# prepare_japan()
# prepare_netherlands()
# prepare_niva()
# prepare_niva_geogenic()
# prepare_niger()
# prepare_yzeron("yzeron_301502401_chem.csv", "V301502401", 258L)
# prepare_yzeron("yzeron_v301502402_chem.csv", "V301502402", 145L)
# prepare_yzeron("yzeron_v3015810_chem.csv", "V3015810", 301L)
# prepare_deduplicated_sites()
# validate_elbe_name()
# prepare_legacy_usgs()
# prepare_current_usgs()
# validate_long_units_key()
# ---- end replacement: individual-source-choices ----

# ---- sidney: usgs and older master repairs ----
# these functions rebuild usgs nitrogen and tds and repair the older combined master
# the other-source script loads them without changing the repair order
# current file: chemistry-inputs/prepare_usgs_legacy.R
# ---- begin replacement: usgs-legacy-repairs ----
# # usgs and older combined-master repairs
# # loaded automatically by the other-source preparation script
# # uses the same paths and shared functions and keeps the repair order unchanged
#
# # ---- usgs total nitrogen rebuild ----
#
# build_usgs_total_nitrogen_replacement <- function(legacy_data) {
#   legacy_filename <- "20221030_masterdata_chem_V2.csv"
#   nitrogen_codes <- c("00600", "62855", "71887")
#   dissolved_nitrogen_codes <- c("00602", "62854")
#   all_codes <- c(nitrogen_codes, dissolved_nitrogen_codes)
#
#   site_reference <- read_source(
#     "Site_Reference_Table.csv",
#     directory = key_dir
#   )
#   site_lookup <- data.table::as.data.table(site_reference[
#     site_reference$LTER == "USGS",
#     c(
#       "Stream_Name", "Alt_Stream_Name", "Original_Stream_Name",
#       "USGSGageNumber"
#     ),
#     drop = FALSE
#   ])
#   site_lookup[, location_id := paste0("USGS-", USGSGageNumber)]
#
#   if (
#     any(is_blank(site_lookup$Stream_Name)) ||
#       any(is_blank(site_lookup$USGSGageNumber)) ||
#       anyDuplicated(site_lookup$Stream_Name) > 0L ||
#       anyDuplicated(site_lookup$location_id) > 0L
#   ) {
#     stop("the reviewed USGS site-id mapping is not one-to-one")
#   }
#
#   alternate_names <- strsplit(
#     ifelse(is_blank(site_lookup$Alt_Stream_Name), "", site_lookup$Alt_Stream_Name),
#     ";",
#     fixed = TRUE
#   )
#   alternate_lookup <- data.table::data.table(
#     source_site = trimws(unlist(alternate_names, use.names = FALSE)),
#     location_id = rep(site_lookup$location_id, lengths(alternate_names))
#   )
#   original_lookup <- site_lookup[
#     !is_blank(Original_Stream_Name),
#     .(source_site = trimws(Original_Stream_Name), location_id)
#   ]
#   source_site_lookup <- unique(data.table::rbindlist(list(
#     site_lookup[, .(
#       source_site = Stream_Name,
#       location_id
#     )],
#     alternate_lookup,
#     original_lookup
#   )))
#   source_site_lookup <- source_site_lookup[!is_blank(source_site)]
#   alias_conflicts <- source_site_lookup[
#     , .(location_ids = data.table::uniqueN(location_id)),
#     by = source_site
#   ][location_ids > 1L]
#   if (nrow(alias_conflicts) > 0L) {
#     stop("a USGS site alias maps to more than one station id")
#   }
#
#   deduplicated_source <- read_source(
#     "Deduplicated_Sites_AllSites_chemistry.csv",
#     directory = input_dir
#   )
#   deduplicated_bad_tn <- find_tds_labeled_tn(deduplicated_source)
#   deduplicated_scope <- data.table::as.data.table(deduplicated_source[
#     deduplicated_bad_tn,
#     c("Stream_Name", "date"),
#     drop = FALSE
#   ])
#   data.table::setnames(deduplicated_scope, "Stream_Name", "source_site")
#   deduplicated_scope[, date := data.table::as.IDate(date)]
#   deduplicated_scope <- merge(
#     deduplicated_scope,
#     source_site_lookup,
#     by = "source_site",
#     all.x = TRUE
#   )
#
#   if (anyNA(deduplicated_scope$location_id)) {
#     stop("a deduplicated TDS-to-TN row lacks a station id")
#   }
#   assert_count(nrow(deduplicated_scope), 0L,
#                "deduplicated TDS-to-TN provider scope")
#   assert_count(data.table::uniqueN(deduplicated_scope$source_site), 0L,
#                "deduplicated TDS-to-TN sites")
#   assert_count(
#     data.table::uniqueN(deduplicated_scope[, .(location_id, date)]),
#     0L,
#     "deduplicated TDS-to-TN dates"
#   )
#
#   current_rows <- which(
#     legacy_data$LTER == "USGS" &
#       legacy_data$variable == "TN" &
#       legacy_data$units == "uM"
#   )
#   current_scope <- data.table::data.table(
#     legacy_row_id = legacy_data[[1L]][current_rows],
#     source_site = legacy_data$Site.Stream.Name[current_rows],
#     date = data.table::as.IDate(legacy_data$Sampling.Date[current_rows])
#   )
#   current_scope <- merge(
#     current_scope,
#     source_site_lookup,
#     by = "source_site",
#     all.x = TRUE
#   )
#
#   if (anyNA(current_scope$location_id)) {
#     stop("a residual legacy USGS total-nitrogen row lacks a station id")
#   }
#
#   exclusions <- data.table::as.data.table(readRDS(usgs_exclusion_file))
#   exclusion_scope <- unique(exclusions[
#     source_file == legacy_filename,
#     .(source_row, location_id, date = data.table::as.IDate(date))
#   ])
#
#   assert_count(nrow(current_scope), 13497L,
#                "residual legacy USGS total-nitrogen scope")
#   assert_count(data.table::uniqueN(exclusion_scope$source_row), 8625L,
#                "excluded legacy USGS total-nitrogen scope")
#
#   current_keys <- unique(current_scope[, .(location_id, date)])
#   exclusion_keys <- unique(exclusion_scope[, .(location_id, date)])
#   deduplicated_keys <- unique(deduplicated_scope[, .(location_id, date)])
#   assert_count(nrow(merge(
#     current_keys,
#     exclusion_keys,
#     by = c("location_id", "date")
#   )), 0L, "overlapping legacy USGS total-nitrogen scopes")
#   assert_count(nrow(merge(
#     data.table::rbindlist(list(current_keys, exclusion_keys)),
#     deduplicated_keys,
#     by = c("location_id", "date")
#   )), 0L, "overlapping deduplicated and legacy total-nitrogen scopes")
#
#   scope_keys <- unique(data.table::rbindlist(list(
#     current_keys,
#     exclusion_keys,
#     deduplicated_keys
#   )))
#   assert_count(nrow(scope_keys), 22122L,
#                "original legacy USGS total-nitrogen dates")
#   assert_count(data.table::uniqueN(scope_keys$location_id), 96L,
#                "original legacy USGS total-nitrogen sites")
#
#   provider_columns <- c(
#     "Location_Identifier",
#     "Activity_ActivityIdentifier",
#     "Activity_StartDate",
#     "Result_ResultDetectionCondition",
#     "Result_Characteristic",
#     "Result_CharacteristicUserSupplied",
#     "Result_MethodSpeciation",
#     "Result_SampleFraction",
#     "Result_MeasureIdentifier",
#     "Result_Measure",
#     "Result_MeasureUnit",
#     "Result_MeasureQualifierCode",
#     "Result_MeasureStatusIdentifier",
#     "DetectionLimit_TypeA",
#     "DetectionLimit_MeasureA",
#     "DetectionLimit_MeasureUnitA",
#     "DetectionLimit_TypeB",
#     "DetectionLimit_MeasureB",
#     "DetectionLimit_MeasureUnitB",
#     "USGSpcode"
#   )
#
#   read_provider_file <- function(path) {
#     provider_rows <- data.table::fread(
#       path,
#       select = provider_columns,
#       colClasses = "character",
#       showProgress = FALSE
#     )
#     provider_rows <- provider_rows[USGSpcode %in% all_codes]
#     provider_rows[, provider_file := basename(path)]
#     provider_rows
#   }
#
#   provider_file_rows <- data.table::rbindlist(
#     lapply(c(usgs_provider_files, usgs_clinton_file), read_provider_file),
#     use.names = TRUE,
#     fill = TRUE
#   )
#   identity_columns <- setdiff(names(provider_file_rows), "provider_file")
#   provider <- provider_file_rows[
#     , .(provider_files = paste(sort(unique(provider_file)), collapse = ";")),
#     by = identity_columns
#   ]
#   data.table::setnames(
#     provider,
#     c(
#       "Location_Identifier",
#       "Activity_StartDate",
#       "Result_Measure",
#       "Result_MeasureUnit"
#     ),
#     c("location_id", "date", "provider_value", "provider_unit")
#   )
#   provider[, date := data.table::as.IDate(date)]
#   provider[, provider_value_text := provider_value]
#   provider[, limit_a_text := DetectionLimit_MeasureA]
#   provider[, limit_b_text := DetectionLimit_MeasureB]
#   provider[, provider_value := suppressWarnings(as.numeric(provider_value))]
#   provider[, limit_a := suppressWarnings(as.numeric(DetectionLimit_MeasureA))]
#   provider[, limit_b := suppressWarnings(as.numeric(DetectionLimit_MeasureB))]
#
#   code_key <- data.table::data.table(
#     USGSpcode = c("00600", "62855", "71887", "00602", "62854"),
#     variable = c("TN", "TN", "TN", "TDN", "TDN"),
#     basis = c("N", "N", "NO3", "N", "N"),
#     code_priority = c(2L, 1L, 3L, 2L, 1L)
#   )
#   # use official usgs parameter codes to keep tn, tdn, and tds identities separate
#   provider <- merge(provider, code_key, by = "USGSpcode")
#
#   nitrogen_molecular_weight <- 14.0067
#   nitrate_molecular_weight <- nitrogen_molecular_weight + 15.9994 * 3
#   provider[, value_uM := NA_real_]
#   provider[basis == "N" & provider_unit == "mg/L", value_uM :=
#     provider_value * 1000 / nitrogen_molecular_weight]
#   provider[basis == "NO3" & provider_unit == "mg/L", value_uM :=
#     provider_value * 1000 / nitrate_molecular_weight]
#   provider[, limit_a_uM := NA_real_]
#   provider[basis == "N" & DetectionLimit_MeasureUnitA == "mg/L", limit_a_uM :=
#     limit_a * 1000 / nitrogen_molecular_weight]
#   provider[basis == "NO3" & DetectionLimit_MeasureUnitA == "mg/L", limit_a_uM :=
#     limit_a * 1000 / nitrate_molecular_weight]
#   provider[, limit_b_uM := NA_real_]
#   provider[basis == "N" & DetectionLimit_MeasureUnitB == "mg/L", limit_b_uM :=
#     limit_b * 1000 / nitrogen_molecular_weight]
#   provider[basis == "NO3" & DetectionLimit_MeasureUnitB == "mg/L", limit_b_uM :=
#     limit_b * 1000 / nitrate_molecular_weight]
#   provider[, threshold_uM := data.table::fcoalesce(limit_a_uM, limit_b_uM)]
#   provider[, detected := !is.na(value_uM)]
#   provider[, censored :=
#     !detected &
#       Result_ResultDetectionCondition == "Not Detected" &
#       !is.na(threshold_uM)]
#   provider[, above_limit :=
#     !detected &
#       Result_ResultDetectionCondition ==
#         "Present Above Quantification Limit" &
#       !is.na(threshold_uM)]
#   provider[, unresolved := !detected & !censored & !above_limit]
#
#   scoped <- merge(
#     provider,
#     scope_keys,
#     by = c("location_id", "date")
#   )
#   expected_scoped_codes <- c(
#     "00600" = 15998L,
#     "00602" = 13348L,
#     "62854" = 9831L,
#     "62855" = 1068L,
#     "71887" = 1794L
#   )
#   observed_scoped_codes <- table(factor(
#     scoped$USGSpcode,
#     levels = names(expected_scoped_codes)
#   ))
#   if (!identical(as.integer(observed_scoped_codes),
#                  unname(expected_scoped_codes))) {
#     stop(
#       "the scoped USGS total-nitrogen parameter-code counts changed: ",
#       paste(names(observed_scoped_codes), observed_scoped_codes, collapse = ", ")
#     )
#   }
#   assert_count(sum(scoped$unresolved), 0L,
#                "unresolved provider total-nitrogen representations")
#   assert_count(sum(
#     scoped$Result_ResultDetectionCondition ==
#       "Present Above Quantification Limit"
#   ), 2L, "above-quantification provider representations")
#   assert_count(sum(scoped$above_limit), 2L,
#                "usable above-quantification provider representations")
#   assert_count(sum(
#     scoped$detected &
#       !is_blank(scoped$Result_ResultDetectionCondition)
#   ), 0L, "numeric USGS total-nitrogen rows with censor conditions")
#   assert_count(sum(
#     !is_blank(scoped$Result_MeasureQualifierCode)
#   ), 0L, "unexpected USGS total-nitrogen measure qualifiers")
#   supported_detection_condition <-
#     is_blank(scoped$Result_ResultDetectionCondition) |
#     scoped$Result_ResultDetectionCondition %in% c(
#       "Not Detected",
#       "Present Above Quantification Limit"
#     )
#   assert_count(sum(!supported_detection_condition), 0L,
#                "unsupported USGS total-nitrogen detection conditions")
#
#   if (any(is_blank(scoped$Activity_ActivityIdentifier))) {
#     stop("a scoped USGS total-nitrogen row lacks an activity id")
#   }
#   scoped[, best_priority := min(code_priority), by = .(
#     variable,
#     location_id,
#     date,
#     Activity_ActivityIdentifier
#   )]
#   selected <- scoped[code_priority == best_priority]
#
#   expected_selected_codes <- c(
#     "00600" = 15411L,
#     "00602" = 4629L,
#     "62854" = 9831L,
#     "62855" = 1068L,
#     "71887" = 0L
#   )
#   observed_selected_codes <- table(factor(
#     selected$USGSpcode,
#     levels = names(expected_selected_codes)
#   ))
#   if (!identical(as.integer(observed_selected_codes),
#                  unname(expected_selected_codes))) {
#     stop(
#       "the preferred USGS total-nitrogen code counts changed: ",
#       paste(
#         names(observed_selected_codes),
#         observed_selected_codes,
#         collapse = ", "
#       )
#     )
#   }
#   assert_count(sum(selected$unresolved), 0L,
#                "unresolved selected USGS total-nitrogen rows")
#
#   selected_above_limit <- selected[above_limit == TRUE]
#   assert_count(nrow(selected_above_limit), 1L,
#                "selected above-quantification total-nitrogen rows")
#   if (
#     selected_above_limit$location_id != "USGS-07241550" ||
#       selected_above_limit$date != data.table::as.IDate("1978-11-15") ||
#       selected_above_limit$USGSpcode != "00600" ||
#       selected_above_limit$Activity_ActivityIdentifier !=
#         "65602254-5132-49e1-8ea5-5e635eb2c345" ||
#       selected_above_limit$Result_MeasureIdentifier !=
#         "db18689f-699f-418d-98a7-b3f3e427e224" ||
#       selected_above_limit$DetectionLimit_TypeA != "Censoring Level" ||
#       selected_above_limit$DetectionLimit_MeasureUnitA != "mg/L"
#   ) {
#     stop("the reviewed above-quantification total-nitrogen row changed")
#   }
#   assert_values(
#     selected_above_limit$limit_a,
#     22,
#     "above-quantification total-nitrogen source limit"
#   )
#   assert_values(
#     selected_above_limit$threshold_uM,
#     22 * 1000 / nitrogen_molecular_weight,
#     "above-quantification total-nitrogen converted limit"
#   )
#
#   selected <- selected[unresolved == FALSE]
#   assert_count(nrow(selected[variable == "TN"]), 16479L,
#                "selected USGS total-nitrogen provider rows")
#   assert_count(nrow(selected[variable == "TDN"]), 14460L,
#                "selected USGS dissolved-total-nitrogen provider rows")
#   assert_count(sum(selected$variable == "TN" & selected$censored), 796L,
#                "selected censored USGS total-nitrogen rows")
#   assert_count(sum(selected$variable == "TDN" & selected$censored), 518L,
#                "selected censored USGS dissolved-total-nitrogen rows")
#   assert_count(sum(selected$variable == "TN" & selected$above_limit), 1L,
#                "selected above-limit USGS total-nitrogen rows")
#   assert_count(
#     data.table::uniqueN(selected[variable == "TN"]$location_id),
#     85L,
#     "selected USGS total-nitrogen sites"
#   )
#   assert_count(
#     data.table::uniqueN(selected[variable == "TDN"]$location_id),
#     84L,
#     "selected USGS dissolved-total-nitrogen sites"
#   )
#   assert_count(sum(selected$censored & selected$threshold_uM <= 0), 0L,
#                "nonpositive USGS total-nitrogen thresholds")
#   assert_count(sum(selected$above_limit & selected$threshold_uM <= 0), 0L,
#                "nonpositive USGS total-nitrogen lower limits")
#
#   # detected usgs nitrogen results define the daily mean when the day also has limits
#   # all-limit days retain their inequality qualifier
#   daily_review <- selected[, {
#     if (any(detected)) {
#       list(
#         value_qualifier = NA_character_,
#         detected_rows = sum(detected),
#         censored_rows = sum(censored),
#         above_limit_rows = sum(above_limit)
#       )
#     } else if (all(censored)) {
#       list(
#         value_qualifier = "<",
#         detected_rows = 0L,
#         censored_rows = sum(censored),
#         above_limit_rows = 0L
#       )
#     } else if (all(above_limit)) {
#       list(
#         value_qualifier = ">",
#         detected_rows = 0L,
#         censored_rows = 0L,
#         above_limit_rows = sum(above_limit)
#       )
#     } else {
#       stop("mixed USGS total-nitrogen inequality directions remain")
#     }
#   }, by = .(variable, location_id, date)]
#   expected_daily <- data.table::data.table(
#     variable = c("TN", "TDN"),
#     rows = c(15744L, 14190L),
#     detected_days = c(14963L, 13681L),
#     censored_days = c(780L, 509L),
#     above_limit_days = c(1L, 0L),
#     mixed_days = c(9L, 6L)
#   )
#   observed_daily <- daily_review[, .(
#     rows = .N,
#     detected_days = sum(is.na(value_qualifier)),
#     censored_days = sum(value_qualifier == "<", na.rm = TRUE),
#     above_limit_days = sum(value_qualifier == ">", na.rm = TRUE),
#     mixed_days = sum(
#       detected_rows > 0L & (censored_rows > 0L | above_limit_rows > 0L)
#     )
#   ), by = variable]
#   observed_daily <- observed_daily[match(
#     expected_daily$variable,
#     observed_daily$variable
#   )]
#   if (!identical(observed_daily, expected_daily)) {
#     stop(
#       "the expected USGS total-nitrogen daily results changed: ",
#       paste(capture.output(print(observed_daily)), collapse = " ")
#     )
#   }
#
#   provider_tn_keys <- unique(selected[
#     variable == "TN",
#     .(location_id, date)
#   ])
#   matched_legacy_tn <- merge(
#     current_scope[, .(legacy_row_id, location_id, date)],
#     provider_tn_keys,
#     by = c("location_id", "date")
#   )
#   replaced_legacy_ids <- unique(matched_legacy_tn$legacy_row_id)
#   retained_legacy_ids <- setdiff(
#     current_scope$legacy_row_id,
#     replaced_legacy_ids
#   )
#   assert_count(length(replaced_legacy_ids), 9124L,
#                "legacy total-nitrogen rows with provider replacements")
#   assert_count(length(retained_legacy_ids), 4373L,
#                "legacy total-nitrogen rows without provider replacements")
#
#   selected <- merge(
#     selected,
#     site_lookup[, .(location_id, Stream_Name)],
#     by = "location_id",
#     all.x = TRUE
#   )
#   if (anyNA(selected$Stream_Name)) {
#     stop("a rebuilt USGS total-nitrogen row lacks a reviewed site name")
#   }
#   data.table::setorder(
#     selected,
#     location_id,
#     date,
#     variable,
#     USGSpcode,
#     Activity_ActivityIdentifier
#   )
#
#   selected[, amount_uM := data.table::fifelse(
#     detected,
#     value_uM,
#     threshold_uM
#   )]
#   value_text <- format(
#     selected$amount_uM,
#     digits = 15L,
#     scientific = FALSE,
#     trim = TRUE
#   )
#   value_text[selected$censored] <- paste0("<", value_text[selected$censored])
#   value_text[selected$above_limit] <- paste0(">", value_text[selected$above_limit])
#
#   replacement <- selected[, .(
#     V1 = sprintf("USGS_TOTAL_N_%05d", seq_len(.N)),
#     variable,
#     LTER = "USGS",
#     Site.Stream.Name = Stream_Name,
#     site = Stream_Name,
#     Sampling.Date = as.character(date),
#     value = value_text,
#     units = "uM",
#     USGS.Provider.Parameter.Code = USGSpcode,
#     USGS.Provider.Activity.ID = Activity_ActivityIdentifier,
#     USGS.Provider.Result.ID = Result_MeasureIdentifier,
#     USGS.Provider.Source.Files = provider_files,
#     USGS.Provider.Detection.Condition = data.table::fifelse(
#       is_blank(Result_ResultDetectionCondition),
#       NA_character_,
#       Result_ResultDetectionCondition
#     ),
#     USGS.Provider.Sample.Fraction = Result_SampleFraction,
#     USGS.Provider.Measure.Status = Result_MeasureStatusIdentifier,
#     USGS.Provider.Characteristic = Result_Characteristic,
#     USGS.Provider.Description = Result_CharacteristicUserSupplied,
#     USGS.Provider.Method.Speciation = data.table::fifelse(
#       is_blank(Result_MethodSpeciation),
#       NA_character_,
#       Result_MethodSpeciation
#     ),
#     USGS.Provider.Raw.Value = data.table::fifelse(
#       is_blank(provider_value_text),
#       NA_character_,
#       provider_value_text
#     ),
#     USGS.Provider.Raw.Unit = data.table::fifelse(
#       is_blank(provider_unit),
#       NA_character_,
#       provider_unit
#     ),
#     USGS.Provider.Measure.Qualifier = data.table::fifelse(
#       is_blank(Result_MeasureQualifierCode),
#       NA_character_,
#       Result_MeasureQualifierCode
#     ),
#     USGS.Provider.Detection.Limit.Type.A = data.table::fifelse(
#       is_blank(DetectionLimit_TypeA),
#       NA_character_,
#       DetectionLimit_TypeA
#     ),
#     USGS.Provider.Detection.Limit.Value.A = data.table::fifelse(
#       is_blank(limit_a_text),
#       NA_character_,
#       limit_a_text
#     ),
#     USGS.Provider.Detection.Limit.Unit.A = data.table::fifelse(
#       is_blank(DetectionLimit_MeasureUnitA),
#       NA_character_,
#       DetectionLimit_MeasureUnitA
#     ),
#     USGS.Provider.Detection.Limit.Type.B = data.table::fifelse(
#       is_blank(DetectionLimit_TypeB),
#       NA_character_,
#       DetectionLimit_TypeB
#     ),
#     USGS.Provider.Detection.Limit.Value.B = data.table::fifelse(
#       is_blank(limit_b_text),
#       NA_character_,
#       limit_b_text
#     ),
#     USGS.Provider.Detection.Limit.Unit.B = data.table::fifelse(
#       is_blank(DetectionLimit_MeasureUnitB),
#       NA_character_,
#       DetectionLimit_MeasureUnitB
#     )
#   )]
#   data.table::setnames(replacement, "V1", "")
#
#   if (
#     nrow(replacement) != 30939L ||
#       anyDuplicated(replacement$USGS.Provider.Result.ID) > 0L ||
#       sum(startsWith(replacement$value, "<")) != 1314L ||
#       sum(startsWith(replacement$value, ">")) != 1L
#   ) {
#     stop("the rebuilt USGS total-nitrogen provider rows changed")
#   }
#
#   replacement <- as.data.frame(replacement, stringsAsFactors = FALSE)
#   attr(replacement, "legacy_tn_ids_replaced") <- replaced_legacy_ids
#   attr(replacement, "legacy_tn_ids_retained") <- retained_legacy_ids
#   replacement
# }
#
# # ---- usgs tds backfill ----
#
# build_usgs_tds_backfill <- function() {
#   legacy_filename <- "20221030_masterdata_chem_V2.csv"
#   tds_codes <- c("00515", "00525", "70300", "70301")
#
#   site_reference <- read_source(
#     "Site_Reference_Table.csv",
#     directory = key_dir
#   )
#   site_lookup <- data.table::as.data.table(site_reference[
#     site_reference$LTER == "USGS",
#     c("Stream_Name", "USGSGageNumber"),
#     drop = FALSE
#   ])
#   site_lookup[, location_id := paste0("USGS-", USGSGageNumber)]
#
#   if (
#     any(is_blank(site_lookup$Stream_Name)) ||
#       any(is_blank(site_lookup$USGSGageNumber)) ||
#       anyDuplicated(site_lookup$Stream_Name) > 0L ||
#       anyDuplicated(site_lookup$location_id) > 0L
#   ) {
#     stop("the reviewed USGS site-id mapping is not one-to-one")
#   }
#
#   exclusions <- data.table::as.data.table(readRDS(usgs_exclusion_file))
#   required_exclusion_columns <- c(
#     "location_id", "date", "provider_result_id", "parameter_code",
#     "provider_value", "provider_unit"
#   )
#   missing_exclusion_columns <- setdiff(
#     required_exclusion_columns,
#     names(exclusions)
#   )
#   if (length(missing_exclusion_columns) > 0L) {
#     stop(
#       "the USGS TDS exclusion review lacks columns: ",
#       paste(missing_exclusion_columns, collapse = ", ")
#     )
#   }
#
#   exact_target <- unique(exclusions[
#     !is_blank(provider_result_id),
#     .(
#       location_id,
#       date = data.table::as.IDate(date),
#       provider_result_id,
#       expected_code = parameter_code,
#       expected_value = as.numeric(provider_value),
#       expected_unit = provider_unit
#     )
#   ])
#
#   if (
#     nrow(exact_target) != 8835L ||
#       data.table::uniqueN(exact_target$provider_result_id) != 8835L ||
#       data.table::uniqueN(exact_target, by = c("location_id", "date")) !=
#         8621L ||
#       data.table::uniqueN(exact_target$location_id) != 26L ||
#       anyNA(exact_target$expected_value) ||
#       any(exact_target$expected_unit != "mg/L")
#   ) {
#     stop("the exact-match USGS TDS review scope changed")
#   }
#
#   reviewed_susquehanna <- read_source(
#     basename(reviewed_susquehanna_file),
#     directory = dirname(reviewed_susquehanna_file)
#   )
#   susquehanna_location_id <- site_lookup[
#     Stream_Name == "SUSQUEHANNA RIVER",
#     location_id
#   ]
#   if (length(susquehanna_location_id) != 1L) {
#     stop("the reviewed Susquehanna station id changed")
#   }
#   current_tds_keys <- unique(data.table::data.table(
#     location_id = susquehanna_location_id,
#     date = data.table::as.IDate(reviewed_susquehanna$date[
#       reviewed_susquehanna$LTER == "USGS" &
#         reviewed_susquehanna$Stream_Name == "SUSQUEHANNA RIVER" &
#         reviewed_susquehanna$solute == "tot dissolved solids" &
#         reviewed_susquehanna$solute_units == "mg/L" &
#         !is_blank(reviewed_susquehanna$amount)
#     ])
#   ))
#   assert_count(nrow(current_tds_keys), 575L,
#                "reviewed Susquehanna TDS dates")
#
#   exact_overlap <- exact_target[
#     current_tds_keys,
#     on = .(location_id, date),
#     nomatch = 0L
#   ]
#   assert_count(nrow(exact_overlap), 442L,
#                "provider-result overlap with reviewed Susquehanna TDS")
#   assert_count(
#     data.table::uniqueN(exact_overlap, by = c("location_id", "date")),
#     420L,
#     "date overlap with reviewed Susquehanna TDS"
#   )
#
#   exact_target <- exact_target[
#     !current_tds_keys,
#     on = .(location_id, date)
#   ]
#   if (
#     nrow(exact_target) != 8393L ||
#       data.table::uniqueN(exact_target$provider_result_id) != 8393L ||
#       data.table::uniqueN(exact_target, by = c("location_id", "date")) !=
#         8201L ||
#       data.table::uniqueN(exact_target$location_id) != 25L
#   ) {
#     stop("the missing exact-match USGS TDS scope changed")
#   }
#
#   provider_columns <- c(
#     "Location_Identifier",
#     "Activity_ActivityIdentifier",
#     "Activity_StartDate",
#     "Result_ResultDetectionCondition",
#     "Result_Characteristic",
#     "Result_CharacteristicUserSupplied",
#     "Result_MethodSpeciation",
#     "Result_SampleFraction",
#     "Result_MeasureIdentifier",
#     "Result_Measure",
#     "Result_MeasureUnit",
#     "Result_MeasureQualifierCode",
#     "Result_MeasureStatusIdentifier",
#     "DetectionLimit_TypeA",
#     "DetectionLimit_MeasureA",
#     "DetectionLimit_MeasureUnitA",
#     "DetectionLimit_TypeB",
#     "DetectionLimit_MeasureB",
#     "DetectionLimit_MeasureUnitB",
#     "USGSpcode"
#   )
#
#   read_provider_file <- function(path) {
#     provider_rows <- data.table::fread(
#       path,
#       select = provider_columns,
#       colClasses = "character",
#       showProgress = FALSE
#     )
#     provider_rows <- provider_rows[USGSpcode %in% tds_codes]
#     provider_rows[, provider_file := basename(path)]
#     provider_rows
#   }
#
#   provider_file_rows <- data.table::rbindlist(
#     lapply(c(usgs_provider_files, usgs_clinton_file), read_provider_file),
#     use.names = TRUE,
#     fill = TRUE
#   )
#   identity_columns <- setdiff(names(provider_file_rows), "provider_file")
#   provider <- provider_file_rows[
#     , .(provider_files = paste(sort(unique(provider_file)), collapse = ";")),
#     by = identity_columns
#   ]
#   data.table::setnames(
#     provider,
#     c(
#       "Location_Identifier",
#       "Activity_StartDate",
#       "Result_Measure",
#       "Result_MeasureUnit"
#     ),
#     c("location_id", "date", "provider_value", "provider_unit")
#   )
#   provider[, date := data.table::as.IDate(date)]
#   provider[, provider_value_text := provider_value]
#   provider[, limit_a_text := DetectionLimit_MeasureA]
#   provider[, limit_b_text := DetectionLimit_MeasureB]
#   provider[, provider_value := suppressWarnings(as.numeric(provider_value))]
#   provider[, limit_a := suppressWarnings(as.numeric(DetectionLimit_MeasureA))]
#   provider[, limit_b := suppressWarnings(as.numeric(DetectionLimit_MeasureB))]
#   provider_output_columns <- names(provider)
#
#   expected_target <- exact_target[, .(
#     Result_MeasureIdentifier = provider_result_id,
#     expected_location_id = location_id,
#     expected_date = date,
#     expected_code,
#     expected_value,
#     expected_unit
#   )]
#   exact_provider <- merge(
#     provider,
#     expected_target,
#     by = "Result_MeasureIdentifier"
#   )
#
#   if (
#     nrow(exact_provider) != 8393L ||
#       anyDuplicated(exact_provider$Result_MeasureIdentifier) > 0L ||
#       length(setdiff(
#         expected_target$Result_MeasureIdentifier,
#         exact_provider$Result_MeasureIdentifier
#       )) > 0L ||
#       any(exact_provider$location_id != exact_provider$expected_location_id) ||
#       any(exact_provider$date != exact_provider$expected_date) ||
#       any(exact_provider$USGSpcode != exact_provider$expected_code) ||
#       any(exact_provider$provider_unit != exact_provider$expected_unit) ||
#       any(abs(
#         exact_provider$provider_value - exact_provider$expected_value
#       ) > 1e-10)
#   ) {
#     stop("the exact provider records for the USGS TDS backfill changed")
#   }
#
#   code_priority <- c(
#     "70300" = 1L,
#     "00515" = 2L,
#     "70301" = 3L,
#     "00525" = 4L
#   )
#   exact_provider[, code_priority := unname(code_priority[USGSpcode])]
#   if (anyNA(exact_provider$code_priority)) {
#     stop("an unsupported USGS TDS parameter code remains")
#   }
#   exact_provider[, best_priority := min(code_priority), by = .(
#     location_id,
#     date
#   )]
#   selected_exact <- exact_provider[code_priority == best_priority]
#
#   expected_exact_codes <- c("70300" = 8206L, "70301" = 2L)
#   observed_exact_codes <- table(factor(
#     selected_exact$USGSpcode,
#     levels = names(expected_exact_codes)
#   ))
#   exact_daily <- selected_exact[, .(
#     rows = .N,
#     values = data.table::uniqueN(provider_value),
#     methods = data.table::uniqueN(USGSpcode)
#   ), by = .(location_id, date)]
#   if (
#     nrow(selected_exact) != 8208L ||
#       !identical(as.integer(observed_exact_codes),
#                  unname(expected_exact_codes)) ||
#       nrow(exact_daily) != 8201L ||
#       sum(exact_daily$rows > 1L) != 7L ||
#       max(exact_daily$rows) != 2L ||
#       any(exact_daily$values != 1L) ||
#       any(exact_daily$methods != 1L)
#   ) {
#     stop("the preferred-method USGS TDS records changed")
#   }
#   selected_exact <- selected_exact[, ..provider_output_columns]
#
#   clinton_location_id <- site_lookup[
#     Stream_Name == "Mississippi River at Clinton",
#     location_id
#   ]
#   if (
#     length(clinton_location_id) != 1L ||
#       clinton_location_id != "USGS-05420500"
#   ) {
#     stop("the reviewed Clinton station id changed")
#   }
#   clinton_expected <- data.table::data.table(
#     date = data.table::as.IDate(c("2005-06-20", "2013-05-13")),
#     expected_value = c(258, 186)
#   )
#   clinton_provider <- provider[
#     location_id == clinton_location_id &
#       date %in% clinton_expected$date &
#       USGSpcode == "70300" &
#       Result_Characteristic == "Total dissolved solids" &
#       provider_unit == "mg/L" &
#       !is.na(provider_value)
#   ]
#   clinton_check <- merge(
#     clinton_provider[, .(date, provider_value)],
#     clinton_expected,
#     by = "date"
#   )
#   if (
#     nrow(clinton_provider) != 2L ||
#       nrow(clinton_check) != 2L ||
#       any(abs(
#         clinton_check$provider_value - clinton_check$expected_value
#       ) > 1e-10)
#   ) {
#     stop("the reviewed Clinton TDS provider rows changed")
#   }
#
#   selected <- data.table::rbindlist(
#     list(selected_exact, clinton_provider[, ..provider_output_columns]),
#     use.names = TRUE,
#     fill = TRUE
#   )
#
#   # rebuild all valid tds days for four dsi sites removed by full deduplication
#   four_site_ids <- c(
#     "USGS-11264500", "USGS-10343500", "USGS-09352900", "USGS-15565447"
#   )
#   four_site_provider <- provider[
#     location_id %in% four_site_ids &
#       Result_Characteristic == "Total dissolved solids" &
#       provider_unit == "mg/L" &
#       is.finite(provider_value) & provider_value > 0 &
#       is_blank(Result_ResultDetectionCondition) &
#       is_blank(Result_MeasureQualifierCode)
#   ]
#   four_site_provider[, code_priority := unname(code_priority[USGSpcode])]
#   if (anyNA(four_site_provider$code_priority)) {
#     stop("an unsupported four-site USGS TDS parameter code remains")
#   }
#   four_site_provider[, best_priority := min(code_priority), by = .(
#     location_id,
#     date
#   )]
#   four_site_selected <- four_site_provider[code_priority == best_priority]
#
#   four_site_scope <- four_site_selected[, .(
#     source_rows = .N,
#     code_count = data.table::uniqueN(USGSpcode),
#     unit_count = data.table::uniqueN(provider_unit),
#     characteristic_count = data.table::uniqueN(Result_Characteristic)
#   ), by = .(location_id, date)]
#   if (
#     nrow(four_site_selected) != 2223L ||
#       nrow(four_site_scope) != 2163L ||
#       sum(four_site_scope$source_rows > 1L) != 59L ||
#       max(four_site_scope$source_rows) != 3L ||
#       any(four_site_scope$code_count != 1L) ||
#       any(four_site_scope$unit_count != 1L) ||
#       any(four_site_scope$characteristic_count != 1L)
#   ) {
#     stop("the four-site provider TDS daily scope changed")
#   }
#
#   expected_four_site_days <- c(
#     "USGS-09352900" = 696L,
#     "USGS-10343500" = 474L,
#     "USGS-11264500" = 729L,
#     "USGS-15565447" = 264L
#   )
#   observed_four_site_days <- table(factor(
#     four_site_scope$location_id,
#     levels = names(expected_four_site_days)
#   ))
#   if (!identical(
#     as.integer(observed_four_site_days),
#     unname(expected_four_site_days)
#   )) {
#     stop("the four-site provider TDS site counts changed")
#   }
#
#   expected_four_site_codes <- c(
#     "00515" = 13L,
#     "70300" = 1021L,
#     "70301" = 1129L,
#     "00525" = 0L
#   )
#   observed_four_site_codes <- table(factor(
#     four_site_selected[, unique(USGSpcode), by = .(location_id, date)]$V1,
#     levels = names(expected_four_site_codes)
#   ))
#   if (!identical(
#     as.integer(observed_four_site_codes),
#     unname(expected_four_site_codes)
#   )) {
#     stop("the four-site provider TDS code counts changed")
#   }
#
#   collapse_provider_values <- function(values) {
#     values <- values[!is.na(values) & values != ""]
#     if (length(values) == 0L) return(NA_character_)
#     paste(sort(unique(values)), collapse = ";")
#   }
#   four_site_daily <- four_site_selected[, {
#     daily <- .SD[1L]
#     daily$provider_value <- mean(provider_value)
#     daily$provider_value_text <- format(
#       daily$provider_value,
#       digits = 15L,
#       scientific = FALSE,
#       trim = TRUE
#     )
#     daily$Activity_ActivityIdentifier <- collapse_provider_values(
#       Activity_ActivityIdentifier
#     )
#     daily$Result_MeasureIdentifier <- collapse_provider_values(
#       Result_MeasureIdentifier
#     )
#     daily$provider_files <- collapse_provider_values(provider_files)
#     daily
#   }, by = .(location_id, date)]
#   four_site_daily <- four_site_daily[, ..provider_output_columns]
#
#   selected <- data.table::rbindlist(
#     list(selected, four_site_daily),
#     use.names = TRUE,
#     fill = TRUE
#   )
#   selected_daily <- selected[, .(
#     rows = .N,
#     values = data.table::uniqueN(provider_value),
#     methods = data.table::uniqueN(USGSpcode)
#   ), by = .(location_id, date)]
#   selected_code_counts <- table(factor(
#     selected$USGSpcode,
#     levels = c("00515", "70300", "70301")
#   ))
#
#   if (
#     nrow(selected) != 10373L ||
#       data.table::uniqueN(selected$Result_MeasureIdentifier) != 10373L ||
#       nrow(selected_daily) != 10366L ||
#       data.table::uniqueN(selected$location_id) != 29L ||
#       !identical(as.integer(selected_code_counts), c(13L, 9229L, 1131L)) ||
#       any(selected_daily$values != 1L) ||
#       any(selected_daily$methods != 1L) ||
#       any(is_blank(selected$Activity_ActivityIdentifier)) ||
#       any(is_blank(selected$Result_MeasureIdentifier)) ||
#       any(selected$Result_Characteristic != "Total dissolved solids") ||
#       any(selected$provider_unit != "mg/L") ||
#       any(!is.finite(selected$provider_value)) ||
#       any(selected$provider_value <= 0) ||
#       any(!is_blank(selected$Result_ResultDetectionCondition)) ||
#       any(!is_blank(selected$Result_MeasureQualifierCode))
#   ) {
#     stop("the final USGS TDS backfill records changed")
#   }
#
#   if (nrow(merge(
#     unique(selected[, .(location_id, date)]),
#     current_tds_keys,
#     by = c("location_id", "date")
#   )) > 0L) {
#     stop("the USGS TDS backfill overlaps the reviewed Susquehanna source")
#   }
#
#   unresolved_keys <- data.table::data.table(
#     location_id = c("USGS-01578310", "USGS-05420500"),
#     date = data.table::as.IDate(c("1996-02-24", "1978-04-22"))
#   )
#   usable_unresolved <- provider[
#     unresolved_keys,
#     on = .(location_id, date),
#     nomatch = 0L
#   ][
#     provider_unit == "mg/L" &
#       Result_Characteristic == "Total dissolved solids" &
#       !is.na(provider_value)
#   ]
#   assert_count(nrow(usable_unresolved), 0L,
#                "usable provider values for the two unresolved TDS dates")
#   assert_count(nrow(selected[
#     unresolved_keys,
#     on = .(location_id, date),
#     nomatch = 0L
#   ]), 0L, "unresolved TDS dates in the backfill")
#
#   selected <- merge(
#     selected,
#     site_lookup[, .(location_id, Stream_Name)],
#     by = "location_id",
#     all.x = TRUE
#   )
#   if (anyNA(selected$Stream_Name)) {
#     stop("a USGS TDS backfill row lacks a reviewed site name")
#   }
#   data.table::setorder(
#     selected,
#     location_id,
#     date,
#     USGSpcode,
#     Activity_ActivityIdentifier
#   )
#
#   backfill <- selected[, .(
#     V1 = sprintf("USGS_TDS_%05d", seq_len(.N)),
#     variable = "TDS",
#     LTER = "USGS",
#     Site.Stream.Name = Stream_Name,
#     site = Stream_Name,
#     Sampling.Date = as.character(date),
#     value = provider_value_text,
#     units = "mg/L",
#     USGS.Provider.Parameter.Code = USGSpcode,
#     USGS.Provider.Activity.ID = Activity_ActivityIdentifier,
#     USGS.Provider.Result.ID = Result_MeasureIdentifier,
#     USGS.Provider.Source.Files = provider_files,
#     USGS.Provider.Detection.Condition = data.table::fifelse(
#       is_blank(Result_ResultDetectionCondition),
#       NA_character_,
#       Result_ResultDetectionCondition
#     ),
#     USGS.Provider.Sample.Fraction = Result_SampleFraction,
#     USGS.Provider.Measure.Status = Result_MeasureStatusIdentifier,
#     USGS.Provider.Characteristic = Result_Characteristic,
#     USGS.Provider.Description = Result_CharacteristicUserSupplied,
#     USGS.Provider.Method.Speciation = data.table::fifelse(
#       is_blank(Result_MethodSpeciation),
#       NA_character_,
#       Result_MethodSpeciation
#     ),
#     USGS.Provider.Raw.Value = provider_value_text,
#     USGS.Provider.Raw.Unit = provider_unit,
#     USGS.Provider.Measure.Qualifier = data.table::fifelse(
#       is_blank(Result_MeasureQualifierCode),
#       NA_character_,
#       Result_MeasureQualifierCode
#     ),
#     USGS.Provider.Detection.Limit.Type.A = data.table::fifelse(
#       is_blank(DetectionLimit_TypeA),
#       NA_character_,
#       DetectionLimit_TypeA
#     ),
#     USGS.Provider.Detection.Limit.Value.A = data.table::fifelse(
#       is_blank(limit_a_text),
#       NA_character_,
#       limit_a_text
#     ),
#     USGS.Provider.Detection.Limit.Unit.A = data.table::fifelse(
#       is_blank(DetectionLimit_MeasureUnitA),
#       NA_character_,
#       DetectionLimit_MeasureUnitA
#     ),
#     USGS.Provider.Detection.Limit.Type.B = data.table::fifelse(
#       is_blank(DetectionLimit_TypeB),
#       NA_character_,
#       DetectionLimit_TypeB
#     ),
#     USGS.Provider.Detection.Limit.Value.B = data.table::fifelse(
#       is_blank(limit_b_text),
#       NA_character_,
#       limit_b_text
#     ),
#     USGS.Provider.Detection.Limit.Unit.B = data.table::fifelse(
#       is_blank(DetectionLimit_MeasureUnitB),
#       NA_character_,
#       DetectionLimit_MeasureUnitB
#     )
#   )]
#   data.table::setnames(backfill, "V1", "")
#
#   if (
#     nrow(backfill) != 10373L ||
#       anyDuplicated(backfill$USGS.Provider.Result.ID) > 0L ||
#       any(startsWith(backfill$value, "<")) ||
#       any(startsWith(backfill$value, ">"))
#   ) {
#     stop("the prepared USGS TDS backfill rows changed")
#   }
#
#   as.data.frame(backfill, stringsAsFactors = FALSE)
# }
#
# # ---- carey prepared input rules ----
#
# build_carey_shift_repair <- function(template_data) {
#   if (!requireNamespace("readxl", quietly = TRUE)) {
#     stop("the carey repair requires the readxl package")
#   }
#
#   source <- readxl::read_excel(
#     carey_file,
#     sheet = "Raw Stream Data",
#     range = "A4:AL200",
#     col_names = FALSE,
#     .name_repair = "minimal"
#   )
#   source <- as.data.frame(source, stringsAsFactors = FALSE)
#   names(source) <- c(
#     "lter", "site", "date", "time", "treatment", "daily_q",
#     "instant_q", "stage", "temp", "conductivity",
#     "specific_conductance", "turbidity", "tss", "vss", "dsi",
#     "tn", "din", "nox", "no3", "no2", "nh4", "tp", "po4",
#     "srp", "doc", "toc", "alkalinity", "anc", "ph", "dic",
#     "suspended_chl", "benthic_chl", "na", "k", "ca", "mg",
#     "so4", "cl"
#   )
#   source$date <- as.Date(source$date)
#   source <- source[
#     !is.na(source$date) &
#       source$date >= as.Date("2011-06-15") &
#       source$date <= as.Date("2011-09-28"),
#     ,
#     drop = FALSE
#   ]
#
#   assert_count(nrow(source), 60L, "carey shifted source rows")
#   assert_count(length(unique(source$date)), 15L,
#                "carey shifted source dates")
#   assert_count(length(unique(source$site)), 4L,
#                "carey shifted source sites")
#
#   numeric_columns <- c(
#     "temp", "conductivity", "specific_conductance", "turbidity",
#     "dsi", "tn", "din", "nox", "no3", "nh4", "po4", "srp"
#   )
#   for (column_name in numeric_columns) {
#     source[[column_name]] <- as_number(source[[column_name]])
#   }
#
#   if (
#     any(abs(source$dsi - source$tn - source$no3) > 1e-10) ||
#       any(abs(source$tn - source$din - source$nox) > 1e-10)
#   ) {
#     stop("the carey shifted nitrogen identities changed")
#   }
#   # repeated nitrogen identities confirm one consistent header shift in this window
#
#   site_map <- c(
#     "Aberjona" = "Aberjona",
#     "Ipswich at Middleton" = "Ipswich at Middleton",
#     "Ipswich at Ipswich" = "Ipswich at Ipswich",
#     "Parker" = "Parker River at Byfield, MA"
#   )
#   repaired_site <- unname(site_map[source$site])
#   if (any(is.na(repaired_site))) {
#     stop("the carey shifted site labels changed")
#   }
#
#   conductivity <- ifelse(
#     source$date == as.Date("2011-09-28"),
#     source$po4,
#     source$conductivity
#   )
#   ph <- ifelse(
#     source$date == as.Date("2011-09-28"),
#     source$conductivity,
#     source$po4
#   )
#   reviewed_conductivity <- source$site == "Parker" &
#     (
#       source$date == as.Date("2011-06-24") &
#         abs(conductivity - 17.5) < 1e-10 |
#         source$date == as.Date("2011-07-02") &
#           abs(conductivity - 24.6) < 1e-10
#     )
#   reviewed_temperature <- source$site == "Parker" &
#     source$date == as.Date("2011-07-02") &
#     abs(source$temp - 190.5) < 1e-10
#   assert_count(sum(reviewed_conductivity), 2L,
#                "carey reviewed conductivity rows")
#   assert_count(sum(reviewed_temperature), 1L,
#                "carey reviewed temperature rows")
#   # keep these conductivity and temperature values for later outlier review
#   assert_values(
#     conductivity[reviewed_conductivity],
#     c(17.5, 24.6),
#     "prepared Carey reviewed conductivity values"
#   )
#   assert_values(
#     source$temp[reviewed_temperature],
#     190.5,
#     "prepared Carey reviewed temperature value"
#   )
#
#   # rebuild the affected fields from their reviewed workbook positions
#   repaired_values <- list(
#     "temp" = list(values = source$temp, units = "C"),
#     "Conductivity" = list(values = conductivity, units = "uS/cm"),
#     "TSS" = list(values = source$specific_conductance, units = "mg/L"),
#     "DSi" = list(values = source$turbidity, units = "uM"),
#     "DIN" = list(values = source$dsi, units = "uM"),
#     "NOx" = list(values = source$tn, units = "uM"),
#     "NO3" = list(values = source$din, units = "uM"),
#     "NO2" = list(values = source$nox, units = "uM"),
#     "NH4" = list(values = source$no3, units = "uM"),
#     "PO4" = list(values = source$nh4, units = "uM"),
#     "pH" = list(values = ph, units = "SU"),
#     "suspended Chl" = list(values = source$srp * 1000, units = "ug/L")
#   )
#
#   repaired <- lapply(names(repaired_values), function(variable_name) {
#     values <- repaired_values[[variable_name]]$values
#     keep <- !is.na(values) & is.finite(values)
#     data.frame(
#       V1 = NA_character_,
#       variable = variable_name,
#       LTER = "Ipswitch(Carey)",
#       Site.Stream.Name = repaired_site[keep],
#       site = repaired_site[keep],
#       Sampling.Date = format(source$date[keep], "%Y-%m-%d"),
#       value = format(
#         values[keep],
#         scientific = FALSE,
#         trim = TRUE,
#         digits = 15
#       ),
#       units = repaired_values[[variable_name]]$units,
#       stringsAsFactors = FALSE,
#       check.names = FALSE
#     )
#   })
#   repaired <- do.call(rbind, repaired)
#   rownames(repaired) <- NULL
#
#   expected_counts <- c(
#     temp = 60L,
#     Conductivity = 60L,
#     TSS = 55L,
#     DSi = 60L,
#     DIN = 60L,
#     NOx = 60L,
#     NO3 = 60L,
#     NO2 = 60L,
#     NH4 = 60L,
#     PO4 = 60L,
#     pH = 48L,
#     `suspended Chl` = 60L
#   )
#   observed_counts <- table(factor(
#     repaired$variable,
#     levels = names(expected_counts)
#   ))
#   if (!identical(as.integer(observed_counts), unname(expected_counts))) {
#     stop("the carey repaired variable counts changed")
#   }
#
#   names(repaired)[1L] <- names(template_data)[1L]
#   next_id <- max(as_number(template_data[[1L]]), na.rm = TRUE)
#   repaired[[1L]] <- as.character(next_id + seq_len(nrow(repaired)))
#   extra_columns <- setdiff(names(template_data), names(repaired))
#   for (column_name in extra_columns) {
#     repaired[[column_name]] <- NA_character_
#   }
#   column_order <- match(names(template_data), names(repaired))
#   if (anyNA(column_order)) {
#     stop("the carey repair output columns changed")
#   }
#   repaired <- repaired[, column_order, drop = FALSE]
#   names(repaired) <- names(template_data)
#   repaired
# }
#
# # ---- prepared arc input rules ----
#
# arc_provider_column_map <- c(
#   Temp_C = "Temp.C",
#   Cond_uS = "Spec.Cond",
#   pH = "pH",
#   Alk_ueqL = "alkalinity",
#   CO2_uM = "DIC",
#   DOC_uM = "DOC",
#   NH4_phen_uM = "NH4",
#   NH4_OPA_uM = "NH4",
#   NH4_uM = "NH4",
#   PO4_uM = "PO4",
#   NO3_uM = "NOx",
#   TDN_uM = "TN",
#   TDP_uM = "TP",
#   Ca_uM = "Ca",
#   Mg_uM = "Mg",
#   Na_uM = "Na",
#   K_uM = "K",
#   Si_uM = "DSi",
#   SO4_uM = "SO4",
#   Cl_uM = "Cl"
# )
#
# read_arc_provider_long <- function(provider_file) {
#   source <- read_source(
#     basename(provider_file),
#     directory = dirname(provider_file)
#   )
#   required_columns <- c(
#     "SortChem", "Site", "Date", "Alk_ueqL", "CO2_uM", "TDN_uM", "TDP_uM"
#   )
#   missing_columns <- setdiff(required_columns, names(source))
#   if (length(missing_columns) > 0L) {
#     stop(
#       "the ARC provider file is missing: ",
#       paste(missing_columns, collapse = ", ")
#     )
#   }
#
#   sample_rows <- grepl("^[0-9]{4}-[0-9]{4}$", source$SortChem) &
#     grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", source$Date)
#   expected_samples <- c(
#     "1988-2010_Kling_Akchem.csv" = 8187L,
#     "legacy_audit_ARC_2002-2024_Kling_AON_Imnavait_Chemistry.csv" = 1022L,
#     "legacy_audit_ARC_2012-2020_Kling_Akchem.02.csv" = 1784L
#   )
#   filename <- basename(provider_file)
#   assert_count(
#     sum(sample_rows),
#     expected_samples[[filename]],
#     paste0("ARC provider samples in ", filename)
#   )
#   source <- source[sample_rows, , drop = FALSE]
#   if (!"water_type" %in% names(source)) {
#     source$water_type <- NA_character_
#   }
#
#   available_columns <- intersect(names(arc_provider_column_map), names(source))
#   # turn each measured column into rows, keeping the original column order
#   long_parts <- vector("list", length(available_columns))
#   for (column_number in seq_along(available_columns)) {
#     provider_column <- available_columns[column_number]
#     provider_value <- as_number(source[[provider_column]])
#     keep <- is.finite(provider_value)
#     long_parts[[column_number]] <- data.frame(
#       provider_file = filename,
#       sample_id = source$SortChem[keep],
#       site = source$Site[keep],
#       date = source$Date[keep],
#       water_type = source$water_type[keep],
#       provider_field = provider_column,
#       legacy_variable = unname(arc_provider_column_map[[provider_column]]),
#       value = provider_value[keep],
#       stringsAsFactors = FALSE
#     )
#   }
#   provider_long <- do.call(rbind, long_parts)
#   rownames(provider_long) <- NULL
#
#   expected_long_rows <- c(
#     "1988-2010_Kling_Akchem.csv" = 96383L,
#     "legacy_audit_ARC_2002-2024_Kling_AON_Imnavait_Chemistry.csv" = 12717L,
#     "legacy_audit_ARC_2012-2020_Kling_Akchem.02.csv" = 29477L
#   )
#   assert_count(
#     nrow(provider_long),
#     expected_long_rows[[filename]],
#     paste0("ARC provider measurements in ", filename)
#   )
#   provider_long
# }
#
# find_arc_provider_matches <- function(data, row_numbers, provider, tolerance = 1e-8) {
#   provider_key <- paste(
#     provider$site,
#     provider$date,
#     provider$legacy_variable,
#     sep = "\r"
#   )
#   provider_rows_by_key <- split(seq_len(nrow(provider)), provider_key)
#   data_key <- paste(
#     data$Site.Stream.Name[row_numbers],
#     data$Sampling.Date[row_numbers],
#     data$variable[row_numbers],
#     sep = "\r"
#   )
#   data_value <- as_number(data$value[row_numbers])
#
#   # a match needs the same site, date, variable, and value within the reviewed tolerance
#   matches <- vector("list", length(row_numbers))
#   for (position in seq_along(row_numbers)) {
#     matches[[position]] <- integer()
#     candidates <- provider_rows_by_key[[data_key[[position]]]]
#     if (is.null(candidates) || !is.finite(data_value[[position]])) {
#       next
#     }
#     matches[[position]] <- candidates[
#       abs(provider$value[candidates] - data_value[[position]]) <= tolerance
#     ]
#   }
#   matches
# }
#
# refresh_arc_legacy <- function(data) {
#   provider_tables <- vector("list", length(arc_provider_files))
#   for (file_number in seq_along(arc_provider_files)) {
#     provider_tables[[file_number]] <- read_arc_provider_long(arc_provider_files[file_number])
#   }
#   provider <- do.call(rbind, provider_tables)
#   rownames(provider) <- NULL
#   assert_count(nrow(provider), 138577L, "combined ARC provider measurements")
#
#   semantic_variables <- c(
#     DIC = "CO2",
#     TN = "TDN",
#     TP = "TDP",
#     alkalinity = "alkalinity"
#   )
#   expected_semantic_counts <- c(
#     DIC = 6651L,
#     TN = 6917L,
#     TP = 6426L,
#     alkalinity = 7519L
#   )
#   semantic_rows <- which(
#     data$LTER == "ARC" & data$variable %in% names(semantic_variables)
#   )
#   observed_semantic_counts <- table(factor(
#     data$variable[semantic_rows],
#     levels = names(expected_semantic_counts)
#   ))
#   if (!identical(
#     as.integer(observed_semantic_counts),
#     unname(expected_semantic_counts)
#   )) {
#     stop("the ARC legacy semantic-label counts changed")
#   }
#   semantic_matches <- find_arc_provider_matches(
#     data,
#     semantic_rows,
#     provider
#   )
#   if (any(lengths(semantic_matches) == 0L)) {
#     stop("an ARC semantic correction lacks an exact provider match")
#   }
#
#   mixed_sites <- c(
#     "Dennis Lake Outlet",
#     "E 01 Inlet",
#     "E 01 into Toolik",
#     "LTER 347 Outlet",
#     "NE 14 Lake Slump Inlet"
#   )
#   mixed_rows <- which(
#     data$LTER == "ARC" & data$Site.Stream.Name %in% mixed_sites
#   )
#   assert_count(length(mixed_rows), 612L, "mixed-water-type ARC rows")
#   mixed_matches <- find_arc_provider_matches(data, mixed_rows, provider)
#   if (any(lengths(mixed_matches) == 0L)) {
#     stop("a mixed-water-type ARC row lacks an exact provider match")
#   }
#
#   matched_sample_counts <- vapply(mixed_matches, function(matches) {
#     length(unique(provider$sample_id[matches]))
#   }, integer(1))
#   matched_water_type <- vapply(mixed_matches, function(matches) {
#     water_type <- unique(provider$water_type[matches])
#     water_type <- water_type[!is.na(water_type) & trimws(water_type) != ""]
#     if (length(water_type) != 1L) {
#       return(NA_character_)
#     }
#     water_type
#   }, character(1))
#   if (
#     any(matched_sample_counts != 1L) ||
#       any(is.na(matched_water_type)) ||
#       any(!matched_water_type %in% c("ST", "LA"))
#   ) {
#     stop("the ARC provider sample identity or water type is ambiguous")
#   }
#   assert_count(sum(matched_water_type == "ST"), 415L,
#                "stream-water ARC rows")
#   assert_count(sum(matched_water_type == "LA"), 197L,
#                "lake-water ARC rows")
#   assert_count(
#     sum(data$variable[mixed_rows] == "DSi" & matched_water_type == "ST"),
#     23L,
#     "stream-water ARC Si rows"
#   )
#   assert_count(
#     sum(data$variable[mixed_rows] == "DSi" & matched_water_type == "LA"),
#     11L,
#     "lake-water ARC Si rows"
#   )
#
#   remove_rows <- rep(FALSE, nrow(data))
#   remove_rows[mixed_rows[matched_water_type == "LA"]] <- TRUE
#   data <- data[!remove_rows, , drop = FALSE]
#   rownames(data) <- NULL
#
#   retained_arc <- data$LTER == "ARC"
#   data$variable[retained_arc & data$variable == "DIC"] <- "CO2"
#   data$variable[retained_arc & data$variable == "TN"] <- "TDN"
#   data$variable[retained_arc & data$variable == "TP"] <- "TDP"
#   data$units[retained_arc & data$variable == "alkalinity"] <- "ueq/L"
#
#   expected_retained_counts <- c(
#     CO2 = 6640L,
#     TDN = 6906L,
#     TDP = 6418L,
#     alkalinity = 7506L
#   )
#   observed_retained_counts <- c(
#     CO2 = sum(retained_arc & data$variable == "CO2"),
#     TDN = sum(retained_arc & data$variable == "TDN"),
#     TDP = sum(retained_arc & data$variable == "TDP"),
#     alkalinity = sum(retained_arc & data$variable == "alkalinity")
#   )
#   if (!identical(observed_retained_counts, expected_retained_counts)) {
#     stop("the prepared ARC semantic-label counts changed")
#   }
#   assert_count(
#     sum(retained_arc & data$variable == "alkalinity" & data$units == "ueq/L"),
#     7506L,
#     "prepared ARC alkalinity-unit rows"
#   )
#   assert_count(sum(retained_arc & data$variable %in% c("DIC", "TN", "TP")),
#                0L, "prepared ARC old semantic labels")
#   data
# }
#
# # ---- prepared usgs input rules ----
#
# prepare_legacy_usgs <- function() {
#   filename <- "20221030_masterdata_chem_V2.csv"
#   before <- read_source(filename)
#   after <- before
#   value <- as_number(after$value)
#
#   assert_count(nrow(after), 1025369L, "corrected legacy chemistry rows")
#   after <- refresh_arc_legacy(after)
#   value <- as_number(after$value)
#   assert_count(nrow(after), 1025172L,
#                "legacy chemistry rows after the ARC provider refresh")
#
#   four_dates <- c("1980-03-24", "1980-04-03", "1996-02-24", "2010-12-07")
#   four_tds <- after$LTER == "USGS" &
#     after$Site.Stream.Name == "SUSQUEHANNA RIVER" &
#     after$Sampling.Date %in% four_dates & after$variable == "TN" &
#     after$units == "uM"
#   assert_count(sum(four_tds, na.rm = TRUE), 0L,
#                "four corrected legacy Susquehanna TDS rows")
#
#   clinton_expected <- c(
#     "1978-04-22" = 171,
#     "2005-06-20" = 255,
#     "2013-05-13" = 196
#   )
#   clinton_tds <- after$LTER == "USGS" &
#     after$Site.Stream.Name == "Misssissippi River at Clinton" &
#     after$Sampling.Date %in% names(clinton_expected) &
#     after$variable == "TN" & after$units == "uM"
#   assert_count(sum(clinton_tds, na.rm = TRUE), 3L,
#                "remaining legacy Clinton TDS rows")
#   clinton_back_converted <- value[clinton_tds] * 14.0067 / 1000
#   clinton_dates <- after$Sampling.Date[clinton_tds]
#   assert_values(
#     clinton_back_converted[order(clinton_dates)],
#     unname(clinton_expected[order(names(clinton_expected))]),
#     "remaining legacy Clinton TDS values"
#   )
#
#   withdrawn_mackenzie_temp <- after[[1L]] == "871741" &
#     after$LTER == "GRO" &
#     after$site == "Mackenzie" &
#     after$Site.Stream.Name == "Mackenzie" &
#     after$Sampling.Date == "2017-02-22" &
#     after$variable == "Temp.C" &
#     after$units == "uM" &
#     value == -2
#   withdrawn_mackenzie_temp[is.na(withdrawn_mackenzie_temp)] <- FALSE
#   assert_count(sum(withdrawn_mackenzie_temp), 1L,
#                "withdrawn Mackenzie temperature row")
#   # keep the reported temperature for later outlier review
#   assert_values(as_number(after$value[withdrawn_mackenzie_temp]), -2,
#                 "prepared Mackenzie reviewed temperature value")
#
#   source_name_map <- data.frame(
#     LTER = c(
#       "USGS", "USGS", "ARC", "ARC", "ARC", "Ipswitch(Carey)",
#       "PIE", "PIE", "PIE", "PIE", "PIE"
#     ),
#     old = c(
#       "Misssissippi River at Clinton", "North Sylamore", "I4 into I5",
#       "I7 Into I9", "I8 into I9", "Parker", "MB-SAL",
#       "YSI-CC", "YSI-IR", "YSI-SB", "10"
#     ),
#     new = c(
#       "Mississippi River at Clinton", "North Slyamore Creek", "I4 Into I5",
#       "I7 into I9", "I8 Into I9", "Parker River at Byfield, MA", "MB-62",
#       "Cart Creek", "Ipswich at North Reading", "Sawmill Brook", "Parker"
#     ),
#     expected = c(2751L, 748L, 1279L, 4L, 3411L, 429L, 9L, 35L, 27L, 32L, 44L),
#     stringsAsFactors = FALSE
#   )
#
#   for (source_row in seq_len(nrow(source_name_map))) {
#     target <- after$LTER == source_name_map$LTER[source_row] &
#       after$site == source_name_map$old[source_row] &
#       after$Site.Stream.Name == source_name_map$old[source_row]
#     assert_count(
#       sum(target, na.rm = TRUE),
#       source_name_map$expected[source_row],
#       paste0("legacy source name ", source_name_map$old[source_row])
#     )
#     after$site[target] <- source_name_map$new[source_row]
#     after$Site.Stream.Name[target] <- source_name_map$new[source_row]
#   }
#
#   # match the older niva spelling to the site reference name
#   niva_osteglo_alias <- after$LTER == "NIVA" &
#     after$site == "ASTEGLO" &
#     after$Site.Stream.Name == "ØSTEGLO"
#   assert_count(sum(niva_osteglo_alias, na.rm = TRUE), 362L,
#                "legacy NIVA OSTEGLO alias rows")
#   after$site[niva_osteglo_alias] <- "OSTEGLO"
#   after$Site.Stream.Name[niva_osteglo_alias] <- "OSTEGLO"
#
#   east_rows <- list(
#     doe = after$LTER == "DOE SFA East River" &
#       after$site == "Coal Creek11" & after$Site.Stream.Name == "Coal Creek11",
#     sfa = after$LTER == "East River SFA" &
#       after$site == "Coal Creek" & after$Site.Stream.Name == "Coal Creek"
#   )
#   assert_count(sum(east_rows$doe), 7515L, "legacy DOE East River rows")
#   assert_count(sum(east_rows$sfa), 827L, "legacy SFA East River rows")
#   for (target in east_rows) {
#     after$LTER[target] <- "EastRiverSFA"
#     after$site[target] <- "coal_11"
#     after$Site.Stream.Name[target] <- "coal_11"
#   }
#
#   east_cations <- after$LTER == "EastRiverSFA" &
#     after$site == "coal_11" &
#     after$Site.Stream.Name == "coal_11" &
#     after$variable %in% c("Ca", "K", "Mg", "Na") &
#     after$units == "uM"
#   east_cation_counts <- table(after$variable[east_cations])
#   expected_east_cation_counts <- c(Ca = 644L, K = 644L, Mg = 644L, Na = 644L)
#   assert_count(sum(east_cations), 2576L,
#                "legacy East River cation rows")
#   if (
#     !identical(names(east_cation_counts), names(expected_east_cation_counts)) ||
#       !identical(
#         as.integer(east_cation_counts),
#         unname(expected_east_cation_counts)
#       ) ||
#       min(after$Sampling.Date[east_cations]) != "2015-12-31" ||
#       max(after$Sampling.Date[east_cations]) != "2019-10-18"
#   ) {
#     stop("the reviewed legacy East River cation scope changed")
#   }
#   # the original source reports these cations in ppb, equivalent to ug/l
#   # the legacy conversion treated them as mg/l and made them 1,000 times high
#   after$value[east_cations] <- as.character(value[east_cations] / 1000)
#
#   east_don <- after$LTER == "EastRiverSFA" & after$site == "coal_11" &
#     after$variable == "DON" & after$units == "uM"
#   krycklan_no3 <- after$site == "Site 7" & after$variable == "NO3" &
#     after$units == "uM"
#   krycklan_srp <- after$site == "Site 7" & after$variable == "SRP"
#   camels_po4 <- after$site %in% c("SOPCHOPPY RIVER", "Wild River") &
#     after$variable == "PO4" & after$units == "uM"
#   umr_nh4 <- after$LTER == "UMR(Jankowski)" & after$variable == "NH4" &
#     after$units == "uM"
#
#   assert_count(sum(east_don), 507L, "legacy East River mislabeled DON rows")
#   assert_count(sum(krycklan_no3), 358L, "legacy Krycklan NO3 rows")
#   assert_count(sum(krycklan_srp), 375L, "legacy Krycklan SRP rows")
#   assert_count(sum(camels_po4), 188L, "legacy CAMELS PO4 rows")
#   assert_count(sum(umr_nh4), 5059L, "legacy UMR NH4 rows")
#   after$variable[east_don] <- "TDN"
#   after$variable[umr_nh4] <- "NHX"
#
#   susquehanna_nhx_dates <- c(
#     "1980-06-16", "1981-02-13", "1981-02-14", "1986-09-10",
#     "1988-02-24", "1988-07-06", "1996-01-21", "1999-06-09",
#     "2004-09-20"
#   )
#   susquehanna_provider <- read_source(
#     basename(reviewed_susquehanna_file),
#     directory = dirname(reviewed_susquehanna_file)
#   )
#   # the restored source uses both nh4 and nhx for reported ammonia
#   # use either label only to verify that the legacy date is represented
#   susquehanna_provider_ammonia_dates <- unique(susquehanna_provider$date[
#     susquehanna_provider$LTER == "USGS" &
#       susquehanna_provider$Stream_Name == "SUSQUEHANNA RIVER" &
#       susquehanna_provider$solute %in% c("NH4", "NHX") &
#       susquehanna_provider$solute_units == "uM"
#   ])
#   susquehanna_nh4 <- after$LTER == "USGS" &
#     after$site == "SUSQUEHANNA RIVER" & after$variable == "NH4" &
#     after$units == "uM"
#   reviewed_susquehanna_nhx <- susquehanna_nh4 &
#     after$Sampling.Date %in% susquehanna_nhx_dates
#   remaining_susquehanna_nh4 <- susquehanna_nh4 &
#     !reviewed_susquehanna_nhx
#   provider_covered_remaining_nh4 <- remaining_susquehanna_nh4 &
#     after$Sampling.Date %in% susquehanna_provider_ammonia_dates
#   assert_count(sum(susquehanna_nh4), 1087L, "legacy Susquehanna NH4 rows")
#   assert_count(sum(reviewed_susquehanna_nhx), 9L,
#                "previously reviewed legacy Susquehanna NHX rows")
#   assert_count(sum(remaining_susquehanna_nh4), 1078L,
#                "remaining legacy Susquehanna NH4 rows")
#   assert_count(length(susquehanna_provider_ammonia_dates), 1198L,
#                "restored Susquehanna provider ammonia dates")
#   assert_count(sum(provider_covered_remaining_nh4), 1078L,
#                "provider-covered legacy Susquehanna NH4 rows")
#   if (!identical(
#     sort(after$Sampling.Date[reviewed_susquehanna_nhx]),
#     sort(susquehanna_nhx_dates)
#   )) {
#     stop("legacy Susquehanna NHX dates changed")
#   }
#   reviewed_dates_without_restored_match <- setdiff(
#     after$Sampling.Date[reviewed_susquehanna_nhx],
#     susquehanna_provider_ammonia_dates
#   )
#   if (!identical(reviewed_dates_without_restored_match, "1980-06-16")) {
#     stop("the previously reviewed Susquehanna ammonia exception changed")
#   }
#   after$variable[susquehanna_nh4] <- "NHX"
#
#   legacy_label_map <- c(
#     "Daily.Avg.Q.(Discharge)" = "daily Q",
#     "Instantaneous.Q.(Discharge)" = "instant Q",
#     "Spec.Cond" = "specific conductivity",
#     "Temp.C" = "temp",
#     "Suspended.Chl" = "suspended Chl"
#   )
#   expected_labels <- c(
#     "Daily.Avg.Q.(Discharge)" = 3531L,
#     "Instantaneous.Q.(Discharge)" = 1033L,
#     "Spec.Cond" = 30928L,
#     "Temp.C" = 19548L,
#     "Suspended.Chl" = 7082L
#   )
#   observed_labels <- vapply(
#     names(legacy_label_map),
#     function(label) sum(after$variable == label),
#     integer(1)
#   )
#   if (!identical(observed_labels, expected_labels)) {
#     stop("legacy variable-label counts changed")
#   }
#   invalid_temperature <- after$variable == "Temp.C" & value > 50
#   assert_count(sum(invalid_temperature, na.rm = TRUE), 3L,
#                "legacy invalid-temperature rows")
#   # keep the reported temperatures for later outlier review
#   assert_values(
#     as_number(after$value[invalid_temperature]),
#     c(53.4, 190.5, 64.5),
#     "prepared legacy reviewed temperature values"
#   )
#   after$units[after$variable == "Temp.C"] <- "C"
#   for (old_label in names(legacy_label_map)) {
#     after$variable[after$variable == old_label] <-
#       unname(legacy_label_map[[old_label]])
#   }
#
#   carey_shift_window <- after$LTER == "Ipswitch(Carey)" &
#     after$Sampling.Date >= "2011-06-15" &
#     after$Sampling.Date <= "2011-09-28"
#   carey_shifted_labels <- c(
#     "Conductivity", "DIN", "DSi", "NH4", "NOx", "PO4",
#     "specific conductivity", "temp", "Turbidity"
#   )
#   carey_shifted <- carey_shift_window &
#     after$variable %in% carey_shifted_labels
#   carey_unexpected <- carey_shift_window &
#     after$variable != "instant Q" &
#     !after$variable %in% carey_shifted_labels
#   assert_count(sum(carey_shifted), 523L,
#                "carey shifted legacy rows replaced")
#   assert_count(sum(carey_unexpected), 0L,
#                "carey unexpected rows in the shifted date window")
#   expected_shifted_counts <- c(
#     Conductivity = 60L,
#     DIN = 60L,
#     DSi = 60L,
#     NH4 = 60L,
#     NOx = 60L,
#     PO4 = 48L,
#     `specific conductivity` = 55L,
#     temp = 60L,
#     Turbidity = 60L
#   )
#   observed_shifted_counts <- table(factor(
#     after$variable[carey_shifted],
#     levels = names(expected_shifted_counts)
#   ))
#   if (!identical(
#     as.integer(observed_shifted_counts),
#     unname(expected_shifted_counts)
#   )) {
#     stop("the carey shifted source-variable counts changed")
#   }
#   carey_repair <- build_carey_shift_repair(after)
#   after <- rbind(after[!carey_shifted, , drop = FALSE], carey_repair)
#   rownames(after) <- NULL
#   value <- as_number(after$value)
#
#   clinton_tds <- after$LTER == "USGS" &
#     after$Site.Stream.Name == "Mississippi River at Clinton" &
#     after$Sampling.Date %in% names(clinton_expected) &
#     after$variable == "TN" &
#     after$units == "uM"
#   krycklan_no3 <- after$site == "Site 7" & after$variable == "NO3" &
#     after$units == "uM"
#   krycklan_srp <- after$site == "Site 7" & after$variable == "SRP"
#   camels_po4 <- after$site %in% c("SOPCHOPPY RIVER", "Wild River") &
#     after$variable == "PO4" & after$units == "uM"
#
#   # standardize the reviewed north slyamore ammonia series
#   combined_usgs <- read_source(
#     "Deduplicated_Sites_AllSites_chemistry.csv",
#     directory = output_dir
#   )
#   slyamore_provider_dates <- unique(combined_usgs$date[
#     combined_usgs$LTER == "USGS" &
#       combined_usgs$Stream_Name == "North Slyamore Creek" &
#       combined_usgs$solute %in% c("NH4", "NHX") &
#       combined_usgs$solute_units == "uM"
#   ])
#   slyamore_nh4 <- after$LTER == "USGS" &
#     after$site == "North Slyamore Creek" &
#     after$variable == "NH4" &
#     after$units == "uM"
#   slyamore_provider_overlap <- slyamore_nh4 &
#     after$Sampling.Date %in% slyamore_provider_dates
#   slyamore_legacy_only <- slyamore_nh4 & !slyamore_provider_overlap
#
#   assert_count(sum(slyamore_nh4), 150L,
#                "legacy North Slyamore NH4 rows")
#   assert_count(length(slyamore_provider_dates), 0L,
#                "deduplicated North Slyamore ammonia dates")
#   assert_count(sum(slyamore_provider_overlap), 0L,
#                "overlapping North Slyamore ammonia rows")
#   assert_count(sum(slyamore_legacy_only), 150L,
#                "retained legacy North Slyamore ammonia rows")
#   after$variable[slyamore_nh4] <- "NHX"
#
#   # remove eight exact lower-priority conductivity copies
#   conductivity_copy_key <- data.frame(
#     site = c(
#       "CN00.1M", "I080.2M", "M078.0B", "M764.3A",
#       "M786.2C", "MQ02.1M", "SG16.2C", "WP02.6M"
#     ),
#     date = c(
#       "1996-12-31", "1996-12-31", "1996-12-11", "1996-12-30",
#       "1996-12-31", "1996-12-31", "1996-12-31", "1996-12-30"
#     ),
#     value = c(699, 874, 524, 484, 477, 635, 1016, 578),
#     stringsAsFactors = FALSE
#   )
#   conductivity_copy_signature <- paste(
#     conductivity_copy_key$site,
#     conductivity_copy_key$date,
#     conductivity_copy_key$value,
#     sep = "\r"
#   )
#   legacy_conductivity_copy <- after$LTER == "UMR(Jankowski)" &
#     after$variable == "specific conductivity" &
#     after$units == "uS/cm" &
#     paste(after$site, after$Sampling.Date, value, sep = "\r") %in%
#       conductivity_copy_signature
#   assert_count(sum(legacy_conductivity_copy, na.rm = TRUE), 8L,
#                "legacy copied conductivity rows")
#
#   invalid_potomac <- after$site == "POTOMAC RIVER" &
#     after$Sampling.Date == "1995-05-15" & after$variable == "DOC" &
#     after$units == "uM" & abs(value - 1165627.31564355) < 1e-6
#   invalid_cb <- after$site == "CB01.5" &
#     after$Sampling.Date == "2016-02-16" &
#     after$variable == "specific conductivity" & after$units == "uS/cm" &
#     value == 6197
#   assert_count(sum(invalid_potomac, na.rm = TRUE), 1L,
#                "legacy Potomac invalid DOC row")
#   assert_count(sum(invalid_cb, na.rm = TRUE), 1L,
#                "legacy CB01.5 invalid conductivity row")
#   # keep these reported doc and conductivity values for later outlier review
#   assert_values(
#     as_number(after$value[invalid_potomac | invalid_cb]),
#     c(1165627.31564355, 6197),
#     "prepared legacy reviewed source values"
#   )
#
#   remove_rows <- clinton_tds | krycklan_no3 | krycklan_srp | camels_po4 |
#     slyamore_provider_overlap | legacy_conductivity_copy
#   assert_count(sum(remove_rows), 932L, "legacy permanent exclusions")
#   after <- after[!remove_rows, , drop = FALSE]
#   rownames(after) <- NULL
#
#   assert_count(nrow(after), 1024420L,
#                "legacy rows before site-reference exclusions")
#
#   site_reference <- read_source(
#     "Site_Reference_Table.csv",
#     directory = key_dir
#   )
#   alternate_names <- unlist(
#     strsplit(
#       ifelse(is.na(site_reference$Alt_Stream_Name),
#              "", site_reference$Alt_Stream_Name),
#       ";",
#       fixed = TRUE
#     ),
#     use.names = FALSE
#   )
#   reference_names <- unique(normalize_site_name(c(
#     site_reference$Stream_Name,
#     alternate_names,
#     site_reference$Original_Stream_Name
#   )))
#   reference_names <- reference_names[
#     !is.na(reference_names) & reference_names != ""
#   ]
#
#   finnish_alias_counts <- c(
#     "Koskenkyl<e4>njoki 6030" = 3073L,
#     "Pyh<e4>joki Hourunk 11400" = 2552L,
#     "N<e4>rpi<f6>njoki mts 6761" = 1186L
#   )
#   for (source_name in names(finnish_alias_counts)) {
#     alias_rows <- after$LTER == "Finnish Environmental Institute" &
#       after$Site.Stream.Name == source_name
#     assert_count(
#       sum(alias_rows, na.rm = TRUE),
#       finnish_alias_counts[[source_name]],
#       paste0("legacy Finnish alias ", source_name)
#     )
#   }
#
#   unmatched_reference <- !normalize_site_name(after$Site.Stream.Name) %in%
#     reference_names
#   retained_finnish_alias <-
#     after$LTER == "Finnish Environmental Institute" &
#     after$Site.Stream.Name %in% names(finnish_alias_counts)
#   excluded_site_rows <- unmatched_reference & !retained_finnish_alias
#
#   # reviewed site additions recover 24,996 previously unmatched rows
#   assert_count(sum(unmatched_reference), 63355L,
#                "legacy rows outside the current site lookup")
#   assert_count(sum(retained_finnish_alias), 6811L,
#                "legacy Finnish alias rows")
#   assert_count(sum(unmatched_reference & retained_finnish_alias), 0L,
#                "legacy Finnish aliases outside the current site lookup")
#   assert_count(sum(excluded_site_rows), 63355L,
#                "legacy accepted site exclusions")
#   assert_count(length(unique(after$Site.Stream.Name[excluded_site_rows])),
#                615L, "legacy accepted excluded site labels")
#   assert_count(sum(
#     after$site[excluded_site_rows] !=
#       after$Site.Stream.Name[excluded_site_rows],
#     na.rm = TRUE
#   ), 0L, "legacy excluded rows with differing site fields")
#
#   expected_excluded_rows <- c(
#     ARC = 28902L,
#     BNZ = 14L,
#     BcCZO = 12448L,
#     "Finnish Environmental Institute" = 21991L
#   )
#   expected_excluded_names <- c(
#     ARC = 563L,
#     BNZ = 1L,
#     BcCZO = 44L,
#     "Finnish Environmental Institute" = 7L
#   )
#   observed_excluded_rows <- table(after$LTER[excluded_site_rows])
#   observed_excluded_names <- tapply(
#     after$Site.Stream.Name[excluded_site_rows],
#     after$LTER[excluded_site_rows],
#     function(x) length(unique(x))
#   )
#   if (
#     !identical(
#       as.integer(observed_excluded_rows[names(expected_excluded_rows)]),
#       unname(expected_excluded_rows)
#     ) ||
#       !identical(
#         as.integer(observed_excluded_names[names(expected_excluded_names)]),
#         unname(expected_excluded_names)
#       ) ||
#       length(observed_excluded_rows) != length(expected_excluded_rows)
#   ) {
#     stop("legacy accepted site-exclusion scope changed")
#   }
#
#   exclusion_scope <- aggregate(
#     list(rows = after$Site.Stream.Name[excluded_site_rows]),
#     list(
#       LTER = after$LTER[excluded_site_rows],
#       Stream_Name = after$Site.Stream.Name[excluded_site_rows]
#     ),
#     length
#   )
#   exclusion_scope <- exclusion_scope[
#     order(exclusion_scope$LTER, exclusion_scope$Stream_Name),
#     ,
#     drop = FALSE
#   ]
#   signature_path <- tempfile()
#   writeLines(
#     paste(
#       exclusion_scope$LTER,
#       exclusion_scope$Stream_Name,
#       exclusion_scope$rows,
#       sep = "\t"
#     ),
#     signature_path,
#     useBytes = TRUE
#   )
#   exclusion_signature <- unname(tools::md5sum(signature_path))
#   unlink(signature_path)
#   if (!identical(exclusion_signature, "b83ba43fa25ae39e6ac867d5afb14be7")) {
#     stop(
#       "legacy accepted site-exclusion labels changed: ",
#       exclusion_signature
#     )
#   }
#
#   after <- after[!excluded_site_rows, , drop = FALSE]
#   rownames(after) <- NULL
#
#   # these totals exclude 197 lake-water rows confirmed in the original source
#   assert_count(nrow(after), 961065L, "prepared legacy chemistry rows")
#   assert_count(sum(after$LTER == "UMR(Jankowski)" & after$variable == "NH4"),
#                0L, "prepared legacy UMR NH4 rows")
#   assert_count(sum(after$LTER == "UMR(Jankowski)" & after$variable == "NHX"),
#                5059L, "prepared legacy UMR NHX rows")
#   assert_count(sum(after$LTER == "USGS" & after$site == "SUSQUEHANNA RIVER" &
#     after$variable == "NH4"), 0L, "prepared legacy Susquehanna NH4 rows")
#   assert_count(sum(after$LTER == "USGS" & after$site == "SUSQUEHANNA RIVER" &
#     after$variable == "NHX"), 1087L, "prepared legacy Susquehanna NHX rows")
#   assert_count(sum(after$LTER == "EastRiverSFA" & after$site == "coal_11" &
#     after$variable == "DON"), 0L, "prepared legacy East River DON rows")
#   assert_count(sum(after$LTER == "EastRiverSFA" & after$site == "coal_11" &
#     after$variable == "TDN"), 507L, "prepared legacy East River TDN rows")
#   corrected_east_cations <- after$LTER == "EastRiverSFA" &
#     after$site == "coal_11" & after$variable %in% c("Ca", "K", "Mg", "Na") &
#     after$units == "uM"
#   assert_count(sum(corrected_east_cations), 2576L,
#                "prepared legacy East River cation rows")
#   if (any(as_number(after$value[corrected_east_cations]) >= 500)) {
#     stop("the reviewed legacy East River cation correction changed")
#   }
#   assert_count(sum(after$variable %in% names(legacy_label_map)), 0L,
#                "prepared legacy old variable labels")
#   assert_count(sum(after$variable == "temp" & after$units != "C"), 0L,
#                "prepared legacy temperature units")
#
#   legacy_usgs_tn <- after$LTER == "USGS" &
#     after$variable == "TN" &
#     after$units == "uM"
#   assert_count(sum(legacy_usgs_tn), 13494L,
#                "prepared residual legacy USGS total-nitrogen rows")
#
#   usgs_total_nitrogen <- build_usgs_total_nitrogen_replacement(before)
#   replaced_legacy_ids <- attr(
#     usgs_total_nitrogen,
#     "legacy_tn_ids_replaced"
#   )
#   retained_legacy_ids <- attr(
#     usgs_total_nitrogen,
#     "legacy_tn_ids_retained"
#   )
#   replace_legacy_tn <- legacy_usgs_tn &
#     after[[1L]] %in% replaced_legacy_ids
#   retain_unmatched_tn <- legacy_usgs_tn &
#     after[[1L]] %in% retained_legacy_ids
#   # two removed clinton tds rows had tn replacements; the third did not
#   assert_count(sum(replace_legacy_tn), 9122L,
#                "legacy total-nitrogen rows replaced by provider results")
#   assert_count(sum(retain_unmatched_tn), 4372L,
#                "legacy total-nitrogen rows retained without provider results")
#   after <- after[!replace_legacy_tn, , drop = FALSE]
#   provider_columns <- setdiff(names(usgs_total_nitrogen), names(after))
#   for (column_name in provider_columns) {
#     after[[column_name]] <- NA_character_
#   }
#   missing_replacement_columns <- setdiff(names(after), names(usgs_total_nitrogen))
#   if (length(missing_replacement_columns) > 0L) {
#     stop(
#       "the USGS total-nitrogen replacement lacks source columns: ",
#       paste(missing_replacement_columns, collapse = ", ")
#     )
#   }
#   replacement_order <- match(names(after), names(usgs_total_nitrogen))
#   usgs_total_nitrogen <- usgs_total_nitrogen[
#     , replacement_order,
#     drop = FALSE
#   ]
#   names(usgs_total_nitrogen) <- names(after)
#   after <- rbind(after, usgs_total_nitrogen)
#   rownames(after) <- NULL
#
#   assert_count(nrow(after), 982882L,
#                "prepared chemistry rows after the USGS nitrogen rebuild")
#   assert_count(sum(
#     after$LTER == "USGS" & after$variable == "TN" & after$units == "uM"
#   ), 20851L, "rebuilt USGS total-nitrogen source rows")
#   assert_count(sum(
#     after$LTER == "USGS" & after$variable == "TDN" & after$units == "uM"
#   ), 14460L, "rebuilt USGS dissolved-total-nitrogen source rows")
#
#   usgs_tds_backfill <- build_usgs_tds_backfill()
#   missing_tds_columns <- setdiff(names(after), names(usgs_tds_backfill))
#   extra_tds_columns <- setdiff(names(usgs_tds_backfill), names(after))
#   if (length(missing_tds_columns) > 0L || length(extra_tds_columns) > 0L) {
#     stop("the USGS TDS backfill columns do not match the legacy source")
#   }
#   tds_column_order <- match(names(after), names(usgs_tds_backfill))
#   if (anyNA(tds_column_order)) {
#     stop("the USGS TDS backfill column order changed")
#   }
#   usgs_tds_backfill <- usgs_tds_backfill[
#     , tds_column_order,
#     drop = FALSE
#   ]
#   names(usgs_tds_backfill) <- names(after)
#   after <- rbind(after, usgs_tds_backfill)
#   rownames(after) <- NULL
#
#   prepared_tds <- after$LTER == "USGS" &
#     after$variable == "TDS" & after$units == "mg/L"
#   assert_count(nrow(after), 993255L,
#                "prepared chemistry rows after the USGS TDS backfill")
#   assert_count(sum(prepared_tds), 10373L,
#                "prepared USGS TDS backfill source rows")
#   assert_count(length(unique(paste(
#     after$Site.Stream.Name[prepared_tds],
#     after$Sampling.Date[prepared_tds],
#     sep = "\r"
#   ))), 10366L, "prepared USGS TDS backfill daily keys")
#
#   after$source_method_flag <- NA_character_
#   bnz_date <- as.Date(after$Sampling.Date)
#   bnz_filtered_si <- after$LTER == "BNZ" &
#     after$variable == "DSi" &
#     after$units == "uM" &
#     (
#       after$site %in% c("C1", "C2", "C4", "P6") |
#         (after$site %in% c("C3", "CJ") & bnz_date <= as.Date("2006-12-31"))
#     )
#   bnz_undocumented_si <- after$LTER == "BNZ" &
#     after$variable == "DSi" &
#     after$units == "uM" &
#     (
#       after$site %in% c("CB", "PC", "PJ") |
#         (after$site %in% c("C3", "CJ") & bnz_date >= as.Date("2007-01-01"))
#     )
#   assert_count(sum(bnz_filtered_si), 345L,
#                "BNZ filtered-silicon rows")
#   assert_count(sum(bnz_undocumented_si), 243L,
#                "BNZ undocumented silicon-fraction rows")
#   if (!setequal(
#     unique(after$site[bnz_filtered_si]),
#     c("C1", "C2", "C3", "C4", "CJ", "P6")
#   )) {
#     stop("the reviewed BNZ filtered-silicon sites changed")
#   }
#   after$source_method_flag[bnz_filtered_si] <-
#     "filtered Si, BNZ stream protocol"
#   after$source_method_flag[bnz_undocumented_si] <-
#     "BNZ Si fraction not documented"
#   retained_unmatched_tn <- after$LTER == "USGS" &
#     after$variable == "TN" &
#     after$units == "uM" &
#     after[[1L]] %in% retained_legacy_ids
#   after$source_method_flag[retained_unmatched_tn] <-
#     "legacy TN retained without matching USGS result"
#   assert_count(sum(retained_unmatched_tn), 4372L,
#                "flagged unmatched legacy total-nitrogen rows")
#
#   write_prepared(filename, after)
# }
#
# prepare_current_usgs <- function() {
#   expected_rows <- c(
#     "USGS__USGS__Choptank_River__da9275d27028.csv" = 23088L,
#     "USGS__USGS__Frio_River__4e8764089639.csv" = 7192L,
#     "USGS__USGS__Mississippi_River_above_Vicksburg__d2360d259841.csv" = 7420L,
#     "USGS__USGS__SUSQUEHANNA_RIVER__4db2531edf53.csv" = 31148L
#   )
#
#   for (filename in names(expected_rows)) {
#     before <- read_source(filename, directory = input_dir)
#     after <- before
#     assert_count(nrow(before), expected_rows[[filename]],
#                  paste0(filename, " rows"))
#
#     if (grepl("Frio_River", filename, fixed = TRUE)) {
#       reviewed_do_percent <- after$Stream_Name == "Frio River" &
#         after$date == "2016-10-25" & after$solute == "do percent" &
#         after$solute_units == "%"
#       assert_count(sum(reviewed_do_percent, na.rm = TRUE), 1L,
#                    "current Frio reviewed oxygen-percent row")
#       reviewed_value <- as_number(after$amount[reviewed_do_percent])
#       if (is.finite(reviewed_value) && reviewed_value == 221.5) {
#         # keep the reported value for later outlier review
#       } else if (is_blank(after$amount[reviewed_do_percent])) {
#         raw_source <- read_source(filename, directory = input_dir)
#         raw_row <- raw_source$Stream_Name == "Frio River" &
#           raw_source$date == "2016-10-25" &
#           raw_source$solute == "do percent" &
#           raw_source$solute_units == "%"
#         assert_count(sum(raw_row, na.rm = TRUE), 1L,
#                      "raw Frio reviewed oxygen-percent row")
#         assert_values(as_number(raw_source$amount[raw_row]), 221.5,
#                       "raw Frio reviewed oxygen-percent value")
#         after$amount[reviewed_do_percent] <- "221.5"
#       } else {
#         stop("the current Frio reviewed oxygen-percent value changed")
#       }
#     }
#
#     if (grepl("SUSQUEHANNA_RIVER", filename, fixed = TRUE)) {
#       exclusions <- readRDS(usgs_exclusion_file)
#       exclusion_review <- exclusions[exclusions$source_file == filename, ]
#       exclusion_review <- exclusion_review[
#         !duplicated(exclusion_review$source_row),
#         ,
#         drop = FALSE
#       ]
#       exclusion_review <- exclusion_review[
#         order(exclusion_review$source_row),
#         ,
#         drop = FALSE
#       ]
#       exclusion_rows <- exclusion_review$source_row
#       assert_count(length(exclusion_rows), 424L,
#                    "Susquehanna provider-confirmed TDS rows")
#       if (any(exclusion_rows < 1L | exclusion_rows > nrow(after))) {
#         stop("a Susquehanna exclusion row is outside the source file")
#       }
#       if (
#         any(after$solute[exclusion_rows] != "TN") ||
#           any(after$solute_units[exclusion_rows] != "uM") ||
#           any(after$date[exclusion_rows] != as.character(exclusion_review$date)) ||
#           any(
#             abs(
#               as_number(after$amount[exclusion_rows]) -
#                 exclusion_review$source_value
#             ) > 1e-8
#           )
#       ) {
#         stop("the Susquehanna TDS-as-TN source rows changed")
#       }
#
#       # the saved source retains its reported labels
#       # exclude only rows matched to official tds parameter codes here
#       after <- after[-exclusion_rows, , drop = FALSE]
#       rownames(after) <- NULL
#       assert_count(nrow(after), 30724L, "prepared Susquehanna rows")
#       assert_count(sum(after$solute == "TN", na.rm = TRUE), 0L,
#                    "prepared Susquehanna mislabeled TN rows")
#       source_nh4 <- after$solute == "NH4" & after$solute_units == "uM"
#       assert_count(sum(source_nh4, na.rm = TRUE), 1078L,
#                    "current Susquehanna NH4 rows")
#       after$solute[source_nh4] <- "NHX"
#     }
#
#     write_prepared(filename, after)
#   }
# }
# ---- end replacement: usgs-legacy-repairs ----

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

# ---- sidney: shared chemistry rules ----
# names, conversion factors, source choices, date notes, and expected counts are kept together
# current file: chemistry_rules.R
# ---- begin replacement: shared-rules ----
# # ---- gems silicon gap filling ----
# # use only reviewed matches to sites already in the chemistry dataset
# gems_usgs_si_sites <- tibble::tribble(
#   ~gems_station, ~usgs_station,
#   "USA00004", "06934500",
#   "USA00008", "01463500",
#   "USA00011", "11447650",
#   "USA00013", "06775900",
#   "USA00021", "04264331"
# )
# gems_filtered_reactive_methods <- c("COL-HB", "COL-HB-AA-F", "VMV_3628")
# gems_reactive_si_flag <- "reactive Si, filtered (GEMS)"
# gems_si_exclusions <- tibble::tribble(
#   ~gems_station, ~date, ~reason,
#   "USA00004", "1991-05-28", "no matching result in the reviewed USGS download"
# )
# gems_si_expected_days <- c(Canada = 1L, USGS = 56L)
#
# # the original catalogue identifies the sample fractions and reported chemical forms
# # total metals, total sulfate, and total reactive phosphorus are not substitutes
# gems_gap_parameters <- function() {
#   tibble::tribble(
#     ~source_entry, ~parameter_code, ~provider_units, ~variable, ~units, ~conversion, ~limit_is_clear,
#     "Aluminium.csv", "Al-Dis", "mg/l", "Al", "uM", 1000 / atomic_mass[["Al"]], TRUE,
#     "Calcium.csv", "Ca-Dis", "mg/l", "Ca", "uM", 1000 / atomic_mass[["Ca"]], TRUE,
#     "Chloride.csv", "Cl-Dis", "mg/l", "Cl", "uM", 1000 / atomic_mass[["Cl"]], TRUE,
#     "Pigment.csv", "Chl-a", "mg/l", "chlorophyll a", "ug/L", 1000, TRUE,
#     "Carbon.csv", "DOC", "mg/l", "dissolved org C", "uM", 1000 / atomic_mass[["C"]], TRUE,
#     "Carbon.csv", "TOC", "mg/l", "tot org C", "uM", 1000 / atomic_mass[["C"]], TRUE,
#     "Fluoride.csv", "F-Dis", "mg/l", "F", "uM", 1000 / atomic_mass[["F"]], TRUE,
#     "Iron.csv", "Fe-Dis", "mg/l", "Fe", "uM", 1000 / atomic_mass[["Fe"]], TRUE,
#     "Bicarbonate.csv", "HCO3", "mg/l", "HCO3", "uM", 1000 / molecule_mass[["HCO3"]], TRUE,
#     "Potassium.csv", "K-Dis", "mg/l", "K", "uM", 1000 / atomic_mass[["K"]], TRUE,
#     "Magnesium.csv", "Mg-Dis", "mg/l", "Mg", "uM", 1000 / atomic_mass[["Mg"]], TRUE,
#     "Manganese.csv", "Mn-Dis", "mg/l", "Mn", "uM", 1000 / atomic_mass[["Mn"]], TRUE,
#     "Sodium.csv", "Na-Dis", "mg/l", "Na", "uM", 1000 / atomic_mass[["Na"]], TRUE,
#     "Other_Nitrogen.csv", "NH4N", "mg/l", "NHX", "uM", 1000 / atomic_mass[["N"]], TRUE,
#     "Other_Nitrogen.csv", "TN", "mg/l", "TN", "uM", 1000 / atomic_mass[["N"]], TRUE,
#     "Other_Nitrogen.csv", "TKN", "mg/l", "tot kjeldahl N", "uM", 1000 / atomic_mass[["N"]], TRUE,
#     "Oxidized_Nitrogen.csv", "NO2N", "mg/l", "NO2", "uM", 1000 / atomic_mass[["N"]], TRUE,
#     "Oxidized_Nitrogen.csv", "NO3N", "mg/l", "NO3", "uM", 1000 / atomic_mass[["N"]], TRUE,
#     "Oxidized_Nitrogen.csv", "NOxN", "mg/l", "NOx", "uM", 1000 / atomic_mass[["N"]], TRUE,
#     "Phosphorus.csv", "TP", "mg/l", "TP", "uM", 1000 / atomic_mass[["P"]], TRUE,
#     "Phosphorus.csv", "TDP", "mg/l", "tdp", "uM", 1000 / atomic_mass[["P"]], TRUE,
#     "Phosphorus.csv", "DRP", "mg/l", "SRP", "uM", 1000 / atomic_mass[["P"]], TRUE,
#     "Sulfur.csv", "SO4-Dis", "mg/l", "SO4", "uM", 1000 / molecule_mass[["SO4"]], TRUE,
#     "Alkalinity.csv", "Alk-Tot", "mg/l", "alkalinity", "ueq/L", 2000 / 100.0869, TRUE,
#     "Water.csv", "TSS", "mg/l", "susp partic matter", "mg/L", 1, TRUE,
#     "Water.csv", "TDS", "mg/l", "tot dissolved solids", "mg/L", 1, TRUE,
#     "Zinc.csv", "Zn-Dis", "mg/l", "Zn", "uM", 1000 / atomic_mass[["Zn"]], TRUE,
#     "Dissolved_Gas.csv", "O2-Dis", "mg/l", "dissolved oxygen", "uM", 1000 / (2 * atomic_mass[["O"]]), FALSE,
#     "Dissolved_Gas.csv", "O2-Dis-Sat", "%", "do percent", "percent", 1, FALSE,
#     "Temperature.csv", "TEMP", "°C", "temp", "C", 1, FALSE,
#     "Electrical_Conductance.csv", "EC", "µS/cm", "conductivity", "uS/cm", 1, FALSE,
#     "pH.csv", "pH", "---", "pH", NA_character_, 1, FALSE,
#     "Optical.csv", "TURB", "NTU", "turbidity", "NTU", 1, FALSE
#   )
# }
#
# # ---- raw labels ----
# deduplicated_filename <- "Deduplicated_Sites_AllSites_chemistry.csv"
# deduplicated_raw_aliases <- tibble::tribble(
#   ~solute, ~Units,
#   "ca_mgl", "mg_L",
#   "cl_mgl", "mg_L",
#   "sio2_mgl", "mg_SiO2_L",
#   "k_mgl", "mg_L",
#   "mg_mgl", "mg_L",
#   "nh4_mgl", "mg_NH4_L",
#   "na_mgl", "mg_L",
#   "po4_mgl", "mg_PO4_L",
#   "so4_mgl", "mg_SO4_L",
#   "tp_ugl", "ug_P_L",
#   "doc_mgl", "mg_C_L",
#   "no3_mgl", "mg_NO3_L",
#   "al_ugl", "ug_L",
#   "f_mgl", "mg_L",
#   "fe_ugl", "ug_L",
#   "mn_ugl", "ug_L",
#   "anc_ueql", "ueq_L",
#   "temp_c", "C",
#   "tdn_mgl", "mg_N_L"
# ) |>
#   dplyr::mutate(Raw_Filename = deduplicated_filename) |>
#   dplyr::select(Raw_Filename, solute, Units)
# fixed_missing_tokens <- c(
#   "", "NA", "NaN", "N/A", "n/a", "<LOD", "wrong filtering",
#   "no filter", "nd"
# )
# source_review_columns <- list(
#   "EastRiverSFA_chem.csv" = c("solute", "value"),
#   "CatalinaJemez_chemistry_2009-2019_V2.csv" = c("SampleCode", "SiteCode", "DateTime")
# )
#
# # ---- unit label aliases ----
# unit_label_aliases <- c(
#   "na" = NA_character_,
#   "-" = "",
#   "/" = "",
#   "mg/l" = "mg/L",
#   "milligrams/litre" = "mg/L",
#   "milligramsperliter" = "mg/L",
#   "mg(s)/l" = "mg S/L",
#   "mg(so4)/l" = "mg SO4/L",
#   "ug/l" = "ug/L",
#   "micrograms/litre" = "ug/L",
#   "microsiemens/cm" = "uS/cm",
#   "microsiemens cm" = "uS/cm",
#   "microsiemenspercentimeter" = "uS/cm",
#   "deg c" = "C",
#   "oc" = "C",
#   "degrees celsius" = "C",
#   "%" = "percent",
#   "pcu" = "PCU",
#   "fnu" = "FNU",
#   "ntu" = "NTU",
#   "ueq/l" = "ueq/L"
# )
#
# # ---- variable names and preferred columns ----
# # the order below controls which equivalent column is used first
# # converting between reported chemical forms is separate from matching names
# ph_columns <- c(
#   "pH", "ph", "pH_pH", "ph_SU", "ph_", "fldph", "fldph_", "labph", "labph_"
# )
# conductivity_columns <- c(
#   "conduct_uS_cm", "conductivity_uS_cm", "cond_uscm_uS_cm",
#   "fldcond_uscm_uS_cm", "labcond_uscm_uS_cm", "conductivity_us_cm",
#   "ec_uS_cm", "specificconductance_uS_cm", "conductivity_at_25°c_uS_cm"
# )
# merge_rules <- list(
#   alkalinity_ueq_L = c("alkalinity_ueq_L", "alkalinity_uEq_l"),
#   anc_ueq_L = c("anc_ueq_L", "anc_ueql_ueq_L", "anc_ueq_l_ueq_L"),
#   do_percent = c("do_percent", "do_pct_percent", "do_percent_percent", "do_percent_%"),
#   colour_PCU = c("colour_PCU", "colour_pcu", "colour_pcu_", "colour_pcu_PCU"),
#   do_uM = c("do_uM", "dissolved_oxygen_uM"),
#   don_uM = c("don_uM", "dissolved_org_n_uM"),
#   din_uM = c("din_uM", "dissolved_inorg_n_uM"),
#   srp_uM = c("srp_uM", "soluble_reactive_p_uM"),
#   tdn_uM = c("tdn_uM", "tot_dissolved_n_uM"),
#   tn_uM = c("tn_uM", "tot_n_uM"),
#   tkn_uM = c("tkn_uM", "tot_kjeldahl_n_uM"),
#   tp_uM = c("tp_uM", "tot_p_uM"),
#   al_ug_L = c("al_ug_L", "al_ugl_ug_L"),
#   fe_ug_L = c("fe_ug_L", "fe_ugl_ug_L"),
#   nh4_mg_NH4_L = c(
#     "nh4_mg_NH4_L", "nh4_mgl_mg_NH4_L"
#   ),
#   tdn_mg_N_L = c("tdn_mg_N_L", "tdn_mgl_mg_N_L", "tdn_mg_L"),
#   chla_ug_L = c(
#     "chl_a_ug_L", "chla_ug_L", "suspended_chl_ug_L",
#     "chla_ugl_ug_L", "chlorophyll_a_ug_L"
#   ),
#   do_mg_L = c("do_mg_L", "do_mg_O2_L"),
#   ca_mg_L = c("ca_mgl_mg_L", "ca_mg_L", "calcium_mg_L"),
#   ca_ug_L = c("ca_ug_L", "calcium_ug_L"),
#   cl_mg_L = c("cl_mgl_mg_L", "cl_mg_L"),
#   cl_uM = c("chloride_uM", "cl_uM"),
#   doc_mg_C_L = c(
#     "doc_mgl_mg_C_L", "doc_mg_C_L", "carbon_dissolved_organic_mg_C_L"
#   ),
#   doc_uM = c("dissolved_org_c_uM", "doc_uM"),
#   dic_mg_C_L = "dic_mg_C_L",
#   dic_uM = c("dic_uM", "dissolved_inorg_c_uM"),
#   toc_mg_C_L = c("toc_mgl_mg_C_L", "toc_mg_C_L"),
#   f_mg_L = c("f_mgl_mg_L", "f_mg_L"),
#   hco3_mg_L = c("hco3_mgl_mg_L", "hco3_mg_L"),
#   k_mg_L = c("k_mgl_mg_L", "k_mg_L", "potassium_mg_L"),
#   mg_mg_L = c("mg_mg_L", "mg_mgl_mg_L", "magnesium_mg_L"),
#   mg_ug_L = c("magnesium_ug_L", "mg_ug_L"),
#   mn_ug_L = c("mn_ugl_ug_L", "mn_ug_L"),
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
#   no3_mg_NO3_L = c("no3_mgl_mg_NO3_L", "no3_mg_NO3_L"),
#   no3_uM = c("no3_uM", "nitrate_uM"),
#   # keep combined nitrate plus nitrite as nox while using its reported n basis
#   nox_mg_NO3_N_L = c(
#     "nox_mg_NO3_N_L", "nox_mg_N_L", "no3+no2___n_mg_NO3_N_L"
#   ),
#   no3_mg_NO3_N_L = c(
#     "no3_mgl_mg_NO3_N_L", "no3_mg_NO3_N_L",
#     "dissolved_nitrogen_nitrate_mg_NO3_N_L"
#   ),
#   no2_mg_NO2_N_L = c("no2_mgl_mg_NO2_N_L", "no2_mg_NO2_N_L"),
#   po4_mg_PO4_L = c("po4_mg_PO4_L", "po4_mgl_mg_PO4_L"),
#   po4_uM = c("phosphate_uM", "po4_uM"),
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
#   toc_uM = c("toc_uM", "tot_org_c_uM"),
#   tp_ug_P_L = c("tp_ug_L", "tp_ugl_ug_P_L"),
#   turbidity_NTU = c("turbidity_NTU", "turb_NTU"),
#   tds_mg_L = c("tds_mg_L", "tot_dissolved_solids_mg_L"),
#   tss_mg_L = c("tss___dry_mass_mg_L", "tss_mg_L"),
#   vss_mg_L = c("volatile_susp_solids_mg_L", "vss_mg_L"),
#   tdn_mg_L = "nitrogen_total_dissolved_mg_N_L",
#   tn_mg_L = c("tn_mg_N_L", "nitrogen_total_mg_N_L", "ntot_mgl_mg_N_L"),
#   tp_mg_L = "phosphorus_total_mg_P_L",
#   tdp_mg_L = "phosphorus_total_dissolved_mg_P_L",
#   dsi_mg_Si_L = c(
#     "dsi_mg_Si_L", "si_mg_Si_L", "silicon_extractable_mg_Si_L"
#   ),
#   dsi_ug_Si_L = c(
#     "dsi_ug_Si_L", "si_ug_Si_L", "silicon_ug_Si_L"
#   )
# )
# optional_measurement_columns <- c("don_mg_L", "so4_mg_S_L", "zn_mg_L")
# variable_names <- c(
#   ph = "pH", dsi = "DSi", do = "dissolved oxygen", chla = "chlorophyll a",
#   co2 = "CO2", dic = "dissolved inorg C", din = "dissolved inorg N",
#   doc = "dissolved org C", don = "dissolved org N", srp = "SRP",
#   ssc = "susp sediment conc", tdn = "tot dissolved N",
#   tds = "tot dissolved solids", tkn = "tot kjeldahl N", tn = "TN",
#   toc = "tot org C", tp = "TP", vss = "volatile susp solids"
# )
#
# # ---- unit conversions ----
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
# conversion_rules <- tibble::tribble(
#   ~target, ~source, ~operation, ~amount,
#   "alkalinity_ueq_L", "alkalinity_uM", "keep", NA_real_,
#   "alkalinity_ueq_L", "alkalinity_meq_L", "multiply", 1000,
#   "alkalinity_ueq_L", "alkalinity_ug_L", "divide", 50.04345,
#   "alkalinity_ueq_L", "alkalinity_mg_CaCO3_L", "multiply_1000_then_divide", 50.04345,
#   "conductivity_uS_cm", "conductivity_mS_m", "multiply", 10,
#   "al_uM", "al_mg_L", "divide_then_1000", atomic_mass[["Al"]],
#   "al_uM", "al_ug_L", "divide", atomic_mass[["Al"]],
#   "br_uM", "br_mg_L", "divide_then_1000", atomic_mass[["Br"]],
#   "ca_uM", "ca_mg_L", "divide_then_1000", atomic_mass[["Ca"]],
#   "ca_uM", "ca_ug_L", "divide", atomic_mass[["Ca"]],
#   "cl_uM", "cl_mg_L", "divide_then_1000", atomic_mass[["Cl"]],
#   "cl_uM", "cl_ug_L", "divide", atomic_mass[["Cl"]],
#   "doc_uM", "doc_mg_C_L", "divide_then_1000", atomic_mass[["C"]],
#   "doc_uM", "doc_ug_C_L", "divide", atomic_mass[["C"]],
#   "dic_uM", "dic_mg_C_L", "divide_then_1000", atomic_mass[["C"]],
#   "do_uM", "do_mg_L", "divide_then_1000", atomic_mass[["O"]] * 2,
#   "dsi_uM", "dsi_mg_Si_L", "divide_then_1000", atomic_mass[["Si"]],
#   "dsi_uM", "dsi_ug_Si_L", "divide", atomic_mass[["Si"]],
#   "dsi_uM", "dsi_mg_SiO2_L", "divide_then_1000", molecule_mass[["SiO2"]],
#   "dsi_uM", "sio2_ug_SiO2_L", "divide", molecule_mass[["SiO2"]],
#   "f_uM", "f_mg_L", "divide_then_1000", atomic_mass[["F"]],
#   "fe_uM", "fe_mg_L", "divide_then_1000", atomic_mass[["Fe"]],
#   "fe_uM", "fe_ug_L", "divide", atomic_mass[["Fe"]],
#   "hco3_uM", "hco3_mg_L", "divide_then_1000", molecule_mass[["HCO3"]],
#   "k_uM", "k_mg_L", "divide_then_1000", atomic_mass[["K"]],
#   "k_uM", "k_ug_L", "divide", atomic_mass[["K"]],
#   "k_uM", "potassium_ug_L", "divide", atomic_mass[["K"]],
#   "li_uM", "li_mg_L", "divide_then_1000", atomic_mass[["Li"]],
#   "mg_uM", "mg_mg_L", "divide_then_1000", atomic_mass[["Mg"]],
#   "mg_uM", "mg_ug_L", "divide", atomic_mass[["Mg"]],
#   "mn_uM", "mn_mg_L", "divide_then_1000", atomic_mass[["Mn"]],
#   "mn_uM", "mn_ug_L", "divide", atomic_mass[["Mn"]],
#   "na_uM", "na_mg_L", "divide_then_1000", atomic_mass[["Na"]],
#   "na_uM", "na_ug_L", "divide", atomic_mass[["Na"]],
#   "na_uM", "sodium_ug_L", "divide", atomic_mass[["Na"]],
#   "nh3_uM", "nh3_mg_NH3_N_L", "divide_then_1000", atomic_mass[["N"]],
#   "nh3_uM", "nh3_mg_NH3_L", "divide_then_1000", molecule_mass[["NH3"]],
#   "nh4_uM", "nh4_mg_NH4_N_L", "divide_then_1000", atomic_mass[["N"]],
#   "nh4_uM", "nh4_ug_NH4_N_L", "divide", atomic_mass[["N"]],
#   "nh4_uM", "nh4_mg_NH4_L", "divide_then_1000", molecule_mass[["NH4"]],
#   "nhx_uM", "nhx_mg_NH4_N_L", "divide_then_1000", atomic_mass[["N"]],
#   "nhx_uM", "nhx_mg_N_L", "divide_then_1000", atomic_mass[["N"]],
#   "nhx_uM", "nhx_ug_N_L", "divide", atomic_mass[["N"]],
#   "no3_uM", "no3_mg_NO3_L", "divide_then_1000", molecule_mass[["NO3"]],
#   "no3_uM", "no3_ug_NO3_L", "divide", molecule_mass[["NO3"]],
#   "no3_uM", "no3_mg_NO3_N_L", "divide_then_1000", atomic_mass[["N"]],
#   "no3_uM", "no3_mg_N_L", "divide_then_1000", atomic_mass[["N"]],
#   "no3_uM", "no3_ug_NO3_N_L", "divide", atomic_mass[["N"]],
#   "nox_uM", "nox_mg_NO3_N_L", "divide_then_1000", atomic_mass[["N"]],
#   "nox_uM", "nox_ug_NO3_N_L", "divide", atomic_mass[["N"]],
#   "no2_uM", "no2_mg_NO2_N_L", "divide_then_1000", atomic_mass[["N"]],
#   "po4_uM", "po4_mg_PO4_L", "divide_then_1000", molecule_mass[["PO4"]],
#   "po4_uM", "po4_mg_P_L", "divide_then_1000", atomic_mass[["P"]],
#   "po4_uM", "po4_p_ug_PO4_P_L", "divide", atomic_mass[["P"]],
#   "po4_uM", "po4_ug_PO4_P_L", "divide", atomic_mass[["P"]],
#   "so4_uM", "so4_ug_SO4_L", "divide", molecule_mass[["SO4"]],
#   "so4_uM", "so4_ug_S_L", "divide", atomic_mass[["S"]],
#   "so4_uM", "so4_mg_SO4_L", "divide_then_1000", molecule_mass[["SO4"]],
#   "so4_uM", "so4_mg_S_L", "divide_then_1000", atomic_mass[["S"]],
#   "sr_uM", "sr_mg_L", "divide_then_1000", atomic_mass[["Sr"]],
#   "sr_uM", "sr_ug_L", "divide", atomic_mass[["Sr"]],
#   "srp_uM", "srp_mg_P_L", "divide_then_1000", atomic_mass[["P"]],
#   "srp_uM", "srp_ug_P_L", "divide", atomic_mass[["P"]],
#   "tdn_uM", "tdn_mg_L", "divide_then_1000", atomic_mass[["N"]],
#   "tdn_uM", "tdn_mg_N_L", "divide_then_1000", atomic_mass[["N"]],
#   "tdn_uM", "tdn_ug_N_L", "divide", atomic_mass[["N"]],
#   "tkn_uM", "tkn_mg_N_L", "divide_then_1000", atomic_mass[["N"]],
#   "tn_uM", "tn_mg_L", "divide_then_1000", atomic_mass[["N"]],
#   "tn_uM", "tn_ug_L", "divide", atomic_mass[["N"]],
#   "tn_uM", "tn_ug_N_L", "divide", atomic_mass[["N"]],
#   "don_uM", "don_mg_L", "divide_then_1000", atomic_mass[["N"]],
#   "toc_uM", "toc_mg_C_L", "divide_then_1000", atomic_mass[["C"]],
#   "tp_uM", "tp_mg_P_L", "divide_then_1000", atomic_mass[["P"]],
#   "tp_uM", "tp_mg_L", "divide_then_1000", atomic_mass[["P"]],
#   "tp_uM", "tp_ug_P_L", "divide", atomic_mass[["P"]],
#   "tdp_uM", "tdp_mg_P_L", "divide_then_1000", atomic_mass[["P"]],
#   "tdp_uM", "tdp_mg_L", "divide_then_1000", atomic_mass[["P"]],
#   "tdp_uM", "tdp_ug_P_L", "divide", atomic_mass[["P"]],
#   "zn_uM", "zn_mg_L", "divide_then_1000", atomic_mass[["Zn"]],
#   "zn_uM", "zn_ug_L", "divide", atomic_mass[["Zn"]]
# )
#
# # ---- reviewed overlap choices ----
# drop_no3_lters <- "Krycklan"
# drop_nox_streams <- "Biscuit Brook"
# drop_no3_streams <- c(
#   "Frio River",
#   "Cane Creek",
#   "Merced River",
#   "Turkey River",
#   "James River",
#   "Corral Gulch",
#   "Flat Brook",
#   "Choptank River",
#   "Green River",
#   "Smith Creek",
#   "Sopchoppy River"
# )
# drop_camels_no3_streams <- c(
#   "North Slyamore Creek",
#   "Vallecito Creek",
#   "West Clear Creek"
# )
# drop_srp_lters <- "Krycklan"
# drop_srp_streams <- c("MGWEIR", "ORlow")
#
# # prefer the better documented source for the same site, date, variable, and unit
# preferred_source_rules <- tibble::tribble(
#   ~preferred_file, ~superseded_file, ~variables, ~expected_overlap,
#   paste0(
#     "Canada__Canada__ALSEK_RIVER_ABOVE_BATES_RIVER_IN_",
#     "KLUANE_NATIONAL_PARK__b577690976a4.csv"
#   ),
#   "Canada_WQ_dat.csv",
#   "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 570L,
#   paste0(
#     "Canada__Canada__BEAVER_RIVER_ABOVE_HIGHWAY_1_IN_GLACIER_",
#     "NATIONAL_PARK__8db5e916b53d.csv"
#   ),
#   "Canada_WQ_dat.csv",
#   "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 1661L,
#   paste0(
#     "Canada__Canada__KICKING_HORSE_RIVER_AT_FIELD_IN_YOHO_",
#     "NATIONAL_PARK__78eed20cf9d6.csv"
#   ),
#   "Canada_WQ_dat.csv",
#   "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 2058L,
#   paste0(
#     "Canada__Canada__KOOTENAY_RIVER_ABOVE_HIGHWAY_93_IN_",
#     "KOOTENAY_NATIONAL_PARK__7a1cbddf7523.csv"
#   ),
#   "Canada_WQ_dat.csv",
#   "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 2313L,
#   "Canada__Canada__LIARD_RIVER_AT_UPPER_CROSSING__47916feca4a8.csv",
#   "Canada_WQ_dat.csv",
#   "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 1279L,
#   "Canada__Canada__SKEENA_RIVER_AT_USK__6692e2a39888.csv",
#   "Canada_WQ_dat.csv",
#   "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 2317L,
#   "MD__MD__Lock_5__092e7030faa8.csv",
#   "MurrayDarlingChem_Merged.csv", NA_character_, 16882L,
#   "MD__MD__Merbein__9df18bcf9b24.csv",
#   "MurrayDarlingChem_Merged.csv", NA_character_, 15025L,
#   "USGS_geogenic_solutes.csv", "20221030_masterdata_chem_V2.csv",
#   "dissolved org C", 14285L,
#   "MCM_Chem_clean.csv", "20221030_masterdata_chem_V2.csv",
#   "alkalinity;Ca;Cl;DSi;K;Mg;Na;SO4", 7612L,
#   "USGS__USGS__SUSQUEHANNA_RIVER__4db2531edf53.csv",
#   "20221030_masterdata_chem_V2.csv",
#   "dissolved org C;DSi;NHX;NOx;PO4;tot dissolved N;TP", 6171L,
#   "USGS_geogenic_solutes.csv",
#   "USGS__USGS__SUSQUEHANNA_RIVER__4db2531edf53.csv",
#   "Ca;Cl;dissolved org C;K;Mg;Na;SO4", 3831L,
#   "Swedish_Geo_Updated_10062025.csv", "SiSyn_DataTemplate_Sweden_102423.csv",
#   "Cl", 3418L,
#   "USGS_geogenic_solutes.csv",
#   "USGS__USGS__Choptank_River__da9275d27028.csv",
#   "Ca;Cl;dissolved org C;K;Mg;Na;SO4", 2457L,
#   "EastRiverSFA_chem.csv", "20221030_masterdata_chem_V2.csv",
#   "Cl;NO3;PO4;SO4;tot dissolved N", 0L,
#   "USGS_geogenic_solutes.csv",
#   "USGS__USGS__Frio_River__4e8764089639.csv",
#   "Ca;Cl;dissolved org C;K;Mg;Na;SO4", 2044L,
#   "CAMELS_USGS_N_P.csv",
#   "USGS__USGS__Choptank_River__da9275d27028.csv",
#   "NO3;NOx;PO4", 1769L,
#   "USGS__USGS__Mississippi_River_above_Vicksburg__d2360d259841.csv",
#   "USGS_NWQA_Chemistry_MissRiverSites.csv", NA_character_, 1453L,
#   "Sweden__Sweden__Nykopingsan_Spanga__2cdeeea43708.csv",
#   "SiSyn_DataTemplate_Sweden_102423.csv",
#   "Cl;DSi;NH4;NOx;PO4;tot org C", 939L,
#   "Sweden__Sweden__Ljungan_Skallboleforsen__c0d37822b53a.csv",
#   "SiSyn_DataTemplate_Sweden_102423.csv",
#   "Cl;DSi;NH4;NOx;PO4;tot org C", 929L,
#   "Sweden__Sweden__Orekilsalven_Munkedal__96dcdaa96ada.csv",
#   "SiSyn_DataTemplate_Sweden_102423.csv",
#   "Cl;DSi;NH4;NOx;PO4;tot org C", 926L,
#   "UK_Si.csv", "UK__UK__BLACKWATER_AT_LANGFORD__e4ddc46f9d7c.csv",
#   "DSi", 847L,
#   "UK_Si.csv", "UK__UK__CHELMER_AT_RUSHES_LOCK__f7addd85ee9b.csv",
#   "DSi", 847L,
#   "USGS__USGS__Choptank_River__da9275d27028.csv",
#   "20221030_masterdata_chem_V2.csv", "DSi", 726L,
#   "Krycklan_NP.csv", "20221030_masterdata_chem_V2.csv", "NH4", 403L,
#   "USGS_geogenic_solutes.csv", "USGS_NWQA_Chemistry_MissRiverSites.csv",
#   "dissolved org C", 400L,
#   "CAMELS_USGS_N_P.csv", "USGS__USGS__Frio_River__4e8764089639.csv",
#   "NO3;NOx;PO4", 288L,
#   "CAMELS_USGS_N_P.csv", "20221030_masterdata_chem_V2.csv",
#   "NOx", 1879L,
#   "UMR_si_update_existing_sites.csv", "20221030_masterdata_chem_V2.csv",
#   NA_character_, 136L,
#   "USGS__USGS__Frio_River__4e8764089639.csv",
#   "20221030_masterdata_chem_V2.csv", "DSi", 84L
# )
# niva_preferred_files <- c(
#   "Norway_NIVA_AllSites__NIVA__AAGEVEG__19eac75d0edc.csv",
#   "Norway_NIVA_AllSites__NIVA__FINETAN__bcbb2dcfe9fb.csv",
#   "Norway_NIVA_AllSites__NIVA__FINEPAS__da624d256e41.csv",
#   "Norway_NIVA_AllSites__NIVA__HOREVOS__756e629d446e.csv",
#   "Norway_NIVA_AllSites__NIVA__MROEDRI__76754047396e.csv",
#   "Norway_NIVA_AllSites__NIVA__OSLEALN__9abcb2378cbb.csv",
#   "Norway_NIVA_AllSites__NIVA__ROGEBJE__9eabc44c847b.csv",
#   "Norway_NIVA_AllSites__NIVA__ROGEVIK__455000df4ac3.csv",
#   "Norway_NIVA_AllSites__NIVA__SFJENAU__44ddce831990.csv",
#   "Norway_NIVA_AllSites__NIVA__STRENID__a56da145d7a8.csv"
# )
# preferred_source_rules <- dplyr::bind_rows(
#   preferred_source_rules,
#   tibble::tibble(
#     preferred_file = niva_preferred_files,
#     superseded_file = "20221030_masterdata_chem_V2.csv",
#     variables = "DSi",
#     expected_overlap = c(61L, 61L, 60L, 60L, 60L, 60L, 59L, 60L, 59L, 59L)
#   )
# )
# expected_superseded_rows <- 93646L
#
# # ---- reviewed unmatched sites ----
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
# # ---- date review ----
# # inferred dates keep a flag in the master and an explanation in its metadata
# catalina_date_note <- tibble::tibble(
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
# cameroon_date_notes <- tibble::tribble(
#   ~Stream_Name, ~workbook_row,
#   "Mbalmayo", 514L,
#   "Messam", 495L,
#   "Nsimi_outlet", 515L,
#   "Nsimi_spring", 547L,
#   "Olama", 511L,
#   "Pont_So'o", 540L
# ) |>
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
# date_notes <- dplyr::bind_rows(
#   catalina_date_note,
#   cameroon_date_notes
# )
# camrex_manacapuru_dates <- as.Date(c(
#   "1982-05-12", "1982-08-26", "1982-12-07", "1983-03-30",
#   "1983-07-06", "1983-11-05", "1984-02-25", "1984-07-22",
#   "1985-08-11", "1988-12-02", "1990-05-01", "1991-08-30"
# ))
# camrex_date_note <- paste0(
#   "Date shifted back one day because the provider defines Julian day 1 as ",
#   "January 1, 1982; an earlier conversion treated that date as day 0."
# )
# camrex_corrected_events <- 182L
# camrex_corrected_rows <- 2059L
#
# # ---- flags and daily groups ----
# allowed_value_qualifiers <- c("<", ">")
# allowed_date_flags <- c("corrected", "inferred", "restored")
# allowed_source_lab_flags <- c(
#   "below reported limit",
#   "above reported limit",
#   "below-limit results excluded from daily mean",
#   "above-limit results excluded from daily mean",
#   "reported value below historical detection limit",
#   "provider < flag; meaning not confirmed",
#   "estimated value"
# )
# allowed_source_lab_flags <- c(allowed_source_lab_flags,
#   paste0(setdiff(allowed_source_lab_flags, "estimated value"), "; estimated value"))
# valid_lab_flags <- allowed_source_lab_flags
# # method flags and notes come from the shared workflow-rules lookup
#
# # ---- exact exclusions ----
# source_method_exclusions <- tibble::tibble(
#   source_method_flag = c(
#     "ECCC SiO2 method 2329, VMV 14108",
#     "ECCC SiO2 method 2328, VMV 14109",
#     "BNZ Si fraction not documented"
#   ),
#   allowed_counts = list(0L, 0L, 243L)
# )
# canada_extractable_filename <- "Canada_WQ_dat.csv"
# canada_extractable_label <- "SILICON EXTRACTABLE"
# canada_extractable_count <- 1549L
# hbr_undocumented_si_count <- 3L
# walker_lab_code_columns <- c(
#   Al = "al_lab_code",
#   Fe = "fe_lab_code",
#   K = "k_lab_code",
#   Mn = "mn_lab_code",
#   Sr = "sr_lab_code",
#   Zn = "zn_lab_code"
# )
# arctic_lake_events <- tibble::tribble(
#   ~LTER, ~Stream_Name, ~date,
#   "ARC", "I Swamp Outlet", as.Date("2012-08-08"),
#   "ARC", "I1 Outlet", as.Date("2012-08-07"),
#   "ARC", "I2 Outlet", as.Date("2012-08-07"),
#   "ARC", "I3 Outlet", as.Date("2012-08-07"),
#   "ARC", "I4 Outlet", as.Date("2012-08-07"),
#   "ARC", "I5 Outlet", as.Date("2012-08-07"),
#   "ARC", "I6 Outlet", as.Date("2012-08-08"),
#   "ARC", "I7 Outlet", as.Date("2012-08-08"),
#   "ARC", "I8 Outlet", as.Date("2012-08-08"),
#   "ARC", "Toolik Outlet", as.Date("2012-06-04")
# )
# arctic_lake_measurement_count <- 177L
# flowing_types <- c("River", "Creek", "Stream", "Brook")
#
# # ---- source review counts ----
# east_river_source_counts <- c(
#   ammonia_rows = 1114L,
#   nondetect_zeros = 32L,
#   missing_codes = 1L
# )
# east_river_nondetect_value <- "<0.0077"
#
# # ---- expected output ----
# # the seine fraction check removes 482 daily si rows and no complete sites
# # gems fills reviewed chemistry gaps at existing sites without adding sites
# expected_release_counts <- c(
#   corrected_date_rows = 2059L,
#   inferred_date_rows = 79L,
#   restored_date_rows = 0L,
#   less_than_rows = 58820L,
#   greater_than_rows = 43L,
#   mixed_below_daily_rows = 914L,
#   final_rows = 4686420L,
#   final_sites = 1170L,
#   final_silicon_sites = 1170L,
#   final_silicon_rows = 296848L
# )
# ---- end replacement: shared-rules ----

# ---- sidney: shared reading, conversion, and flag functions ----
# the master and preparation scripts use these same functions
# the later date, daily-average, and export replacements also use this file
# current file: chemistry_functions.R
# ---- begin replacement: shared-functions ----
# # ---- settings and shared files ----
# # read a true or false environment setting
# read_boolean_setting <- function(name, default = FALSE) {
#   default_text <- if (isTRUE(default)) "true" else "false"
#   value <- tolower(trimws(Sys.getenv(name, unset = default_text)))
#   if (!value %in% c("true", "false")) {
#     stop(name, " must be set to true or false", call. = FALSE)
#   }
#   value == "true"
# }
# # read an environment setting that cannot be blank
# read_required_setting <- function(name) {
#   value <- trimws(Sys.getenv(name, unset = ""))
#   if (value == "") {
#     stop("set ", name, " before running the workflow", call. = FALSE)
#   }
#   value
# }
# # read the shared method flags, notes, and date-based rules
# read_workflow_rules <- function(path) {
#   required_columns <- c(
#     "Source_Method_Flag", "Method_Flag", "LTER", "Variable",
#     "Start_Date", "End_Date", "Method_Note", "Expected_Count", "Apply_To"
#   )
#   rules <- readxl::read_excel(path, sheet = "Workflow rules")
#   missing_columns <- setdiff(required_columns, names(rules))
#   if (length(missing_columns) > 0L) {
#     stop(
#       "the workflow-rules lookup is missing: ",
#       paste(missing_columns, collapse = ", "),
#       call. = FALSE
#     )
#   }
#   rules <- rules |>
#     dplyr::select(dplyr::all_of(required_columns)) |>
#     dplyr::mutate(
#       dplyr::across(-Expected_Count, ~ dplyr::na_if(trimws(as.character(.x)), "")),
#       expected_count = suppressWarnings(as.integer(Expected_Count)),
#       start_date = as.Date(Start_Date),
#       end_date = as.Date(End_Date),
#       expected_variables = strsplit(Variable, ";", fixed = TRUE)
#     ) |>
#     dplyr::mutate(
#       expected_variables = lapply(expected_variables, trimws)
#     ) |>
#     dplyr::transmute(
#       source_method_flag = Source_Method_Flag,
#       apply_to = Apply_To,
#       Method_Flag,
#       rule_lter = LTER,
#       expected_variables,
#       start_date,
#       end_date,
#       Method_Note,
#       expected_count
#     )
#   stop_if(
#     nrow(rules) == 0L || anyNA(rules$Method_Flag) ||
#       anyNA(rules$Method_Note) || anyDuplicated(rules$Method_Flag) > 0L,
#     "the workflow-rules lookup needs one complete row per method flag"
#   )
#   source_flags <- rules$source_method_flag[!is.na(rules$source_method_flag)]
#   # require the current shared rules instead of filling in older copies
#   required_flags <- c(
#     "soluble reactive Si, filtered ICP-OES", "reported as SiO2-Dis",
#     "reactive Si, filtered (GEMS)", "shallowest sample depth used"
#   )
#   missing_flags <- setdiff(required_flags, source_flags)
#   stop_if(
#     length(missing_flags) > 0L,
#     paste("refresh the shared lab workbook; required method rules are missing:",
#       paste(missing_flags, collapse = ", "))
#   )
#   stop_if(
#     any(grepl("; ", c(source_flags, rules$Method_Flag), fixed = TRUE)),
#     "individual method labels cannot contain '; ' because it separates combined flags"
#   )
#   stop_if(
#     anyDuplicated(source_flags) > 0L ||
#       any(!is.na(rules$start_date) & !is.na(rules$end_date) &
#             rules$end_date < rules$start_date),
#     "the workflow-rules lookup contains duplicate source flags or invalid dates"
#   )
#   stop_if(
#     anyNA(rules$apply_to) ||
#       any(!rules$apply_to %in% c("source flag only", "network and dates")) ||
#       any(rules$apply_to == "source flag only" & is.na(rules$source_method_flag)),
#     "each method rule needs an explicit application setting and source label where required"
#   )
#   stop_if(
#     any(rules$apply_to == "network and dates" & is.na(rules$rule_lter)),
#     "network-wide method rules must name their network"
#   )
#   rules
# }
#
# # find one method flag inside a semicolon-separated field
# method_flag_present <- function(values, flag) {
#   values <- dplyr::na_if(trimws(as.character(values)), "")
#   unique_values <- unique(values[!is.na(values)])
#   present <- logical(length(unique_values))
#   names(present) <- unique_values
#   for (value in unique_values) {
#     parts <- strsplit(value, "; ", fixed = TRUE)[[1]]
#     present[value] <- flag %in% parts
#   }
#   output <- rep(FALSE, length(values))
#   output[!is.na(values)] <- unname(present[values[!is.na(values)]])
#   output
# }
#
# # list labels that need a matching workflow rule
# unsupported_method_flags <- function(values, allowed_flags) {
#   values <- dplyr::na_if(trimws(as.character(values)), "")
#   unique_values <- unique(values[!is.na(values)])
#   parts <- unique(unlist(strsplit(unique_values, "; ", fixed = TRUE), use.names = FALSE))
#   setdiff(parts, allowed_flags)
# }
#
# # confirm every source method flag has a lookup row
# source_method_flags_supported <- function(values, allowed_flags) {
#   length(unsupported_method_flags(values, allowed_flags)) == 0L
# }
#
# # convert source method flags to the plain final labels
# map_source_method_flags <- function(data, method_rules) {
#   values <- dplyr::na_if(trimws(as.character(data$source_method_flag)), "")
#   unique_values <- unique(values[!is.na(values)])
#   lookup <- method_rules |>
#     dplyr::filter(!is.na(source_method_flag))
#   mapped <- character(length(unique_values))
#   names(mapped) <- unique_values
#   for (value in unique_values) {
#     parts <- strsplit(value, "; ", fixed = TRUE)[[1]]
#     matches <- match(parts, lookup$source_method_flag)
#     if (anyNA(matches)) {
#       stop("a source method flag is missing from the workflow rules", call. = FALSE)
#     }
#     mapped[value] <- paste(unique(lookup$Method_Flag[matches]), collapse = "; ")
#   }
#   data$Method_Flag <- NA_character_
#   data$Method_Flag[!is.na(values)] <- unname(mapped[values[!is.na(values)]])
#   data
# }
#
# # attach the note for every method flag in a combined field
# add_method_notes <- function(data, method_rules) {
#   values <- dplyr::na_if(trimws(as.character(data$Method_Flag)), "")
#   unique_values <- unique(values[!is.na(values)])
#   notes <- character(length(unique_values))
#   names(notes) <- unique_values
#   for (value in unique_values) {
#     parts <- strsplit(value, "; ", fixed = TRUE)[[1]]
#     matches <- match(parts, method_rules$Method_Flag)
#     if (anyNA(matches)) {
#       stop("a method flag is missing from the workflow rules", call. = FALSE)
#     }
#     notes[value] <- paste(unique(method_rules$Method_Note[matches]), collapse = " ")
#   }
#   data$Method_Note <- NA_character_
#   data$Method_Note[!is.na(values)] <- unname(notes[values[!is.na(values)]])
#   data
# }
#
# # add a flag across a network only when the rule explicitly says to do that
# apply_interval_method_rules <- function(data, method_rules) {
#   interval_rules <- method_rules |>
#     dplyr::filter(apply_to == "network and dates")
#   for (row in seq_len(nrow(interval_rules))) {
#     matches <- data$variable %in% interval_rules$expected_variables[[row]]
#     if (!is.na(interval_rules$rule_lter[[row]])) {
#       matches <- matches & data$LTER == interval_rules$rule_lter[[row]]
#     }
#     if (!is.na(interval_rules$start_date[[row]])) {
#       matches <- matches & data$date >= interval_rules$start_date[[row]]
#     }
#     if (!is.na(interval_rules$end_date[[row]])) {
#       matches <- matches & data$date <= interval_rules$end_date[[row]]
#     }
#     already_present <- method_flag_present(
#       data$Method_Flag,
#       interval_rules$Method_Flag[[row]]
#     )
#     add_flag <- matches & !already_present
#     data$Method_Flag[add_flag] <- dplyr::if_else(
#       is.na(data$Method_Flag[add_flag]),
#       interval_rules$Method_Flag[[row]],
#       paste(
#         data$Method_Flag[add_flag],
#         interval_rules$Method_Flag[[row]],
#         sep = "; "
#       )
#     )
#   }
#   data
# }
#
# # ---- source file checks ----
# # read each source header once and keep it by file name
# read_source_headers <- function(input_dir, file_names) {
#   file_paths <- file.path(input_dir, file_names)
#   missing_files <- file_names[!file.exists(file_paths)]
#   if (length(missing_files) > 0L) {
#     stop("chemistry files are missing: ", paste(missing_files, collapse = ", "), call. = FALSE)
#   }
#   headers <- lapply(file_paths, function(path) {
#     names(read.csv(path, nrows = 0))
#   })
#   names(headers) <- file_names
#   headers
# }
# # import only mapped columns plus fields used by source review rules
# read_selected_source <- function(input_dir, file_name, selected_columns, source_headers) {
#   headers <- source_headers[[file_name]]
#   needed <- unique(c(selected_columns, source_review_columns[[file_name]]))
#   check_expected_columns(headers, needed, file_name)
#   column_classes <- ifelse(headers %in% needed, NA_character_, "NULL")
#   read.csv(
#     file.path(input_dir, file_name),
#     na.strings = "",
#     colClasses = column_classes
#   )
# }
# # require one key mapping for every file that carries a source flag
# check_source_flag_mappings <- function(source_headers, data_key) {
#   flag_names <- list(
#     source_lab_flag = c("Source_Lab_Flag", "source_lab_flag"),
#     source_method_flag = c("Source_Method_Flag", "source_method_flag")
#   )
#   for (standard_name in names(flag_names)) {
#     files_with_flag <- names(source_headers)[vapply(
#       source_headers,
#       function(column_names) any(column_names %in% flag_names[[standard_name]]),
#       logical(1)
#     )]
#     mapped_files <- data_key$Raw_Filename[
#       data_key$Standardized_Column_Name == standard_name
#     ]
#     mapping_counts <- table(mapped_files[!is.na(mapped_files) & mapped_files != ""])
#     if (!setequal(files_with_flag, names(mapping_counts)) || any(mapping_counts != 1L)) {
#       stop("every flag-bearing input needs one ", standard_name, " mapping", call. = FALSE)
#     }
#   }
#   invisible(TRUE)
# }
# # check that every column named in the data key exists in its source file
# check_expected_columns <- function(actual_names, expected_names, file_name) {
#   missing_names <- setdiff(unique(expected_names), actual_names)
#   if (length(missing_names) > 0L) {
#     detail <- paste0("'", missing_names, "'", collapse = " and ")
#     stop("the data key expects missing columns in '", file_name, "': ", detail, call. = FALSE)
#   }
#   invisible(TRUE)
# }
# # count values in selected columns while treating blank text as missing
# count_present_values <- function(data, columns) {
#   present_columns <- intersect(columns, names(data))
#   if (length(present_columns) == 0L) {
#     return(stats::setNames(integer(), character()))
#   }
#   vapply(
#     data[present_columns],
#     function(values) {
#       present <- !is.na(values)
#       if (is.character(values)) {
#         present <- present & trimws(values) != ""
#       }
#       sum(present)
#     },
#     integer(1)
#   )
# }
# # stop when columns that should be unused still contain values
# stop_if_columns_have_values <- function(data, columns, text) {
#   value_counts <- count_present_values(data, columns)
#   used_columns <- names(value_counts)[value_counts > 0L]
#   if (length(used_columns) > 0L) {
#     stop(text, ": ", paste(used_columns, collapse = ", "), call. = FALSE)
#   }
#   invisible(value_counts)
# }
# # keep only columns containing a value plus any named workflow fields
# drop_empty_columns <- function(data, keep = character()) {
#   has_values <- vapply(
#     data,
#     function(values) {
#       present <- !is.na(values)
#       if (is.character(values)) {
#         present <- present & trimws(values) != ""
#       }
#       any(present)
#     },
#     logical(1)
#   )
#   data[has_values | names(data) %in% keep]
# }
#
# # ---- units and qualified values ----
# # match common unit spellings without changing the reported chemical form
# normalize_unit_label <- function(values) {
#   values <- trimws(as.character(values))
#   values <- stringr::str_replace_all(values, c("µ" = "u", "μ" = "u"))
#   lower_values <- tolower(values)
#   matched <- !is.na(lower_values) & lower_values %in% names(unit_label_aliases)
#   values[matched] <- unname(unit_label_aliases[lower_values[matched]])
#   values
# }
# # split reported signs from numeric chemistry values
# parse_qualified_numbers <- function(values, missing_tokens = character()) {
#   values <- as.character(values)
#   trimmed_values <- trimws(values)
#   missing_tokens <- unique(c(missing_tokens, fixed_missing_tokens))
#   is_missing <- is.na(values) | trimmed_values %in% missing_tokens
#   is_below <- !is_missing & stringr::str_detect(values, "^\\s*<")
#   is_above <- !is_missing & stringr::str_detect(values, "^\\s*>")
#   numbers <- suppressWarnings(
#     as.numeric(stringr::str_remove(values, "^\\s*[<>]\\s*"))
#   )
#   is_unparsed <- !is_missing & is.na(numbers)
#   if (any(is_unparsed)) {
#     detail <- paste(unique(values[is_unparsed]), collapse = ", ")
#     stop("unparsed repeated chemistry values remain: ", detail, call. = FALSE)
#   }
#   list(
#     numbers = numbers,
#     is_missing = is_missing,
#     is_below = is_below,
#     is_above = is_above
#   )
# }
# # average repeated values while keeping a reported limit when needed
# mean_qualified_measurement <- function(values, missing_tokens = character()) {
#   parsed <- parse_qualified_numbers(values, missing_tokens)
#   detected <- parsed$numbers[
#     !parsed$is_missing & !parsed$is_below & !parsed$is_above
#   ]
#   below_limits <- parsed$numbers[parsed$is_below]
#   above_limits <- parsed$numbers[parsed$is_above]
#   if (length(detected) > 0L) {
#     return(as.character(mean(detected)))
#   }
#   if (length(below_limits) > 0L) {
#     if (length(above_limits) > 0L) {
#       stop("mixed less-than and greater-than repeated measurements remain", call. = FALSE)
#     }
#     return(paste0("<", mean(below_limits)))
#   }
#   if (length(above_limits) > 0L) {
#     return(paste0(">", mean(above_limits)))
#   }
#   NA_character_
# }
#
# # read the value and limit sign, then average repeated measurements
# collapse_repeated_amounts <- function(data, missing_tokens, mean_function) {
#   parsed <- parse_qualified_numbers(data$amount, missing_tokens)
#   normalized <- as.character(parsed$numbers)
#   normalized[parsed$is_below] <- paste0("<", normalized[parsed$is_below])
#   normalized[parsed$is_above] <- paste0(">", normalized[parsed$is_above])
#   normalized[parsed$is_missing] <- NA_character_
#   data$amount <- normalized
#   key_columns <- setdiff(names(data), "amount")
#   collapse_repeated_cells(data, mean_function) |>
#     dplyr::arrange(dplyr::across(dplyr::all_of(key_columns)))
# }
#
# # average repeated measurements before turning variables into columns
# collapse_repeated_cells <- function(data, mean_function) {
#   key_columns <- setdiff(names(data), "amount")
#   repeated <- vctrs::vec_duplicate_detect(data[key_columns])
#   if (!any(repeated)) {
#     return(data)
#   }
#   repeated_rows <- data[repeated, , drop = FALSE] |>
#     dplyr::group_by(dplyr::across(dplyr::all_of(key_columns))) |>
#     dplyr::summarise(amount = mean_function(amount), .groups = "drop")
#   dplyr::bind_rows(
#     data[!repeated, , drop = FALSE],
#     repeated_rows
#   )
# }
#
# # ---- reshaping ----
# # turn columns into rows in smaller groups to limit memory use
# chunked_pivot_longer <- function(
#     data, id_columns, names_to, values_to,
#     chunk_size = 10000L, values_drop_na = TRUE
# ) {
#   if (!is.numeric(chunk_size) || length(chunk_size) != 1L || chunk_size < 1L) {
#     stop("chunk_size must be a positive number", call. = FALSE)
#   }
#   missing_id_columns <- setdiff(id_columns, names(data))
#   if (length(missing_id_columns) > 0L) {
#     stop(
#       "pivot id columns are missing: ",
#       paste(missing_id_columns, collapse = ", "),
#       call. = FALSE
#     )
#   }
#   if (nrow(data) == 0L) {
#     return(tidyr::pivot_longer(
#       data,
#       cols = -tidyselect::all_of(id_columns),
#       names_to = names_to,
#       values_to = values_to,
#       values_drop_na = values_drop_na
#     ))
#   }
#   row_numbers <- seq_len(nrow(data))
#   row_groups <- split(row_numbers, (row_numbers - 1L) %/% as.integer(chunk_size))
#   pieces <- lapply(row_groups, function(rows) {
#     tidyr::pivot_longer(
#       data[rows, , drop = FALSE],
#       cols = -tidyselect::all_of(id_columns),
#       names_to = names_to,
#       values_to = values_to,
#       values_drop_na = values_drop_na
#     )
#   })
#   dplyr::bind_rows(pieces)
# }
#
# # ---- column merging and conversion ----
# # return the first available value across selected columns
# first_present_value <- function(data, columns, missing_value = NA_complex_) {
#   present_columns <- intersect(columns, names(data))
#   if (length(present_columns) == 0L) {
#     return(rep(missing_value, nrow(data)))
#   }
#   purrr::reduce(data[present_columns], dplyr::coalesce)
# }
# # merge equivalent columns in the stated order
# merge_columns <- function(data, target, precedence) {
#   present_columns <- intersect(unique(c(target, precedence)), names(data))
#   if (length(present_columns) == 0L) {
#     return(data)
#   }
#   data[[target]] <- purrr::reduce(data[present_columns], dplyr::coalesce)
#   data[, setdiff(names(data), setdiff(present_columns, target)), drop = FALSE]
# }
# # combine each group of equivalent columns in the listed order
# apply_merge_rules <- function(data, merge_rules) {
#   for (target in names(merge_rules)) {
#     data <- merge_columns(data, target, merge_rules[[target]])
#   }
#   data
# }
# # convert source columns and merge them into one standard column
# convert_measurement <- function(values, operation, amount) {
#   switch(
#     operation,
#     keep = values,
#     multiply = values * amount,
#     divide = values / amount,
#     divide_then_1000 = (values / amount) * 1000,
#     multiply_1000_then_divide = (values * 1000) / amount,
#     stop("unsupported unit-conversion operation", call. = FALSE)
#   )
# }
# merge_converted_columns <- function(data, target, rules) {
#   if (!target %in% names(data)) {
#     data[[target]] <- NA_complex_
#   }
#   keep <- rules$source %in% names(data) & rules$source != target
#   rules <- rules[keep, , drop = FALSE]
#   converted_values <- Map(
#     function(column, operation, amount) {
#       convert_measurement(data[[column]], operation, amount)
#     },
#     rules$source,
#     rules$operation,
#     rules$amount
#   )
#   candidates <- c(list(data[[target]]), converted_values)
#   data[[target]] <- purrr::reduce(candidates, dplyr::coalesce)
#   data[, setdiff(names(data), rules$source), drop = FALSE]
# }
# # apply several unit-conversion groups
# apply_conversion_rules <- function(data, conversion_rules) {
#   for (target in unique(conversion_rules$target)) {
#     data <- merge_converted_columns(
#       data,
#       target,
#       conversion_rules[conversion_rules$target == target, ]
#     )
#   }
#   data
# }
#
# # ---- date formats ----
# # identify one date order used by a source file
# detect_date_format <- function(date_strings, delimiter = "/") {
#   parts <- stringr::str_split_fixed(as.character(date_strings), delimiter, 3)
#   if (ncol(parts) < 3L || all(parts == "")) {
#     return("NO_DELIM_MATCH")
#   }
#   first <- suppressWarnings(as.numeric(parts[, 1]))
#   second <- suppressWarnings(as.numeric(parts[, 2]))
#   third <- suppressWarnings(as.numeric(parts[, 3]))
#   if (all(is.na(first)) && all(is.na(second)) && all(is.na(third))) {
#     return("UNPARSEABLE")
#   }
#   year_positions <- which(vapply(
#     seq_len(ncol(parts)),
#     function(column) any(stringr::str_detect(trimws(parts[, column]), "^[0-9]{4}$")),
#     logical(1)
#   ))
#   if (length(year_positions) != 1L) {
#     return("AMBIGUOUS_YEAR_POSITION")
#   }
#   year_position <- year_positions[[1]]
#   if (year_position == 1L) {
#     return("ymd")
#   }
#   if (year_position == 3L) {
#     largest_first <- max(first, na.rm = TRUE)
#     largest_second <- max(second, na.rm = TRUE)
#     if (largest_first > 12 && largest_second <= 12) {
#       return("dmy")
#     }
#     if (largest_second > 12 && largest_first <= 12) {
#       return("mdy")
#     }
#     if (largest_first > 12 && largest_second > 12) {
#       return("CONFLICTING_BOTH_OVER_12")
#     }
#     return("AMBIGUOUS_DAY_MONTH")
#   }
#   "UNEXPECTED_YEAR_POSITION"
# }
#
# # ---- daily flags ----
# # an estimate is a measurement, not a detection-limit threshold
# interpret_gems_qualifiers <- function(qualifier, limit_is_clear) {
#   qualifier <- dplyr::coalesce(trimws(as.character(qualifier)), "")
#   if (any(!qualifier %in% c("", "<", ">", "~"))) {
#     stop("a GEMS result has an unsupported qualifier", call. = FALSE)
#   }
#   lab_flag <- dplyr::case_when(
#     qualifier == "~" ~ "estimated value",
#     !limit_is_clear & qualifier == "<" ~ "provider < flag; meaning not confirmed",
#     TRUE ~ NA_character_
#   )
#   qualifier[qualifier == "~" | (!limit_is_clear & qualifier == "<")] <- ""
#   list(qualifier = qualifier, lab_flag = lab_flag)
# }
#
# # keep the estimate note alongside any note about excluded daily samples
# add_estimated_lab_flag <- function(flag, estimated) {
#   dplyr::case_when(
#     estimated & !is.na(flag) ~ paste0(flag, "; estimated value"),
#     estimated ~ "estimated value",
#     TRUE ~ flag
#   )
# }
#
# # keep one nonblank source flag within a daily group
# reduce_daily_flag <- function(values, flag_label) {
#   values <- trimws(as.character(values))
#   flags <- unique(values[!is.na(values) & values != ""])
#   if (length(flags) > 1L) {
#     stop("conflicting ", flag_label, " flags remain within a daily source key", call. = FALSE)
#   }
#   if (length(flags) == 0L) {
#     return(NA_character_)
#   }
#   flags[[1]]
# }
# # describe how reported limits contributed to one daily value
# daily_lab_flag <- function(detected, below, above) {
#   if (isTRUE(any(below > 0L & above > 0L))) {
#     stop("less-than and greater-than results share a daily source key", call. = FALSE)
#   }
#   dplyr::case_when(
#     detected > 0L & below > 0L ~ "below-limit results excluded from daily mean",
#     detected > 0L & above > 0L ~ "above-limit results excluded from daily mean",
#     detected == 0L & below > 0L ~ "below reported limit",
#     detected == 0L & above > 0L ~ "above reported limit",
#     TRUE ~ NA_character_
#   )
# }
#
# # carry site and sample review notes into the final rows
# add_site_and_sample_flags <- function(data, site_reference) {
#   spatial_note <- "spatial data not yet available"
#   site_flags <- site_reference |>
#     dplyr::transmute(
#       LTER, Stream_Name,
#       Site_Flag = dplyr::na_if(trimws(as.character(Flag)), ""),
#       needs_spatial_note = !is.na(Include_in_Master_Chem) &
#         Include_in_Master_Chem == "Yes" &
#         !is.na(Has_Spatial_Data) & Has_Spatial_Data == "No"
#     ) |>
#     dplyr::mutate(
#       has_spatial_note = !is.na(Site_Flag) & stringr::str_detect(
#         stringr::str_to_lower(Site_Flag),
#         stringr::fixed(spatial_note)
#       ),
#       Site_Flag = dplyr::case_when(
#         needs_spatial_note & !has_spatial_note & is.na(Site_Flag) ~ spatial_note,
#         needs_spatial_note & !has_spatial_note ~ paste(
#           Site_Flag,
#           spatial_note,
#           sep = "; "
#         ),
#         TRUE ~ Site_Flag
#       )
#     ) |>
#     dplyr::select(-has_spatial_note) |>
#     dplyr::distinct()
#   stop_if(
#     anyDuplicated(site_flags[c("LTER", "Stream_Name")]) > 0L,
#     "the site reference has duplicate site flags"
#   )
#   stop_if(
#     nrow(dplyr::anti_join(
#       dplyr::distinct(data, LTER, Stream_Name), site_flags,
#       by = c("LTER", "Stream_Name")
#     )) > 0L,
#     "some final chemistry sites are missing from the site reference"
#   )
#   row_keys <- c("LTER", "Stream_Name", "date", "variable", "units")
#   rows_before <- nrow(data)
#   keys_before <- data[row_keys]
#   data <- data |>
#     dplyr::left_join(site_flags, by = c("LTER", "Stream_Name")) |>
#     dplyr::mutate(
#       Sample_Flag = paste0(
#         dplyr::if_else(!is.na(Lab_Flag), paste0("lab: ", Lab_Flag, "; "), ""),
#         dplyr::if_else(!is.na(Date_Flag), paste0("date: ", Date_Flag, "; "), ""),
#         dplyr::if_else(!is.na(Method_Flag), paste0("method: ", Method_Flag), "")
#       ),
#       Sample_Flag = sub("; $", "", Sample_Flag),
#       Sample_Flag = dplyr::na_if(Sample_Flag, "")
#     )
#   stop_if(
#     any(
#       data$needs_spatial_note &
#         (
#           is.na(data$Site_Flag) |
#             !stringr::str_detect(
#               stringr::str_to_lower(data$Site_Flag),
#               stringr::fixed(spatial_note)
#             )
#         )
#     ),
#     "a site without spatial data is missing its site flag"
#   )
#   data <- data |>
#     dplyr::select(-needs_spatial_note) |>
#     dplyr::relocate(Site_Flag, Sample_Flag, .after = Stream_Name)
#   has_detail <- with(data, !is.na(Lab_Flag) | !is.na(Date_Flag) | !is.na(Method_Flag))
#   stop_if(
#     nrow(data) != rows_before || !identical(data[row_keys], keys_before),
#     "adding site flags changed the chemistry rows or row order"
#   )
#   stop_if(
#     any(is.na(data$Sample_Flag) != !has_detail),
#     "the sample flag does not match the detailed sample flags"
#   )
#   data
# }
#
# # ---- checks ----
# # stop when one reviewed condition is true
# stop_if <- function(condition, text) {
#   if (isTRUE(condition)) {
#     stop(text, call. = FALSE)
#   }
#   invisible(condition)
# }
# # require two file lists to contain the same names in the same order
# check_file_list <- function(actual_files, expected_files, text) {
#   if (!identical(actual_files, expected_files)) {
#     stop(text, call. = FALSE)
#   }
#   invisible(TRUE)
# }
# # require one reviewed count or an accepted fallback count
# check_reviewed_count <- function(actual, expected, text) {
#   if (!actual %in% expected) {
#     stop(
#       text,
#       " (expected ", paste(expected, collapse = " or "),
#       "; found ", actual, ")",
#       call. = FALSE
#     )
#   }
#   invisible(actual)
# }
#
# # report related count differences together before changing or exporting data
# check_reviewed_counts <- function(actual, expected, labels) {
#   stop_if(
#     length(actual) != length(expected) || length(actual) != length(labels),
#     "reviewed count checks have different lengths"
#   )
#   wrong <- is.na(actual) | is.na(expected) | actual != expected
#   if (any(wrong)) {
#     stop(
#       paste(
#         paste0(labels[wrong], ": expected ", expected[wrong], ", found ", actual[wrong]),
#         collapse = "\n"
#       ),
#       call. = FALSE
#     )
#   }
#   invisible(TRUE)
# }
#
# # keep the preferred source only where two files report the same daily result
# apply_preferred_source_rules <- function(data, rules, expected_removed) {
#   daily_key <- c("LTER", "Stream_Name", "date", "variable", "units")
#   required_columns <- c("Raw_Filename", daily_key)
#   stop_if(
#     any(!required_columns %in% names(data)),
#     "source selection is missing a required chemistry column"
#   )
#   stop_if(
#     any(!c(
#       "preferred_file", "superseded_file", "variables", "expected_overlap"
#     ) %in% names(rules)),
#     "the preferred-source rules are missing a required column"
#   )
#
#   # find each source file once so every rule works with only the rows it needs
#   rows_by_file <- split(seq_len(nrow(data)), data$Raw_Filename, drop = TRUE)
#   removal_keys <- vector("list", nrow(rules))
#   observed_overlap <- integer(nrow(rules))
#   for (row in seq_len(nrow(rules))) {
#     preferred_index <- rows_by_file[[rules$preferred_file[[row]]]]
#     superseded_index <- rows_by_file[[rules$superseded_file[[row]]]]
#     if (is.null(preferred_index)) {
#       preferred_index <- integer()
#     }
#     if (is.null(superseded_index)) {
#       superseded_index <- integer()
#     }
#
#     variable_text <- rules$variables[[row]]
#     if (!is.na(variable_text)) {
#       variables <- strsplit(variable_text, ";", fixed = TRUE)[[1]]
#       preferred_index <- preferred_index[
#         data$variable[preferred_index] %in% variables
#       ]
#       superseded_index <- superseded_index[
#         data$variable[superseded_index] %in% variables
#       ]
#     }
#
#     preferred_keys <- data[preferred_index, daily_key, drop = FALSE] |>
#       dplyr::distinct()
#     overlapping_rows <- data[
#       superseded_index,
#       c("Raw_Filename", daily_key),
#       drop = FALSE
#     ] |>
#       dplyr::semi_join(preferred_keys, by = daily_key)
#     observed_overlap[row] <- nrow(overlapping_rows)
#     removal_keys[[row]] <- overlapping_rows
#   }
#
#   check_reviewed_counts(
#     observed_overlap,
#     rules$expected_overlap,
#     paste("source overlap:", rules$preferred_file, "and", rules$superseded_file)
#   )
#
#   removal_keys <- dplyr::bind_rows(removal_keys) |>
#     dplyr::distinct()
#   check_reviewed_count(
#     nrow(removal_keys),
#     expected_removed,
#     "the reviewed superseded-row count changed"
#   )
#   rows_before <- nrow(data)
#   data <- dplyr::anti_join(
#     data,
#     removal_keys,
#     by = c("Raw_Filename", daily_key)
#   )
#   check_reviewed_count(
#     rows_before - nrow(data),
#     expected_removed,
#     "the source-selection removal count changed"
#   )
#   data
# }
#
# # ---- prepare each source ----
# harmonize_source <- function(file_name, data_key, units_key, run_input_dir, source_headers) {
#   mean_measurement <- function(values) {
#     missing_tokens <- unique(c(data_key$NA_indicator, fixed_missing_tokens))
#     mean_qualified_measurement(values, missing_tokens)
#   }
#   key_rows <- data_key |>
#     dplyr::filter(
#       Raw_Filename == file_name,
#       !Standardized_Column_Name %in% c("Dataset", "Raw_Filename"),
#       !is.na(Standardized_Column_Name),
#       Standardized_Column_Name != ""
#     )
#   raw_data <- read_selected_source(
#     run_input_dir, file_name, key_rows$Raw_Column_Name, source_headers
#   ) |>
#     dplyr::select(-dplyr::any_of(c("Dataset", "Raw_Filename")))
#   raw_data <- apply_source_review_rules(raw_data, file_name)
#   raw_data <- raw_data[unique(key_rows$Raw_Column_Name)]
#   keyed_data <- raw_data |>
#     dplyr::mutate(
#       row_num = dplyr::row_number(),
#       Raw_Filename = file_name,
#       .before = dplyr::everything()
#     ) |>
#     dplyr::mutate(dplyr::across(dplyr::everything(), as.character)) |>
#     tidyr::pivot_longer(
#       -row_num:-Raw_Filename,
#       names_to = "Raw_Column_Name",
#       values_to = "values"
#     ) |>
#     dplyr::left_join(key_rows, by = c("Raw_Filename", "Raw_Column_Name")) |>
#     dplyr::filter(!is.na(Standardized_Column_Name)) |>
#     dplyr::mutate(
#       Units = dplyr::na_if(normalize_unit_label(Units), ""),
#       unit_suffix = gsub("\\/| |\\-", "_", Units),
#       standard_name = dplyr::if_else(
#         is.na(unit_suffix),
#         Standardized_Column_Name,
#         paste0(Standardized_Column_Name, "_", unit_suffix)
#       )
#     ) |>
#     dplyr::select(row_num, Dataset, Raw_Filename, standard_name, values) |>
#     tidyr::pivot_wider(names_from = standard_name, values_from = values) |>
#     dplyr::select(-row_num) |>
#     dplyr::distinct()
#   if (!identical(unique(key_rows$Dataset_orientation), "long")) {
#     return(keyed_data)
#   }
#   source_units <- units_key |>
#     dplyr::filter(Raw_Filename == file_name) |>
#     dplyr::mutate(
#       Units = gsub("/| |\\-", "_", normalize_unit_label(Units)),
#       solute = tolower(gsub("/| |\\-", "_", Variable))
#     ) |>
#     dplyr::distinct(Raw_Filename, solute, Units)
#   if (file_name == deduplicated_filename) {
#     source_units <- dplyr::bind_rows(source_units, deduplicated_raw_aliases) |>
#       dplyr::distinct(Raw_Filename, solute, Units)
#   }
#   has_row_units <- "solute_units" %in% names(keyed_data)
#   if (!has_row_units) {
#     keyed_data$solute_units <- NA_character_
#   }
#   long_rows <- keyed_data |>
#     dplyr::mutate(
#       solute = tolower(gsub("/| |\\-", "_", solute)),
#       solute_units = gsub("/| |\\-", "_", normalize_unit_label(solute_units)),
#       solute_units = dplyr::na_if(solute_units, "")
#     ) |>
#     collapse_repeated_amounts(data_key$NA_indicator, mean_measurement)
#   if (file_name == deduplicated_filename) {
#     long_rows$solute_units[
#       long_rows$solute == "nh4_mgl" & long_rows$solute_units == "mg_L"
#     ] <- "mg_NH4_L"
#   }
#   unit_counts <- source_units |>
#     dplyr::count(Raw_Filename, solute, name = "unit_count")
#   keyed_rows <- long_rows |>
#     dplyr::filter(solute %in% source_units$solute)
#   joined_units <- keyed_rows |>
#     dplyr::left_join(unit_counts, by = c("Raw_Filename", "solute")) |>
#     dplyr::left_join(
#       source_units,
#       by = c("Raw_Filename", "solute"),
#       relationship = "many-to-many"
#     ) |>
#     dplyr::filter(
#       (unit_count == 1L & (
#         !has_row_units |
#           (is.na(solute_units) & (is.na(Units) | Units == "")) |
#           (!is.na(solute_units) & solute_units == Units)
#       )) |
#         (unit_count > 1L & has_row_units & solute_units == Units)
#     )
#   stop_if(
#     nrow(joined_units) != nrow(keyed_rows),
#     paste0("row-level units did not resolve in '", file_name, "'")
#   )
#   wide_rows <- joined_units |>
#     dplyr::mutate(
#       standard_name = dplyr::if_else(
#         is.na(Units) | Units == "",
#         solute,
#         paste0(solute, "_", Units)
#       )
#     ) |>
#     dplyr::select(-solute, -solute_units, -Units, -unit_count) |>
#     collapse_repeated_cells(mean_measurement)
#   tidyr::pivot_wider(
#     wide_rows,
#     names_from = standard_name,
#     values_from = amount
#   )
# }
# # ---- qualifiers and units ----
# # put one source into the shared format before combining files
# standardize_source_data <- function(chemistry_wide, data_key) {
#   for (column in c("Dataset", "LTER", "Stream_Name", "date")) {
#     if (!column %in% names(chemistry_wide)) {
#       chemistry_wide[[column]] <- NA_character_
#     }
#   }
#   if (!"source_lab_flag" %in% names(chemistry_wide)) {
#     chemistry_wide$source_lab_flag <- NA_character_
#   }
#   if (!"source_method_flag" %in% names(chemistry_wide)) {
#     chemistry_wide$source_method_flag <- NA_character_
#   }
#   chemistry_wide <- chemistry_wide |>
#     dplyr::relocate(
#       Dataset, Raw_Filename, LTER, Stream_Name,
#       source_lab_flag, source_method_flag, date,
#       .before = dplyr::everything()
#     ) |>
#     dplyr::relocate(
#       dplyr::any_of(unname(walker_lab_code_columns)),
#       .before = date
#     ) |>
#     dplyr::mutate(
#       LTER = dplyr::coalesce(LTER, Dataset),
#       Dataset = dplyr::coalesce(Dataset, LTER),
#       Stream_Name = dplyr::case_when(
#         Raw_Filename == "NigerRiver.csv" & is.na(Stream_Name) ~ "Niger",
#         Raw_Filename == "ElbeRiverChem.csv" ~ "Elbe",
#         TRUE ~ Stream_Name
#       )
#     ) |>
#     dplyr::select(-dplyr::ends_with("_NA"), -dplyr::any_of("Dataset_orientation")) |>
#     drop_empty_columns(c("source_lab_flag", "source_method_flag")) |>
#     dplyr::mutate(row_num = dplyr::row_number(), .before = dplyr::everything())
#   id_columns <- names(chemistry_wide)[seq_len(match("date", names(chemistry_wide)))]
#   chemistry_long <- chunked_pivot_longer(
#     chemistry_wide,
#     id_columns,
#     "solute",
#     "measurement"
#   ) |>
#     dplyr::mutate(
#       measurement = dplyr::case_when(
#         trimws(measurement) %in% unique(c(
#           data_key$NA_indicator,
#           fixed_missing_tokens
#         )) ~ NA_character_,
#         suppressWarnings(as.numeric(trimws(measurement))) == -9999 ~ NA_character_,
#         TRUE ~ measurement
#       )
#     ) |>
#     dplyr::filter(!is.na(measurement), measurement != "")
#   unparsed <- chemistry_long |>
#     dplyr::filter(is.na(suppressWarnings(as.complex(
#       stringr::str_remove(measurement, "^\\s*[<>]\\s*")
#     ))))
#   stop_if(
#     nrow(unparsed) > 0L,
#     paste(
#       "unparsed chemistry values remain:",
#       paste(unique(unparsed$measurement), collapse = ", ")
#     )
#   )
#   # store the reported sign while units are converted
#   chemistry_wide <- chemistry_long |>
#     dplyr::mutate(
#       value = as.complex(stringr::str_remove(measurement, "^\\s*[<>]\\s*")) +
#         (stringr::str_detect(measurement, "^\\s*<") -
#            stringr::str_detect(measurement, "^\\s*>")) * 1i
#     ) |>
#     dplyr::select(-measurement) |>
#     dplyr::arrange(solute) |>
#     tidyr::pivot_wider(names_from = solute, values_from = value) |>
#     dplyr::select(-row_num)
#   # keep the known ph method when catalina's generic ph field is blank
#   catalina_rows <- chemistry_wide$Raw_Filename ==
#     "CatalinaJemez_chemistry_2009-2019_V2.csv"
#   catalina_generic_ph <- catalina_rows &
#     !is.na(first_present_value(chemistry_wide, "pH"))
#   catalina_field_ph <- catalina_rows & !catalina_generic_ph &
#     !is.na(first_present_value(chemistry_wide, "fldph"))
#   catalina_lab_ph <- catalina_rows & !catalina_generic_ph &
#     !catalina_field_ph &
#     !is.na(first_present_value(chemistry_wide, "labph"))
#   if (any(catalina_rows)) {
#     check_reviewed_count(
#       sum(catalina_field_ph),
#       28L,
#       "the reviewed Catalina field-pH fallback count changed"
#     )
#     check_reviewed_count(
#       sum(catalina_lab_ph),
#       75L,
#       "the reviewed Catalina laboratory-pH fallback count changed"
#     )
#   }
#   chemistry_wide$pH_method_flag <- dplyr::case_when(
#     catalina_field_ph ~ "field pH",
#     catalina_lab_ph ~ "laboratory pH fallback",
#     TRUE ~ NA_character_
#   )
#   chemistry_wide <- chemistry_wide |>
#     dplyr::relocate(pH_method_flag, .before = date)
#   stop_if_columns_have_values(
#     chemistry_wide,
#     c(
#       "ammonia_n_mg_L", "nh4_mgl_mg_L", "no3_mg_L", "no2_mg_L",
#       "nox_mg_L", "po4_mg_L", "srp_mg_L", "tn_mg_L", "tn_ug_L",
#       "tdn_mg_L", "tkn_mg_L", "tp_mg_L", "tp_ug_L", "tdp_mg_L", "don_mg_L"
#     ),
#     "nutrient mass units are missing an N or P reporting basis"
#   )
#   stop_if_columns_have_values(
#     chemistry_wide,
#     c(
#       "doc_mg_L", "doc_mgl_mg_L", "carbon_dissolved_organic_mg_L",
#       "dic_mg_L", "toc_mg_L", "toc_mgl_mg_L", "so4_mg_L", "so4_ug_L"
#     ),
#     "carbon or sulfate mass units are missing a C, SO4, or S reporting basis"
#   )
#   # use field measurements first and keep laboratory values as a fallback
#   chemistry_wide$pH <- dplyr::if_else(
#     chemistry_wide$Dataset == "ColoradoAlpine",
#     first_present_value(
#       chemistry_wide,
#       c("fldph", "fldph_", "labph", "labph_", ph_columns)
#     ),
#     first_present_value(chemistry_wide, ph_columns)
#   )
#   chemistry_wide <- chemistry_wide |>
#     dplyr::select(-dplyr::any_of(setdiff(ph_columns, "pH")))
#   chemistry_wide$conductivity_uS_cm <- dplyr::case_when(
#     chemistry_wide$Dataset == "Guadeloupe" ~ first_present_value(
#       chemistry_wide, "conductivity_at_25°c_uS_cm"
#     ),
#     chemistry_wide$Dataset == "ColoradoAlpine" ~ first_present_value(
#       chemistry_wide, c("fldcond_uscm_uS_cm", "labcond_uscm_uS_cm")
#     ),
#     TRUE ~ first_present_value(chemistry_wide, conductivity_columns)
#   )
#   chemistry_wide <- chemistry_wide |>
#     dplyr::select(-dplyr::any_of(setdiff(conductivity_columns, "conductivity_uS_cm"))) |>
#     apply_merge_rules(merge_rules)
#   for (column in optional_measurement_columns) {
#     if (!column %in% names(chemistry_wide)) {
#       chemistry_wide[[column]] <- NA_complex_
#     }
#   }
#   # convert each reported chemical basis to the standard output unit
#   chemistry_wide <- apply_conversion_rules(chemistry_wide, conversion_rules)
#   east_rows <- chemistry_wide$Raw_Filename == "EastRiverSFA_chem.csv"
#   if (any(east_rows)) {
#     stop_if(
#       !any(east_rows & !is.na(chemistry_wide$nhx_uM)) ||
#         any(east_rows & !is.na(chemistry_wide$nh4_uM)),
#       "East River ammonia-N was not retained as combined ammonia"
#     )
#   }
#   chemistry_wide <- merge_columns(
#     chemistry_wide,
#     "susp_partic_matter_mg_L",
#     c("spm_mg_L", "tss_mg_L")
#   ) |>
#     dplyr::select(-dplyr::contains("_q_"))
#   stop_if(
#     anyDuplicated(names(chemistry_wide)) > 0L,
#     "duplicate column names remain before the final long-format reshape"
#   )
#   id_columns <- names(chemistry_wide)[seq_len(match("date", names(chemistry_wide)))]
#   standardized <- chunked_pivot_longer(
#     chemistry_wide,
#     id_columns,
#     "raw_variable",
#     "value"
#   ) |>
#     dplyr::mutate(
#       value_qualifier = dplyr::case_when(
#         Im(value) > 0 ~ "<",
#         Im(value) < 0 ~ ">",
#         TRUE ~ NA_character_
#       ),
#       value = Re(value),
#       variable_units = stringr::str_replace(
#         raw_variable,
#         "_(uM|ueq_L|uS_cm|ug(?:_[A-Za-z0-9]+)*_L|mg(?:_[A-Za-z0-9]+)*_L|PCU|C|NTU|FNU|percent|ppt)$",
#         "--\\1"
#       )
#     ) |>
#     tidyr::separate_wider_delim(
#       variable_units,
#       delim = "--",
#       names = c("raw_name", "raw_units"),
#       too_few = "align_start"
#     ) |>
#     dplyr::mutate(
#       units = gsub("_", "/", raw_units),
#       variable = dplyr::coalesce(
#         unname(variable_names[raw_name]),
#         dplyr::case_when(
#           nchar(raw_name) == 1 ~ toupper(raw_name),
#           nchar(raw_name) == 2 ~ stringr::str_to_title(raw_name),
#           nchar(raw_name) < 4 & stringr::str_detect(raw_name, "x") ~ toupper(raw_name),
#           stringr::str_detect(raw_name, "[[:digit:]]") ~ toupper(raw_name),
#           TRUE ~ gsub("_", " ", raw_name)
#         )
#       ),
#       variable = gsub("Ph", "pH", variable),
#       variable = gsub("Ec", "EC", variable),
#       variable = gsub("NOX", "NOx", variable)
#     ) |>
#     dplyr::mutate(
#       source_method_flag = dplyr::case_when(
#         Raw_Filename == "WalkerBranch_Chem.csv" &
#           variable == "DSi" ~ "dissolved Si, 0.45 um filtered",
#         variable == "pH" & !is.na(pH_method_flag) ~ pH_method_flag,
#         TRUE ~ source_method_flag
#       )
#     )
#   walker_v1 <- rep(FALSE, nrow(standardized))
#   for (variable_name in names(walker_lab_code_columns)) {
#     code_column <- walker_lab_code_columns[[variable_name]]
#     if (code_column %in% names(standardized)) {
#       walker_v1 <- walker_v1 |
#         (
#           standardized$Raw_Filename == "WalkerBranch_Chem.csv" &
#             standardized$variable == variable_name &
#             standardized[[code_column]] == "V1"
#         )
#     }
#   }
#   walker_v1[is.na(walker_v1)] <- FALSE
#   stop_if(
#     any(walker_v1 & !is.na(standardized$source_lab_flag)),
#     "Walker Branch source lab flags conflict"
#   )
#   standardized$source_lab_flag[walker_v1] <-
#     "reported value below historical detection limit"
#   standardized |>
#     dplyr::select(
#       -raw_variable, -raw_name, -raw_units, -pH_method_flag,
#       -dplyr::any_of(unname(walker_lab_code_columns))
#     ) |>
#     dplyr::relocate(value, value_qualifier, .after = dplyr::everything())
# }
#
# # ---- seine sample fractions ----
# # compare si only with the saved official station, date, and reported value
# # the older input converted sio2 to si using 28/60 and rounded to two decimals
# review_seine_si <- function(data, site_reference, provider_file) {
#   selected <- which(data$Raw_Filename == "Cleaned_Seine_Data.csv" & data$variable == "DSi")
#   if (!length(selected)) return(data)
#   provider <- readRDS(provider_file)
#   stop_if(!is.null(provider[["next"]]) || nrow(provider$data) != provider$count,
#     "the saved seine silica download is incomplete")
#   sites <- site_reference |>
#     dplyr::filter(LTER == "Seine") |>
#     dplyr::transmute(Stream_Name,
#       station = sprintf("%08d", as.integer(Original_Stream_Name)))
#   stop_if(anyDuplicated(sites$station) > 0L,
#     "seine station codes must identify one site each")
#   lookup <- provider$data |>
#     dplyr::filter(code_parametre == "1348", symbole_unite == "mg(SiO2)/L") |>
#     dplyr::mutate(station = as.character(code_station),
#       sample_date = as.Date(date_prelevement), si = round(resultat * 28 / 60, 2)) |>
#     dplyr::group_by(station, sample_date, si) |>
#     dplyr::summarise(filtered = any(code_fraction == "3"),
#       unfiltered = any(code_fraction == "23"), .groups = "drop")
#   # unmatched values and matches to both fractions are not called unfiltered
#   target <- data[selected, , drop = FALSE] |>
#     dplyr::transmute(Stream_Name, sample_date = date,
#       si = round(value * atomic_mass[["Si"]] / 1000, 2)) |>
#     dplyr::left_join(sites, by = "Stream_Name", relationship = "many-to-one") |>
#     dplyr::left_join(lookup, by = c("station", "sample_date", "si"),
#       relationship = "many-to-one")
#   remove <- !is.na(target$unfiltered) & target$unfiltered & !target$filtered
#   confirmed_filtered <- !is.na(target$filtered) & target$filtered & !target$unfiltered
#   uncertain <- selected[!remove & !confirmed_filtered]
#   old <- data$source_method_flag[uncertain]
#   label <- "Si filtration not confirmed"
#   data$source_method_flag[uncertain] <- ifelse(is.na(old) | old == "", label,
#     paste(old, label, sep = "; "))
#   keep <- rep(TRUE, nrow(data))
#   keep[selected[remove]] <- FALSE
#   data[keep, , drop = FALSE]
# }
#
# # ---- workflow inputs ----
# # read the local keys and prepared chemistry files
# load_chemistry_inputs <- function(root_dir, run_input_dir) {
#   key_dir <- file.path(root_dir, "keys")
#   output_dir <- file.path(root_dir, "tidy")
#   dir.create(key_dir, recursive = TRUE, showWarnings = FALSE)
#   dir.create(run_input_dir, recursive = TRUE, showWarnings = FALSE)
#   dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
#   key_files <- file.path(
#     key_dir,
#     c(
#       "SiSyn_Data_Key.csv",
#       "SiSyn_Chem_Units_Key.csv",
#       "Site_Reference_Table.csv",
#       "Chemistry_detection_limits_and_methods.xlsx"
#     )
#   )
#   stop_if(
#     any(!file.exists(key_files)),
#     paste(
#       "required key files are missing:",
#       paste(basename(key_files[!file.exists(key_files)]), collapse = ", ")
#     )
#   )
#   data_key_raw <- read.csv(key_files[[1]], na.strings = "") |>
#     dplyr::filter(Data_type == "chemistry")
#   units_key <- read.csv(key_files[[2]], na.strings = "") |>
#     dplyr::filter(Data_type == "chemistry") |>
#     dplyr::select(-dplyr::any_of(c("Data_type", "Notes", "X"))) |>
#     dplyr::mutate(Variable = tolower(Variable))
#   text_columns <- c(USGSGageNumber = "character", Discharge_Site_Name = "character")
#   site_reference <- read.csv(key_files[[3]], na.strings = "", colClasses = text_columns)
#   method_rules <- read_workflow_rules(key_files[[4]])
#   chemistry_files <- sort(unique(trimws(as.character(data_key_raw$Raw_Filename))))
#   chemistry_files <- chemistry_files[!is.na(chemistry_files) & chemistry_files != ""]
#   check_file_list(
#     sort(list.files(run_input_dir, pattern = "[.]csv$")),
#     chemistry_files,
#     "the local master chemistry inputs do not match the data key"
#   )
#   source_headers <- read_source_headers(run_input_dir, chemistry_files)
#   check_source_flag_mappings(source_headers, data_key_raw)
#   data_key <- data_key_raw |>
#     dplyr::select(-dplyr::any_of(c("Data_type", "Notes"))) |>
#     dplyr::mutate(
#       Raw_Column_Name = gsub(" |\\+|\\(|\\)|\\/|\\-", ".", Raw_Column_Name)
#     )
#   list(
#     data_key = data_key,
#     units_key = units_key,
#     site_reference = site_reference,
#     method_rules = method_rules,
#     chemistry_files = chemistry_files,
#     source_headers = source_headers,
#     output_dir = output_dir
#   )
# }
# # apply source review choices without changing the source file
# apply_source_review_rules <- function(raw_data, file_name) {
#   # exclude canada extractable si because method f055 confirms no filtration
#   if (file_name == canada_extractable_filename) {
#     remove_rows <- raw_data$VARIABLE == canada_extractable_label
#     remove_rows[is.na(remove_rows)] <- FALSE
#     check_reviewed_count(
#       sum(remove_rows),
#       canada_extractable_count,
#       "the reviewed Canada extractable, unfiltered silicon count changed"
#     )
#     raw_data <- raw_data[!remove_rows, , drop = FALSE]
#   }
#   # interpret documented zero ammonia codes as below the reporting limit
#   if (file_name == "EastRiverSFA_chem.csv") {
#     is_ammonia <- raw_data$solute == "ammonia_n"
#     is_ammonia[is.na(is_ammonia)] <- FALSE
#     is_zero <- is_ammonia & trimws(as.character(raw_data$value)) == "0"
#     is_missing <- is_ammonia & trimws(as.character(raw_data$value)) == "-9999"
#     is_zero[is.na(is_zero)] <- FALSE
#     is_missing[is.na(is_missing)] <- FALSE
#     actual_counts <- c(
#       ammonia_rows = sum(is_ammonia),
#       nondetect_zeros = sum(is_zero),
#       missing_codes = sum(is_missing)
#     )
#     stop_if(
#       !identical(actual_counts, east_river_source_counts),
#       "the documented East River ammonia source codes changed"
#     )
#     raw_data$value[is_zero] <- east_river_nondetect_value
#   }
#   # confirm the catalina inferred date still belongs to the reviewed sample
#   if (file_name == "CatalinaJemez_chemistry_2009-2019_V2.csv") {
#     inferred <- raw_data$SampleCode == "AZ_S1793"
#     inferred_key <- inferred & raw_data$SiteCode == "MGWEIR" &
#       as.Date(raw_data$DateTime) == as.Date("2018-04-19")
#     same_site_date <- raw_data$SiteCode == "MGWEIR" &
#       as.Date(raw_data$DateTime) == as.Date("2018-04-19")
#     stop_if(
#       sum(inferred, na.rm = TRUE) != 1L ||
#         sum(inferred_key, na.rm = TRUE) != 1L ||
#         sum(same_site_date, na.rm = TRUE) != 1L,
#       "the reviewed Catalina inferred-date sample changed"
#     )
#   }
#   raw_data
# }
#
# # ---- preparation helpers ----
#
# # ---- paths ----
#
# required_path <- function(name) {
#   value <- trimws(Sys.getenv(name, unset = ""))
#   if (identical(value, "")) {
#     stop("set ", name, " before running this script")
#   }
#   value
# }
#
# normalize_path <- function(path) {
#   normalizePath(path.expand(path), winslash = "/", mustWork = FALSE)
# }
#
# paths_overlap <- function(first, second) {
#   first <- normalize_path(first)
#   second <- normalize_path(second)
#   identical(first, second) ||
#     startsWith(first, paste0(second, "/")) ||
#     startsWith(second, paste0(first, "/"))
# }
#
# # ---- reading source files ----
#
# # read all source fields as text so qualifiers and station codes survive
# # use a prepared file instead of the download only when its folder is supplied
# read_source_csv <- function(filename, directory, clean_names = FALSE, preferred_dir = NULL) {
#   path <- file.path(directory, filename)
#   if (!is.null(preferred_dir)) {
#     preferred_path <- file.path(preferred_dir, filename)
#     if (file.exists(preferred_path)) {
#       path <- preferred_path
#     }
#   }
#   read.csv(
#     path,
#     check.names = clean_names,
#     stringsAsFactors = FALSE,
#     na.strings = "",
#     colClasses = "character",
#     comment.char = ""
#   )
# }
#
# # ---- checking inputs and output folders ----
#
# # report missing inputs before any preparation starts
# check_required_files <- function(paths, label = "required input files") {
#   missing <- paths[!file.exists(paths)]
#   if (length(missing)) {
#     stop(label, " are missing: ", paste(basename(missing), collapse = ", "))
#   }
# }
#
# # confirm that original source downloads match the versions we checked
# check_file_versions <- function(paths, expected, label) {
#   check_required_files(paths)
#   observed <- unname(tools::md5sum(paths))
#   if (!identical(observed, expected)) {
#     stop("the reviewed ", label, " changed")
#   }
# }
#
# # check the destination without creating it or changing any input folder
# check_work_folder <- function(output_dir, input_dirs, label) {
#   for (input_dir in input_dirs) {
#     if (paths_overlap(output_dir, input_dir)) {
#       stop(label, " output must not overlap an input folder")
#     }
#   }
#   if (file.exists(output_dir)) {
#     stop("the ", label, " output folder already exists")
#   }
# }
#
# # ---- saving prepared files ----
#
# # write a working copy and confirm that reading it back preserves every value
# write_prepared_csv <- function(filename, data, directory) {
#   write.csv(data, file.path(directory, filename), row.names = FALSE, na = "")
#   written <- read_source_csv(filename, directory)
#   if (identical(data, written)) {
#     return(invisible(NULL))
#   }
#   if (!identical(names(data), names(written)) || nrow(data) != nrow(written)) {
#     stop("written row or column structure changed in '", filename, "'")
#   }
#   changed <- character()
#   for (column in names(data)) {
#     if (!identical(data[[column]], written[[column]])) {
#       changed <- c(changed, column)
#     }
#   }
#   stop("written values changed in '", filename, "' for columns: ",
#     paste(changed, collapse = ", "))
# }
#
# # ---- checking and formatting values ----
#
# is_blank <- function(value) {
#   is.na(value) | trimws(as.character(value)) %in% c("", "NA")
# }
#
# assert_count <- function(observed, expected, label) {
#   observed_count <- as.integer(observed)
#   expected_count <- as.integer(expected)
#   if (!identical(observed_count, expected_count)) {
#     stop(label, " changed: expected ", expected, ", found ", observed)
#   }
# }
#
# format_amount <- function(value, qualifier = NA_character_) {
#   value_text <- sprintf("%.17g", value)
#   use_qualifier <- !is.na(qualifier) & qualifier %in% c("<", ">")
#   value_text[use_qualifier] <- paste0(
#     qualifier[use_qualifier],
#     value_text[use_qualifier]
#   )
#   value_text
# }
#
# # ---- workflow helpers ----
#
# # check the supporting files before any chemistry preparation starts
# preparation_files <- function(provider_dir, evidence_dir, preparation_dir) {
#   scripts <- file.path(preparation_dir, c(
#     "prepare_chemistry.R", "prepare_eccc_gems_danube.R",
#     "prepare_qualifier_inputs.R", "prepare_other_sources.R", "prepare_usgs_legacy.R"
#   ))
#   evidence <- file.path(evidence_dir, c(
#     "Chemistry_detection_limits_and_methods.xlsx",
#     "usgs_05420500_tn_tds_20261009.csv", "usgs_tds_as_tn_exclusions.rds",
#     "SiSyn_DataTemplate_Carey.xlsx", "yanase_chemistry_tables.json",
#     "netherlands_rws_target_chemistry.rds"
#   ))
#   providers <- file.path(provider_dir, c(
#     "canada_eccc/Water-Qual-Eau-Sites-National.csv",
#     "canada_eccc/Water-Qual-Eau-VariableInfo.csv",
#     "gems_gemstat/GFQA_v3.zip", "danube_icpdr/danube_wq_v1.csv",
#     "france_seine/seine_silica_provider_20261022.rds",
#     "amazon_hybam/Manacapuru.csv",
#     "cameroon_mtropics/Cameroon_MTROPICS_HYDROCHEMICAL_DATA_NYONG_20260915.zip",
#     "australia_mdba/MDBA_RMWQMP_physico-chemical-data_20260914.csv",
#     "usa_arc/1988-2010_Kling_Akchem.csv",
#     "usa_arc/legacy_audit_ARC_2002-2024_Kling_AON_Imnavait_Chemistry.csv",
#     "usa_arc/legacy_audit_ARC_2012-2020_Kling_Akchem.02.csv",
#     "usa_arc/1988-2010_Kling_Akchem_eml.xml"
#   ))
#   canada <- sort(list.files(file.path(provider_dir, "canada_eccc"),
#     pattern = "2000-present[.]csv$", full.names = TRUE))
#   usgs <- sort(list.files(file.path(provider_dir, "usa_usgs_samples", "responses"),
#     pattern = "^samples_sites.*_pcodes(06|16).*\\.csv$", full.names = TRUE))
#   problems <- character()
#   missing <- c(scripts, evidence, providers)
#   missing <- missing[!file.exists(missing)]
#   if (length(missing)) problems <- c(problems, paste("missing:", missing))
#   if (length(canada) != 17L) {
#     problems <- c(problems, paste("expected 17 Canada data files; found", length(canada)))
#   }
#   if (length(usgs) != 40L) {
#     problems <- c(problems, paste("expected 40 USGS provider files; found", length(usgs)))
#   }
#   if (length(problems)) stop(paste(problems, collapse = "\n"), call. = FALSE)
#   list(provider = c(providers, canada, usgs), evidence = evidence)
# }
#
# check_chemistry_setup <- function(provider_dir, evidence_dir, preparation_dir) {
#   files <- preparation_files(provider_dir, evidence_dir, preparation_dir)
#   packages <- c("librarian", "purrr", "readxl", "stringr", "tidyverse",
#     "data.table", "dplyr", "jsonlite")
#   missing <- packages[!vapply(packages, requireNamespace, logical(1), quietly = TRUE)]
#   if (length(missing)) {
#     stop("install these R packages before running: ", paste(missing, collapse = ", "),
#       call. = FALSE)
#   }
#   if (!nzchar(Sys.which("unzip"))) {
#     stop("the unzip command is needed to read the provider archives", call. = FALSE)
#   }
#   method_file <- file.path(evidence_dir, "Chemistry_detection_limits_and_methods.xlsx")
#   sheets <- readxl::excel_sheets(method_file)
#   if (!all(c("MDL_methods", "Workflow rules") %in% sheets)) {
#     stop("download the complete lab and method workbook, not a single tab", call. = FALSE)
#   }
#   provider_archives <- files$provider[grepl("[.]zip$", files$provider)]
#   for (archive in provider_archives) {
#     contents <- tryCatch(utils::unzip(archive, list = TRUE), error = function(error) NULL)
#     if (is.null(contents) || nrow(contents) == 0L) {
#       stop("the provider archive cannot be read: ", basename(archive), call. = FALSE)
#     }
#   }
#
#   # check the original source versions required by the preparation scripts
#   paths <- c(
#     file.path(provider_dir, "cameroon_mtropics",
#       "Cameroon_MTROPICS_HYDROCHEMICAL_DATA_NYONG_20260915.zip"),
#     file.path(provider_dir, "australia_mdba",
#       "MDBA_RMWQMP_physico-chemical-data_20260914.csv"),
#     file.path(evidence_dir, "yanase_chemistry_tables.json"),
#     file.path(evidence_dir, "netherlands_rws_target_chemistry.rds"),
#     file.path(provider_dir, "usa_arc", c("1988-2010_Kling_Akchem.csv",
#       "legacy_audit_ARC_2002-2024_Kling_AON_Imnavait_Chemistry.csv",
#       "legacy_audit_ARC_2012-2020_Kling_Akchem.02.csv",
#       "1988-2010_Kling_Akchem_eml.xml"))
#   )
#   hashes <- c("8b9238192be6fca67e70bbb3cc697168", "5832d9fb5276ed53e75a84c6e0f4bf89",
#     "69c4167a601ebd77cc890a79e3a13251", "dd92c6ab6756479e569bb11312185ac4",
#     "a3db20ce1bb2a460a7ee9544a7d87980", "9fcb805438e69755aa436d52d6a48d30",
#     "0d3f435c4154ac18d333732bd97f30dd", "774af538a9b09bcb4ec50900584dd6d3")
#   changed <- unname(tools::md5sum(paths)) != hashes
#   if (any(changed)) {
#     stop("reviewed provider files differ: ", paste(basename(paths[changed]), collapse = ", "),
#       call. = FALSE)
#   }
#   invisible(files)
# }
#
# # save file names, sizes, and checksums so input changes can be detected
# file_state <- function(directory) {
#   files <- sort(list.files(
#     directory,
#     all.files = TRUE,
#     full.names = TRUE,
#     recursive = TRUE,
#     include.dirs = FALSE,
#     no.. = TRUE
#   ))
#
#   data.frame(
#     file = substring(files, nchar(directory) + 2L),
#     size = unname(file.info(files)$size),
#     md5 = unname(tools::md5sum(files)),
#     stringsAsFactors = FALSE
#   )
# }
#
# # save the starting file checks for each input folder
# record_input_states <- function(directories) {
#   states <- vector("list", length(directories))
#   names(states) <- names(directories)
#   for (folder in seq_along(directories)) {
#     states[[folder]] <- file_state(directories[[folder]])
#     if (nrow(states[[folder]]) == 0L) {
#       stop("one or more workflow input folders contain no files", call. = FALSE)
#     }
#   }
#   states
# }
#
# # compare the current inputs with the checks saved before preparation
# check_input_files <- function(states, directories, context = "the workflow") {
#   changed <- logical(length(directories))
#   names(changed) <- names(directories)
#   for (folder in seq_along(directories)) {
#     current <- file_state(directories[[folder]])
#     changed[folder] <- !identical(states[[folder]], current)
#   }
#   if (any(changed)) {
#     stop(
#       "input folders changed during ",
#       context,
#       ": ",
#       paste(names(changed)[changed], collapse = ", "),
#       call. = FALSE
#     )
#   }
#   invisible(TRUE)
# }
#
# # save date and method notes in one readable csv without repeating chemistry rows
# write_chemistry_metadata <- function(metadata, file) {
#   names(metadata) <- c("date", "method")
#   notes <- dplyr::bind_rows(metadata, .id = "Note_Type")
#   notes <- as.data.frame(notes, stringsAsFactors = FALSE)
#   notes[] <- lapply(notes, as.character)
#   write.csv(notes, file, row.names = FALSE, na = "")
#   saved <- read.csv(file, colClasses = "character", na.strings = "",
#     check.names = FALSE)
#   if (!identical(saved, notes)) {
#     stop("the saved metadata changed", call. = FALSE)
#   }
#   invisible(notes)
# }
#
# # run each preparation script in a separate r session
# # keep its full log if it fails, without filling the rstudio console
# run_r_script <- function(
#   script,
#   script_args = character(),
#   settings = character(),
#   log_prefix = "chemistry-step-"
# ) {
#   setting_names <- names(settings)
#   if (length(settings) > 0L) {
#     if (is.null(setting_names) || any(setting_names == "")) {
#       stop("script settings must be named", call. = FALSE)
#     }
#
#     old_values <- Sys.getenv(setting_names, unset = NA_character_)
#     do.call(Sys.setenv, as.list(settings))
#     on.exit({
#       missing_before <- is.na(old_values)
#       if (any(missing_before)) {
#         Sys.unsetenv(setting_names[missing_before])
#       }
#       if (any(!missing_before)) {
#         do.call(
#           Sys.setenv,
#           as.list(stats::setNames(
#             old_values[!missing_before],
#             setting_names[!missing_before]
#           ))
#         )
#       }
#     }, add = TRUE)
#   }
#
#   log_dir <- Sys.getenv("SISYN_CHEM_LOG_DIR", unset = tempdir())
#   if (!dir.exists(log_dir) && !dir.create(log_dir, recursive = TRUE)) {
#     stop("the error-log folder could not be created: ", log_dir, call. = FALSE)
#   }
#   log_file <- tempfile(pattern = log_prefix, tmpdir = log_dir, fileext = ".log")
#   # remove successful temporary logs but keep the full log if a script fails
#   status <- suppressWarnings(system2(
#     file.path(R.home("bin"), "Rscript"),
#     args = c("--vanilla", shQuote(script), shQuote(script_args)),
#     stdout = log_file,
#     stderr = log_file
#   ))
#   if (status != 0L) {
#     detail <- tail(readLines(log_file, warn = FALSE), 200L)
#     stop(
#       basename(script),
#       " failed with exit status ",
#       status,
#       "\nfull log: ", log_file, ":\n",
#       paste(detail, collapse = "\n"),
#       call. = FALSE
#     )
#   }
#   unlink(log_file)
#   invisible(status)
# }
# ---- end replacement: shared-functions ----

# ---- sidney: read each source separately ----
# use only the mapped columns and finish each source before combining files
# current file: 00-harmonize_chemistry_sidney.R
# ---- begin replacement: main-source-reader ----
# # ---- prepare each source ----
# # process each source separately to avoid one large table of mostly empty columns
# standardize_source <- function(file_name) {
#   harmonize_source(file_name, data_key, units_key, run_input_dir, source_headers) |>
#     standardize_source_data(data_key)
# }
#
# ---- end replacement: main-source-reader ----

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

# ---- sidney: combine the prepared sources ----
# the shared functions above have already put each source into the same variable names and units
# current file: 00-harmonize_chemistry_sidney.R
# ---- begin replacement: main-combine-sources ----
# # ---- variables, sites, and dates ----
# chemistry_long <- purrr::map(chemistry_files, standardize_source) |>
#   dplyr::bind_rows()
# ---- end replacement: main-combine-sources ----

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

# ---- sidney: nutrient overlaps and site names ----
# keep reported outliers for the later standardized review
# compare overlapping nutrient forms only on the same date
# match the main and alternate names in the site reference table
# current file: 00-harmonize_chemistry_sidney.R
# ---- begin replacement: main-nutrients-and-sites ----
# # compare nutrient forms only at the same site on the same date
# site_day <- paste(
#   chemistry_long$LTER,
#   chemistry_long$Stream_Name,
#   chemistry_long$date,
#   sep = "\r"
# )
# has_no3 <- site_day %in% site_day[chemistry_long$variable == "NO3"]
# has_nox <- site_day %in% site_day[chemistry_long$variable == "NOx"]
# has_po4 <- site_day %in% site_day[chemistry_long$variable == "PO4"]
# drop_nitrogen <- with(
#   chemistry_long,
#   (LTER %in% drop_no3_lters & variable == "NO3" & has_nox) |
#     (Stream_Name %in% drop_nox_streams & variable == "NOx" & has_no3) |
#     (Stream_Name %in% drop_no3_streams & variable == "NO3" & has_nox) |
#     (
#       Raw_Filename == "CAMELS_USGS_N_P.csv" &
#         Stream_Name %in% drop_camels_no3_streams & variable == "NO3" & has_nox
#     )
# )
# drop_phosphorus <- with(
#   chemistry_long,
#   (
#     (LTER %in% drop_srp_lters | Stream_Name %in% drop_srp_streams) &
#       variable == "SRP" & has_po4
#   )
# )
# # keep the reviewed nutrient form only where both occur on the same day
# chemistry_long <- chemistry_long |>
#   dplyr::filter(!drop_nitrogen, !drop_phosphorus) |>
#   dplyr::mutate(
#     Stream_Name = dplyr::if_else(
#       Raw_Filename == "CAMELS_USGS_N_P.csv" & Stream_Name == "Sopchoppy River",
#       "SOPCHOPPY RIVER",
#       Stream_Name
#     )
#   )
# rm(
#   site_day, has_no3, has_nox, has_po4,
#   drop_nitrogen, drop_phosphorus
# )
# name_lookup <- dplyr::bind_rows(
#   site_reference |>
#     dplyr::transmute(
#       target_lter = LTER,
#       target_name = Stream_Name,
#       source_name = Stream_Name
#     ),
#   site_reference |>
#     dplyr::transmute(
#       target_lter = LTER,
#       target_name = Stream_Name,
#       source_name = Alt_Stream_Name
#     ) |>
#     tidyr::separate_longer_delim(source_name, delim = ";"),
#   site_reference |>
#     dplyr::transmute(
#       target_lter = LTER,
#       target_name = Stream_Name,
#       source_name = Original_Stream_Name
#     )
# ) |>
#   dplyr::filter(!is.na(source_name), trimws(source_name) != "") |>
#   dplyr::mutate(name_key = stringr::str_to_lower(stringr::str_squish(source_name))) |>
#   dplyr::distinct(name_key, target_lter, target_name)
# stop_if(
#   anyDuplicated(name_lookup$name_key) > 0L,
#   "the site reference contains ambiguous primary or alternate names"
# )
# chemistry_long <- chemistry_long |>
#   dplyr::mutate(name_key = stringr::str_to_lower(stringr::str_squish(Stream_Name))) |>
#   dplyr::left_join(name_lookup, by = "name_key")
# unmatched_sites <- chemistry_long |>
#   dplyr::filter(is.na(target_name)) |>
#   dplyr::group_by(Raw_Filename, Stream_Name) |>
#   dplyr::summarise(has_dsi = any(variable == "DSi"), .groups = "drop")
# stop_if(
#   nrow(dplyr::anti_join(
#     unmatched_sites, reviewed_unmatched_sites,
#     by = c("Raw_Filename", "Stream_Name")
#   )) > 0L ||
#     nrow(dplyr::anti_join(
#       reviewed_unmatched_sites, unmatched_sites,
#       by = c("Raw_Filename", "Stream_Name")
#     )) > 0L,
#   "the reviewed unmatched-site set changed"
# )
# unmatched_sites <- unmatched_sites |>
#   dplyr::left_join(reviewed_unmatched_sites, by = c("Raw_Filename", "Stream_Name"))
# stop_if(
#   any(unmatched_sites$reason == "no_dsi" & unmatched_sites$has_dsi) ||
#     any(unmatched_sites$reason == "not_river" & !unmatched_sites$has_dsi),
#   "the reviewed unmatched-site scope changed"
# )
# chemistry_long <- chemistry_long |>
#   dplyr::anti_join(reviewed_unmatched_sites, by = c("Raw_Filename", "Stream_Name"))
# stop_if(
#   anyNA(chemistry_long$target_name),
#   "unreviewed site names are missing from the reference table"
# )
# chemistry_long <- chemistry_long |>
#   dplyr::mutate(Stream_Name = target_name, LTER = target_lter) |>
#   dplyr::select(-name_key, -target_name, -target_lter) |>
#   dplyr::distinct() |>
#   dplyr::mutate(
#     date_text = gsub(" [[:digit:]]{1,2}\\:[[:digit:]]{1,2}", "", date),
#     date_text = gsub("\\_|\\-", "/", date_text),
#     date_text = gsub("\\:00", "", date_text)
#   )
# ---- end replacement: main-nutrients-and-sites ----

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

# ---- sidney: dates and seine si checks ----
# the preparation sections above make the flagged catalina and cameroon date inferences
# this section checks the final dates and applies the seine sample-fraction check
# current file: 00-harmonize_chemistry_sidney.R
# ---- begin replacement: main-dates ----
# date_tokens <- stringr::str_split_fixed(chemistry_long$date_text, "/", 3)
# year_token_count <- rowSums(
#   apply(date_tokens, 2, stringr::str_detect, "^[0-9]{4}$")
# )
# stop_if(
#   any(year_token_count != 1L),
#   "chemistry dates must contain exactly one four-digit year token"
# )
# date_formats <- chemistry_long |>
#   dplyr::group_by(Raw_Filename) |>
#   dplyr::summarise(date_format = detect_date_format(date_text), .groups = "drop")
# stop_if(
#   any(!date_formats$date_format %in% c("ymd", "mdy", "dmy")),
#   "one or more source files have an ambiguous date format"
# )
# chemistry_long <- chemistry_long |>
#   dplyr::left_join(date_formats, by = "Raw_Filename") |>
#   tidyr::separate_wider_delim(
#     date_text,
#     delim = "/",
#     names = c("date1", "date2", "date3"),
#     too_many = "merge"
#   ) |>
#   dplyr::mutate(
#     dplyr::across(date1:date3, as.numeric),
#     day = dplyr::case_when(
#       date_format == "ymd" ~ date3,
#       date_format == "mdy" ~ date2,
#       TRUE ~ date1
#     ),
#     month = dplyr::case_when(
#       date_format == "ymd" ~ date2,
#       date_format == "mdy" ~ date1,
#       TRUE ~ date2
#     ),
#     year = dplyr::case_when(
#       date_format == "ymd" ~ date1,
#       TRUE ~ date3
#     ),
#     date = as.Date(paste(day, month, year, sep = "-"), "%d-%m-%Y")
#   ) |>
#   dplyr::select(-date1, -date2, -date3, -day, -month, -year, -date_format)
# stop_if(anyNA(chemistry_long$date), "unparsed or missing chemistry dates remain")
#
# # retain unverified seine si with a flag and remove confirmed unfiltered si
# chemistry_long <- review_seine_si(chemistry_long, site_reference,
#   file.path(root_dir, "keys", "seine_silica_provider_20261022.rds"))
#
# ---- end replacement: main-dates ----

# Check out current dates
sort(unique(tidy_v8h$date))

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


# ---- sidney: daily averages, flags, sites, and counts ----
# average detected samples without including below-limit or above-limit thresholds
# keep a daily flag when samples were left out of that average
# keep site and sample flags and check the final counts
# current file: 00-harmonize_chemistry_sidney.R
# ---- begin replacement: main-daily-values ----
# # ---- daily master chemistry ----
# stop_if(
#   any(
#     !is.na(chemistry_long$source_lab_flag) &
#       chemistry_long$source_lab_flag != "" &
#       !chemistry_long$source_lab_flag %in% allowed_source_lab_flags
#   ),
#   "unsupported source lab flags remain"
# )
# for (row in seq_len(nrow(source_method_exclusions))) {
#   remove_rows <- chemistry_long$source_method_flag ==
#     source_method_exclusions$source_method_flag[[row]]
#   remove_rows[is.na(remove_rows)] <- FALSE
#   check_reviewed_count(
#     sum(remove_rows),
#     source_method_exclusions$allowed_counts[[row]],
#     "a reviewed source-method count changed"
#   )
#   chemistry_long <- chemistry_long[!remove_rows, , drop = FALSE]
# }
# unknown_method_flags <- unsupported_method_flags(
#   chemistry_long$source_method_flag,
#   allowed_source_method_flags
# )
# stop_if(
#   length(unknown_method_flags) > 0L,
#   paste("unsupported source method flags:", paste(unknown_method_flags, collapse = ", "))
# )
# # daily averages are calculated after unit conversion and date checks
# # average only detected values when other samples are below or above a limit
# # flag that the below-limit or above-limit samples were left out of the average
# # if every result has the same limit sign, average the thresholds and keep the sign
# daily_rows <- chemistry_long |>
#   dplyr::filter(!is.na(value)) |>
#   dplyr::group_by(Dataset, Raw_Filename, LTER, Stream_Name, date, variable, units) |>
#   dplyr::summarise(
#     detected = sum(is.na(value_qualifier)),
#     below = sum(value_qualifier == "<", na.rm = TRUE),
#     above = sum(value_qualifier == ">", na.rm = TRUE),
#     value = if (detected > 0L) mean(value[is.na(value_qualifier)]) else mean(value),
#     value_qualifier = if (detected > 0L) NA_character_ else dplyr::first(value_qualifier),
#     estimated = any(source_lab_flag == "estimated value", na.rm = TRUE),
#     source_lab_flag = reduce_daily_flag(
#       source_lab_flag[source_lab_flag != "estimated value"], "source lab"),
#     source_method_flag = reduce_daily_flag(source_method_flag, "source method"),
#     .groups = "drop"
#   )
# daily_rows <- daily_rows |>
#   dplyr::mutate(
#     calculated_lab_flag = daily_lab_flag(detected, below, above)
#   )
# stop_if(
#   any(
#     !is.na(daily_rows$calculated_lab_flag) &
#       !is.na(daily_rows$source_lab_flag) &
#       daily_rows$calculated_lab_flag != daily_rows$source_lab_flag
#   ),
#   "source and qualifier-derived lab flags conflict"
# )
# daily_rows <- daily_rows |>
#   dplyr::mutate(Lab_Flag = add_estimated_lab_flag(
#     dplyr::coalesce(calculated_lab_flag, source_lab_flag), estimated)) |>
#   dplyr::select(-detected, -below, -above, -estimated, -calculated_lab_flag, -source_lab_flag)
# # use the combined file only on dates it actually covers
# preferred_rows <- daily_rows |>
#   dplyr::filter(Raw_Filename == deduplicated_filename)
# preferred_keys <- preferred_rows |>
#   dplyr::distinct(LTER, Stream_Name, date, variable, units)
# master_chemistry <- daily_rows |>
#   dplyr::filter(Raw_Filename != deduplicated_filename) |>
#   dplyr::anti_join(
#     preferred_keys,
#     by = c("LTER", "Stream_Name", "date", "variable", "units")
#   ) |>
#   dplyr::bind_rows(preferred_rows) |>
#   dplyr::distinct()
# rm(chemistry_long, daily_rows, preferred_rows, preferred_keys)
# # exclude three hbr si values that predate the documented dissolved series
# hbr_si_before_documented_period <- master_chemistry$LTER == "HBR" &
#   master_chemistry$variable == "DSi" &
#   master_chemistry$date < hbr_documented_start
# check_reviewed_count(
#   sum(hbr_si_before_documented_period),
#   hbr_undocumented_si_count,
#   "the reviewed Hubbard Brook Si count before the documented period changed"
# )
# master_chemistry <- master_chemistry[!hbr_si_before_documented_period, ]
# # record the documented one-day camrex date correction in the final rows
# camrex_corrected <- master_chemistry$Raw_Filename ==
#   "CAMREX_filled_template.csv" |
#   (
#     master_chemistry$Raw_Filename == "Manacapuru_chemistry_combined.csv" &
#       master_chemistry$date %in% camrex_manacapuru_dates
#   )
# check_reviewed_count(
#   sum(camrex_corrected),
#   camrex_corrected_rows,
#   "the reviewed CAMREX corrected-date row count changed"
# )
# camrex_date_notes <- master_chemistry[camrex_corrected, ] |>
#   dplyr::distinct(Dataset, Raw_Filename, LTER, Stream_Name, date) |>
#   dplyr::mutate(
#     Date_Flag = "corrected",
#     Date_Note = camrex_date_note
#   )
# check_reviewed_count(
#   nrow(camrex_date_notes),
#   camrex_corrected_events,
#   "the reviewed CAMREX corrected-date event count changed"
# )
# date_notes <- dplyr::bind_rows(date_notes, camrex_date_notes)
# master_chemistry <- master_chemistry |>
#   dplyr::left_join(
#     date_notes,
#     by = c("Dataset", "Raw_Filename", "LTER", "Stream_Name", "date")
#   ) |>
#   dplyr::relocate(Date_Flag, Date_Note, .after = date) |>
#   dplyr::mutate(source_method_flag = dplyr::na_if(source_method_flag, "")) |>
#   map_source_method_flags(method_rules) |>
#   apply_interval_method_rules(method_rules)
# flagged_date_events <- master_chemistry |>
#   dplyr::filter(!is.na(Date_Flag)) |>
#   dplyr::distinct(Dataset, Raw_Filename, LTER, Stream_Name, date)
# stop_if(
#   nrow(dplyr::anti_join(
#     dplyr::select(date_notes, Dataset, Raw_Filename, LTER, Stream_Name, date),
#     flagged_date_events,
#     by = c("Dataset", "Raw_Filename", "LTER", "Stream_Name", "date")
#   )) > 0L,
#   "a reviewed inferred-date event is missing before site filtering"
# )
# master_chemistry <- master_chemistry |>
#   add_method_notes(method_rules) |>
#   dplyr::select(-source_method_flag) |>
#   dplyr::relocate(Method_Flag, Method_Note, .after = Lab_Flag)
# stop_if(
#   anyNA(master_chemistry$date) || anyNA(master_chemistry$value) ||
#     any(!is.finite(master_chemistry$value)),
#   "missing or nonfinite dates or values remain"
# )
# stop_if(
#   any(is.na(master_chemistry$Date_Flag) != is.na(master_chemistry$Date_Note)) ||
#     any(is.na(master_chemistry$Method_Flag) != is.na(master_chemistry$Method_Note)),
#   "sample flags and notes do not match"
# )
# stop_if(
#   any(!is.na(master_chemistry$value_qualifier) &
#         !master_chemistry$value_qualifier %in% allowed_value_qualifiers) ||
#     any(!is.na(master_chemistry$Lab_Flag) &
#           !master_chemistry$Lab_Flag %in% valid_lab_flags) ||
#     any(!is.na(master_chemistry$Date_Flag) &
#           !master_chemistry$Date_Flag %in% allowed_date_flags) ||
#     !source_method_flags_supported(
#       master_chemistry$Method_Flag,
#       valid_method_flags
#     ),
#   "unsupported sample flags remain"
# )
# for (row in seq_len(nrow(method_rules))) {
#   has_flag <- method_flag_present(
#     master_chemistry$Method_Flag,
#     method_rules$Method_Flag[[row]]
#   )
#   outside_scope <- !master_chemistry$variable %in% method_rules$expected_variables[[row]]
#   if (!is.na(method_rules$rule_lter[[row]])) {
#     outside_scope <- outside_scope | master_chemistry$LTER != method_rules$rule_lter[[row]]
#   }
#   if (!is.na(method_rules$start_date[[row]])) {
#     outside_scope <- outside_scope | master_chemistry$date < method_rules$start_date[[row]]
#   }
#   if (!is.na(method_rules$end_date[[row]])) {
#     outside_scope <- outside_scope | master_chemistry$date > method_rules$end_date[[row]]
#   }
#   wrong_pairs <- master_chemistry[which(has_flag & outside_scope), c("Raw_Filename", "variable")] |>
#     dplyr::distinct()
#   stop_if(
#     nrow(wrong_pairs) > 0L,
#     paste(
#       "method flag", method_rules$Method_Flag[[row]], "is outside its variable, network, or date rule:",
#       paste(paste(wrong_pairs$Raw_Filename, wrong_pairs$variable, sep = " / "), collapse = ", ")
#     )
#   )
# }
# stop_if(
#   any(master_chemistry$value_qualifier == "<" &
#         master_chemistry$Lab_Flag != "below reported limit", na.rm = TRUE) ||
#     any(master_chemistry$value_qualifier == ">" &
#           master_chemistry$Lab_Flag != "above reported limit", na.rm = TRUE) ||
#     any(!is.na(master_chemistry$value_qualifier) & master_chemistry$value <= 0),
#   "value qualifiers and lab flags do not match"
# )
# stop_if(
#   anyDuplicated(master_chemistry) > 0L ||
#     any(master_chemistry$variable != "pH" &
#           (is.na(master_chemistry$units) | trimws(master_chemistry$units) == "")),
#   "duplicate rows or blank units remain"
# )
# stop_if(
#   any(master_chemistry$variable == "tdp" & master_chemistry$units != "uM", na.rm = TRUE),
#   "nonstandard total dissolved phosphorus units remain"
# )
# stop_if(
#   any(
#     master_chemistry$variable == "susp partic matter" &
#       master_chemistry$units != "mg/L",
#     na.rm = TRUE
#   ),
#   "nonstandard suspended particulate matter units remain"
# )
# arctic_lake_rows <- master_chemistry |>
#   dplyr::semi_join(
#     arctic_lake_events,
#     by = c("LTER", "Stream_Name", "date")
#   )
# check_reviewed_count(
#   nrow(arctic_lake_rows),
#   arctic_lake_measurement_count,
#   "the provider-confirmed ARC lake-water measurement count changed"
# )
# master_chemistry <- dplyr::anti_join(
#   master_chemistry,
#   arctic_lake_events,
#   by = c("LTER", "Stream_Name", "date")
# )
# rm(arctic_lake_rows)
# si_sites <- master_chemistry |>
#   dplyr::filter(variable == "DSi") |>
#   dplyr::distinct(LTER, Stream_Name)
# # keep river and stream sites with si after the method checks
# included_sites <- site_reference |>
#   dplyr::filter(Waterbody %in% flowing_types, Include_in_Master_Chem == "Yes") |>
#   dplyr::distinct(LTER, Stream_Name) |>
#   dplyr::semi_join(si_sites, by = c("LTER", "Stream_Name"))
# master_chemistry <- dplyr::semi_join(
#   master_chemistry,
#   included_sites,
#   by = c("LTER", "Stream_Name")
# )
# master_chemistry <- apply_preferred_source_rules(
#   master_chemistry,
#   preferred_source_rules,
#   expected_superseded_rows
# )
# daily_key <- c("LTER", "Stream_Name", "date", "variable", "units")
# stop_if(
#   anyDuplicated(master_chemistry[daily_key]) > 0L,
#   "duplicate site-day-variable-unit rows remain after reviewed source selection"
# )
# stop_if(nrow(master_chemistry) == 0L, "no chemistry rows remain after site checks")
# # carry site notes onto every row and combine the sample-level flags
# master_chemistry <- add_site_and_sample_flags(master_chemistry, site_reference)
# checked_methods <- which(!is.na(method_rules$expected_count))
# observed_method_counts <- vapply(checked_methods, function(row) {
#   sum(method_flag_present(master_chemistry$Method_Flag, method_rules$Method_Flag[[row]]))
# }, integer(1))
# check_reviewed_counts(
#   observed_method_counts,
#   method_rules$expected_count[checked_methods],
#   paste("method count:", method_rules$Method_Flag[checked_methods])
# )
# stop_if(
#   any(master_chemistry$variable == "DSi" &
#         master_chemistry$Raw_Filename == canada_extractable_filename) ||
#     any(master_chemistry$variable == "DSi" &
#           master_chemistry$Raw_Filename == "EastRiverSFA_chem.csv") ||
#     any(master_chemistry$LTER == "HBR" &
#           master_chemistry$variable == "DSi" &
#           master_chemistry$date < hbr_documented_start),
#   "silicon with an undocumented sample fraction entered the final master"
# )
# release_counts <- c(
#   corrected_date_rows = sum(master_chemistry$Date_Flag == "corrected", na.rm = TRUE),
#   inferred_date_rows = sum(master_chemistry$Date_Flag == "inferred", na.rm = TRUE),
#   restored_date_rows = sum(master_chemistry$Date_Flag == "restored", na.rm = TRUE),
#   less_than_rows = sum(master_chemistry$value_qualifier == "<", na.rm = TRUE),
#   greater_than_rows = sum(master_chemistry$value_qualifier == ">", na.rm = TRUE),
#   mixed_below_daily_rows = sum(
#     master_chemistry$Lab_Flag == "below-limit results excluded from daily mean",
#     na.rm = TRUE
#   ),
#   final_rows = nrow(master_chemistry),
#   final_sites = dplyr::n_distinct(master_chemistry$LTER, master_chemistry$Stream_Name),
#   final_silicon_sites = dplyr::n_distinct(
#     master_chemistry$LTER[master_chemistry$variable == "DSi"],
#     master_chemistry$Stream_Name[master_chemistry$variable == "DSi"]
#   ),
#   final_silicon_rows = sum(master_chemistry$variable == "DSi")
# )
# if (!identical(release_counts, expected_release_counts)) {
#   count_text <- paste(
#     names(release_counts),
#     release_counts,
#     sep = "=",
#     collapse = ", "
#   )
#   stop("reviewed final chemistry counts changed: ", count_text, call. = FALSE)
# }
#
# ---- end replacement: main-daily-values ----

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

# ---- sidney: master and metadata csvs ----
# keep the flags on each chemistry row and put the long explanations in a separate csv
# the runner shown above handles the uploads after the checks pass
# current file: 00-harmonize_chemistry_sidney.R
# ---- begin replacement: main-export ----
# # ---- output ----
# output_file <- file.path(output_dir, paste0(gsub("-", "", Sys.Date()), "_masterdata_chem.csv"))
#
# # keep long explanations once in metadata and leave all flags on chemistry rows
# date_note_keys <- c("Dataset", "Raw_Filename", "LTER", "Stream_Name", "date", "Date_Flag")
# output_date_notes <- master_chemistry |>
#   dplyr::filter(!is.na(Date_Note)) |>
#   dplyr::distinct(dplyr::across(dplyr::all_of(c(date_note_keys, "Date_Note"))))
# output_method_notes <- master_chemistry |>
#   dplyr::filter(!is.na(Method_Note)) |>
#   dplyr::distinct(Method_Flag, Method_Note)
# stop_if(
#   anyDuplicated(output_date_notes[date_note_keys]) > 0L ||
#     anyDuplicated(output_method_notes$Method_Flag) > 0L,
#   "the metadata needs one explanation per date event or method flag"
# )
# metadata <- list(date_notes = output_date_notes, method_notes = output_method_notes)
# metadata_dir <- file.path(dirname(output_dir), "metadata")
# dir.create(metadata_dir, showWarnings = FALSE)
# metadata_file <- file.path(
#   metadata_dir,
#   sub("[.]csv$", "_metadata.csv", basename(output_file))
# )
# write_chemistry_metadata(metadata, metadata_file)
#
# # removing notes does not change values, row order, or flags
# master_output <- dplyr::select(master_chemistry, -Date_Note, -Method_Note)
# write.csv(master_output, output_file, na = "", row.names = FALSE)
# ---- end replacement: main-export ----

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

