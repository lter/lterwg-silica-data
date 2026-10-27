# ---- full chemistry workflow ----
# workflow updates: sidney bush
# source files stay unchanged

# ---- start here in rstudio ----
# keep this file with the rest of the repository and open it in rstudio
# click source, choose a save folder, and sign in to google when prompted
# there are no paths to edit or files to download by hand
# the script installs missing packages and downloads the shared inputs
# it checks those inputs before preparing and combining the chemistry
# each run gets a new dated folder so earlier files are left alone
# the final csv is saved in chemistry-run/master/tidy inside that folder
# keep rstudio open and the computer awake until the run finishes
# if the run fails, the error gives the saved log location
# the master and its notes file are saved here and uploaded to the shared drive

# ---- download the shared inputs ----

# download source files, supporting data, and live keys from the shared drive
download_chemistry_inputs <- function(input_root) {
  if (!requireNamespace("googledrive", quietly = TRUE)) {
    stop("install the googledrive package before downloading the inputs", call. = FALSE)
  }
  googledrive::local_drive_quiet()
  input_root <- path.expand(input_root)
  if (file.exists(input_root)) {
    stop("choose a new input folder so existing downloads are not overwritten", call. = FALSE)
  }
  # these shared locations are also linked in the readme
  folder_id <- "1da8AkSvforPehp-gPJsmA7-pPAJ913mz"
  support_name <- "Chemistry_workflow_support_20261022.zip"
  shared <- googledrive::drive_ls(googledrive::as_id(folder_id), n_max = Inf)
  source_rows <- grepl("[.]csv$", shared$name)
  support_rows <- shared$name == support_name
  if (sum(source_rows) != 236L || sum(support_rows) != 1L ||
      anyDuplicated(tolower(shared$name[source_rows]))) {
    stop("the shared chemistry file list differs from the reviewed release", call. = FALSE)
  }
  source_dir <- file.path(input_root, "Chemistry_Files")
  key_dir <- file.path(input_root, "keys")
  evidence_dir <- file.path(input_root, "evidence")
  for (directory in c(source_dir, key_dir, evidence_dir)) {
    if (!dir.create(directory, recursive = TRUE)) {
      stop("the download folder could not be created: ", directory, call. = FALSE)
    }
  }

  # the fixed file count describes the reviewed october 2026 input set
  # download source csvs only, not older sheets or subfolders
  sources <- shared[source_rows, ]
  for (row in seq_len(nrow(sources))) {
    path <- file.path(source_dir, sources$name[row])
    googledrive::drive_download(sources[row, ], path = path)
    expected <- sources$drive_resource[[row]]$md5Checksum
    if (is.null(expected) || !identical(unname(tools::md5sum(path)), expected)) {
      stop("the downloaded source failed its file check: ", basename(path), call. = FALSE)
    }
    if (row %% 25L == 0L || row == nrow(sources)) {
      message("chemistry files downloaded: ", row, " of ", nrow(sources))
    }
  }
  archive <- tempfile(fileext = ".zip")
  on.exit(unlink(archive), add = TRUE)
  support <- shared[support_rows, ]
  googledrive::drive_download(support, path = archive)
  expected <- support$drive_resource[[1]]$md5Checksum
  if (is.null(expected) || !identical(unname(tools::md5sum(archive)), expected)) {
    stop("the supporting-data download failed its file check", call. = FALSE)
  }
  entries <- utils::unzip(archive, list = TRUE)$Name
  if (any(!grepl("^(providers|evidence)/", entries)) ||
      any(grepl("(^|[/\\\\])\\.\\.([/\\\\]|$)", entries))) {
    stop("the supporting archive contains unexpected paths", call. = FALSE)
  }
  utils::unzip(archive, exdir = input_root)

  # export the live keys rather than downloading older saved copies
  keys <- data.frame(
    id = c("1gKe7zdMtFHeBZJPfzu5l4jjtvP0AFsfrdCreB7OZvkU",
      "15KDcbCAs4ZzZWoPN2g4MpgOMnYQFaxtAuZnQzwAAONI",
      "11t9YYTzN_T12VAQhHuY5TpVjGS50ymNmKznJK4rKTIU",
      "1PshqCYF2hAR9WhpgN65KLBm0qiqcBmihwkGdSesxI3Q"),
    path = c(file.path(key_dir, c("SiSyn_Data_Key.csv", "SiSyn_Chem_Units_Key.csv",
      "Site_Reference_Table.csv")),
      file.path(evidence_dir, "Chemistry_detection_limits_and_methods.xlsx")),
    type = c("csv", "csv", "csv", "xlsx")
  )
  for (row in seq_len(nrow(keys))) {
    googledrive::drive_download(googledrive::as_id(keys$id[row]),
      path = keys$path[row], type = keys$type[row])
  }
  invisible(list(
    source_dir = source_dir,
    provider_dir = file.path(input_root, "providers"),
    key_dir = key_dir,
    evidence_dir = evidence_dir,
    work_dir = file.path(input_root, "chemistry-run")
  ))
}

# ---- run the workflow ----

run_chemistry <- function(
  # downloaded csv files directly inside chemistry_files
  source_dir,
  # original source downloads from the shared supporting-data zip
  provider_dir,
  # current data key, units key, and site reference exports
  key_dir,
  # lab workbook and supporting records from the shared download
  evidence_dir,
  # a new output folder, separate from the downloaded inputs
  work_dir,
  workflow_dir = getwd(),
  check_only = FALSE,
  # the rstudio run uploads the master; background tests leave this off
  upload = FALSE
) {
  workflow_dir <- normalizePath(workflow_dir, winslash = "/", mustWork = TRUE)
  if (isTRUE(upload) && !requireNamespace("googledrive", quietly = TRUE)) {
    stop("the googledrive package is needed to upload the master", call. = FALSE)
  }

  # keep preparation code with the workflow in github
  preparation_dir <- file.path(workflow_dir, "chemistry-inputs")
  if (!dir.exists(preparation_dir)) {
    stop("the chemistry-inputs folder is missing", call. = FALSE)
  }
  helper_file <- file.path(workflow_dir, "chemistry_functions.R")
  if (!file.exists(helper_file)) {
    stop("chemistry_functions.R is missing", call. = FALSE)
  }
  source(helper_file, local = TRUE)

  source_dir <- normalizePath(source_dir, winslash = "/", mustWork = TRUE)
  provider_dir <- normalizePath(provider_dir, winslash = "/", mustWork = TRUE)
  key_dir <- normalizePath(key_dir, winslash = "/", mustWork = TRUE)
  evidence_dir <- normalizePath(evidence_dir, winslash = "/", mustWork = TRUE)
  work_parent <- normalizePath(
    dirname(path.expand(work_dir)),
    winslash = "/",
    mustWork = TRUE
  )
  work_dir <- file.path(work_parent, basename(path.expand(work_dir)))
  if (file.exists(work_dir)) {
    stop("the work folder must not already exist", call. = FALSE)
  }

  read_only_dirs <- c(source_dir, provider_dir, key_dir, evidence_dir)
  if (any(vapply(
    read_only_dirs,
    function(input_dir) paths_overlap(work_dir, input_dir),
    logical(1)
  ))) {
    stop(
      "the work folder must be separate from every read-only input folder",
      call. = FALSE
    )
  }

  builder <- file.path(preparation_dir, "prepare_chemistry.R")
  master_script <- file.path(workflow_dir, "00-harmonize_chemistry_sidney.R")
  helper_files <- file.path(
    workflow_dir,
    c("chemistry_functions.R", "chemistry_rules.R")
  )
  method_file <- file.path(
    evidence_dir,
    "Chemistry_detection_limits_and_methods.xlsx"
  )
  seine_file <- file.path(provider_dir, "france_seine", "seine_silica_provider_20261022.rds")
  key_files <- file.path(
    key_dir,
    c("SiSyn_Data_Key.csv", "SiSyn_Chem_Units_Key.csv", "Site_Reference_Table.csv")
  )
  required_files <- c(builder, master_script, helper_files, method_file, seine_file, key_files)
  if (any(!file.exists(required_files))) {
    stop(
      "required workflow files are missing: ",
      paste(basename(required_files[!file.exists(required_files)]), collapse = ", "),
      call. = FALSE
    )
  }

  top_level_files <- sort(list.files(
    source_dir,
    pattern = "[.]csv$",
    full.names = TRUE
  ))
  if (length(top_level_files) != 236L) {
    stop(
      "the source folder must contain exactly 236 chemistry csv files",
      call. = FALSE
    )
  }
  source_files <- top_level_files

  source_names <- basename(source_files)
  repeated_names <- unique(source_names[
    duplicated(tolower(source_names)) |
      duplicated(tolower(source_names), fromLast = TRUE)
  ])
  gems_files <- grep("^GEMS__GEMS__.*[.]csv$", source_names, value = TRUE)
  other_files <- setdiff(source_names, gems_files)
  if (length(repeated_names) > 0L) {
    stop(
      "source chemistry filenames are repeated: ",
      paste(repeated_names, collapse = ", "),
      call. = FALSE
    )
  }
  if (
    length(source_names) != 236L ||
      length(other_files) != 76L ||
      length(gems_files) != 160L ||
      "Canada_ECCC_AllSites_chemistry.csv" %in% source_names
  ) {
    stop(
      paste(
        "the source folder must provide exactly 76 main files and 160 GEMS",
        "site files, without the generated combined Canada file"
      ),
      call. = FALSE
    )
  }

  # check all supporting files and packages before starting preparation
  check_chemistry_setup(provider_dir, evidence_dir, preparation_dir)

  # check the setup without creating outputs or starting preparation
  if (isTRUE(check_only)) {
    return(invisible(list(
      source_dir = source_dir,
      provider_dir = provider_dir,
      key_dir = key_dir,
      evidence_dir = evidence_dir,
      work_dir = work_dir,
      source_files = length(source_files)
    )))
  }

  # record every input folder so the complete run can verify it stayed unchanged
  input_dirs <- c(
    source = source_dir,
    provider = provider_dir,
    keys = key_dir,
    evidence = evidence_dir
  )
  input_states <- record_input_states(input_dirs)
  verify_inputs <- function() {
    check_input_files(
      input_states,
      input_dirs,
      context = "the full chemistry workflow"
    )
  }
  on.exit(verify_inputs(), add = TRUE)

  dir.create(work_dir)
  source_input_dir <- source_dir
  run_input_dir <- file.path(work_dir, "run-inputs")
  master_root <- file.path(work_dir, "master")
  master_key_dir <- file.path(master_root, "keys")
  dir.create(master_key_dir, recursive = TRUE)
  dir.create(file.path(master_root, "tidy"))

  # prepare each file before combining the chemistry
  run_r_script(
    builder,
    c(source_input_dir, provider_dir, key_dir, evidence_dir, run_input_dir),
    settings = c(SISYN_CHEM_LOG_DIR = file.path(work_dir, "logs")),
    log_prefix = "chemistry-inputs-"
  )

  # save the keys used for this run beside the master output
  copied_keys <- file.copy(
    c(key_files, method_file, seine_file),
    master_key_dir,
    overwrite = TRUE,
    copy.mode = FALSE
  )
  if (!all(copied_keys)) {
    stop("the working copies of the shared keys could not be created", call. = FALSE)
  }

  # convert units, calculate daily averages, add flags, and save the master
  run_r_script(
    master_script,
    settings = c(
      SISYN_CHEM_LOG_DIR = file.path(work_dir, "logs"),
      SISYN_CHEM_INTERNAL_PREPARED_RUN = "true",
      SISYN_CHEM_LOCAL_ROOT = master_root,
      SISYN_CHEM_LOCAL_INPUT_DIR = run_input_dir
    ),
    log_prefix = "master-chemistry-"
  )

  verify_inputs()
  output_files <- list.files(
    file.path(master_root, "tidy"),
    pattern = "[.]csv$",
    full.names = TRUE
  )
  if (length(output_files) != 1L) {
    stop("the workflow did not create exactly one master file", call. = FALSE)
  }
  metadata_file <- file.path(
    master_root, "metadata",
    sub("[.]csv$", "_metadata.csv", basename(output_files[[1]]))
  )
  if (!file.exists(metadata_file)) {
    stop("the master chemistry metadata file is missing", call. = FALSE)
  }
  message("master chemistry written to: ", output_files[[1]])
  message("chemistry notes written to: ", metadata_file)

  # upload the notes first, then the finished master after all checks pass
  # keep any existing drive file with the same name rather than replacing it
  if (isTRUE(upload)) {
    destination <- googledrive::drive_get(
      googledrive::as_id("1dTENIB5W2ClgW0z-8NbjqARiaGO2_A7W")
    )
    upload_files <- c(metadata_file, output_files[[1]])
    existing <- googledrive::drive_ls(destination, n_max = Inf)
    if (any(basename(upload_files) %in% existing$name)) {
      stop("a master or metadata file with this name is already on drive; local files were kept",
        call. = FALSE)
    }
    for (file in upload_files) {
      uploaded <- googledrive::drive_upload(media = file, path = destination, overwrite = FALSE)
      message(basename(file), " uploaded to: https://drive.google.com/file/d/",
        uploaded$id[[1]], "/view")
    }
  }
}

# ---- rstudio startup ----

# use the open script to locate the workflow, not a computer-specific path
start_chemistry <- function() {
  if (!requireNamespace("rstudioapi", quietly = TRUE)) {
    utils::install.packages("rstudioapi", repos = "https://cloud.r-project.org")
  }
  if (!rstudioapi::isAvailable()) {
    stop("open this script in rstudio and click source", call. = FALSE)
  }
  script <- rstudioapi::getSourceEditorContext()$path
  if (!identical(basename(script), "run_full_chemistry_workflow.R")) {
    stop("open the full chemistry runner in rstudio before clicking source", call. = FALSE)
  }
  workflow_dir <- dirname(normalizePath(script, mustWork = TRUE))
  parent <- rstudioapi::selectDirectory(caption = "choose where to save chemistry inputs and results")
  if (is.null(parent) || !nzchar(parent)) return(invisible(NULL))

  packages <- c("googledrive", "librarian", "tidyverse", "readxl", "data.table", "jsonlite")
  missing <- packages[!vapply(packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing)) {
    utils::install.packages(missing, repos = "https://cloud.r-project.org")
  }
  input_root <- file.path(parent, paste0("sisyn-chemistry-", format(Sys.time(), "%Y%m%d-%H%M%S")))
  settings <- download_chemistry_inputs(input_root)
  settings$workflow_dir <- workflow_dir
  do.call(run_chemistry, c(settings, list(check_only = TRUE)))
  do.call(run_chemistry, c(settings, list(upload = TRUE)))
}

# clicking source in rstudio starts the guided run
# command-line runs use the five folder paths listed below
if (interactive()) {
  start_chemistry()
} else if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) != 5L) {
    stop(
      "supply the source, provider, key, evidence, and new work folders",
      call. = FALSE
    )
  }
  script_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(script_argument) != 1L) {
    stop("the workflow folder could not be identified", call. = FALSE)
  }
  run_chemistry(
    source_dir = args[[1]],
    provider_dir = args[[2]],
    key_dir = args[[3]],
    evidence_dir = args[[4]],
    work_dir = args[[5]],
    workflow_dir = dirname(normalizePath(sub("^--file=", "", script_argument)))
  )
}
