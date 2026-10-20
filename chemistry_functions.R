# ---- settings and shared files ----
# read a true or false environment setting
read_boolean_setting <- function(name, default = FALSE) {
  default_text <- if (isTRUE(default)) "true" else "false"
  value <- tolower(trimws(Sys.getenv(name, unset = default_text)))
  if (!value %in% c("true", "false")) {
    stop(name, " must be set to true or false", call. = FALSE)
  }
  value == "true"
}
# read an environment setting that cannot be blank
read_required_setting <- function(name) {
  value <- trimws(Sys.getenv(name, unset = ""))
  if (value == "") {
    stop("set ", name, " before running the workflow", call. = FALSE)
  }
  value
}
# download one named google sheet as a csv file
download_named_sheet <- function(drive_files, sheet_name, destination) {
  matches <- drive_files[drive_files$name == sheet_name, , drop = FALSE]
  if (nrow(matches) != 1L) {
    stop("expected one shared sheet named '", sheet_name, "'", call. = FALSE)
  }
  dir.create(dirname(destination), recursive = TRUE, showWarnings = FALSE)
  googledrive::drive_download(
    file = googledrive::as_id(matches$id[[1]]),
    path = destination,
    overwrite = TRUE,
    type = "csv"
  )
  invisible(destination)
}

# ---- source file checks ----
# read each source header once and keep it by file name
read_source_headers <- function(input_dir, file_names) {
  file_paths <- file.path(input_dir, file_names)
  missing_files <- file_names[!file.exists(file_paths)]
  if (length(missing_files) > 0L) {
    stop("chemistry files are missing: ", paste(missing_files, collapse = ", "), call. = FALSE)
  }
  headers <- lapply(file_paths, function(path) {
    names(read.csv(path, nrows = 0))
  })
  names(headers) <- file_names
  headers
}
# import only mapped columns plus fields used by source review rules
read_selected_source <- function(input_dir, file_name, selected_columns, source_headers) {
  headers <- source_headers[[file_name]]
  needed <- unique(c(selected_columns, source_review_columns[[file_name]]))
  check_expected_columns(headers, needed, file_name)
  column_classes <- ifelse(headers %in% needed, NA_character_, "NULL")
  read.csv(
    file.path(input_dir, file_name),
    na.strings = "",
    colClasses = column_classes
  )
}
# require one key mapping for every file that carries a source flag
check_source_flag_mappings <- function(source_headers, data_key) {
  flag_names <- list(
    source_lab_flag = c("Source_Lab_Flag", "source_lab_flag"),
    source_method_flag = c("Source_Method_Flag", "source_method_flag")
  )
  for (standard_name in names(flag_names)) {
    files_with_flag <- names(source_headers)[vapply(
      source_headers,
      function(column_names) any(column_names %in% flag_names[[standard_name]]),
      logical(1)
    )]
    mapped_files <- data_key$Raw_Filename[
      data_key$Standardized_Column_Name == standard_name
    ]
    mapping_counts <- table(mapped_files[!is.na(mapped_files) & mapped_files != ""])
    if (!setequal(files_with_flag, names(mapping_counts)) || any(mapping_counts != 1L)) {
      stop("every flag-bearing input needs one ", standard_name, " mapping", call. = FALSE)
    }
  }
  invisible(TRUE)
}
# require every key-selected column to exist in its source file
check_expected_columns <- function(actual_names, expected_names, file_name) {
  missing_names <- setdiff(unique(expected_names), actual_names)
  if (length(missing_names) > 0L) {
    detail <- paste0("'", missing_names, "'", collapse = " and ")
    stop("the data key expects missing columns in '", file_name, "': ", detail, call. = FALSE)
  }
  invisible(TRUE)
}
# count values in selected columns while treating blank text as missing
count_present_values <- function(data, columns) {
  present_columns <- intersect(columns, names(data))
  if (length(present_columns) == 0L) {
    return(stats::setNames(integer(), character()))
  }
  vapply(
    data[present_columns],
    function(values) {
      present <- !is.na(values)
      if (is.character(values)) {
        present <- present & trimws(values) != ""
      }
      sum(present)
    },
    integer(1)
  )
}
# stop when columns that should be unused still contain values
stop_if_columns_have_values <- function(data, columns, text) {
  value_counts <- count_present_values(data, columns)
  used_columns <- names(value_counts)[value_counts > 0L]
  if (length(used_columns) > 0L) {
    stop(text, ": ", paste(used_columns, collapse = ", "), call. = FALSE)
  }
  invisible(value_counts)
}
# keep only columns containing at least one value
drop_empty_columns <- function(data) {
  keep <- vapply(
    data,
    function(values) {
      present <- !is.na(values)
      if (is.character(values)) {
        present <- present & trimws(values) != ""
      }
      any(present)
    },
    logical(1)
  )
  data[keep]
}

# ---- units and qualified values ----
# normalize common spellings without changing chemical basis labels
normalize_unit_label <- function(values) {
  values <- trimws(as.character(values))
  values <- stringr::str_replace_all(values, c("µ" = "u", "μ" = "u"))
  lower_values <- tolower(values)
  matched <- !is.na(lower_values) & lower_values %in% names(unit_label_aliases)
  values[matched] <- unname(unit_label_aliases[lower_values[matched]])
  values
}
# average repeated values while keeping a reported limit when needed
mean_qualified_measurement <- function(values, missing_tokens = character()) {
  values <- as.character(values)
  trimmed_values <- trimws(values)
  missing_tokens <- unique(c(missing_tokens, fixed_missing_tokens))
  is_missing <- is.na(values) | trimmed_values %in% missing_tokens
  is_below <- !is_missing & stringr::str_detect(values, "^\\s*<")
  is_above <- !is_missing & stringr::str_detect(values, "^\\s*>")
  numbers <- suppressWarnings(
    as.numeric(stringr::str_remove(values, "^\\s*[<>]\\s*"))
  )
  is_unparsed <- !is_missing & is.na(numbers)
  if (any(is_unparsed)) {
    detail <- paste(unique(values[is_unparsed]), collapse = ", ")
    stop("unparsed repeated chemistry values remain: ", detail, call. = FALSE)
  }
  detected <- numbers[!is_missing & !is_below & !is_above]
  below_limits <- numbers[is_below]
  above_limits <- numbers[is_above]
  if (length(detected) > 0L) {
    return(as.character(mean(detected)))
  }
  if (length(below_limits) > 0L) {
    if (length(above_limits) > 0L) {
      stop("mixed less-than and greater-than repeated measurements remain", call. = FALSE)
    }
    return(paste0("<", mean(below_limits)))
  }
  if (length(above_limits) > 0L) {
    return(paste0(">", mean(above_limits)))
  }
  NA_character_
}

# ---- reshaping ----
# pivot a large table in smaller row groups
chunked_pivot_longer <- function(
    data, id_columns, names_to, values_to,
    chunk_size = 10000L, values_drop_na = TRUE
) {
  if (!is.numeric(chunk_size) || length(chunk_size) != 1L || chunk_size < 1L) {
    stop("chunk_size must be a positive number", call. = FALSE)
  }
  missing_id_columns <- setdiff(id_columns, names(data))
  if (length(missing_id_columns) > 0L) {
    stop(
      "pivot id columns are missing: ",
      paste(missing_id_columns, collapse = ", "),
      call. = FALSE
    )
  }
  if (nrow(data) == 0L) {
    return(tidyr::pivot_longer(
      data,
      cols = -tidyselect::all_of(id_columns),
      names_to = names_to,
      values_to = values_to,
      values_drop_na = values_drop_na
    ))
  }
  row_numbers <- seq_len(nrow(data))
  row_groups <- split(row_numbers, (row_numbers - 1L) %/% as.integer(chunk_size))
  pieces <- lapply(row_groups, function(rows) {
    tidyr::pivot_longer(
      data[rows, , drop = FALSE],
      cols = -tidyselect::all_of(id_columns),
      names_to = names_to,
      values_to = values_to,
      values_drop_na = values_drop_na
    )
  })
  dplyr::bind_rows(pieces)
}

# ---- column merging and conversion ----
# return the first available value across selected columns
first_present_value <- function(data, columns, missing_value = NA_complex_) {
  present_columns <- intersect(columns, names(data))
  if (length(present_columns) == 0L) {
    return(rep(missing_value, nrow(data)))
  }
  purrr::reduce(data[present_columns], dplyr::coalesce)
}
# merge equivalent columns in the stated order
merge_columns <- function(data, target, precedence) {
  present_columns <- intersect(unique(c(target, precedence)), names(data))
  if (length(present_columns) == 0L) {
    return(data)
  }
  data[[target]] <- purrr::reduce(data[present_columns], dplyr::coalesce)
  data[, setdiff(names(data), setdiff(present_columns, target)), drop = FALSE]
}
# apply several ordered column merges
apply_merge_rules <- function(data, merge_rules) {
  for (target in names(merge_rules)) {
    data <- merge_columns(data, target, merge_rules[[target]])
  }
  data
}
# convert source columns and merge them into one standard column
convert_measurement <- function(values, operation, amount) {
  switch(
    operation,
    keep = values,
    multiply = values * amount,
    divide = values / amount,
    divide_then_1000 = (values / amount) * 1000,
    multiply_1000_then_divide = (values * 1000) / amount,
    stop("unsupported unit-conversion operation", call. = FALSE)
  )
}
merge_converted_columns <- function(data, target, rules) {
  if (!target %in% names(data)) {
    data[[target]] <- NA_complex_
  }
  keep <- rules$source %in% names(data) & rules$source != target
  rules <- rules[keep, , drop = FALSE]
  converted_values <- Map(
    function(column, operation, amount) {
      convert_measurement(data[[column]], operation, amount)
    },
    rules$source,
    rules$operation,
    rules$amount
  )
  candidates <- c(list(data[[target]]), converted_values)
  data[[target]] <- purrr::reduce(candidates, dplyr::coalesce)
  data[, setdiff(names(data), rules$source), drop = FALSE]
}
# apply several unit-conversion groups
apply_conversion_rules <- function(data, conversion_rules) {
  for (target in unique(conversion_rules$target)) {
    data <- merge_converted_columns(
      data,
      target,
      conversion_rules[conversion_rules$target == target, ]
    )
  }
  data
}

# ---- date formats ----
# identify one date order used by a source file
detect_date_format <- function(date_strings, delimiter = "/") {
  parts <- stringr::str_split_fixed(as.character(date_strings), delimiter, 3)
  if (ncol(parts) < 3L || all(parts == "")) {
    return("NO_DELIM_MATCH")
  }
  first <- suppressWarnings(as.numeric(parts[, 1]))
  second <- suppressWarnings(as.numeric(parts[, 2]))
  third <- suppressWarnings(as.numeric(parts[, 3]))
  if (all(is.na(first)) && all(is.na(second)) && all(is.na(third))) {
    return("UNPARSEABLE")
  }
  year_positions <- which(vapply(
    seq_len(ncol(parts)),
    function(column) any(stringr::str_detect(trimws(parts[, column]), "^[0-9]{4}$")),
    logical(1)
  ))
  if (length(year_positions) != 1L) {
    return("AMBIGUOUS_YEAR_POSITION")
  }
  year_position <- year_positions[[1]]
  if (year_position == 1L) {
    return("ymd")
  }
  if (year_position == 3L) {
    largest_first <- max(first, na.rm = TRUE)
    largest_second <- max(second, na.rm = TRUE)
    if (largest_first > 12 && largest_second <= 12) {
      return("dmy")
    }
    if (largest_second > 12 && largest_first <= 12) {
      return("mdy")
    }
    if (largest_first > 12 && largest_second > 12) {
      return("CONFLICTING_BOTH_OVER_12")
    }
    return("AMBIGUOUS_DAY_MONTH")
  }
  "UNEXPECTED_YEAR_POSITION"
}

# ---- daily flags ----
# keep one nonblank source flag within a daily group
reduce_daily_flag <- function(values, flag_label) {
  values <- trimws(as.character(values))
  flags <- unique(values[!is.na(values) & values != ""])
  if (length(flags) > 1L) {
    stop("conflicting ", flag_label, " flags remain within a daily source key", call. = FALSE)
  }
  if (length(flags) == 0L) {
    return(NA_character_)
  }
  flags[[1]]
}
# describe how reported limits contributed to one daily value
daily_lab_flag <- function(detected, below, above) {
  if (below > 0L && above > 0L) {
    stop("less-than and greater-than results share a daily source key", call. = FALSE)
  }
  if (detected > 0L && below > 0L) {
    return("below-limit results excluded from daily mean")
  }
  if (detected > 0L && above > 0L) {
    return("above-limit results excluded from daily mean")
  }
  if (detected == 0L && below > 0L) {
    return("below reported limit")
  }
  if (detected == 0L && above > 0L) {
    return("above reported limit")
  }
  NA_character_
}

# carry site and sample review notes into the final rows
add_site_and_sample_flags <- function(data, site_reference) {
  site_flags <- site_reference |>
    dplyr::transmute(
      LTER, Stream_Name,
      Site_Flag = dplyr::na_if(trimws(as.character(Flag)), "")
    ) |>
    dplyr::distinct()
  stop_if(
    anyDuplicated(site_flags[c("LTER", "Stream_Name")]) > 0L,
    "the site reference has duplicate site flags"
  )
  stop_if(
    nrow(dplyr::anti_join(
      dplyr::distinct(data, LTER, Stream_Name), site_flags,
      by = c("LTER", "Stream_Name")
    )) > 0L,
    "some final chemistry sites are missing from the site reference"
  )
  row_keys <- c("LTER", "Stream_Name", "date", "variable", "units")
  rows_before <- nrow(data)
  keys_before <- data[row_keys]
  data <- data |>
    dplyr::left_join(site_flags, by = c("LTER", "Stream_Name")) |>
    dplyr::mutate(
      Sample_Flag = paste0(
        dplyr::if_else(!is.na(Lab_Flag), paste0("lab: ", Lab_Flag, "; "), ""),
        dplyr::if_else(!is.na(Date_Flag), paste0("date: ", Date_Flag, "; "), ""),
        dplyr::if_else(!is.na(Method_Flag), paste0("method: ", Method_Flag), "")
      ),
      Sample_Flag = sub("; $", "", Sample_Flag),
      Sample_Flag = dplyr::na_if(Sample_Flag, "")
    ) |>
    dplyr::relocate(Site_Flag, Sample_Flag, .after = Stream_Name)
  has_detail <- with(data, !is.na(Lab_Flag) | !is.na(Date_Flag) | !is.na(Method_Flag))
  stop_if(
    nrow(data) != rows_before || !identical(data[row_keys], keys_before),
    "adding site flags changed the chemistry rows or row order"
  )
  stop_if(
    any(is.na(data$Sample_Flag) != !has_detail),
    "the sample flag does not match the detailed sample flags"
  )
  data
}

# ---- checks ----
# stop when one reviewed condition is true
stop_if <- function(condition, text) {
  if (isTRUE(condition)) {
    stop(text, call. = FALSE)
  }
  invisible(condition)
}
# require two file lists to contain the same names in the same order
check_file_list <- function(actual_files, expected_files, text) {
  if (!identical(actual_files, expected_files)) {
    stop(text, call. = FALSE)
  }
  invisible(TRUE)
}
# require one reviewed count or an accepted fallback count
check_reviewed_count <- function(actual, expected, text) {
  if (!actual %in% expected) {
    stop(
      text,
      " (expected ", paste(expected, collapse = " or "),
      "; found ", actual, ")",
      call. = FALSE
    )
  }
  invisible(actual)
}

# ---- workflow inputs ----
# download and read the shared keys and selected chemistry files
load_chemistry_inputs <- function(root_dir, raw_input_dir, download_shared_inputs) {
  key_dir <- file.path(root_dir, "keys")
  output_dir <- file.path(root_dir, "tidy")
  dir.create(key_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(raw_input_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  if (download_shared_inputs) {
    key_id <- "https://drive.google.com/drive/folders/1hbkUsTdo4WAEUnlPReOUuXdeeXm92mg-"
    key_drive <- googledrive::drive_ls(googledrive::as_id(key_id))
    purrr::walk(
      c("SiSyn_Data_Key", "SiSyn_Chem_Units_Key"),
      ~ download_named_sheet(key_drive, .x, file.path(key_dir, paste0(.x, ".csv")))
    )
    site_id <- "https://drive.google.com/drive/u/1/folders/0AIPkWhVuXjqFUk9PVA"
    site_drive <- googledrive::drive_ls(googledrive::as_id(site_id))
    site_path <- file.path(key_dir, "Site_Reference_Table.csv")
    download_named_sheet(site_drive, "Site_Reference_Table", site_path)
  }
  key_files <- file.path(
    key_dir,
    c("SiSyn_Data_Key.csv", "SiSyn_Chem_Units_Key.csv", "Site_Reference_Table.csv")
  )
  stop_if(
    any(!file.exists(key_files)),
    paste(
      "required key files are missing:",
      paste(basename(key_files[!file.exists(key_files)]), collapse = ", ")
    )
  )
  data_key_raw <- read.csv(key_files[[1]], na.strings = "") |>
    dplyr::filter(Data_type == "chemistry")
  units_key <- read.csv(key_files[[2]], na.strings = "") |>
    dplyr::filter(Data_type == "chemistry") |>
    dplyr::select(-dplyr::any_of(c("Data_type", "Notes", "X"))) |>
    dplyr::mutate(Variable = tolower(Variable))
  text_columns <- c(USGSGageNumber = "character", Discharge_Site_Name = "character")
  site_reference <- read.csv(key_files[[3]], na.strings = "", colClasses = text_columns)
  raw_files <- sort(unique(trimws(as.character(data_key_raw$Raw_Filename))))
  raw_files <- raw_files[!is.na(raw_files) & raw_files != ""]
  if (download_shared_inputs) {
    chemistry_id <- paste0(
      "https://drive.google.com/drive/u/1/folders/",
      "1da8AkSvforPehp-gPJsmA7-pPAJ913mz"
    )
    chemistry_drive <- googledrive::drive_ls(
      googledrive::as_id(chemistry_id),
      n_max = Inf
    )
    selected_files <- chemistry_drive |>
      dplyr::filter(name %in% raw_files)
    stop_if(
      nrow(selected_files) != length(raw_files) ||
        anyDuplicated(selected_files$name) > 0L ||
        !setequal(selected_files$name, raw_files),
      "the chemistry folder needs one copy of each data-key file"
    )
    purrr::walk2(selected_files$id, selected_files$name, function(id, name) {
      googledrive::drive_download(
        file = id,
        path = file.path(raw_input_dir, name),
        overwrite = TRUE
      )
    })
  }
  check_file_list(
    sort(list.files(raw_input_dir, pattern = "[.]csv$")),
    raw_files,
    "the local master chemistry inputs do not match the data key"
  )
  source_headers <- read_source_headers(raw_input_dir, raw_files)
  check_source_flag_mappings(source_headers, data_key_raw)
  data_key <- data_key_raw |>
    dplyr::select(-dplyr::any_of(c("Data_type", "Notes"))) |>
    dplyr::mutate(
      Raw_Column_Name = gsub(" |\\+|\\(|\\)|\\/|\\-", ".", Raw_Column_Name)
    )
  list(
    data_key = data_key,
    units_key = units_key,
    site_reference = site_reference,
    raw_files = raw_files,
    source_headers = source_headers,
    output_dir = output_dir
  )
}
# apply source review choices without changing the source file
apply_source_review_rules <- function(raw_data, file_name) {
  # interpret documented zero ammonia codes as below the reporting limit
  if (file_name == "EastRiverSFA_chem.csv") {
    is_ammonia <- raw_data$solute == "ammonia_n"
    is_ammonia[is.na(is_ammonia)] <- FALSE
    is_zero <- is_ammonia & trimws(as.character(raw_data$value)) == "0"
    is_missing <- is_ammonia & trimws(as.character(raw_data$value)) == "-9999"
    is_zero[is.na(is_zero)] <- FALSE
    is_missing[is.na(is_missing)] <- FALSE
    actual_counts <- c(
      ammonia_rows = sum(is_ammonia),
      nondetect_zeros = sum(is_zero),
      missing_codes = sum(is_missing)
    )
    stop_if(
      !identical(actual_counts, east_river_source_counts),
      "the documented East River ammonia source codes changed"
    )
    raw_data$value[is_zero] <- east_river_nondetect_value
  }
  # confirm the catalina inferred date still belongs to the reviewed sample
  if (file_name == "CatalinaJemez_chemistry_2009-2019_V2.csv") {
    inferred <- raw_data$SampleCode == "AZ_S1793"
    inferred_key <- inferred & raw_data$SiteCode == "MGWEIR" &
      as.Date(raw_data$DateTime) == as.Date("2018-04-19")
    same_site_date <- raw_data$SiteCode == "MGWEIR" &
      as.Date(raw_data$DateTime) == as.Date("2018-04-19")
    stop_if(
      sum(inferred, na.rm = TRUE) != 1L ||
        sum(inferred_key, na.rm = TRUE) != 1L ||
        sum(same_site_date, na.rm = TRUE) != 1L,
      "the reviewed Catalina inferred-date sample changed"
    )
  }
  # confirm the restored guadeloupe dates still match the provider records
  if (file_name %in% names(guadeloupe_dates)) {
    source_dates <- as.Date(raw_data$datetime)
    restored_counts <- vapply(
      guadeloupe_dates[[file_name]],
      function(value) sum(source_dates == value, na.rm = TRUE),
      integer(1)
    )
    stop_if(
      nrow(raw_data) != guadeloupe_rows[[file_name]] ||
        anyNA(source_dates) ||
        any(restored_counts != guadeloupe_rows_per_restored_date),
      "the reviewed Guadeloupe source-restored dates changed"
    )
  }
  raw_data
}
