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
# read the shared method flags, notes, and date-based rules
read_workflow_rules <- function(path) {
  required_columns <- c(
    "Source_Method_Flag", "Method_Flag", "LTER", "Variable",
    "Start_Date", "End_Date", "Method_Note", "Expected_Count", "Apply_To"
  )
  rules <- readxl::read_excel(path, sheet = "Workflow rules")
  missing_columns <- setdiff(required_columns, names(rules))
  if (length(missing_columns) > 0L) {
    stop(
      "the workflow-rules lookup is missing: ",
      paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }
  rules <- rules |>
    dplyr::select(dplyr::all_of(required_columns)) |>
    dplyr::mutate(
      dplyr::across(-Expected_Count, ~ dplyr::na_if(trimws(as.character(.x)), "")),
      expected_count = suppressWarnings(as.integer(Expected_Count)),
      start_date = as.Date(Start_Date),
      end_date = as.Date(End_Date),
      expected_variables = strsplit(Variable, ";", fixed = TRUE)
    ) |>
    dplyr::mutate(
      expected_variables = lapply(expected_variables, trimws)
    ) |>
    dplyr::transmute(
      source_method_flag = Source_Method_Flag,
      apply_to = Apply_To,
      Method_Flag,
      rule_lter = LTER,
      expected_variables,
      start_date,
      end_date,
      Method_Note,
      expected_count
    )
  stop_if(
    nrow(rules) == 0L || anyNA(rules$Method_Flag) ||
      anyNA(rules$Method_Note) || anyDuplicated(rules$Method_Flag) > 0L,
    "the workflow-rules lookup needs one complete row per method flag"
  )
  source_flags <- rules$source_method_flag[!is.na(rules$source_method_flag)]
  # require the current shared rules instead of filling in older copies
  required_flags <- c(
    "soluble reactive Si, filtered ICP-OES", "reported as SiO2-Dis",
    "reactive Si, filtered (GEMS)", "shallowest sample depth used"
  )
  missing_flags <- setdiff(required_flags, source_flags)
  stop_if(
    length(missing_flags) > 0L,
    paste("refresh the shared lab workbook; required method rules are missing:",
      paste(missing_flags, collapse = ", "))
  )
  stop_if(
    any(grepl("; ", c(source_flags, rules$Method_Flag), fixed = TRUE)),
    "individual method labels cannot contain '; ' because it separates combined flags"
  )
  stop_if(
    anyDuplicated(source_flags) > 0L ||
      any(!is.na(rules$start_date) & !is.na(rules$end_date) &
            rules$end_date < rules$start_date),
    "the workflow-rules lookup contains duplicate source flags or invalid dates"
  )
  stop_if(
    anyNA(rules$apply_to) ||
      any(!rules$apply_to %in% c("source flag only", "network and dates")) ||
      any(rules$apply_to == "source flag only" & is.na(rules$source_method_flag)),
    "each method rule needs an explicit application setting and source label where required"
  )
  stop_if(
    any(rules$apply_to == "network and dates" & is.na(rules$rule_lter)),
    "network-wide method rules must name their network"
  )
  rules
}

# find one method flag inside a semicolon-separated field
method_flag_present <- function(values, flag) {
  values <- dplyr::na_if(trimws(as.character(values)), "")
  unique_values <- unique(values[!is.na(values)])
  present <- logical(length(unique_values))
  names(present) <- unique_values
  for (value in unique_values) {
    parts <- strsplit(value, "; ", fixed = TRUE)[[1]]
    present[value] <- flag %in% parts
  }
  output <- rep(FALSE, length(values))
  output[!is.na(values)] <- unname(present[values[!is.na(values)]])
  output
}

# list labels that need a matching workflow rule
unsupported_method_flags <- function(values, allowed_flags) {
  values <- dplyr::na_if(trimws(as.character(values)), "")
  unique_values <- unique(values[!is.na(values)])
  parts <- unique(unlist(strsplit(unique_values, "; ", fixed = TRUE), use.names = FALSE))
  setdiff(parts, allowed_flags)
}

# confirm every source method flag has a lookup row
source_method_flags_supported <- function(values, allowed_flags) {
  length(unsupported_method_flags(values, allowed_flags)) == 0L
}

# convert source method flags to the plain final labels
map_source_method_flags <- function(data, method_rules) {
  values <- dplyr::na_if(trimws(as.character(data$source_method_flag)), "")
  unique_values <- unique(values[!is.na(values)])
  lookup <- method_rules |>
    dplyr::filter(!is.na(source_method_flag))
  mapped <- character(length(unique_values))
  names(mapped) <- unique_values
  for (value in unique_values) {
    parts <- strsplit(value, "; ", fixed = TRUE)[[1]]
    matches <- match(parts, lookup$source_method_flag)
    if (anyNA(matches)) {
      stop("a source method flag is missing from the workflow rules", call. = FALSE)
    }
    mapped[value] <- paste(unique(lookup$Method_Flag[matches]), collapse = "; ")
  }
  data$Method_Flag <- NA_character_
  data$Method_Flag[!is.na(values)] <- unname(mapped[values[!is.na(values)]])
  data
}

# attach the note for every method flag in a combined field
add_method_notes <- function(data, method_rules) {
  values <- dplyr::na_if(trimws(as.character(data$Method_Flag)), "")
  unique_values <- unique(values[!is.na(values)])
  notes <- character(length(unique_values))
  names(notes) <- unique_values
  for (value in unique_values) {
    parts <- strsplit(value, "; ", fixed = TRUE)[[1]]
    matches <- match(parts, method_rules$Method_Flag)
    if (anyNA(matches)) {
      stop("a method flag is missing from the workflow rules", call. = FALSE)
    }
    notes[value] <- paste(unique(method_rules$Method_Note[matches]), collapse = " ")
  }
  data$Method_Note <- NA_character_
  data$Method_Note[!is.na(values)] <- unname(notes[values[!is.na(values)]])
  data
}

# add a flag across a network only when the rule explicitly says to do that
apply_interval_method_rules <- function(data, method_rules) {
  interval_rules <- method_rules |>
    dplyr::filter(apply_to == "network and dates")
  for (row in seq_len(nrow(interval_rules))) {
    matches <- data$variable %in% interval_rules$expected_variables[[row]]
    if (!is.na(interval_rules$rule_lter[[row]])) {
      matches <- matches & data$LTER == interval_rules$rule_lter[[row]]
    }
    if (!is.na(interval_rules$start_date[[row]])) {
      matches <- matches & data$date >= interval_rules$start_date[[row]]
    }
    if (!is.na(interval_rules$end_date[[row]])) {
      matches <- matches & data$date <= interval_rules$end_date[[row]]
    }
    already_present <- method_flag_present(
      data$Method_Flag,
      interval_rules$Method_Flag[[row]]
    )
    add_flag <- matches & !already_present
    data$Method_Flag[add_flag] <- dplyr::if_else(
      is.na(data$Method_Flag[add_flag]),
      interval_rules$Method_Flag[[row]],
      paste(
        data$Method_Flag[add_flag],
        interval_rules$Method_Flag[[row]],
        sep = "; "
      )
    )
  }
  data
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
# check that every column named in the data key exists in its source file
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
# keep only columns containing a value plus any named workflow fields
drop_empty_columns <- function(data, keep = character()) {
  has_values <- vapply(
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
  data[has_values | names(data) %in% keep]
}

# ---- units and qualified values ----
# match common unit spellings without changing the reported chemical form
normalize_unit_label <- function(values) {
  values <- trimws(as.character(values))
  values <- stringr::str_replace_all(values, c("µ" = "u", "μ" = "u"))
  lower_values <- tolower(values)
  matched <- !is.na(lower_values) & lower_values %in% names(unit_label_aliases)
  values[matched] <- unname(unit_label_aliases[lower_values[matched]])
  values
}
# split reported signs from numeric chemistry values
parse_qualified_numbers <- function(values, missing_tokens = character()) {
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
  list(
    numbers = numbers,
    is_missing = is_missing,
    is_below = is_below,
    is_above = is_above
  )
}
# reproduce the previous grouping only for days without below-limit samples
mean_qualified_measurement <- function(values, missing_tokens = character()) {
  parsed <- parse_qualified_numbers(values, missing_tokens)
  detected <- parsed$numbers[
    !parsed$is_missing & !parsed$is_below & !parsed$is_above
  ]
  below_limits <- parsed$numbers[parsed$is_below]
  above_limits <- parsed$numbers[parsed$is_above]
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

# read the value and limit sign, then average repeated measurements
collapse_repeated_amounts <- function(data, missing_tokens, mean_function) {
  parsed <- parse_qualified_numbers(data$amount, missing_tokens)
  normalized <- as.character(parsed$numbers)
  normalized[parsed$is_below] <- paste0("<", normalized[parsed$is_below])
  normalized[parsed$is_above] <- paste0(">", normalized[parsed$is_above])
  normalized[parsed$is_missing] <- NA_character_
  data$amount <- normalized
  key_columns <- setdiff(names(data), "amount")
  collapse_repeated_cells(data, mean_function) |>
    dplyr::arrange(dplyr::across(dplyr::all_of(key_columns)))
}

# average repeated measurements before turning variables into columns
collapse_repeated_cells <- function(data, mean_function) {
  key_columns <- setdiff(names(data), "amount")
  repeated <- vctrs::vec_duplicate_detect(data[key_columns])
  if (!any(repeated)) {
    return(data)
  }
  repeated_rows <- data[repeated, , drop = FALSE] |>
    dplyr::group_by(dplyr::across(dplyr::all_of(key_columns))) |>
    dplyr::summarise(amount = mean_function(amount), .groups = "drop")
  dplyr::bind_rows(
    data[!repeated, , drop = FALSE],
    repeated_rows
  )
}

# ---- reshaping ----
# turn columns into rows in smaller groups to limit memory use
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
# combine each group of equivalent columns in the listed order
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
# use half the reported threshold only for confirmed less-than results
# a calculated mean is an estimate, not a new detection threshold
daily_limit_mean <- function(value, qualifier) {
  below <- !is.na(qualifier) & qualifier == "<"
  above <- !is.na(qualifier) & qualifier == ">"
  if (any(below) && any(above)) {
    stop("less-than and greater-than results share a daily source key", call. = FALSE)
  }
  if (any(below & (!is.finite(value) | value <= 0))) {
    stop("a less-than result has no usable threshold", call. = FALSE)
  }
  value[below] <- value[below] / 2
  if (any(!above)) return(mean(value[!above]))
  mean(value)
}

# name the substituted samples out of all observations used in the daily average
half_limit_flag <- function(count, total) {
  if (length(count) != length(total) || anyNA(count) || anyNA(total) ||
      any(!is.finite(count) | !is.finite(total)) ||
      any(count < 0 | total < count | count != floor(count) | total != floor(total))) {
    stop("daily substitution counts must be whole numbers within the observation total", call. = FALSE)
  }
  ifelse(count > 0L,
    paste0("half reported limit used for ", count, " of ", total,
      ifelse(total == 1L, " observation", " observations"), " in daily average"),
    NA_character_)
}

is_half_limit_flag <- function(flag) {
  matched <- !is.na(flag) & grepl(paste0(
    "^half reported limit used for [1-9][0-9]* of [1-9][0-9]* observations? in daily average",
    "(; estimated value)?$"), flag)
  count <- suppressWarnings(as.numeric(sub(
    "^half reported limit used for ([0-9]+) of .*$", "\\1", flag[matched])))
  total <- suppressWarnings(as.numeric(sub(
    "^half reported limit used for [0-9]+ of ([0-9]+) .*$", "\\1", flag[matched])))
  matched[matched] <- is.finite(count) & is.finite(total) & count <= total
  matched
}

# accept counted substitution flags without listing every possible count
lab_flags_supported <- function(flags, allowed) {
  is.na(flags) | flags == "" | flags %in% allowed | is_half_limit_flag(flags)
}

# an estimate is a measurement, not a detection-limit threshold
interpret_gems_qualifiers <- function(qualifier, limit_is_clear) {
  qualifier <- dplyr::coalesce(trimws(as.character(qualifier)), "")
  if (any(!qualifier %in% c("", "<", ">", "~"))) {
    stop("a GEMS result has an unsupported qualifier", call. = FALSE)
  }
  lab_flag <- dplyr::case_when(
    qualifier == "~" ~ "estimated value",
    !limit_is_clear & qualifier == "<" ~ "provider < flag; meaning not confirmed",
    TRUE ~ NA_character_
  )
  qualifier[qualifier == "~" | (!limit_is_clear & qualifier == "<")] <- ""
  list(qualifier = qualifier, lab_flag = lab_flag)
}

# keep the estimate note alongside the daily substitution count
add_estimated_lab_flag <- function(flag, estimated) {
  dplyr::case_when(
    estimated & !is.na(flag) ~ paste0(flag, "; estimated value"),
    estimated ~ "estimated value",
    TRUE ~ flag
  )
}

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
# describe substitutions, observation totals, and estimates in one daily flag
daily_lab_flag <- function(qualifier, source) {
  estimated <- any(grepl("(^|; )estimated value$", source))
  source <- sub("; estimated value$", "", source)
  source[source == "estimated value"] <- NA_character_
  below <- !is.na(qualifier) & qualifier == "<"
  above <- !is.na(qualifier) & qualifier == ">"
  if (any(below) && any(above)) {
    stop("less-than and greater-than results share a daily source key", call. = FALSE)
  }
  # prepared estimates carry their original counts and need no new substitution
  prepared <- is_half_limit_flag(source)
  if (any(prepared) && any(above)) {
    stop("source and daily lab flags conflict", call. = FALSE)
  }
  count <- sum(below) + sum(as.numeric(sub(
    "^half reported limit used for ([0-9]+) of .*$", "\\1", source[prepared])))
  total <- sum(!above & !prepared) + sum(as.numeric(sub(
    "^half reported limit used for [0-9]+ of ([0-9]+) .*$", "\\1", source[prepared])))
  flag <- if (count > 0L) {
    half_limit_flag(count, total)
  } else if (any(above)) {
    if (all(above)) "above reported limit" else "above-limit results excluded from daily mean"
  } else {
    NA_character_
  }
  source_flag <- reduce_daily_flag(source[!prepared], "source lab")
  if (!is.na(flag) && !is.na(source_flag) && flag != source_flag) {
    stop("source and daily lab flags conflict", call. = FALSE)
  }
  add_estimated_lab_flag(dplyr::coalesce(flag, source_flag), estimated)
}

# carry site and sample review notes into the final rows
add_site_and_sample_flags <- function(data, site_reference) {
  spatial_note <- "spatial data not yet available"
  site_flags <- site_reference |>
    dplyr::transmute(
      LTER, Stream_Name,
      Site_Flag = dplyr::na_if(trimws(as.character(Flag)), ""),
      needs_spatial_note = !is.na(Include_in_Master_Chem) &
        Include_in_Master_Chem == "Yes" &
        !is.na(Has_Spatial_Data) & Has_Spatial_Data == "No"
    ) |>
    dplyr::mutate(
      has_spatial_note = !is.na(Site_Flag) & stringr::str_detect(
        stringr::str_to_lower(Site_Flag),
        stringr::fixed(spatial_note)
      ),
      Site_Flag = dplyr::case_when(
        needs_spatial_note & !has_spatial_note & is.na(Site_Flag) ~ spatial_note,
        needs_spatial_note & !has_spatial_note ~ paste(
          Site_Flag,
          spatial_note,
          sep = "; "
        ),
        TRUE ~ Site_Flag
      )
    ) |>
    dplyr::select(-has_spatial_note) |>
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
    )
  stop_if(
    any(
      data$needs_spatial_note &
        (
          is.na(data$Site_Flag) |
            !stringr::str_detect(
              stringr::str_to_lower(data$Site_Flag),
              stringr::fixed(spatial_note)
            )
        )
    ),
    "a site without spatial data is missing its site flag"
  )
  data <- data |>
    dplyr::select(-needs_spatial_note) |>
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

# report related count differences together before changing or exporting data
check_reviewed_counts <- function(actual, expected, labels) {
  stop_if(
    length(actual) != length(expected) || length(actual) != length(labels),
    "reviewed count checks have different lengths"
  )
  wrong <- is.na(actual) | is.na(expected) | actual != expected
  if (any(wrong)) {
    stop(
      paste(
        paste0(labels[wrong], ": expected ", expected[wrong], ", found ", actual[wrong]),
        collapse = "\n"
      ),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

# keep the preferred source only where two files report the same daily result
apply_preferred_source_rules <- function(data, rules, expected_removed) {
  daily_key <- c("LTER", "Stream_Name", "date", "variable", "units")
  required_columns <- c("Raw_Filename", daily_key)
  stop_if(
    any(!required_columns %in% names(data)),
    "source selection is missing a required chemistry column"
  )
  stop_if(
    any(!c(
      "preferred_file", "superseded_file", "variables", "expected_overlap"
    ) %in% names(rules)),
    "the preferred-source rules are missing a required column"
  )

  # find each source file once so every rule works with only the rows it needs
  rows_by_file <- split(seq_len(nrow(data)), data$Raw_Filename, drop = TRUE)
  removal_keys <- vector("list", nrow(rules))
  observed_overlap <- integer(nrow(rules))
  for (row in seq_len(nrow(rules))) {
    preferred_index <- rows_by_file[[rules$preferred_file[[row]]]]
    superseded_index <- rows_by_file[[rules$superseded_file[[row]]]]
    if (is.null(preferred_index)) {
      preferred_index <- integer()
    }
    if (is.null(superseded_index)) {
      superseded_index <- integer()
    }

    variable_text <- rules$variables[[row]]
    if (!is.na(variable_text)) {
      variables <- strsplit(variable_text, ";", fixed = TRUE)[[1]]
      preferred_index <- preferred_index[
        data$variable[preferred_index] %in% variables
      ]
      superseded_index <- superseded_index[
        data$variable[superseded_index] %in% variables
      ]
    }

    preferred_keys <- data[preferred_index, daily_key, drop = FALSE] |>
      dplyr::distinct()
    overlapping_rows <- data[
      superseded_index,
      c("Raw_Filename", daily_key),
      drop = FALSE
    ] |>
      dplyr::semi_join(preferred_keys, by = daily_key)
    observed_overlap[row] <- nrow(overlapping_rows)
    removal_keys[[row]] <- overlapping_rows
  }

  check_reviewed_counts(
    observed_overlap,
    rules$expected_overlap,
    paste("source overlap:", rules$preferred_file, "and", rules$superseded_file)
  )

  removal_keys <- dplyr::bind_rows(removal_keys) |>
    dplyr::distinct()
  check_reviewed_count(
    nrow(removal_keys),
    expected_removed,
    "the reviewed superseded-row count changed"
  )
  rows_before <- nrow(data)
  data <- dplyr::anti_join(
    data,
    removal_keys,
    by = c("Raw_Filename", daily_key)
  )
  check_reviewed_count(
    rows_before - nrow(data),
    expected_removed,
    "the source-selection removal count changed"
  )
  data
}

# ---- prepare each source ----
harmonize_source <- function(file_name, data_key, units_key, run_input_dir, source_headers,
    keep_repeats = TRUE) {
  mean_measurement <- function(values) {
    mean_qualified_measurement(values, unique(c(data_key$NA_indicator, fixed_missing_tokens)))
  }
  key_rows <- data_key |>
    dplyr::filter(
      Raw_Filename == file_name,
      !Standardized_Column_Name %in% c("Dataset", "Raw_Filename"),
      !is.na(Standardized_Column_Name),
      Standardized_Column_Name != ""
    )
  raw_data <- read_selected_source(
    run_input_dir, file_name, key_rows$Raw_Column_Name, source_headers
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
    )
  if (!keep_repeats) {
    long_rows <- collapse_repeated_amounts(long_rows, data_key$NA_indicator, mean_measurement)
  }
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
  wide_rows <- joined_units |>
    dplyr::mutate(
      standard_name = dplyr::if_else(
        is.na(Units) | Units == "",
        solute,
        paste0(solute, "_", Units)
      )
    ) |>
    dplyr::select(-solute, -solute_units, -Units, -unit_count)
  sample_keys <- setdiff(names(wide_rows), "amount")
  has_repeats <- anyDuplicated(wide_rows[sample_keys]) > 0L
  if (keep_repeats) {
    # keep individual samples available for days using half-limit substitution
    wide_rows <- wide_rows |>
      dplyr::group_by(dplyr::across(dplyr::all_of(sample_keys))) |>
      dplyr::mutate(sample_row = dplyr::row_number()) |>
      dplyr::ungroup()
  } else {
    wide_rows <- collapse_repeated_cells(wide_rows, mean_measurement)
  }
  result <- tidyr::pivot_wider(
    wide_rows,
    names_from = standard_name,
    values_from = amount
  ) |>
    dplyr::select(-dplyr::any_of("sample_row"))
  attr(result, "has_repeated_samples") <- has_repeats
  result
}

# preserve previous grouping on unaffected days after site names and dates match
select_daily_inputs <- function(data) {
  data |>
    dplyr::group_by(Dataset, Raw_Filename, LTER, Stream_Name, date, variable, units) |>
    dplyr::mutate(
      use_samples = any(value_qualifier == "<", na.rm = TRUE) |
        any(is_half_limit_flag(source_lab_flag)) |
        !any(averaging_input == "existing"),
      keep_input = (use_samples & averaging_input == "samples") |
        (!use_samples & averaging_input == "existing")
    ) |>
    dplyr::filter(keep_input) |>
    dplyr::ungroup() |>
    dplyr::select(-averaging_input, -use_samples, -keep_input)
}
# ---- qualifiers and units ----
# put one source into the shared format before combining files
standardize_source_data <- function(chemistry_wide, data_key) {
  for (column in c("Dataset", "LTER", "Stream_Name", "date")) {
    if (!column %in% names(chemistry_wide)) {
      chemistry_wide[[column]] <- NA_character_
    }
  }
  if (!"source_lab_flag" %in% names(chemistry_wide)) {
    chemistry_wide$source_lab_flag <- NA_character_
  }
  if (!"source_method_flag" %in% names(chemistry_wide)) {
    chemistry_wide$source_method_flag <- NA_character_
  }
  chemistry_wide <- chemistry_wide |>
    dplyr::relocate(
      Dataset, Raw_Filename, LTER, Stream_Name,
      source_lab_flag, source_method_flag, date,
      .before = dplyr::everything()
    ) |>
    dplyr::relocate(
      dplyr::any_of(unname(walker_lab_code_columns)),
      .before = date
    ) |>
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
    drop_empty_columns(c("source_lab_flag", "source_method_flag")) |>
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
  unparsed <- chemistry_long |>
    dplyr::filter(is.na(suppressWarnings(as.complex(
      stringr::str_remove(measurement, "^\\s*[<>]\\s*")
    ))))
  stop_if(
    nrow(unparsed) > 0L,
    paste(
      "unparsed chemistry values remain:",
      paste(unique(unparsed$measurement), collapse = ", ")
    )
  )
  # store the reported sign while units are converted
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
  # keep the known ph method when catalina's generic ph field is blank
  catalina_rows <- chemistry_wide$Raw_Filename ==
    "CatalinaJemez_chemistry_2009-2019_V2.csv"
  catalina_generic_ph <- catalina_rows &
    !is.na(first_present_value(chemistry_wide, "pH"))
  catalina_field_ph <- catalina_rows & !catalina_generic_ph &
    !is.na(first_present_value(chemistry_wide, "fldph"))
  catalina_lab_ph <- catalina_rows & !catalina_generic_ph &
    !catalina_field_ph &
    !is.na(first_present_value(chemistry_wide, "labph"))
  if (any(catalina_rows)) {
    check_reviewed_count(
      sum(catalina_field_ph),
      28L,
      "the reviewed Catalina field-pH fallback count changed"
    )
    check_reviewed_count(
      sum(catalina_lab_ph),
      75L,
      "the reviewed Catalina laboratory-pH fallback count changed"
    )
  }
  chemistry_wide$pH_method_flag <- dplyr::case_when(
    catalina_field_ph ~ "field pH",
    catalina_lab_ph ~ "laboratory pH fallback",
    TRUE ~ NA_character_
  )
  chemistry_wide <- chemistry_wide |>
    dplyr::relocate(pH_method_flag, .before = date)
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
  if (any(east_rows)) {
    stop_if(
      !any(east_rows & !is.na(chemistry_wide$nhx_uM)) ||
        any(east_rows & !is.na(chemistry_wide$nh4_uM)),
      "East River ammonia-N was not retained as combined ammonia"
    )
  }
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
  standardized <- chunked_pivot_longer(
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
    dplyr::mutate(
      source_method_flag = dplyr::case_when(
        Raw_Filename == "WalkerBranch_Chem.csv" &
          variable == "DSi" ~ "dissolved Si, 0.45 um filtered",
        variable == "pH" & !is.na(pH_method_flag) ~ pH_method_flag,
        TRUE ~ source_method_flag
      )
    )
  walker_v1 <- rep(FALSE, nrow(standardized))
  for (variable_name in names(walker_lab_code_columns)) {
    code_column <- walker_lab_code_columns[[variable_name]]
    if (code_column %in% names(standardized)) {
      walker_v1 <- walker_v1 |
        (
          standardized$Raw_Filename == "WalkerBranch_Chem.csv" &
            standardized$variable == variable_name &
            standardized[[code_column]] == "V1"
        )
    }
  }
  walker_v1[is.na(walker_v1)] <- FALSE
  stop_if(
    any(walker_v1 & !is.na(standardized$source_lab_flag)),
    "Walker Branch source lab flags conflict"
  )
  standardized$source_lab_flag[walker_v1] <-
    "reported value below historical detection limit"
  standardized |>
    dplyr::select(
      -raw_variable, -raw_name, -raw_units, -pH_method_flag,
      -dplyr::any_of(unname(walker_lab_code_columns))
    ) |>
    dplyr::relocate(value, value_qualifier, .after = dplyr::everything())
}

# ---- seine sample fractions ----
# compare si only with the saved official station, date, and reported value
# the older input converted sio2 to si using 28/60 and rounded to two decimals
review_seine_si <- function(data, site_reference, provider_file) {
  selected <- which(data$Raw_Filename == "Cleaned_Seine_Data.csv" & data$variable == "DSi")
  if (!length(selected)) return(data)
  provider <- readRDS(provider_file)
  stop_if(!is.null(provider[["next"]]) || nrow(provider$data) != provider$count,
    "the saved seine silica download is incomplete")
  sites <- site_reference |>
    dplyr::filter(LTER == "Seine") |>
    dplyr::transmute(Stream_Name,
      station = sprintf("%08d", as.integer(Original_Stream_Name)))
  stop_if(anyDuplicated(sites$station) > 0L,
    "seine station codes must identify one site each")
  lookup <- provider$data |>
    dplyr::filter(code_parametre == "1348", symbole_unite == "mg(SiO2)/L") |>
    dplyr::mutate(station = as.character(code_station),
      sample_date = as.Date(date_prelevement), si = round(resultat * 28 / 60, 2)) |>
    dplyr::group_by(station, sample_date, si) |>
    dplyr::summarise(filtered = any(code_fraction == "3"),
      unfiltered = any(code_fraction == "23"), .groups = "drop")
  # unmatched values and matches to both fractions are not called unfiltered
  target <- data[selected, , drop = FALSE] |>
    dplyr::transmute(Stream_Name, sample_date = date,
      si = round(value * atomic_mass[["Si"]] / 1000, 2)) |>
    dplyr::left_join(sites, by = "Stream_Name", relationship = "many-to-one") |>
    dplyr::left_join(lookup, by = c("station", "sample_date", "si"),
      relationship = "many-to-one")
  remove <- !is.na(target$unfiltered) & target$unfiltered & !target$filtered
  confirmed_filtered <- !is.na(target$filtered) & target$filtered & !target$unfiltered
  uncertain <- selected[!remove & !confirmed_filtered]
  old <- data$source_method_flag[uncertain]
  label <- "Si filtration not confirmed"
  data$source_method_flag[uncertain] <- ifelse(is.na(old) | old == "", label,
    paste(old, label, sep = "; "))
  keep <- rep(TRUE, nrow(data))
  keep[selected[remove]] <- FALSE
  data[keep, , drop = FALSE]
}

# ---- workflow inputs ----
# read the local keys and prepared chemistry files
load_chemistry_inputs <- function(root_dir, run_input_dir) {
  key_dir <- file.path(root_dir, "keys")
  output_dir <- file.path(root_dir, "tidy")
  dir.create(key_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(run_input_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  key_files <- file.path(
    key_dir,
    c(
      "SiSyn_Data_Key.csv",
      "SiSyn_Chem_Units_Key.csv",
      "Site_Reference_Table.csv",
      "Chemistry_detection_limits_and_methods.xlsx"
    )
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
  method_rules <- read_workflow_rules(key_files[[4]])
  chemistry_files <- sort(unique(trimws(as.character(data_key_raw$Raw_Filename))))
  chemistry_files <- chemistry_files[!is.na(chemistry_files) & chemistry_files != ""]
  check_file_list(
    sort(list.files(run_input_dir, pattern = "[.]csv$")),
    chemistry_files,
    "the local master chemistry inputs do not match the data key"
  )
  source_headers <- read_source_headers(run_input_dir, chemistry_files)
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
    method_rules = method_rules,
    chemistry_files = chemistry_files,
    source_headers = source_headers,
    output_dir = output_dir
  )
}
# apply source review choices without changing the source file
apply_source_review_rules <- function(raw_data, file_name) {
  # exclude canada extractable si because method f055 confirms no filtration
  if (file_name == canada_extractable_filename) {
    remove_rows <- raw_data$VARIABLE == canada_extractable_label
    remove_rows[is.na(remove_rows)] <- FALSE
    check_reviewed_count(
      sum(remove_rows),
      canada_extractable_count,
      "the reviewed Canada extractable, unfiltered silicon count changed"
    )
    raw_data <- raw_data[!remove_rows, , drop = FALSE]
  }
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
  raw_data
}

# ---- preparation helpers ----

# ---- paths ----

required_path <- function(name) {
  value <- trimws(Sys.getenv(name, unset = ""))
  if (identical(value, "")) {
    stop("set ", name, " before running this script")
  }
  value
}

normalize_path <- function(path) {
  normalizePath(path.expand(path), winslash = "/", mustWork = FALSE)
}

paths_overlap <- function(first, second) {
  first <- normalize_path(first)
  second <- normalize_path(second)
  identical(first, second) ||
    startsWith(first, paste0(second, "/")) ||
    startsWith(second, paste0(first, "/"))
}

# ---- reading source files ----

# read all source fields as text so qualifiers and station codes survive
# use a prepared file instead of the download only when its folder is supplied
read_source_csv <- function(filename, directory, clean_names = FALSE, preferred_dir = NULL) {
  path <- file.path(directory, filename)
  if (!is.null(preferred_dir)) {
    preferred_path <- file.path(preferred_dir, filename)
    if (file.exists(preferred_path)) {
      path <- preferred_path
    }
  }
  read.csv(
    path,
    check.names = clean_names,
    stringsAsFactors = FALSE,
    na.strings = "",
    colClasses = "character",
    comment.char = ""
  )
}

# ---- checking inputs and output folders ----

# report missing inputs before any preparation starts
check_required_files <- function(paths, label = "required input files") {
  missing <- paths[!file.exists(paths)]
  if (length(missing)) {
    stop(label, " are missing: ", paste(basename(missing), collapse = ", "))
  }
}

# confirm that original source downloads match the versions we checked
check_file_versions <- function(paths, expected, label) {
  check_required_files(paths)
  observed <- unname(tools::md5sum(paths))
  if (!identical(observed, expected)) {
    stop("the reviewed ", label, " changed")
  }
}

# check the destination without creating it or changing any input folder
check_work_folder <- function(output_dir, input_dirs, label) {
  for (input_dir in input_dirs) {
    if (paths_overlap(output_dir, input_dir)) {
      stop(label, " output must not overlap an input folder")
    }
  }
  if (file.exists(output_dir)) {
    stop("the ", label, " output folder already exists")
  }
}

# ---- saving prepared files ----

# write a working copy and confirm that reading it back preserves every value
write_prepared_csv <- function(filename, data, directory) {
  write.csv(data, file.path(directory, filename), row.names = FALSE, na = "")
  written <- read_source_csv(filename, directory)
  if (identical(data, written)) {
    return(invisible(NULL))
  }
  if (!identical(names(data), names(written)) || nrow(data) != nrow(written)) {
    stop("written row or column structure changed in '", filename, "'")
  }
  changed <- character()
  for (column in names(data)) {
    if (!identical(data[[column]], written[[column]])) {
      changed <- c(changed, column)
    }
  }
  stop("written values changed in '", filename, "' for columns: ",
    paste(changed, collapse = ", "))
}

# ---- checking and formatting values ----

is_blank <- function(value) {
  is.na(value) | trimws(as.character(value)) %in% c("", "NA")
}

assert_count <- function(observed, expected, label) {
  observed_count <- as.integer(observed)
  expected_count <- as.integer(expected)
  if (!identical(observed_count, expected_count)) {
    stop(label, " changed: expected ", expected, ", found ", observed)
  }
}

format_amount <- function(value, qualifier = NA_character_) {
  value_text <- sprintf("%.17g", value)
  use_qualifier <- !is.na(qualifier) & qualifier %in% c("<", ">")
  value_text[use_qualifier] <- paste0(
    qualifier[use_qualifier],
    value_text[use_qualifier]
  )
  value_text
}

# ---- workflow helpers ----

# check the supporting files before any chemistry preparation starts
preparation_files <- function(provider_dir, evidence_dir, preparation_dir) {
  scripts <- file.path(preparation_dir, c(
    "prepare_chemistry.R", "prepare_eccc_gems_danube.R",
    "prepare_qualifier_inputs.R", "prepare_other_sources.R", "prepare_usgs_legacy.R"
  ))
  evidence <- file.path(evidence_dir, c(
    "Chemistry_detection_limits_and_methods.xlsx",
    "usgs_05420500_tn_tds_20261009.csv", "usgs_tds_as_tn_exclusions.rds",
    "SiSyn_DataTemplate_Carey.xlsx", "yanase_chemistry_tables.json",
    "netherlands_rws_target_chemistry.rds"
  ))
  providers <- file.path(provider_dir, c(
    "canada_eccc/Water-Qual-Eau-Sites-National.csv",
    "canada_eccc/Water-Qual-Eau-VariableInfo.csv",
    "gems_gemstat/GFQA_v3.zip", "danube_icpdr/danube_wq_v1.csv",
    "france_seine/seine_silica_provider_20261022.rds",
    "amazon_hybam/Manacapuru.csv",
    "cameroon_mtropics/Cameroon_MTROPICS_HYDROCHEMICAL_DATA_NYONG_20260915.zip",
    "australia_mdba/MDBA_RMWQMP_physico-chemical-data_20260914.csv",
    "usa_arc/1988-2010_Kling_Akchem.csv",
    "usa_arc/legacy_audit_ARC_2002-2024_Kling_AON_Imnavait_Chemistry.csv",
    "usa_arc/legacy_audit_ARC_2012-2020_Kling_Akchem.02.csv",
    "usa_arc/1988-2010_Kling_Akchem_eml.xml"
  ))
  canada <- sort(list.files(file.path(provider_dir, "canada_eccc"),
    pattern = "2000-present[.]csv$", full.names = TRUE))
  usgs <- sort(list.files(file.path(provider_dir, "usa_usgs_samples", "responses"),
    pattern = "^samples_sites.*_pcodes(06|16).*\\.csv$", full.names = TRUE))
  problems <- character()
  missing <- c(scripts, evidence, providers)
  missing <- missing[!file.exists(missing)]
  if (length(missing)) problems <- c(problems, paste("missing:", missing))
  if (length(canada) != 17L) {
    problems <- c(problems, paste("expected 17 Canada data files; found", length(canada)))
  }
  if (length(usgs) != 40L) {
    problems <- c(problems, paste("expected 40 USGS provider files; found", length(usgs)))
  }
  if (length(problems)) stop(paste(problems, collapse = "\n"), call. = FALSE)
  list(provider = c(providers, canada, usgs), evidence = evidence)
}

check_chemistry_setup <- function(provider_dir, evidence_dir, preparation_dir) {
  files <- preparation_files(provider_dir, evidence_dir, preparation_dir)
  packages <- c("librarian", "purrr", "readxl", "stringr", "tidyverse",
    "data.table", "dplyr", "jsonlite")
  missing <- packages[!vapply(packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing)) {
    stop("install these R packages before running: ", paste(missing, collapse = ", "),
      call. = FALSE)
  }
  if (!nzchar(Sys.which("unzip"))) {
    stop("the unzip command is needed to read the provider archives", call. = FALSE)
  }
  method_file <- file.path(evidence_dir, "Chemistry_detection_limits_and_methods.xlsx")
  sheets <- readxl::excel_sheets(method_file)
  if (!all(c("MDL_methods", "Workflow rules") %in% sheets)) {
    stop("download the complete lab and method workbook, not a single tab", call. = FALSE)
  }
  provider_archives <- files$provider[grepl("[.]zip$", files$provider)]
  for (archive in provider_archives) {
    contents <- tryCatch(utils::unzip(archive, list = TRUE), error = function(error) NULL)
    if (is.null(contents) || nrow(contents) == 0L) {
      stop("the provider archive cannot be read: ", basename(archive), call. = FALSE)
    }
  }

  # check the original source versions required by the preparation scripts
  paths <- c(
    file.path(provider_dir, "cameroon_mtropics",
      "Cameroon_MTROPICS_HYDROCHEMICAL_DATA_NYONG_20260915.zip"),
    file.path(provider_dir, "australia_mdba",
      "MDBA_RMWQMP_physico-chemical-data_20260914.csv"),
    file.path(evidence_dir, "yanase_chemistry_tables.json"),
    file.path(evidence_dir, "netherlands_rws_target_chemistry.rds"),
    file.path(provider_dir, "usa_arc", c("1988-2010_Kling_Akchem.csv",
      "legacy_audit_ARC_2002-2024_Kling_AON_Imnavait_Chemistry.csv",
      "legacy_audit_ARC_2012-2020_Kling_Akchem.02.csv",
      "1988-2010_Kling_Akchem_eml.xml"))
  )
  hashes <- c("8b9238192be6fca67e70bbb3cc697168", "5832d9fb5276ed53e75a84c6e0f4bf89",
    "69c4167a601ebd77cc890a79e3a13251", "dd92c6ab6756479e569bb11312185ac4",
    "a3db20ce1bb2a460a7ee9544a7d87980", "9fcb805438e69755aa436d52d6a48d30",
    "0d3f435c4154ac18d333732bd97f30dd", "774af538a9b09bcb4ec50900584dd6d3")
  changed <- unname(tools::md5sum(paths)) != hashes
  if (any(changed)) {
    stop("reviewed provider files differ: ", paste(basename(paths[changed]), collapse = ", "),
      call. = FALSE)
  }
  invisible(files)
}

# save file names, sizes, and checksums so input changes can be detected
file_state <- function(directory) {
  files <- sort(list.files(
    directory,
    all.files = TRUE,
    full.names = TRUE,
    recursive = TRUE,
    include.dirs = FALSE,
    no.. = TRUE
  ))

  data.frame(
    file = substring(files, nchar(directory) + 2L),
    size = unname(file.info(files)$size),
    md5 = unname(tools::md5sum(files)),
    stringsAsFactors = FALSE
  )
}

# save the starting file checks for each input folder
record_input_states <- function(directories) {
  states <- vector("list", length(directories))
  names(states) <- names(directories)
  for (folder in seq_along(directories)) {
    states[[folder]] <- file_state(directories[[folder]])
    if (nrow(states[[folder]]) == 0L) {
      stop("one or more workflow input folders contain no files", call. = FALSE)
    }
  }
  states
}

# compare the current inputs with the checks saved before preparation
check_input_files <- function(states, directories, context = "the workflow") {
  changed <- logical(length(directories))
  names(changed) <- names(directories)
  for (folder in seq_along(directories)) {
    current <- file_state(directories[[folder]])
    changed[folder] <- !identical(states[[folder]], current)
  }
  if (any(changed)) {
    stop(
      "input folders changed during ",
      context,
      ": ",
      paste(names(changed)[changed], collapse = ", "),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

# save date and method notes in one readable csv without repeating chemistry rows
write_chemistry_metadata <- function(metadata, file) {
  names(metadata) <- c("date", "method")
  notes <- dplyr::bind_rows(metadata, .id = "Note_Type")
  notes <- as.data.frame(notes, stringsAsFactors = FALSE)
  notes[] <- lapply(notes, as.character)
  write.csv(notes, file, row.names = FALSE, na = "")
  saved <- read.csv(file, colClasses = "character", na.strings = "",
    check.names = FALSE)
  if (!identical(saved, notes)) {
    stop("the saved metadata changed", call. = FALSE)
  }
  invisible(notes)
}

# run each preparation script in a separate r session
# keep its full log if it fails, without filling the rstudio console
run_r_script <- function(
  script,
  script_args = character(),
  settings = character(),
  log_prefix = "chemistry-step-"
) {
  setting_names <- names(settings)
  if (length(settings) > 0L) {
    if (is.null(setting_names) || any(setting_names == "")) {
      stop("script settings must be named", call. = FALSE)
    }

    old_values <- Sys.getenv(setting_names, unset = NA_character_)
    do.call(Sys.setenv, as.list(settings))
    on.exit({
      missing_before <- is.na(old_values)
      if (any(missing_before)) {
        Sys.unsetenv(setting_names[missing_before])
      }
      if (any(!missing_before)) {
        do.call(
          Sys.setenv,
          as.list(stats::setNames(
            old_values[!missing_before],
            setting_names[!missing_before]
          ))
        )
      }
    }, add = TRUE)
  }

  log_dir <- Sys.getenv("SISYN_CHEM_LOG_DIR", unset = tempdir())
  if (!dir.exists(log_dir) && !dir.create(log_dir, recursive = TRUE)) {
    stop("the error-log folder could not be created: ", log_dir, call. = FALSE)
  }
  log_file <- tempfile(pattern = log_prefix, tmpdir = log_dir, fileext = ".log")
  # remove successful temporary logs but keep the full log if a script fails
  status <- suppressWarnings(system2(
    file.path(R.home("bin"), "Rscript"),
    args = c("--vanilla", shQuote(script), shQuote(script_args)),
    stdout = log_file,
    stderr = log_file
  ))
  if (status != 0L) {
    detail <- tail(readLines(log_file, warn = FALSE), 200L)
    stop(
      basename(script),
      " failed with exit status ",
      status,
      "\nfull log: ", log_file, ":\n",
      paste(detail, collapse = "\n"),
      call. = FALSE
    )
  }
  unlink(log_file)
  invisible(status)
}
