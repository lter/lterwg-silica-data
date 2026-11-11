# ---- recover missing bnz chemistry ----

# return new daily records and their method notes without changing existing data
bnz_source_additions <- function(master_bnz, site_reference, source_file) {
  columns <- c(
    "Dataset", "Raw_Filename", "LTER", "Stream_Name", "Site_Flag",
    "Sample_Flag", "date", "Date_Flag", "variable", "units", "value",
    "value_qualifier", "Lab_Flag", "Method_Flag"
  )
  daily_key <- c("LTER", "Stream_Name", "date", "variable", "units")
  targets <- c("C2", "C3", "C4", "CB", "CJ", "P6", "PC", "PJ")
  snapshot_md5 <- "3d68f34d70196423c7874a9bbde37788"
  raw_md5 <- "452a7778c28b5300a90a7d5a4d45ea40"
  stopifnot(
    identical(names(master_bnz), columns),
    all(master_bnz$LTER == "BNZ"),
    unname(tools::md5sum(source_file)) == snapshot_md5
  )
  original <- data.table::copy(data.table::as.data.table(master_bnz))
  original$date <- as.Date(original$date)
  stopifnot(
    !anyNA(original[, c("LTER", "Stream_Name", "date", "variable"), with = FALSE]),
    !anyDuplicated(original, by = daily_key)
  )

  # the saved source has already matched the current provider file byte for byte
  saved <- readRDS(source_file)
  stopifnot(
    saved$md5[["bnz"]] == raw_md5,
    nrow(saved$data$bnz) == 12593L
  )
  raw <- unique(data.table::as.data.table(saved$data$bnz))
  repeated_rows <- nrow(saved$data$bnz) - nrow(raw)
  stopifnot(repeated_rows == 68L)
  dates <- as.Date(raw[["Sample Date"]], format = "%m/%d/%Y")
  stopifnot(!anyNA(dates), all(targets %in% raw$Watershed))
  fields <- c(
    Ca = "Calcium(uM)", Mg = "Magnesium(uM)", Na = "Sodium(uM)",
    K = "Potassium(uM)", NO3 = "Nitrate(uM)", SO4 = "Sulfate(uM)",
    Cl = "Chloride(uM)", DSi = "Silica(uM)"
  )
  samples <- list()
  for (solute in names(fields)) {
    reported <- trimws(as.character(raw[[fields[[solute]]]]))
    values <- suppressWarnings(as.numeric(reported))
    missing <- is.na(reported) | reported %in% c("", "NA")
    stopifnot(all(missing | (is.finite(values) & values >= 0)))
    samples[[solute]] <- data.table::data.table(
      Dataset = "BNZ", Raw_Filename = saved$files[["bnz"]], LTER = "BNZ",
      Stream_Name = raw$Watershed, date = dates, variable = solute,
      units = "uM", value = values
    )
  }
  samples <- data.table::rbindlist(samples)
  samples <- samples[Stream_Name %in% targets & is.finite(value)]
  daily <- samples[, list(value = mean(value), observations = .N),
    by = c("Dataset", "Raw_Filename", daily_key)
  ]
  daily$Lab_Flag <- ifelse(daily$observations > 1L,
    paste("daily mean of", daily$observations, "source observations"), NA_character_
  )

  # existing nitrate or combined nitrate and nitrite already supply the nitrogen group
  existing_n <- original[variable %in% c("NO3", "NOx")]
  nitrogen_days <- paste(existing_n$Stream_Name, existing_n$date, sep = "\r")
  new_days <- paste(daily$Stream_Name, daily$date, sep = "\r")
  daily <- daily[variable != "NO3" | !new_days %in% nitrogen_days]
  daily <- daily[!original[, daily_key, with = FALSE], on = daily_key]

  # only the documented summer site-years receive a filtered-si classification
  uncertain_flag <- "Si sample fraction not documented"
  filtered_flag <- "filtered Si, BNZ stream protocol"
  supported <- daily$variable == "DSi" &
    daily$Stream_Name %in% c("C2", "C4", "P6") &
    format(daily$date, "%Y") %in% as.character(2002:2007) &
    format(daily$date, "%m") %in% sprintf("%02d", 6:9)
  daily$Method_Flag <- NA_character_
  daily$Method_Flag[daily$variable == "DSi"] <- uncertain_flag
  daily$Method_Flag[supported] <- filtered_flag
  daily$Method_Note <- NA_character_
  daily$Method_Note[daily$Method_Flag %in% uncertain_flag] <- paste(
    "BNZ:152 reports Si as Silica(uM). The sample fraction is undocumented for",
    "these added site-date records; values are retained with sample-fraction uncertainty.",
    saved$references[["bnz"]]
  )
  daily$Method_Note[supported] <- paste(
    "BNZ:152 reports Si as Silica(uM). For C2, C4 and P6 during June-September",
    "2002-2007, Betts and Jones (2009) documents filtration within 12 hours",
    "using Gelman A/E glass-fiber filters.",
    "https://www.fs.usda.gov/pnw/pubs/journals/pnw_2009_betts001.pdf",
    saved$references[["bnz"]]
  )
  note_key <- c(
    "Dataset", "Raw_Filename", "LTER", "Stream_Name", "variable",
    "Method_Flag"
  )
  method_notes <- unique(daily[variable == "DSi",
    c(note_key, "Method_Note"),
    with = FALSE
  ])
  stopifnot(
    !anyDuplicated(method_notes, by = note_key),
    all(!is.na(daily$Method_Flag[daily$variable == "DSi"])),
    all(!is.na(daily$Method_Note[daily$variable == "DSi"]))
  )

  # apply the user's inclusion rule to a copy of the site reference
  reference <- data.table::copy(as.data.frame(site_reference))
  site_rows <- which(reference$LTER == "BNZ" & reference$Stream_Name %in% targets)
  stopifnot(
    length(site_rows) == length(targets),
    !anyDuplicated(reference$Stream_Name[site_rows])
  )
  restored <- which(reference$LTER == "BNZ" &
    reference$Stream_Name %in% c("CB", "PC", "PJ"))
  reference$Include_in_Master_Chem[restored] <- "Yes"
  for (row in restored) {
    flag <- reference$Flag[row]
    reference$Flag[row] <- if (is.na(flag) || !nzchar(trimws(flag))) uncertain_flag else if (grepl(uncertain_flag, flag, fixed = TRUE)) flag else
      paste(flag, uncertain_flag, sep = "; ")
    if ("CQ_Notes" %in% names(reference)) reference$CQ_Notes[row] <- sub(
      "The site is excluded until the Si fraction can be confirmed.",
      "Silicon is retained with an explicit sample-fraction uncertainty flag.",
      reference$CQ_Notes[row],
      fixed = TRUE
    )
  }
  additions <- as.data.frame(master_bnz)[rep(NA_integer_, nrow(daily)), , drop = FALSE]
  for (field in intersect(names(daily), columns)) additions[[field]] <- daily[[field]]
  additions$Site_Flag <- NULL
  additions$Sample_Flag <- NULL
  rownames(additions) <- NULL
  additions <- add_site_and_sample_flags(additions, reference)
  additions <- additions[columns]
  stopifnot(
    identical(names(additions), columns),
    !anyDuplicated(additions[daily_key]), !anyNA(additions$date),
    all(additions$units == "uM"), all(is.finite(additions$value)),
    all(additions$value >= 0), all(additions$Stream_Name %in% targets),
    nrow(data.table::as.data.table(additions)[original, on = daily_key, nomatch = 0]) == 0L,
    unname(tools::md5sum(source_file)) == snapshot_md5
  )
  counts <- as.data.frame(daily[, list(rows = .N, dates = data.table::uniqueN(date)),
    by = c("LTER", "Stream_Name", "variable", "Method_Flag")
  ])
  list(
    data = additions, method_notes = as.data.frame(method_notes), counts = counts,
    source_check = list(
      passed = TRUE, source_file = source_file,
      source_md5 = snapshot_md5, raw_filename = saved$files[["bnz"]], raw_md5 = raw_md5,
      source_reference = saved$references[["bnz"]], source_rows = nrow(saved$data$bnz),
      exact_duplicate_rows_removed = repeated_rows, targets = targets,
      existing_rows_replaced = 0L, nitrogen_forms_preserved = TRUE
    )
  )
}
