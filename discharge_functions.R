# ---- discharge preparation ----

# limit discharge requirements to the sites in the finalized chemistry file
discharge_site_reference <- function(reference, chemistry_file) {
  if (!file.exists(chemistry_file)) stop("choose the finalized master chemistry csv")
  sites <- unique(as.data.frame(data.table::fread(chemistry_file,
    select = c("LTER", "Stream_Name"), showProgress = FALSE)))
  reference <- as.data.frame(reference)
  key <- function(data) paste(data$LTER, data$Stream_Name, sep = "\r")
  if (anyNA(sites) || anyDuplicated(key(reference))) {
    stop("check missing chemistry site names or repeated reference-table sites")
  }
  rows <- match(key(sites), key(reference))
  if (anyNA(rows)) stop("a finalized chemistry site is missing from the reference table")
  reference <- reference[rows, , drop = FALSE]
  # original usgs station names and pie metadata identify these three wrong assignments
  # pie 114.5 identifies ysi-ir as north reading, not ir-97
  pie_moves <- data.frame(
    file = c("PIE_Parker_Q", "PIE_IpswichMid_Q", "PIE_YSIIR_Q"),
    old_site = c("Sawmill Brook", "Ipswich at North Reading", "IR-97"),
    site = c("Parker River at Byfield, MA", "Ipswich at Middleton", "Ipswich at North Reading"),
    url = c("https://waterdata.usgs.gov/monitoring-location/USGS-01101000/",
      "https://waterdata.usgs.gov/monitoring-location/USGS-01101500/",
      "https://portal.edirepository.org/nis/mapbrowse?packageid=knb-lter-pie.114.5")
  )
  for (i in seq_len(nrow(pie_moves))) {
    assigned <- which(reference$Discharge_File_Name %in% pie_moves$file[i])
    target <- which(reference$LTER == "PIE" & reference$Stream_Name == pie_moves$site[i])
    if (length(assigned) != 1L || length(target) != 1L ||
        reference$LTER[assigned] != "PIE" ||
        !reference$Stream_Name[assigned] %in% c(pie_moves$old_site[i], pie_moves$site[i])) {
      stop("check the PIE discharge assignment for ", pie_moves$file[i])
    }
    if (assigned != target) {
      if (!is.na(reference$Discharge_File_Name[target]) &&
          nzchar(trimws(reference$Discharge_File_Name[target]))) {
        stop("the corrected PIE site already has a different discharge file")
      }
      reference$Discharge_File_Name[assigned] <- NA_character_
      reference$Units[assigned] <- NA_character_
      reference$q_source_URL[assigned] <- NA_character_
      reference$Discharge_File_Name[target] <- pie_moves$file[i]
    }
    reference$Units[target] <- "cms"
    reference$q_source_URL[target] <- pie_moves$url[i]
  }
  # these saved pie daily means already include the original cfs or l/s conversion
  # usgs comparisons: 731 dates per file at 01101000, 01101344, 01101400, 01101500, 01102000
  # pie 111.5 and 114.5: 168 cart creek dates and all 122 ysi-ir dates match l/s divided by 1000
  pie_converted <- c("PIE_Parker_Q", "PIE_USGS01101344_Q", "PIE_USGS01101400_Q",
    "PIE_IpswichMid_Q", "PIE_IpswichIps_Q", "PIE_YSICC_Q", "PIE_YSIIR_Q")
  pie_rows <- reference$Discharge_File_Name %in% pie_converted
  if (any(!reference$Units[pie_rows] %in% c("cms", "cfs", "Ls"))) {
    stop("check an unexpected unit for the source-checked PIE files")
  }
  reference$Units[pie_rows] <- "cms"
  # these four saved files already contain qcms, not unconverted source values
  converted <- c("PIE_USGS01101300_Q", "PIE_USGS01100890_Q", "ARC_NE14_Outlet_Q",
    "ColoradoAlpine_andrewscreek_Q")
  blank <- is.na(reference$Units) | !nzchar(trimws(reference$Units))
  reference$Units[reference$Discharge_File_Name %in% converted & blank] <- "cms"
  reference
}

# preserve fractional seconds in the timestamp used to distinguish readings
discharge_clock_time <- function(data) {
  clock <- lapply(c("HH", "MM", "SS"), function(column) {
    suppressWarnings(as.numeric(data[[column]]))
  })
  if (any(!is.finite(unlist(clock)))) stop("missing or nonnumeric hour, minute, or second fields")
  if (any(clock[[1]] < 0 | clock[[1]] >= 24 | clock[[1]] != trunc(clock[[1]])) ||
      any(clock[[2]] < 0 | clock[[2]] >= 60 | clock[[2]] != trunc(clock[[2]])) ||
      any(clock[[3]] < 0 | clock[[3]] >= 60)) {
    stop("invalid hour, minute, or second fields")
  }
  # seventeen significant digits retain the numeric precision read from the file
  seconds <- trimws(formatC(clock[[3]], digits = 17, format = "fg", flag = "#"))
  seconds[clock[[3]] < 10] <- paste0("0", seconds[clock[[3]] < 10])
  paste0(sprintf("%02d:%02d:", as.integer(clock[[1]]), as.integer(clock[[2]])), seconds)
}

# keep source files unchanged and prepare one daily series per discharge file
prepare_daily_discharge <- function(data, file_name, reference, flow_names, date_names) {
  source_units <- attr(data, "discharge_units")
  reference <- as.data.frame(reference)
  mappings <- reference[reference$Discharge_File_Name == file_name &
    !is.na(reference$Discharge_File_Name), , drop = FALSE]
  # quiock's saved values are m3/hour despite its m3/s export heading
  # dividing by 3600 matched all 515 checked portal timestamps in 2015, 2018, and 2025
  # keep cmh in the reference table and use the conversion above
  flow_column <- intersect(flow_names, names(data))
  date_column <- intersect(date_names, names(data))
  if (length(flow_column) != 1L || length(date_column) != 1L) {
    stop(file_name, " needs one recognized flow column and one date column")
  }
  units <- unique(trimws(as.character(mappings$Units)))
  units <- units[!is.na(units) & nzchar(units)]
  # a newly retrieved original series keeps its documented reporting units
  if (!is.null(source_units)) units <- source_units
  # qcms is already m3/s in the saved pie, arctic, and andrews creek inputs
  if (flow_column %in% c("Qcms", "Q_cms")) {
    if (length(units) && any(units != "cms")) {
      stop(file_name, " has a converted Qcms column but conflicting reference-table units")
    }
    units <- "cms"
  }
  factors <- c(cms = 1, cfs = 0.0283, Ls = 0.001, cmh = 1 / 3600, cmd = 1.15741e-5)
  if (length(units) != 1L || !units %in% names(factors)) {
    stop(file_name, " needs one supported discharge unit in the reference table")
  }

  # blank rows contain no discharge information and do not need an inferred date
  date_text <- trimws(as.character(data[[date_column]]))
  flow_text <- trimws(as.character(data[[flow_column]]))
  empty_date <- is.na(date_text) | date_text %in% c("", "NA")
  empty_flow <- is.na(flow_text) | flow_text %in% c("", "NA", "NaN")
  data <- as.data.frame(data)[!(empty_date & empty_flow), , drop = FALSE]
  if (!nrow(data)) stop(file_name, " contains no dated discharge rows")

  # retain the time before reducing the data to daily means
  date_text <- trimws(as.character(data[[date_column]]))
  iso_date <- grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}", date_text)
  dates <- as.Date(rep(NA_character_, length(date_text)))
  dates[iso_date] <- as.Date(substr(date_text[iso_date], 1L, 10L), "%Y-%m-%d")
  dates[!iso_date] <- as.Date(date_text[!iso_date], "%m/%d/%Y")
  if (anyNA(dates)) stop(file_name, " contains missing or unrecognized dates")
  times <- rep("", nrow(data))
  if ("Time" %in% names(data)) {
    times <- trimws(as.character(data$Time))
  } else if (all(c("HH", "MM", "SS") %in% names(data))) {
    times <- discharge_clock_time(data)
  } else {
    has_time <- grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T].+", date_text)
    times[has_time] <- substring(date_text[has_time], 12L)
  }
  # a blank time is normal for daily data, but must not hide conflicting daily values
  times[is.na(times)] <- ""

  original_flow <- trimws(as.character(data[[flow_column]]))
  flow <- suppressWarnings(as.numeric(original_flow))
  missing_flow <- is.na(original_flow) | original_flow %in% c("", "NA", "NaN")
  if (any(is.na(flow) & !missing_flow)) {
    stop(file_name, " contains flow values that need a source-specific interpretation")
  }
  notes <- rep("", nrow(data))
  if ("Q_Flag" %in% names(data)) notes <- as.character(data$Q_Flag)
  notes[is.na(notes)] <- ""
  if ("indicate" %in% names(data)) {
    notes[tolower(as.character(data$indicate)) %in% "interpolated"] <- "includes interpolated discharge"
  }
  input <- data.table::data.table(Date = dates, Sample_Time = times, Q = flow, source_note = notes)
  original_counts <- input[, .(input_rows = .N), by = Date]

  # equal flows at different times are separate observations, not duplicates
  repeated_counts <- input[, .(duplicate_rows = .N - nrow(unique(.SD))), by = Date]
  input <- unique(input)
  # a missing value beside a measured value is not a conflicting measurement
  input <- input[, if (any(is.finite(Q))) .SD[is.finite(Q)] else .SD,
    by = .(Date, Sample_Time)]
  conflicts <- input[, .(values = data.table::uniqueN(Q[!is.na(Q)])), by = .(Date, Sample_Time)]
  if (any(conflicts$values > 1L)) {
    stop(file_name, " has conflicting flows at the same time, or multiple untimed daily values")
  }
  mixed_times <- input[, .(mixed = any(Sample_Time == "") & any(Sample_Time != "")), by = Date]
  if (any(mixed_times$mixed)) stop(file_name, " mixes daily and timed observations on the same date")

  # retain the existing negative-to-missing rule and keep genuine zeros
  input[, Qcms := Q * unname(factors[[units]])]
  input[!is.finite(Qcms) | Qcms < 0, Qcms := NA_real_]
  daily <- input[, .(
    Qcms = if (all(is.na(Qcms))) NA_real_ else mean(Qcms, na.rm = TRUE),
    readings = .N,
    timed = any(Sample_Time != ""),
    source_note = paste(unique(source_note[nzchar(source_note)]), collapse = "; ")
  ), by = Date]
  daily <- merge(daily, original_counts, by = "Date", sort = FALSE)
  daily <- merge(daily, repeated_counts, by = "Date", sort = FALSE)
  daily[, Q_Flag := ifelse(timed, "daily mean of timed observations", "")]
  daily[, Q_Flag := ifelse(nzchar(source_note),
    ifelse(nzchar(Q_Flag), paste(Q_Flag, source_note, sep = "; "), source_note), Q_Flag)]
  repeated <- daily$duplicate_rows > 0L
  daily$Q_Flag[repeated] <- ifelse(nzchar(daily$Q_Flag[repeated]),
    paste(daily$Q_Flag[repeated], "duplicate input rows removed", sep = "; "),
    "duplicate input rows removed")

  daily[, Discharge_File_Name := file_name]
  daily[, c("Qcms", "Date", "Discharge_File_Name", "Q_Flag"), with = FALSE]
}

# ---- original-source checks ----

# reuse downloads within this run, without saving over the input files
discharge_source_text <- function(url, cache) {
  if (!exists(url, envir = cache, inherits = FALSE)) {
    response <- httr::GET(url, httr::timeout(60))
    httr::stop_for_status(response)
    text <- httr::content(response, as = "text", encoding = "UTF-8")
    if (grepl("<!doctype html|<html", substr(text, 1L, 200L), ignore.case = TRUE)) {
      stop("the original discharge source returned a web page instead of data: ", url)
    }
    assign(url, text, envir = cache)
  }
  get(url, envir = cache, inherits = FALSE)
}

# restore only the inputs whose saved columns lost their original meaning or timing
restore_original_discharge <- function(data, file_name, reference, cache, shingle_source_file = NULL) {
  checked <- c("SHINGLE_CREEK_Q", "LUQ_Q1_Q_fill", "LUQ_Q2_Q_fill", "LUQ_Q3_Q_fill", "Site7_Q")
  if (!file_name %in% checked) return(data)
  data <- as.data.frame(data)
  dates <- as.Date(data$Date)
  if (anyNA(dates)) stop(file_name, " needs valid dates before the original-source check")

  if (file_name == "SHINGLE_CREEK_Q") {
    # read the saved discharge-only download instead of fetching a changing series
    # usgs parameter 00060 is discharge in ft3/s and statistic 00003 is the daily mean
    if (is.null(shingle_source_file) || !file.exists(shingle_source_file)) {
      stop("set shingle_source_file to the saved USGS discharge-only json download")
    }
    series <- jsonlite::fromJSON(shingle_source_file, simplifyVector = FALSE)$value$timeSeries
    if (length(series) != 1L || series[[1]]$sourceInfo$siteCode[[1]]$value != "05288705" ||
        series[[1]]$variable$variableCode[[1]]$value != "00060" ||
        series[[1]]$variable$options$option[[1]]$optionCode != "00003") {
      stop("the shingle creek download must contain only daily mean discharge")
    }
    values <- series[[1]]$values[[1]]$value
    result <- data.frame(
      Date = as.Date(vapply(values, function(x) substr(x$dateTime, 1L, 10L), character(1))),
      Discharge = as.numeric(vapply(values, function(x) x$value, character(1))),
      Q_Flag = "USGS daily discharge restored"
    )
    estimated <- vapply(values, function(x) "e" %in% unlist(x$qualifiers), logical(1))
    result$Q_Flag[estimated] <- paste(result$Q_Flag[estimated], "estimated discharge", sep = "; ")
    provisional <- vapply(values, function(x) "P" %in% unlist(x$qualifiers), logical(1))
    result$Q_Flag[provisional] <- paste(result$Q_Flag[provisional], "provisional discharge", sep = "; ")
    if (anyDuplicated(result$Date) || !nrow(result)) stop("check the shingle creek daily source records")
    # keep dates missing from the original download visible rather than substituting temperature
    result <- merge(data.frame(Date = sort(unique(dates))), result, by = "Date", all.x = TRUE)
    result$Q_Flag[is.na(result$Discharge)] <- "discharge missing from USGS source"
    attr(result, "discharge_units") <- "cfs"
    return(result)
  }

  if (grepl("^LUQ_Q[123]_Q_fill$", file_name)) {
    # the archived daily series identifies the real daily value within each overlap
    url <- paste0("https://www.hydroshare.org/resource/8fb0c17c23da423b8240396342bde0eb/",
      "data/contents/Bis_Q_Day_87_to_10_final_1.csv")
    source <- data.table::fread(text = discharge_source_text(url, cache), showProgress = FALSE)
    source$Date <- as.Date(source$Date, "%m/%d/%Y")
    site <- sub("^LUQ_(Q[123])_Q_fill$", "\\1", file_name)
    # these are conversion areas, not the spatial areas in drainsqkm
    # the historical areas come from the runoff header and schaefer et al. (2000), table 1
    saved_area <- c(Q1 = 0.07, Q2 = 0.07, Q3 = 0.28)[[site]]
    runoff_area <- c(Q1 = 0.067, Q2 = 0.0634, Q3 = 0.35)[[site]]
    source_mm <- source[[paste0(site, "md")]]
    # first identify the old daily rows using the conversion already in the saved file
    expected <- round(source_mm, 3L) * saved_area / 86.4
    overlap <- unique(dates[duplicated(dates) | duplicated(dates, fromLast = TRUE)])
    keep <- rep(TRUE, nrow(data))
    data$Q_Flag <- ""
    for (day in as.character(overlap)) {
      rows <- which(dates == as.Date(day))
      value <- expected[source$Date == as.Date(day)]
      if (length(value) != 1L || !is.finite(value)) stop("the bisley source does not resolve ", day)
      difference <- abs(as.numeric(data$Discharge[rows]) - value)
      best <- which.min(difference)
      # the saved values have fewer digits than the original conversion
      if (!length(best) || difference[best] > 1e-6 * max(0.05, abs(value))) {
        stop("the bisley daily value does not match the original source on ", day)
      }
      keep[rows] <- FALSE
      keep[rows[best]] <- TRUE
      data$Q_Flag[rows[best]] <- if (length(unique(data$Discharge[rows])) > 1L)
        "original daily record retained; extra interpolated row removed" else
        "original daily record retained; duplicate row removed"
    }
    data <- data[keep, , drop = FALSE]
    dates <- as.Date(data$Date)
    historical_end <- as.Date("2006-06-02")
    historical <- dates >= min(source$Date) & dates <= historical_end
    mm <- source_mm[match(dates[historical], source$Date)]
    saved_values <- as.numeric(data$Discharge[historical])
    known <- is.finite(mm)
    expected <- mm * saved_area / 86.4
    # allow the original three-decimal mm/day rounding, including half-way rounding ties
    tolerance <- 0.00051 * saved_area / 86.4 + 1e-7 * pmax(0.05, abs(expected))
    if (any(!is.finite(saved_values[known])) ||
        any(abs(saved_values[known] - expected[known]) > tolerance[known])) {
      stop("a historical bisley value does not match the original mm/day source")
    }
    # verify the old gap estimates are interpolations between historical runoff records
    if (any(!known)) {
      gap_values <- approx(as.numeric(dates[historical][known]), saved_values[known],
        xout = as.numeric(dates[historical][!known]))$y
      if (any(!is.finite(gap_values)) || any(abs(gap_values - saved_values[!known]) >
          1e-6 * pmax(0.05, abs(gap_values)))) {
        stop("check the historical bisley gap estimates before changing their conversion")
      }
    }
    # correct the area used for these runoff-derived values, preserving their saved rounding
    old_left <- as.numeric(data$Discharge[dates == historical_end])
    factor <- runoff_area / saved_area
    data$Discharge[historical] <- saved_values * factor
    note <- paste0("historical mm/day conversion; area ", format(runoff_area, trim = TRUE), " km2")
    data$Q_Flag[historical] <- ifelse(nzchar(data$Q_Flag[historical]),
      paste(data$Q_Flag[historical], note, sep = "; "), note)
    gap_rows <- which(historical)[!known]
    data$Q_Flag[gap_rows] <- paste(data$Q_Flag[gap_rows], "interpolated historical runoff", sep = "; ")

    # the first complete daily cfs record is june 9 in the forest service source
    # leave that record and all later values alone
    cfs_start <- as.Date("2006-06-09")
    right <- as.numeric(data$Discharge[dates == cfs_start])
    source_right <- c(Q1 = 0.08, Q2 = 0.05, Q3 = 0.46)[[site]] * 0.0283
    if (length(old_left) != 1L || length(right) != 1L || abs(right - source_right) > 1e-9) {
      stop("check the bisley transition from historical runoff to daily cfs records")
    }
    # preserve the saved interpolation weights while updating their runoff endpoint
    # this affects estimates only, not observations already reported in cfs
    bridge <- dates > historical_end & dates < cfs_start
    new_left <- old_left * factor
    if (new_left != old_left && any(bridge)) {
      weights <- (as.numeric(data$Discharge[bridge]) - right) / (old_left - right)
      if (any(!is.finite(weights)) || any(weights < 0 | weights > 1)) {
        stop("check the bisley estimates between the runoff and cfs records")
      }
      data$Discharge[bridge] <- weights * new_left + (1 - weights) * right
      data$Q_Flag[bridge] <- "interpolated discharge; historical runoff endpoint updated"
    }
    attr(data, "discharge_units") <- "cms"
    return(data)
  }

  # the c7 coordinates match site 7 and the source explicitly reports daily m3/s
  # use a fixed sites release rather than guessing the missing hourly timestamps
  url <- paste0("https://data.fieldsites.se/licence_accept?ids=",
    "%5B%22adVF13uWj9IlUHOhej0T_EFk%22%5D")
  text <- discharge_source_text(url, cache)
  if (!grepl("SAMPLING POINT: Kallkälsbäcken, C7", text, fixed = TRUE) ||
      !grepl("PARAMETERS AND UNITS: Q (m3 s-1)", text, fixed = TRUE)) {
    stop("check site 7 station identity and units in the original source")
  }
  source <- data.table::fread(text = text, skip = "TIMESTAMP,Q", showProgress = FALSE)
  if (anyDuplicated(source$TIMESTAMP)) stop("the site 7 daily source contains repeated dates")
  result <- data.frame(Date = sort(unique(dates)))
  result$Discharge <- source$Q[match(result$Date, as.Date(source$TIMESTAMP))]
  result$Q_Flag <- "original SITES daily discharge"
  missing <- which(!is.finite(result$Discharge))
  # retain legacy gap values when the published daily source has no value
  # do not remove equal readings from separate hours in those legacy days
  for (i in missing) {
    rows <- which(dates == result$Date[i])
    values <- as.numeric(data$Q[rows])
    result$Discharge[i] <- if (all(is.na(values))) NA_real_ else mean(values, na.rm = TRUE) / 1000
    interpolated <- any(tolower(as.character(data$indicate[rows])) %in% "interpolated")
    result$Q_Flag[i] <- if (interpolated) "legacy interpolated discharge; SITES daily value missing" else
      "legacy discharge; SITES daily value missing; times unavailable"
  }
  attr(result, "discharge_units") <- "cms"
  result
}

# one gauge can serve several chemistry sites, but each file-to-site pair occurs once
attach_discharge_sites <- function(discharge, reference) {
  discharge <- as.data.frame(discharge)
  reference <- as.data.frame(reference)
  mappings <- unique(reference[, c("LTER", "Stream_Name", "Discharge_File_Name")])
  mappings <- mappings[!is.na(mappings$Discharge_File_Name) &
    nzchar(trimws(mappings$Discharge_File_Name)), , drop = FALSE]
  if (anyDuplicated(discharge[c("Discharge_File_Name", "Date")])) {
    stop("discharge preparation did not produce one row per file and date")
  }
  result <- merge(discharge, mappings, by = "Discharge_File_Name", all.x = TRUE, sort = FALSE)
  if (anyNA(result$LTER) || anyNA(result$Stream_Name)) stop("a discharge file has no site mapping")
  if (anyDuplicated(result[c("LTER", "Stream_Name", "Date")])) {
    stop("multiple discharge files supply the same chemistry site and date; review source preference")
  }
  if ("BK01.0M_Q" %in% mappings$Discharge_File_Name &&
      !"BK01.0M_Q" %in% result$Discharge_File_Name) {
    stop("BK01.0M_Q is mapped but absent from the prepared discharge")
  }
  result
}

# ---- verified discharge additions ----

# fill only missing saved values using the checked original usgs daily records
add_verified_discharge <- function(daily, additions) {
  stations <- c(SACRAMENTO_R_Q = "11447650", SAN_JOAQUIN_Q = "11303500", Maumee_River_Q = "04193500")
  files <- unique(daily$Discharge_File_Name)
  if (length(files) != 1L) stop("add verified discharge to one file at a time")
  file <- files[[1]]
  if (!file %in% names(stations)) return(daily)
  original <- additions$usgs[[stations[[file]]]]$value$timeSeries
  if (length(original) != 1L || original[[1]]$sourceInfo$siteCode[[1]]$value != stations[[file]] ||
      original[[1]]$variable$variableCode[[1]]$value != "00060" ||
      original[[1]]$variable$unit$unitCode != "ft3/s" ||
      original[[1]]$variable$options$option[[1]]$optionCode != "00003") {
    stop("check the original daily discharge addition for ", file)
  }
  values <- original[[1]]$values[[1]]$value
  source <- data.frame(Date = as.Date(vapply(values, function(x) substr(x$dateTime, 1L, 10L), character(1))),
    Qcms = as.numeric(vapply(values, function(x) x$value, character(1))) * 0.0283,
    Discharge_File_Name = file, Q_Flag = "USGS daily discharge added where saved value missing")
  if (anyNA(source$Date) || anyDuplicated(source$Date)) stop("check repeated or missing USGS addition dates")
  # retain the same cfs conversion as the existing discharge workflow
  estimated <- vapply(values, function(x) "e" %in% unlist(x$qualifiers), logical(1))
  provisional <- vapply(values, function(x) "P" %in% unlist(x$qualifiers), logical(1))
  source$Q_Flag[estimated] <- paste(source$Q_Flag[estimated], "estimated discharge", sep = "; ")
  source$Q_Flag[provisional] <- paste(source$Q_Flag[provisional], "provisional discharge", sep = "; ")
  source <- source[is.finite(source$Qcms) & source$Qcms >= 0, , drop = FALSE]
  result <- as.data.frame(daily)
  dates <- as.Date(as.character(result$Date))
  matched <- match(dates, source$Date)
  fill <- which(!is.finite(result$Qcms) & !is.na(matched))
  result$Qcms[fill] <- source$Qcms[matched[fill]]
  result$Q_Flag[fill] <- source$Q_Flag[matched[fill]]
  source <- source[!source$Date %in% dates, , drop = FALSE]
  result$Date <- dates
  result <- rbind(result, source[names(result)])
  if (anyDuplicated(result$Date)) stop("repeated dates after adding original discharge")
  result
}

# join saved chemistry to daily flow, then apply the variable-specific walk rule
join_chemistry_discharge <- function(chemistry, discharge, walker_samples, additions, data_key) {
  result <- as.data.frame(chemistry)
  if (any(c("Qcms", "Q_Flag") %in% names(result))) stop("chemistry already has discharge columns")
  flow <- data.table::as.data.table(discharge)[, c("LTER", "Stream_Name", "Date", "Qcms", "Q_Flag"), with = FALSE]
  flow[, date := data.table::as.IDate(as.character(Date))]
  flow[, Date := NULL]
  if (anyNA(flow$date) || anyDuplicated(flow[, c("LTER", "Stream_Name", "date"), with = FALSE])) {
    stop("discharge must have one valid row per site and date")
  }
  index <- data.table::data.table(LTER = result$LTER, Stream_Name = result$Stream_Name,
    date = data.table::as.IDate(as.character(result$date)))
  matched <- flow[index, on = c("LTER", "Stream_Name", "date"), .(Qcms, Q_Flag)]
  if (nrow(matched) != nrow(result)) stop("discharge join changed the number of chemistry rows")
  result$Qcms <- matched$Qcms
  result$Q_Flag <- matched$Q_Flag
  result$Q_Flag[is.na(result$Q_Flag)] <- ""
  apply_walk_discharge_rule(result, walker_samples, additions$walker_daily, data_key)
}

# ---- calculated walker branch discharge ----

# apply after joining chemistry and discharge, since fork contributions vary by solute
apply_walk_discharge_rule <- function(chemistry, walker_samples, walker_daily, data_key) {
  result <- as.data.frame(chemistry)
  required <- c("LTER", "Stream_Name", "date", "variable", "Qcms", "Q_Flag")
  if (!all(required %in% names(result))) stop("join chemistry and discharge before applying the WALK rule")
  rows <- which(result$LTER == "Walker Branch" & result$Stream_Name == "WALK")
  if (!length(rows)) return(result)
  samples <- as.data.frame(walker_samples)
  daily <- as.data.frame(walker_daily)
  key <- as.data.frame(data_key)
  key <- key[key$Raw_Filename == "WalkerBranch_Chem.csv", , drop = FALSE]
  normalize_variable <- function(x) {
    x <- tolower(gsub("_", " ", x, fixed = TRUE))
    aliases <- c(dsi = "si", "dissolved org c" = "doc", "tot dissolved n" = "tdn")
    matched <- match(x, names(aliases))
    x[!is.na(matched)] <- aliases[matched[!is.na(matched)]]
    x
  }
  number <- function(x) suppressWarnings(as.numeric(sub("^[<>]\\s*", "", trimws(as.character(x)))))
  sample_dates <- as.Date(as.character(samples$DATE))
  east <- which(samples$site_name == "East Fork")
  west <- which(samples$site_name == "West Fork")
  walk <- which(samples$site_name == "WALK")
  daily_dates <- as.Date(as.character(daily$DATE), "%Y%m%d")
  if (anyNA(daily_dates) || anyDuplicated(daily_dates) ||
      anyDuplicated(sample_dates[east]) || anyDuplicated(sample_dates[west]) ||
      anyDuplicated(sample_dates[walk])) stop("check repeated or missing Walker source dates")
  dates <- as.Date(as.character(result$date[rows]))
  east_rows <- east[match(dates, sample_dates[east])]
  west_rows <- west[match(dates, sample_dates[west])]
  walk_rows <- walk[match(dates, sample_dates[walk])]
  daily_rows <- match(dates, daily_dates)
  east_sample_q <- number(samples$EF_DISCHARGE[east_rows])
  west_sample_q <- number(samples$WF_DISCHARGE[west_rows])
  east_daily_q <- number(daily$EF_DISCHARGE[daily_rows])
  west_daily_q <- number(daily$WF_DISCHARGE[daily_rows])
  both_flows <- is.finite(east_daily_q) & east_daily_q >= 0 &
    is.finite(west_daily_q) & west_daily_q >= 0
  both_contributed <- rep(FALSE, length(rows))
  for (variable in unique(result$variable[rows])) {
    selected <- which(result$variable[rows] == variable)
    column <- key$Raw_Column_Name[
      normalize_variable(key$Standardized_Column_Name) == normalize_variable(variable)]
    if (length(column) != 1L || !column %in% names(samples)) {
      stop("check the Walker data-key mapping for ", variable)
    }
    east_value <- number(samples[[column]][east_rows[selected]])
    west_value <- number(samples[[column]][west_rows[selected]])
    text <- trimws(as.character(samples[[column]][walk_rows[selected]]))
    reported <- number(text)
    east_q <- east_sample_q[selected]
    west_q <- west_sample_q[selected]
    weighted <- (east_value * east_q + west_value * west_q) / (east_q + west_q)
    # allow only the rounding already present in the reported concentration
    text <- sub("^[<>]\\s*", "", text)
    decimals <- ifelse(grepl("[.]", text), nchar(sub("^[^.]*[.]", "", text)), 0L)
    tolerance <- 0.5 * 10^(-decimals) + 1e-10
    verified <- is.finite(east_value) & is.finite(west_value) &
      is.finite(east_q) & east_q > 0 & is.finite(west_q) & west_q > 0 &
      is.finite(reported) & is.finite(weighted) & abs(reported - weighted) <= tolerance
    verified[is.na(verified)] <- FALSE
    both_contributed[selected] <- verified
  }
  eligible <- both_contributed & both_flows
  result$Qcms[rows] <- NA_real_
  result$Q_Flag[rows] <- "combined flow not assigned: concentration uses one fork or contribution unverified"
  result$Q_Flag[rows[both_contributed & !both_flows]] <-
    "combined flow not assigned: one or both daily fork flows missing"
  result$Qcms[rows[eligible]] <- (east_daily_q[eligible] + west_daily_q[eligible]) / 1000
  result$Q_Flag[rows[eligible]] <- "calculated combined flow: East Fork + West Fork daily discharge"
  code <- as.character(daily$CODE[daily_rows])
  flagged <- eligible & !is.na(code) & nzchar(code) & code != "-9999"
  result$Q_Flag[rows[flagged]] <- paste(result$Q_Flag[rows[flagged]],
    paste0("original daily flow code: ", code[flagged]), sep = "; ")
  result
}
