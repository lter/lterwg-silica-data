# ---- saved release inputs ----

suppressPackageStartupMessages(library(data.table))
source_files <- vapply(sys.frames(), function(frame) {
  if (is.null(frame$ofile)) "" else as.character(frame$ofile)
}, character(1))
source_files <- source_files[nzchar(source_files)]
script <- if (length(source_files)) tail(source_files, 1L) else {
  sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
}
source(file.path(dirname(script), "bess_coverage.R"))

# ---- excluded source fields ----

is_original_potential_evaporation <- function(fields) {
  grepl("(^|_)(pet|pev)(_|$)|potential_evap|reference_et|ref_et",
    fields, ignore.case = TRUE)
}

remove_original_potential_evaporation <- function(data) {
  fields <- names(data)[is_original_potential_evaporation(names(data))]
  if (length(fields)) data[, (fields) := NULL]
  if ("ERA5_Climate_Method" %in% names(data)) {
    data[, ERA5_Climate_Method := gsub(
      ", evapotranspiration, and open-water potential evaporation",
      " and evapotranspiration", ERA5_Climate_Method, fixed = TRUE)]
  }
  data
}

# ---- annual climate fields ----

era5_annual_fields <- function() {
  c("precip_mm", "temp_degC", "temp_min_degC", "temp_max_degC", "evapotrans_mm",
    "snow_cover_fraction", "snow_water_equiv_mm", "runoff_mm", "solar_energy_mj_m2",
    "soil_water_layer_1_fraction", "soil_water_layer_3_fraction", "vapor_pressure_pa")
}

era5_annual_dictionary <- function(dictionary, wide_fields) {
  templates <- dictionary[source == "ERA5-Land" & year == "2000"]
  core <- c("precip_mm", "temp_degC", "evapotrans_mm", "snow_cover_fraction", "snow_water_equiv_mm")
  fields <- era5_annual_fields()
  extra <- setdiff(fields, core)
  extended <- any(paste0("era5_land_", extra, "_1984") %in% wide_fields)
  if (!extended) fields <- core
  expected <- as.vector(outer(paste0("era5_land_", fields, "_"), 1984:2025, paste0))
  stopifnot(nrow(templates) >= 5L, all(expected %in% wide_fields))
  short <- c(precip_mm = "precip", temp_degC = "temp", temp_min_degC = "temp_min",
    temp_max_degC = "temp_max", evapotrans_mm = "et", snow_cover_fraction = "snow_cover",
    snow_water_equiv_mm = "snow_swe", runoff_mm = "runoff", solar_energy_mj_m2 = "solar_energy",
    soil_water_layer_1_fraction = "soil_water_layer_1", soil_water_layer_3_fraction = "soil_water_layer_3",
    vapor_pressure_pa = "vapor_pressure")
  units <- c("mm/year", "degrees C", "degrees C", "degrees C", "mm/year",
    "fraction 0–1", "mm", "mm/year", "MJ/m2/year", "volume fraction", "volume fraction", "Pa")
  definitions <- c(
    "Annual watershed-average precipitation total; site metadata records aggregation and finer sampling",
    "Annual watershed-average air temperature; site metadata records aggregation and finer sampling",
    "Annual mean of daily watershed-average minimum air temperature",
    "Annual mean of daily watershed-average maximum air temperature",
    "Annual watershed-average actual evaporation total; negative values can represent condensation; site metadata records aggregation and finer sampling",
    "Annual maximum of daily watershed-average snow-covered fraction",
    "Annual maximum of daily watershed-average snow water equivalent",
    "Annual sum of daily watershed-average runoff; negative source runoff is set to zero",
    "Annual sum of daily watershed-average downward solar radiation energy; negative source energy is set to zero",
    "Annual mean of daily watershed-average volumetric soil water in ERA5-Land layer 1",
    "Annual mean of daily watershed-average volumetric soil water in ERA5-Land layer 3",
    paste("Annual mean vapor pressure: daily pixel dewpoint in Celsius is converted using",
      "611.2 * exp(17.67 * Td / (Td + 243.5)) before watershed and time averaging"))
  names(units) <- names(definitions) <- era5_annual_fields()
  rows <- lapply(fields, function(field) {
    template <- templates[original_name == paste0("era5_land_", field, "_2000")]
    if (!nrow(template)) template <- templates[1L]
    stopifnot(nrow(template) == 1L)
    result <- template[rep(1L, 42L)]
    result[, `:=`(year = as.character(1984:2025), units = unname(units[field]),
      definition = paste0(definitions[field], "; a complete calendar year is required"))]
    result[, original_name := paste0("era5_land_", field, "_", year)]
    result[, column_name := paste0(short[field], "_", year)]
    result
  })
  rbindlist(rows, use.names = TRUE)
}

# ---- preserved rebuild inputs ----

spatial_baseline_file <- function(path) {
  if (file.exists(path)) return(path)
  expected <- c(
    `20261102_updated_harmonized_spatial.csv` = "25fa69bd305cd915a53789349059678a",
    `20261102_updated_harmonized_spatial_metadata.csv` = "12c2490c58025af7376bd97726d4b7e1",
    `20261102_updated_harmonized_spatial_column_dictionary.csv` = "7f22a0264c64a6511181143ead0f3682")
  saved <- file.path(dirname(dirname(path)), "archive", basename(path))
  hash <- expected[basename(path)]
  if (is.na(hash) || !file.exists(saved) || unname(tools::md5sum(saved)) != unname(hash))
    stop("the unchanged archived rebuild input is unavailable: ", basename(path))
  saved
}

# ---- reviewed geology correction ----

verified_osteglo_change <- function(lter, stream, field, old, new, major_rock, proof) {
  if (!identical(lter, "NIVA") || !identical(stream, "OSTEGLO") ||
      !identical(major_rock, "metamorphic") || !is.list(proof)) return(FALSE)
  expected <- c(
    request_md5 = "5ba927893237f788621236547dc5fee4",
    source_proof_md5 = "5abe0902d03e7ba3bbd1fcf420f166c9",
    native_source_md5 = "f0653f59909a6576a364dee4a29404e2",
    source_geometry_sha256 = "36c574209c946d5168e30644c1be3b8c4b92f326148fe597b37fca6ecb36ba9f",
    current_geometry_sha256 = "6cfc7c6155ec9107ae6b0dd7fe18a9f351c096bdde81a08c63f681447b7e776f")
  if (!all(vapply(names(expected), function(name)
      identical(as.character(proof[[name]]), unname(expected[name])), logical(1)))) return(FALSE)
  fields <- c("rocks_plutonic", "rocks_volcanic")
  row <- match(field, fields)
  values <- suppressWarnings(as.numeric(c(old, new)))
  if (is.na(row) || length(values) != 2L || any(!is.finite(values))) return(FALSE)
  # the saved csv values carry fifteen significant digits
  hashes <- vapply(values, function(value) digest::digest(
    paste(lter, stream, field, sprintf("%.15g", value), sep = "\r"),
    algo = "sha256", serialize = FALSE), character(1))
  old_hash <- c("2ad84853a56b930b3bc4c586e303266811982ccbe0426035ee778480f1f587b7",
    "68c348e5c64b41a1190808d9c8c3a6d6dd4b0c3c96edf60d13218b2acb996f8d")
  new_hash <- c("befd223b46e120e02789db0b5af6a7891c5c94871981e1bce4d2fef4fa90589c",
    "62cf2b136c012e25e12c68ad20d050a19e58e20259c235f44ce23de62b46b221")
  identical(unname(hashes), c(old_hash[row], new_hash[row]))
}

# ---- checked product tables ----

build_wide_spatial_rds <- function(product_file, baseline_dir, dictionary_file, output_dir,
    output_prefix = NULL) {
  if (!nzchar(output_dir) || file.exists(output_dir)) stop("choose a new output directory")
  if (!dir.exists(baseline_dir)) stop("the RDS build requires the current shared dataset directory")
  product_check <- tools::md5sum(product_file)
  products <- readRDS(product_file)
  stopifnot(is.list(products), length(products) == 9L,
    all(vapply(products, is.data.frame, logical(1))))
  metadata_name <- grep("^site-metadata/", names(products), value = TRUE)
  stopifnot(length(metadata_name) == 1L)
  metadata <- remove_original_potential_evaporation(as.data.table(copy(products[[metadata_name]])))
  keys <- c("LTER", "Stream_Name")
  stopifnot(nrow(metadata) %in% c(1170L, 1222L, 1226L),
    !anyNA(metadata[, ..keys]), !anyDuplicated(metadata, by = keys))
  wide <- metadata[, .(LTER, Stream_Name, Latitude, Longitude, drainSqKm, Site_Flag)]
  for (name in setdiff(names(products), metadata_name)) {
    product <- remove_original_potential_evaporation(as.data.table(products[[name]]))
    stopifnot(all(vapply(keys, function(key) identical(product[[key]], wide[[key]]), logical(1))))
    common <- intersect(names(product), names(wide))
    for (field in common) {
      stopifnot(isTRUE(all.equal(product[[field]], wide[[field]], tolerance = 1e-13)))
    }
    for (field in setdiff(names(product), common)) set(wide, j = field, value = product[[field]])
  }
  wide <- withhold_mcm_vegetation(wide)
  metadata[LTER == "MCM", Land_Cover_Reliability :=
    "unsupported at McMurdo; analytical values withheld"]
  metadata[LTER == "MCM", Vegetation_Reliability := mcm_vegetation_note]

  # the current shared files protect checked values added after the product tables
  read_text <- function(path) fread(path, colClasses = "character", na.strings = NULL)
  dictionary_file <- spatial_baseline_file(dictionary_file)
  dictionary <- read_text(dictionary_file)
  dictionary <- dictionary[!is_original_potential_evaporation(original_name) &
    !is_original_potential_evaporation(column_name)]
  dictionary[original_name == "downstream_to_outlet_km", definition :=
    "Downstream river-network distance from the selected HydroRIVERS reach outlet to the final basin outlet"]
  dictionary[original_name == "downstream_to_ocean_km", definition :=
    "Downstream river-network distance from the selected HydroRIVERS reach outlet to the ocean; unavailable for endorheic basins"]
  original_files <- unique(dictionary$file)
  main_file <- original_files[!grepl("_gpp_quality[.]csv$", original_files)]
  quality_file <- original_files[grepl("_gpp_quality[.]csv$", original_files)]
  stopifnot(length(main_file) == 1L, length(quality_file) == 1L)
  baseline_paths <- c(
    main = file.path(baseline_dir, main_file),
    metadata = file.path(baseline_dir, sub("[.]csv$", "_metadata.csv", main_file)),
    quality = file.path(baseline_dir, "Supporting files", quality_file),
    dictionary = dictionary_file
  )
  if (!file.exists(baseline_paths[["quality"]])) {
    baseline_paths[["quality"]] <- file.path(baseline_dir, quality_file)
  }
  baseline_paths <- vapply(baseline_paths, spatial_baseline_file, character(1))
  stopifnot(all(file.exists(baseline_paths)))
  baseline_checks <- tools::md5sum(baseline_paths)
  baseline <- read_text(baseline_paths[["main"]])
  baseline_metadata <- read_text(baseline_paths[["metadata"]])
  baseline_quality <- read_text(baseline_paths[["quality"]])
  site_key <- function(data) paste(data$LTER, data$Stream_Name, sep = "\r")
  baseline_rows <- match(site_key(baseline), site_key(wide))
  stopifnot(nrow(baseline) %in% c(1170L, 1222L, 1226L), !anyNA(baseline_rows),
    !anyDuplicated(site_key(baseline)),
    identical(site_key(baseline), site_key(baseline_metadata)),
    !anyDuplicated(baseline_quality, by = c(keys, "year")))
  ferguson_key <- "WesternAustralia\rFerguson River - Dowdells Rd Bridge"
  ferguson <- match(ferguson_key, site_key(metadata))
  earlier_ferguson <- match(ferguson_key, site_key(baseline_metadata))
  stopifnot(!is.na(ferguson), !is.na(earlier_ferguson),
    abs(metadata$drainSqKm[ferguson] - 104.973609) < .001,
    abs(as.numeric(baseline_metadata$drainSqKm[earlier_ferguson]) - 104.973609) < .001)
  for (field in c("Spatial_Recovery_Note", "Spatial_Notes")) {
    earlier <- baseline_metadata[[field]][earlier_ferguson]
    current <- metadata[[field]][ferguson]
    sentences <- strsplit(current, ". ", fixed = TRUE)[[1]]
    climate_note <- sentences[grepl("native|daily", sentences, ignore.case = TRUE) &
      !grepl("land cover|terrain|lithology|soil", sentences, ignore.case = TRUE)]
    metadata[[field]][ferguson] <- paste(c(earlier, climate_note), collapse = " ")
  }
  if (is.null(output_prefix)) output_prefix <- sub("[.]csv$", "", main_file)
  if (!grepl("^[[:alnum:]_-]+$", output_prefix)) stop("invalid output prefix")
  files <- paste0(output_prefix, c(".csv", "_metadata.csv", "_column_dictionary.csv", "_gpp_quality.csv"))
  dictionary[file == main_file, file := files[1L]]
  dictionary[file == quality_file, file := files[4L]]

  # publish only the first greenup cycle and the observed land-cover map years
  observed_land_years <- c(1985L, 1990L, 1995L, 2000:2022)
  land_year <- suppressWarnings(as.integer(dictionary$year))
  dictionary <- dictionary[!grepl("^greenup_cycle1_", original_name) &
    !grepl("^(major_soil$|soil_)", original_name) &
    !(source == "GLC-FCS30D" & !land_year %in% observed_land_years)]

  # extend the first-cycle date mapping when a checked new year is present
  greenup_fields <- grep("^greenup_cycle0_[0-9]{4}MMDD$", names(wide), value = TRUE)
  for (field in setdiff(greenup_fields, dictionary$original_name)) {
    selected_year <- sub("^greenup_cycle0_([0-9]{4}).*", "\\1", field)
    template_field <- sub(selected_year, "2001", field, fixed = TRUE)
    entry <- copy(dictionary[original_name == template_field])
    stopifnot(nrow(entry) == 1L)
    entry[, `:=`(original_name = field,
      column_name = sub("2001", selected_year, column_name, fixed = TRUE), year = selected_year)]
    dictionary <- rbindlist(list(dictionary, entry), use.names = TRUE)
  }

  # every climate field uses the same complete 1984-2025 year range
  climate <- list(era5_annual_dictionary(dictionary, names(wide)))
  first_climate <- min(which(dictionary$source == "ERA5-Land"))
  before <- dictionary[seq_len(first_climate - 1L)]
  after <- dictionary[seq.int(first_climate, nrow(dictionary))][source != "ERA5-Land"]
  dictionary <- rbindlist(c(list(before), climate, list(after)), use.names = TRUE)
  wide_dictionary <- dictionary[file == files[1L]]
  quality_dictionary <- dictionary[file == files[4L]]
  # the older combined status is preserved from the shared table where available
  if ("modis_vegetation_status" %in% wide_dictionary$original_name &&
      !"modis_vegetation_status" %in% names(wide)) {
    set(wide, j = "modis_vegetation_status", value = rep(NA_character_, nrow(wide)))
  }
  stopifnot(!anyDuplicated(dictionary, by = c("file", "column_name")),
    !anyDuplicated(wide_dictionary$original_name),
    all(wide_dictionary$original_name %in% names(wide)),
    identical(wide_dictionary$column_name[1:2], keys))

  # only named kilogram npp fields are converted; bess gpp stays in grams
  npp_source <- paste0("npp_", 2001:2025, "_kgC_m2_year")
  npp_entries <- wide_dictionary[original_name %in% npp_source]
  stopifnot(setequal(npp_entries$original_name, npp_source),
    all(vapply(wide[, ..npp_source], is.numeric, logical(1))))
  for (field in npp_source) wide[[field]] <- wide[[field]] * 1000
  dictionary[file == files[1L] & original_name %in% npp_source,
    `:=`(units = "g C/m²/year",
      definition = "Annual watershed mean net primary production over valid pixels, converted from kilograms to grams of carbon")]

  # keep earlier non-climate measurements and reject unexplained replacements
  empty <- function(values) is.na(values) | as.character(values) == ""
  non_climate <- dictionary[file == files[1L] & source != "ERA5-Land"]
  restored_cells <- 0L
  changed_status_cells <- 0L
  changed_geology_cells <- 0L
  for (i in seq_len(nrow(non_climate))) {
    field <- non_climate$original_name[i]
    column <- non_climate$column_name[i]
    if (!column %in% names(baseline)) next
    old <- baseline[[column]]
    values <- wide[[field]][baseline_rows]
    present <- !empty(old) & !(baseline$LTER == "MCM" & field %in% mcm_vegetation_fields(field))
    both <- present & !empty(values)
    different <- if (is.numeric(values)) {
      both & abs(as.numeric(old) - values) > 1e-7
    } else {
      both & old != as.character(values)
    }
    if (any(different, na.rm = TRUE)) {
      if (field == "Site_Flag") {
        camrex <- baseline$LTER == "Amazon" & baseline$Stream_Name %in%
          c("Anori", "Jutica", "Paura", "Sao Jose do Amatari", "Tupe", "Xibeco")
        # a checked land-cover update changes only the old availability phrase
        reviewed_land <- site_key(baseline) %in% attr(products, "verified_land_cover_keys")
        land_fields <- grep("^gee_glc_[0-9]{4}_", names(wide), value = TRUE)
        land_fields <- land_fields[!grepl("Filled_Value$", land_fields)]
        measured_land <- rowSums(as.matrix(wide[baseline_rows, ..land_fields]) > 0,
          na.rm = TRUE) > 0
        revised <- gsub("spatial data not yet available", "partial spatial data available",
          old, fixed = TRUE)
        checked_land <- reviewed_land & measured_land & values == revised &
          metadata$Has_Spatial_Data[baseline_rows] == "Yes"
        stopifnot(all(!different | (camrex & values == "partial spatial data available") |
          checked_land))
      } else if (field == "human_results_status") {
        stopifnot(all(!different | values == "dams_verified_other_human_products_missing"))
      } else {
        changed <- which(different)
        reviewed <- vapply(changed, function(row) verified_osteglo_change(
          baseline$LTER[row], baseline$Stream_Name[row], field, old[row], values[row],
          wide$major_rock[baseline_rows[row]], attr(products, "verified_osteglo_geology")), logical(1))
        if (!all(reviewed)) stop("candidate changes a retained non-climate value: ", field)
      }
      if (field %in% c("Site_Flag", "human_results_status")) {
        changed_status_cells <- changed_status_cells + sum(different)
      } else changed_geology_cells <- changed_geology_cells + sum(different)
    }
    fill <- present & empty(values)
    if (any(fill)) {
      value <- if (is.numeric(wide[[field]])) as.numeric(old[fill]) else old[fill]
      wide[[field]][baseline_rows[fill]] <- value
      if (field == "Site_Flag") metadata[[field]][baseline_rows[fill]] <- value
      restored_cells <- restored_cells + sum(fill)
    }
  }

  # retain the supporting gpp coverage fields for every site and year
  years <- 1982:2022
  quality_fields <- quality_dictionary[grepl("YYYY", original_name, fixed = TRUE)]
  stopifnot(identical(quality_fields$original_name, paste0("bess_gpp_YYYY_",
    c("valid_days", "area_fraction", "trend_flag", "trend_note"))))
  stopifnot(nrow(baseline_quality) == nrow(baseline) * length(years),
    setequal(unique(baseline_quality$year), as.character(years)))
  for (year in years) {
    base <- paste0("bess_gpp_", year, "_")
    selected_year <- as.character(year)
    earlier <- baseline_quality[year == selected_year]
    rows <- match(site_key(earlier), site_key(wide))
    stopifnot(!anyNA(rows))
    for (i in seq_len(nrow(quality_fields))) {
      field <- sub("YYYY", year, quality_fields$original_name[i], fixed = TRUE)
      old <- earlier[[quality_fields$column_name[i]]]
      values <- wide[[field]][rows]
      both <- !empty(old) & !empty(values)
      if (is.numeric(values)) {
        stopifnot(all(!both | abs(as.numeric(old) - values) <= 1e-7))
      } else {
        stopifnot(all(!both | old == as.character(values)))
      }
      fill <- !empty(old) & empty(values)
      if (any(fill)) {
        value <- if (is.numeric(wide[[field]])) as.numeric(old[fill]) else old[fill]
        wide[[field]][rows[fill]] <- value
      }
    }
    check_bess_coverage(wide[[paste0(base, "gC_m2_year")]],
      wide[[paste0(base, "valid_days")]], wide[[paste0(base, "area_fraction")]], year)
  }

  # normalize numeric text once, then check all four saved files by readback
  stage <- tempfile("wide-spatial-", tmpdir = dirname(output_dir))
  if (!dir.create(stage)) stop("could not create staging directory")
  on.exit(unlink(stage, recursive = TRUE))
  output <- file.path(stage, files)
  combined <- file.path(stage, "combined.txt")
  fwrite(wide, combined, na = "")
  text <- read_text(combined)
  main <- text[, wide_dictionary$original_name, with = FALSE]
  setnames(main, wide_dictionary$column_name)
  # retain the exact earlier text where its checked measurement is unchanged
  for (i in seq_len(nrow(non_climate))) {
    column <- non_climate$column_name[i]
    field <- non_climate$original_name[i]
    if (!column %in% names(baseline) || field %in% c("Site_Flag", "human_results_status")) next
    present <- !empty(baseline[[column]]) &
      !(baseline$LTER == "MCM" & field %in% mcm_vegetation_fields(field))
    if (field %in% c("rocks_plutonic", "rocks_volcanic")) {
      rows <- which(present & baseline$LTER == "NIVA" & baseline$Stream_Name == "OSTEGLO")
      for (row in rows) {
        if (verified_osteglo_change(baseline$LTER[row], baseline$Stream_Name[row], field,
            baseline[[column]][row], wide[[field]][baseline_rows[row]],
            wide$major_rock[baseline_rows[row]], attr(products, "verified_osteglo_geology"))) present[row] <- FALSE
      }
    }
    main[[column]][baseline_rows[present]] <- baseline[[column]][present]
  }
  quality <- rbindlist(lapply(years, function(year) {
    fields <- sub("YYYY", year, quality_fields$original_name, fixed = TRUE)
    part <- text[, c(keys, fields), with = FALSE]
    setnames(part, c(keys, quality_fields$column_name))
    part[, year := as.character(year)]
    setcolorder(part, quality_dictionary$column_name)
    part
  }))
  quality_rows <- match(paste(site_key(baseline_quality), baseline_quality$year),
    paste(site_key(quality), quality$year))
  stopifnot(!anyNA(quality_rows))
  for (column in quality_fields$column_name) {
    present <- !empty(baseline_quality[[column]])
    quality[[column]][quality_rows[present]] <- baseline_quality[[column]][present]
  }
  fwrite(metadata, output[2L], na = "")
  metadata <- read_text(output[2L])
  stopifnot(nrow(quality) == nrow(metadata) * length(years),
    !anyDuplicated(quality, by = c(keys, "year")),
    identical(names(main), dictionary[file == files[1L], column_name]),
    !any(is_original_potential_evaporation(names(main))))
  tables <- list(main, metadata, dictionary, quality)
  for (i in seq_along(output)) {
    fwrite(tables[[i]], output[i], na = "")
    stopifnot(identical(as.list(read_text(output[i])), as.list(tables[[i]])))
  }
  unlink(combined)
  stopifnot(identical(tools::md5sum(baseline_paths), baseline_checks),
    identical(tools::md5sum(product_file), product_check))
  if (file.exists(output_dir) || !file.rename(stage, output_dir)) stop("could not finish the release")
  result <- file.path(output_dir, files)
  attr(result, "baseline_checksums") <- baseline_checks
  attr(result, "product_checksum") <- product_check
  attr(result, "restored_nonclimate_cells") <- restored_cells
  attr(result, "reviewed_status_changes") <- changed_status_cells
  attr(result, "reviewed_geology_changes") <- changed_geology_cells
  invisible(result)
}

# ---- earlier csv-folder release ----

build_wide_spatial_release <- function(product_dir, provider_file, extraction_file,
    class_file, dictionary_file, output_dir, camrex_file = NULL, output_prefix = NULL) {
  dictionary_file <- spatial_baseline_file(dictionary_file)
  if (file.exists(product_dir) && !dir.exists(product_dir) &&
      tolower(tools::file_ext(product_dir)) == "rds") {
    if (!is.null(camrex_file) && camrex_file != "-") {
      stop("the RDS input already contains its checked CAMREX values")
    }
    return(build_wide_spatial_rds(product_dir, provider_file, dictionary_file, output_dir, output_prefix))
  }
  if (!is.null(output_prefix)) stop("a new output prefix requires the checked RDS input")
  output_dir <- sub("/+$", "", output_dir)
  if (!nzchar(output_dir) || file.exists(output_dir)) stop("choose a new output directory")

  # combine the checked product tables in site metadata order
  paths <- list.files(product_dir, "[.]csv$", recursive = TRUE, full.names = TRUE)
  metadata_path <- paths[basename(paths) == "site_metadata_1170.csv"]
  stopifnot(length(paths) == 9L, length(metadata_path) == 1L)
  metadata <- remove_original_potential_evaporation(fread(metadata_path))
  keys <- c("LTER", "Stream_Name")
  stopifnot(nrow(metadata) == 1170L, !anyDuplicated(metadata, by = keys),
    sum(metadata$Has_Spatial_Data == "No") == 130L)
  wide <- metadata[, .(LTER, Stream_Name, Latitude, Longitude, drainSqKm, Site_Flag)]
  for (path in setdiff(paths, metadata_path)) {
    product <- remove_original_potential_evaporation(fread(path))
    stopifnot(identical(as.list(product[, ..keys]), as.list(wide[, ..keys])))
    common <- intersect(names(product), names(wide))
    for (field in common) {
      stopifnot(isTRUE(all.equal(product[[field]], wide[[field]], tolerance = 1e-13)))
    }
    for (field in setdiff(names(product), common)) wide[[field]] <- product[[field]]
  }
  stopifnot(ncol(wide) == 981L)
  wide <- withhold_mcm_vegetation(wide)
  metadata[LTER == "MCM", Land_Cover_Reliability :=
    "unsupported at McMurdo; analytical values withheld"]
  metadata[LTER == "MCM", Vegetation_Reliability := mcm_vegetation_note]
  # keep only the first modis greenup cycle in the release
  second_greenup <- grep("^greenup_cycle1_", names(wide), value = TRUE)
  wide[, (second_greenup) := NULL]

  # fill only the checked fields for ferguson's corrected watershed
  site <- which(metadata$LTER == "WesternAustralia" &
    metadata$Stream_Name == "Ferguson River - Dowdells Rd Bridge")
  stopifnot(length(site) == 1L, abs(metadata$drainSqKm[site] - 104.973609) < .001)
  # reuse the checked land-cover fields if the extraction cache is unavailable
  if (endsWith(extraction_file, ".csv")) {
    verified_land_csv <- "40d0666d22e336f5afd6c09bf272f507"
    if (!identical(unname(tools::md5sum(extraction_file)), verified_land_csv)) {
      stop("use the verified CAMREX Cycle 0 CSV for Ferguson land cover")
    }
    source <- fread(extraction_file)
    row <- which(source$LTER == metadata$LTER[site] &
      source$Stream_Name == metadata$Stream_Name[site])
    stopifnot(length(row) == 1L, abs(source$drainSqKm[row] - metadata$drainSqKm[site]) < .001,
      abs(source$Latitude[row] - metadata$Latitude[site]) < .001,
      abs(source$Longitude[row] - metadata$Longitude[site]) < .001)
    land_columns <- fread(dictionary_file)[grepl("^gee_glc_", original_name)]
    stopifnot(!anyDuplicated(land_columns$original_name),
      !anyDuplicated(land_columns$column_name), all(land_columns$column_name %in% names(source)))
    land_values <- as.list(source[row, land_columns$column_name, with = FALSE])
    names(land_values) <- land_columns$original_name
    land_values <- land_values[!is.na(land_values)]
    stopifnot(length(land_values) == 286L,
      all(is.finite(unlist(land_values)) & unlist(land_values) >= 0),
      all(abs(tapply(unlist(land_values), substr(names(land_values), 9L, 12L), sum) - 1) < 1e-12))
  } else {
    land <- rbindlist(readRDS(extraction_file)$land)
    stopifnot(length(site) == 1L, abs(metadata$drainSqKm[site] - 104.973609) < .001,
      all(land$Shapefile_Name == metadata$Watershed_File_Name[site]),
      all(land$LTER == metadata$LTER[site]), all(land$Stream_Name == metadata$Stream_Name[site]),
      all(is.finite(land$Area_m2) & land$Area_m2 >= 0))
    classes <- fread(class_file)
    stopifnot(!anyDuplicated(classes$LC_ID))
    land[, Class := classes$Simple_Class[match(LC_ID, classes$LC_ID)]]
    stopifnot(!anyNA(land$Class))
    fractions <- land[, .(area = sum(Area_m2)), by = .(Year, Class)]
    fractions[, fraction := area / sum(area), by = Year]
    stopifnot(all(is.finite(fractions$fraction)))
    land_values <- as.list(setNames(fractions$fraction,
      paste("gee_glc", fractions$Year, fractions$Class, sep = "_")))
  }
  # the saved october 29 results have no watershed id, so check the verified file by checksum
  verified_provider <- "3c1d402eddf0d54cebce5091725ce641"
  if (!identical(unname(tools::md5sum(provider_file)), verified_provider)) {
    stop("use the verified October 29 Ferguson provider results for the corrected watershed")
  }
  static <- readRDS(provider_file)$values
  static <- static[grep("^(elevation_|basin_slope_|major_rock$|rocks_|major_soil$|soil_)", names(static))]
  updates <- c(land_values, static)
  stopifnot(length(updates) == 298L, !anyDuplicated(names(updates)),
    all(names(updates) %in% names(wide)))
  for (field in names(updates)) {
    prior <- wide[[field]][site]
    if (is.na(prior) || (is.character(prior) && !nzchar(prior))) {
      if (is.integer(wide[[field]]) && is.double(updates[[field]])) {
        wide[[field]] <- as.double(wide[[field]])
      }
      set(wide, site, field, updates[[field]])
    } else {
      stopifnot(isTRUE(all.equal(prior, updates[[field]], tolerance = 1e-12)))
    }
  }
  metadata[site, Spatial_Recovery_Note := "Corrected watershed has ERA5-Land, BESS GPP, saved MODIS NPP and greenup, human impacts and routing, checked observed-year land cover, elevation, slope, lithology, and soils."]
  metadata[site, Spatial_Notes := "Ferguson uses 25 connected BoM Geofabric catchments above station 611026 covering 104.973609 km². Land cover and static values come from the checked October 29 repair. Legacy permafrost coverage excludes this site."]

  # the dictionary gives each column its short name and tells us which file it goes in
  prefix <- "20261102_updated_harmonized_spatial"
  files <- paste0(prefix, c(".csv", "_metadata.csv", "_column_dictionary.csv", "_gpp_quality.csv"))
  read_text <- function(path) fread(path, colClasses = "character", na.strings = NULL)
  dictionary <- read_text(dictionary_file)
  dictionary <- dictionary[!is_original_potential_evaporation(original_name) &
    !is_original_potential_evaporation(column_name) &
    !original_name %in% second_greenup &
    !grepl("^npp_gapfill_[0-9]{4}$", column_name)]
  dictionary[original_name == "downstream_to_outlet_km", definition :=
    "Downstream river-network distance from the selected HydroRIVERS reach outlet to the final basin outlet"]
  dictionary[original_name == "downstream_to_ocean_km", definition :=
    "Downstream river-network distance from the selected HydroRIVERS reach outlet to the ocean; unavailable for endorheic basins"]
  wide_dictionary <- dictionary[file == files[1L]]
  quality_dictionary <- dictionary[file == files[4L]]
  quality_field_patterns <- quality_dictionary[grepl("YYYY", original_name, fixed = TRUE)]
  years <- 1982:2022
  # check each source mapping before using the dictionary to rename values
  gpp_rows <- match(paste0("gpp_", years), wide_dictionary$column_name)
  if (!identical(wide_dictionary$original_name[gpp_rows],
      paste0("bess_gpp_", years, "_gC_m2_year")) ||
      !identical(wide_dictionary$year[gpp_rows], as.character(years))) {
    stop("the dictionary must match each GPP total to its source column and year")
  }
  expected_quality <- paste0("bess_gpp_YYYY_",
    c("valid_days", "area_fraction", "trend_flag", "trend_note"))
  if (!identical(quality_field_patterns$original_name, expected_quality) ||
      !identical(quality_field_patterns$column_name, c("gpp_days", "gpp_area", "gpp_flag", "gpp_note"))) {
    stop("the dictionary must match each GPP quality field to its source column")
  }
  for (year in years) {
    base <- paste0("bess_gpp_", year, "_")
    check_bess_coverage(wide[[paste0(base, "gC_m2_year")]],
      wide[[paste0(base, "valid_days")]], wide[[paste0(base, "area_fraction")]], year)
  }
  quality_columns <- lapply(years, function(year) sub("YYYY", year, quality_field_patterns$original_name, fixed = TRUE))
  stopifnot(!anyDuplicated(dictionary, by = c("file", "column_name")),
    !anyDuplicated(wide_dictionary$original_name),
    setequal(c(wide_dictionary$original_name, unlist(quality_columns)), names(wide)),
    identical(wide_dictionary$column_name[1:2], keys))

  # convert only the saved npp source columns, whose names specify kilograms
  npp_years <- 2001:2025
  npp_source <- paste0("npp_", npp_years, "_kgC_m2_year")
  npp_rows <- match(paste0("npp_", npp_years), wide_dictionary$column_name)
  if (!identical(wide_dictionary$original_name[npp_rows], npp_source) ||
      !identical(wide_dictionary$year[npp_rows], as.character(npp_years))) {
    stop("the dictionary must match each NPP total to its kilogram source column and year")
  }
  for (field in npp_source) {
    values <- wide[[field]]
    if (!is.numeric(values) && !all(is.na(values))) {
      stop("expected numeric MODIS NPP in kilograms: ", field)
    }
    wide[[field]] <- values * 1000
  }
  dictionary[file == files[1L] & original_name %in% npp_source,
    `:=`(units = "g C/m²/year",
      definition = "Annual watershed mean net primary production over valid pixels, converted from kilograms to grams of carbon")]

  # match the six verified camrex watersheds before adding their annual results
  if (!is.null(camrex_file)) {
    products <- readRDS(camrex_file)
    npp_table <- grep("^modis-npp/", names(products), value = TRUE)
    stopifnot(length(npp_table) == 1L)
    camrex <- products[[npp_table]]
    sites <- c("Anori", "Jutica", "Paura", "Sao Jose do Amatari", "Tupe", "Xibeco")
    site_key <- function(data) paste(data$LTER, data$Stream_Name, sep = "\r")
    target_keys <- paste("Amazon", sites, sep = "\r")
    target <- match(target_keys, site_key(wide))
    source_rows <- match(target_keys, site_key(camrex))
    stopifnot(!anyNA(target), !anyNA(source_rows), !anyDuplicated(site_key(camrex)),
      all(abs(wide$Latitude[target] - camrex$Latitude[source_rows]) <= .001),
      all(abs(wide$Longitude[target] - camrex$Longitude[source_rows]) <= .001))

    gapfill_names <- paste0("npp_gapfill_", npp_years)
    gapfill_source <- paste0("npp_", npp_years, "_gapfilled_input_days_pct")
    stopifnot(all(c(npp_source, gapfill_source) %in% names(camrex)),
      all(vapply(camrex[c(npp_source, gapfill_source)], is.numeric, logical(1))))
    gapfill_dictionary <- data.table(column_name = gapfill_names,
      original_name = gapfill_source,
      definition = "Watershed mean percentage of growing-season days with gap-filled vegetation inputs for annual MODIS NPP",
      units = "percent 0–100", source = "MODIS MOD17A3HGF v6.1 via AppEEARS",
      year = as.character(npp_years), file = files[1L])
    # generate the quality entries from the saved field names
    dictionary <- rbindlist(list(dictionary[file == files[1L]],
      gapfill_dictionary, dictionary[file != files[1L]]), use.names = TRUE)
  }

  # stage the four files together and check their readback before release
  stage <- tempfile("wide-spatial-", tmpdir = dirname(output_dir))
  if (!dir.create(stage)) stop("could not create staging directory")
  on.exit(unlink(stage, recursive = TRUE))
  output <- file.path(stage, files)
  fwrite(wide, output[1L], na = "")
  wide <- read_text(output[1L])
  main <- wide[, wide_dictionary$original_name, with = FALSE]
  setnames(main, wide_dictionary$column_name)
  if (!is.null(camrex_file)) {
    for (i in seq_along(npp_years)) {
      column <- paste0("npp_", npp_years[i])
      kg <- camrex[[npp_source[i]]][source_rows]
      percent <- camrex[[gapfill_source[i]]][source_rows]
      stopifnot(all(main[[column]][target] == ""), all(is.finite(kg) & kg >= 0),
        all(is.finite(percent) & percent >= 0 & percent <= 100))
      # use the same precision as the checked local camrex release
      set(main, target, column, format(kg * 1000, digits = 16, trim = TRUE, scientific = FALSE))
      gapfill <- rep(NA_real_, nrow(main))
      gapfill[target] <- percent
      main[[gapfill_names[i]]] <- gapfill
    }
  }
  quality <- rbindlist(lapply(seq_along(years), function(i) {
    part <- wide[, c(keys, quality_columns[[i]]), with = FALSE]
    setnames(part, c(keys, quality_field_patterns$column_name))
    part[, year := as.character(years[i])]
    setcolorder(part, c(keys, "year", quality_field_patterns$column_name))
    part
  }))
  stopifnot(identical(names(main), dictionary[file == files[1L], column_name]),
    ncol(main) == if (is.null(camrex_file)) 793L else 818L,
    nrow(quality) == 47970L,
    !anyDuplicated(quality, by = c(keys, "year")),
    setequal(quality_dictionary$column_name, names(quality)),
    !any(is_original_potential_evaporation(names(main))))
  tables <- list(main, metadata, dictionary, quality)
  for (i in seq_along(output)) {
    fwrite(tables[[i]], output[i], na = "")
    actual <- if (i == 2L) fread(output[i]) else read_text(output[i])
    if (i == 1L && !is.null(camrex_file)) {
      for (field in gapfill_names) set(actual, j = field, value = as.numeric(actual[[field]]))
    }
    stopifnot(isTRUE(all.equal(as.list(actual), as.list(tables[[i]]), tolerance = 1e-13)))
  }
  if (file.exists(output_dir) || !file.rename(stage, output_dir)) stop("could not finish the release")
  invisible(file.path(output_dir, files))
}

if (sys.nframe() == 0L) {
  args <- commandArgs(TRUE)
  if (!length(args) %in% 6:8) {
    stop("usage: Rscript build_wide_spatial_release.R PRODUCT_DIR_OR_RDS PROVIDER_RDS_OR_BASELINE_DIR FERGUSON_LAND_INPUT LAND_CLASSES_CSV COLUMN_DICTIONARY_CSV OUTPUT_DIR [CAMREX_NPP_RDS_OR_DASH] [OUTPUT_PREFIX]")
  }
  do.call(build_wide_spatial_release, as.list(args))
}
