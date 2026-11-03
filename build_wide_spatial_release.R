# ---- saved release inputs ----

library(data.table)
script <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
source(file.path(dirname(script), "bess_coverage.R"))
args <- commandArgs(TRUE)
if (!length(args) %in% c(6L, 7L)) {
  stop("usage: Rscript build_wide_spatial_release.R PRODUCT_DIR PROVIDER_RDS FERGUSON_LAND_INPUT LAND_CLASSES_CSV COLUMN_DICTIONARY_CSV OUTPUT_DIR [CAMREX_NPP_RDS]")
}

build_wide_spatial_release <- function(product_dir, provider_file, extraction_file,
    class_file, dictionary_file, output_dir, camrex_file = NULL) {
  output_dir <- sub("/+$", "", output_dir)
  if (!nzchar(output_dir) || file.exists(output_dir)) stop("choose a new output directory")

  # combine the checked product tables in site metadata order
  paths <- list.files(product_dir, "[.]csv$", recursive = TRUE, full.names = TRUE)
  metadata_path <- paths[basename(paths) == "site_metadata_1170.csv"]
  stopifnot(length(paths) == 9L, length(metadata_path) == 1L)
  metadata <- fread(metadata_path)
  keys <- c("LTER", "Stream_Name")
  stopifnot(nrow(metadata) == 1170L, !anyDuplicated(metadata, by = keys),
    sum(metadata$Has_Spatial_Data == "No") == 130L)
  wide <- metadata[, .(LTER, Stream_Name, Latitude, Longitude, drainSqKm, Site_Flag)]
  for (path in setdiff(paths, metadata_path)) {
    product <- fread(path)
    stopifnot(identical(as.list(product[, ..keys]), as.list(wide[, ..keys])))
    common <- intersect(names(product), names(wide))
    for (field in common) {
      stopifnot(isTRUE(all.equal(product[[field]], wide[[field]], tolerance = 1e-13)))
    }
    for (field in setdiff(names(product), common)) wide[[field]] <- product[[field]]
  }
  stopifnot(ncol(wide) == 1007L)
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
  dictionary <- dictionary[!original_name %in% second_greenup &
    !grepl("^npp_gapfill_[0-9]{4}$", column_name)]
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
    ncol(main) == if (is.null(camrex_file)) 819L else 844L,
    nrow(quality) == 47970L,
    !anyDuplicated(quality, by = c(keys, "year")),
    setequal(quality_dictionary$column_name, names(quality)))
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

do.call(build_wide_spatial_release, as.list(args))
