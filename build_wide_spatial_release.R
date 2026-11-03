# ---- saved release inputs ----

library(data.table)
script <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
source(file.path(dirname(script), "bess_coverage.R"))
args <- commandArgs(TRUE)
if (length(args) != 6L) {
  stop("usage: Rscript build_wide_spatial_release.R PRODUCT_DIR PROVIDER_RDS EXTRACTIONS_RDS LAND_CLASSES_CSV COLUMN_DICTIONARY_CSV OUTPUT_DIR")
}

build_wide_spatial_release <- function(product_dir, provider_file, extraction_file,
    class_file, dictionary_file, output_dir) {
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

  # fill only the checked fields for ferguson's corrected watershed
  site <- which(metadata$LTER == "WesternAustralia" &
    metadata$Stream_Name == "Ferguson River - Dowdells Rd Bridge")
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
  # the saved october 29 results have no watershed id, so check the verified file by checksum
  verified_provider <- "3c1d402eddf0d54cebce5091725ce641"
  if (!identical(unname(tools::md5sum(provider_file)), verified_provider)) {
    stop("use the verified October 29 Ferguson provider results for the corrected watershed")
  }
  static <- readRDS(provider_file)$values
  static <- static[grep("^(elevation_|basin_slope_|major_rock$|rocks_|major_soil$|soil_)", names(static))]
  updates <- c(as.list(setNames(fractions$fraction,
    paste("gee_glc", fractions$Year, fractions$Class, sep = "_"))), static)
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

  # stage the four files together and check their readback before release
  stage <- tempfile("wide-spatial-", tmpdir = dirname(output_dir))
  if (!dir.create(stage)) stop("could not create staging directory")
  on.exit(unlink(stage, recursive = TRUE))
  output <- file.path(stage, files)
  fwrite(wide, output[1L], na = "")
  wide <- read_text(output[1L])
  main <- wide[, wide_dictionary$original_name, with = FALSE]
  setnames(main, wide_dictionary$column_name)
  quality <- rbindlist(lapply(seq_along(years), function(i) {
    part <- wide[, c(keys, quality_columns[[i]]), with = FALSE]
    setnames(part, c(keys, quality_field_patterns$column_name))
    part[, year := as.character(years[i])]
    setcolorder(part, c(keys, "year", quality_field_patterns$column_name))
    part
  }))
  stopifnot(ncol(main) == 843L, nrow(quality) == 47970L,
    !anyDuplicated(quality, by = c(keys, "year")),
    setequal(quality_dictionary$column_name, names(quality)))
  tables <- list(main, metadata, dictionary, quality)
  for (i in seq_along(output)) {
    fwrite(tables[[i]], output[i], na = "")
    actual <- if (i == 2L) fread(output[i]) else read_text(output[i])
    stopifnot(isTRUE(all.equal(as.list(actual), as.list(tables[[i]]), tolerance = 1e-13)))
  }
  if (file.exists(output_dir) || !file.rename(stage, output_dir)) stop("could not finish the release")
  invisible(file.path(output_dir, files))
}

do.call(build_wide_spatial_release, as.list(args))
