# ---- saved release inputs ----
library(data.table)
script <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
source(file.path(dirname(script), "bess_coverage.R"))

# read explicit paths so the same build can run outside the original workspace
args <- commandArgs(TRUE)
preflight <- "--preflight" %in% args
args <- args[args != "--preflight"]
if (length(args) != 5L) {
  stop("usage: Rscript build_updated_combined.R BASE_CSV BASE_METADATA ANNUAL_GPP_RDS SAVED_SPATIAL_CSV OUTPUT_DIR [--preflight]")
}
input <- args[1L]
meta_input <- args[2L]
bess_path <- args[3L]
saved_path <- args[4L]
output_dir <- args[5L]
if (file.exists(output_dir)) stop("choose a new output directory")

read_text <- function(...) {
  fread(..., colClasses = "character", na.strings = NULL, showProgress = FALSE)
}
site_key <- function(x) paste(x$LTER, x$Stream_Name, sep = "\r")
year_key <- function(x) paste(site_key(x), x$year, sep = "\r")
meta <- read_text(meta_input)
sites <- meta[Record_Type == "site"]
saved <- fread(saved_path)
bess <- as.data.table(readRDS(bess_path))
stopifnot(nrow(sites) == 1170L, !anyDuplicated(site_key(sites)),
  !anyDuplicated(site_key(saved)), !anyDuplicated(year_key(bess)),
  nrow(bess) == 42640L, identical(sort(unique(bess$year)), 1982:2022))
current <- sites$Has_Spatial_Data == "Yes"
i <- match(site_key(sites), site_key(saved))
match_watershed <- function(x, y) tolower(trimws(x)) == tolower(trimws(y))
eligible <- current & match_watershed(sites$Watershed_File_Name, saved$Shapefile_Name[i]) &
  abs(as.numeric(sites$drainSqKm) - saved$drainSqKm[i]) <= pmax(.001, as.numeric(sites$drainSqKm) * .001)
eligible[is.na(eligible)] <- FALSE
i[!eligible] <- NA_integer_
b <- match(site_key(bess), site_key(sites))
stopifnot(!anyNA(b), all(current[b]),
  all(match_watershed(bess$Shapefile_Name, sites$Watershed_File_Name[b])),
  sum(current) == 1040L, sum(eligible) == 881L)
check_bess_coverage(bess$gpp_gC_m2_year, bess$valid_days, bess$valid_area_fraction, bess$year)
stopifnot(!anyNA(bess$valid_area_fraction))
population <- grep("^human_population_.*_[0-9]{4}$", names(saved), value = TRUE)
population_fields <- unique(sub("_[0-9]{4}$", "", population))
human <- grep("^human_(dam|fertilizer|wastewater)_", names(saved), value = TRUE)
river <- grep("^(hydrorivers_|HYRIV_ID$|MAIN_RIV$|NEXT_DOWN$|HYBAS_L12$|downstream_|upland_skm$|reach_length_km$|discharge_est_cms$|endorheic$|drains_to_ocean$|coast_distance_status$|ORD_|snap_distance_km$|point_match_method$)", names(saved), value = TRUE)
stopifnot(length(population_fields) == 3L, length(human) == 22L,
  all(c("downstream_to_ocean_km", "downstream_to_outlet_km", "endorheic") %in% river))
static <- saved[i, c(human, river), with = FALSE]
stopifnot(all(is.na(static$downstream_to_ocean_km[which(static$endorheic == TRUE)])))
static[, human_results_status := fifelse(eligible, "saved_watershed_match", "not_available_for_current_watershed")]
bess_fields <- c("gpp_gC_m2_year", "valid_days", "valid_area_fraction", "bess_trend_flag", "bess_trend_note")
setnames(bess, bess_fields, c("bess_gpp_gC_m2_year", "bess_gpp_valid_days", "bess_gpp_valid_area_fraction", "bess_trend_flag", "bess_trend_note"))
bess_fields <- c("bess_gpp_gC_m2_year", "bess_gpp_valid_days", "bess_gpp_valid_area_fraction", "bess_trend_flag", "bess_trend_note")
bess_keys <- year_key(bess)
site_keys <- site_key(sites)
header <- names(fread(input, nrows = 0L))
# follow the updated release selection for overlapping legacy products
retired <- grep("^(temp_|precip_|evapotrans_|snow_|greenup_cycle1_)", header, value = TRUE)
keep <- setdiff(header, retired)

# attach annual values on chemistry year and static values on verified site
enrich <- function(x) {
  si <- match(site_key(x), site_keys)
  stopifnot(!anyNA(si))
  bi <- match(year_key(x), bess_keys)
  additions <- cbind(bess[bi, ..bess_fields], static[si])
  for (field in population_fields) {
    values <- rep(NA_real_, nrow(x))
    for (year in intersect(unique(x$year), as.character(2000:2024))) {
      rows <- which(x$year == year)
      values[rows] <- saved[[paste0(field, "_", year)]][i[si[rows]]]
    }
    additions[[field]] <- values
  }
  cbind(x[, ..keep], additions)
}

# map the new fields and retain existing site and chemistry notes
sample <- enrich(read_text(input, nrows = 100))
added <- setdiff(names(sample), keep)
stopifnot(!anyDuplicated(names(sample)), ncol(sample) == 120L,
  all(c("npp_kgC_m2_year", "greenup_cycle0_MMDD") %in% keep))
note <- ifelse(added %in% bess_fields,
  "BESSv2 monthly means weighted by calendar days. Annual 1982-2022, site and watershed matched. GPP in g C/m²/year. Totals require all months and at least 80% area coverage. Original zeros and source flags retained.",
  ifelse(added %in% population_fields,
    "LandScan Global 2000-2024, matched to chemistry year and verified watershed. Units are people, people/km², or coverage fraction as named.",
    ifelse(added %in% river,
      "HydroRIVERS saved routing, matched by watershed name and area. Distances in km. Ocean distance is missing for closed basins.",
      ifelse(added == "human_results_status",
        "Whether the current watershed matches the saved extraction by site, watershed name and area.",
        paste("Saved watershed summary from",
          fifelse(grepl("^human_dam_", added), "Global Dam Watch v1.0",
            fifelse(grepl("^human_fertilizer_", added), "NPKGRIDS circa 2020", "HydroWASTE v1.0")),
          "with units and record counts as named. Unmatched watersheds are blank.")))))
meta <- meta[!(Record_Type == "spatial column match" & Combined_Column %in% retired)]
meta <- rbindlist(list(meta, data.table(Record_Type = "spatial column match",
  Source_Column = added, Combined_Column = added, Note = note)), use.names = TRUE, fill = TRUE)
stopifnot(all(meta[Record_Type == "spatial column match", Combined_Column] %in% names(sample)))
if (preflight) {
  cat("preflight passed:", length(added), "added fields;", sum(eligible), "saved watershed matches\n")
  quit(status = 0L)
}

# write and read back in blocks to keep memory use small
stage <- paste0(sub("/+$", "", output_dir), ".partial")
if (!dir.create(stage, recursive = FALSE)) stop("could not create staging directory")
out <- file.path(stage, "20261102_updated_combined.csv")
out_meta <- file.path(stage, "20261102_updated_combined_metadata.csv")
source_file <- file(input, "r")
source_header <- readLines(source_file, n = 1L)
rows <- 0L
seen <- character()
flow <- 0L
repeat {
  lines <- readLines(source_file, n = 20000L)
  if (!length(lines)) break
  x <- read_text(text = paste(c(source_header, lines), collapse = "\n"))
  stopifnot(nrow(x) == length(lines), identical(names(x), header))
  y <- enrich(x)
  fwrite(y, out, append = rows > 0L, na = "")
  rows <- rows + nrow(x)
  seen <- union(seen, site_key(x))
  flow <- flow + sum(nzchar(x$Qcms))
}
close(source_file)
stopifnot(rows == 4686420L, setequal(seen, site_keys), flow == 3593358L)
fwrite(meta, out_meta, na = "")
metadata_readback <- read_text(out_meta)
stopifnot(nrow(metadata_readback) == nrow(meta),
  identical(names(metadata_readback), names(meta)),
  all(vapply(names(meta), function(column) {
    expected <- as.character(meta[[column]])
    expected[is.na(expected)] <- ""
    identical(metadata_readback[[column]], expected)
  }, logical(1))))
source_file <- file(input, "r")
output_file <- file(out, "r")
readLines(source_file, n = 1L) |> invisible()
out_header <- readLines(output_file, n = 1L)
checked <- 0L
repeat {
  original <- readLines(source_file, n = 20000L)
  observed <- readLines(output_file, n = 20000L)
  stopifnot(length(original) == length(observed))
  if (!length(original)) break
  x <- read_text(text = paste(c(source_header, original), collapse = "\n"))
  y <- read_text(text = paste(c(out_header, observed), collapse = "\n"))
  expected <- enrich(x)
  stopifnot(nrow(y) == nrow(x), identical(names(y), names(expected)))
  for (field in keep) stopifnot(identical(y[[field]], x[[field]]))
  for (field in added) {
    value <- expected[[field]]
    if (is.numeric(value)) {
      actual <- as.numeric(y[[field]])
      stopifnot(identical(is.na(actual), is.na(value)),
        isTRUE(all.equal(actual, value, tolerance = 1e-14)))
    } else {
      value[is.na(value)] <- ""
      stopifnot(identical(y[[field]], as.character(value)))
    }
  }
  checked <- checked + nrow(y)
}
close(source_file)
close(output_file)
stopifnot(checked == rows, !file.exists(output_dir), file.rename(stage, output_dir))
cat("validation passed:", rows, "rows;", length(seen), "sites;", flow, "discharge matches;",
  length(names(sample)), "columns;", nrow(meta), "metadata rows\n")
