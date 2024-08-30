## ------------------------------------------------------- ##
                # WRTDS - Find Watershed Area
## ------------------------------------------------------- ##
# Written by:
## Nick J Lyon

# Purpose:
## Find area of drainage basins for sites where that is not known from expert sources

## ---------------------------------------------- ##
                  # Housekeeping ----
## ---------------------------------------------- ##
# Read needed libraries
# install.packages("librarian")
librarian::shelf(tidyverse, googledrive, sf, terra, nngeo, scicomptools)

# Clear environment
rm(list = ls())

# Identify path to location of shared data
(path <- scicomptools::wd_loc(local = F, remote_path = file.path('/', "home", "shares", "lter-si", "si-watershed-extract")))

## ---------------------------------------------- ##
        # Site Coordinate Preparation ----
## ---------------------------------------------- ##

# If you've never used the `googledrive` R package, you'll need to "authorize" it
## 1) Run the below line with your Google email that has access to the needed folders
# googledrive::drive_auth(email = "...@gmail.com")
## 1B) Choose which Google account to proceed with
## 2) Check the "see, edit, create, and ..." box
## 3) Click "Continue" at the bottom
## 4) Copy the "authorization code"
## 5) Paste it into the field in the Console

# Identify reference table Google ID
ref_id <- googledrive::drive_ls(as_id("https://drive.google.com/drive/u/0/folders/0AIPkWhVuXjqFUk9PVA")) %>%
  dplyr::filter(name == "Site_Reference_Table")

# Download ref table (overwriting previous downloads)
googledrive::drive_download(file = as_id(ref_id),
                            path = file.path(path, "Site_Reference_Table.xlsx"),
                            overwrite = T)

# Read in reference table of all sites
sites_v0 <- readxl::read_excel(path = file.path(path, "Site_Reference_Table.xlsx"))

# Do some pre-processing to pare down to only desired information
sites_v1 <- sites_v0 %>%
  # Filter out sites not used in WRTDS
  dplyr::filter(tolower(Use_WRTDS) != "no") %>%
  # Drop the WRTDS column now that we've subsetted by it
  dplyr::select(-Use_WRTDS) %>%
  # Make a uniqueID column
  dplyr::mutate(uniqueID = paste0(LTER, "_", Stream_Name), 
                .before = dplyr::everything()) %>%
  # Make lat/long truly numeric
  dplyr::mutate(Latitude = as.numeric(Latitude),
                Longitude = as.numeric(Longitude))

# Glimpse that
dplyr::glimpse(sites_v1)

# Split off sites where drainage basin area is known
known_area <- sites_v1 %>%
  dplyr::filter(is.na(drainSqKm) == F)

# Split off where area is NOT known
unk_area <- sites_v1 %>%
  dplyr::filter(is.na(drainSqKm) == T)

# Should be all of the data in either one or the other
nrow(known_area) + nrow(unk_area)
nrow(sites_v1)

# If we don't know area, we need coordinates in lat / long!
unk_area %>% 
  dplyr::filter(is.na(Latitude) | is.na(Longitude) |
                  abs(Latitude) > 90 | abs(Longitude) > 180) %>%
  dplyr::pull(uniqueID)
## Anything here needs to have areas OR coordinates added _in the reference table_
## If MCM, needs area *directly*

# Drop malformed coordinates
unk_v2 <- unk_area %>%
  dplyr::filter(abs(Latitude) <= 90 & abs(Longitude) <= 180)

# What sites had bad coordinates?
setdiff(unk_area$uniqueID, unk_v2$uniqueID)

# Get an explicitly spatial version
sites_spatial <- sf::st_as_sf(unk_v2, coords = c("Longitude", "Latitude"), crs = 4326)

# Check it out
str(sites_spatial)

## ---------------------------------------------- ##
      # Load HydroSHEDS Basin Delineations ----
## ---------------------------------------------- ##

# See HydroSHEDS website (link below) for download links & tech documentation
## https://www.hydrosheds.org/page/hydrobasins

# Load in relevant files
arctic <- sf::st_read(file.path(path, "hydrosheds-raw", "hybas_ar_lev00_v1c.shp"))
# xmin: -180       ymin: 51.20833   xmax: -61.09936   ymax: 83.21723
asia <- sf::st_read(file.path(path, "hydrosheds-raw", "hybas_as_lev00_v1c.shp"))
# xmin: 57.60833   ymin: 1.166667   xmax: 150.9215    ymax: 55.9375
oceania <- sf::st_read(file.path(path, "hydrosheds-raw", "hybas_au_lev00_v1c.shp"))
# xmin: 94.97022   ymin: -55.11667  xmax: 180.0006    ymax: 24.30053
greenland <- sf::st_read(file.path(path, "hydrosheds-raw", "hybas_gr_lev00_v1c.shp"))
# xmin: -73.00067  ymin: 59.74167   xmax: -11.34932   ymax: 83.62564
north_am <- sf::st_read(file.path(path, "hydrosheds-raw", "hybas_na_lev00_v1c.shp"))
# xmin: -137.9625  ymin: 5.495833   xmax: -52.61605   ymax: 62.74232
south_am <- sf::st_read(file.path(path, "hydrosheds-raw", "hybas_sa_lev00_v1c.shp"))
# xmin: -92.00068  ymin: -55.9875   xmax: -32.37453   ymax: 14.88273
siberia <- sf::st_read(file.path(path, "hydrosheds-raw", "hybas_si_lev00_v1c.shp"))
# xmin: 58.95833   ymin: 45.5625    xmax: 180         ymax: 81.26735
africa <- sf::st_read(file.path(path, "hydrosheds-raw", "hybas_af_lev00_v1c.shp"))
# xmin: -18.1631   ymin: 54.5381    xmax: -34.8370    ymax: 37.5631
europe <- sf::st_read(file.path(path, "hydrosheds-raw", "hybas_eu_lev00_v1c.shp"))
# xmin: -24.5423   ymin: 69.5545    xmax: 12.5913     ymax: 81.8589

# Antarctica is not supported by this product so we just want everything else
# (the minimum latitude is -55.2° for any of the slices)

# Examine structure of one for greater detail
str(arctic)
# page 6 of the technical documentation contains an attribute table that defines these fields
# PFAF_[number] refers to the level of specificity in the basin delineation
## PFAF_1 = separates continents from one another
## PFAF_# + N = progressively finer separation

# Bind our files into a single (admittedly giant) object
all_basins <- rbind(arctic, asia, oceania, greenland, north_am,
                    south_am, siberia, africa, europe)

# For ease of manipulation get just the HYBAS_ID
# These uniquely identify the most specific level so they work upstream too (no pun intended)
basin_simp <- all_basins %>%
  dplyr::select(HYBAS_ID, NEXT_DOWN, NEXT_SINK, SUB_AREA)

# Re-check structure
str(basin_simp)

## ---------------------------------------------- ##
            # Identify Focal Polygon ----
## ---------------------------------------------- ##

# Pre-emptively resolve an error with 'invalid spherical geometry'
sf::sf_use_s2(F)
## s2 processing assumes that two points lie on a sphere
## earlier form of processing assumes two points lie on a plane

# Pull out HYBAS_IDs at site coordinates
sites_actual <- sites_spatial %>%
  dplyr::mutate(
    # Find the interaction points as integers
    ixn = as.integer(sf::st_intersects(geometry, basin_simp)),
    # If a given interaction is not NA (meaning it does overlap)...
    HYBAS_ID = ifelse(test = !is.na(ixn),
                      # ...retain the HYBAS_ID of that interaction...
                      yes = basin_simp$HYBAS_ID[ixn],
                      #...if not, retain nothing
                      no = '') ) %>%
  # And handle a missing HYBAS ID manually
  dplyr::mutate(HYBAS_ID = ifelse(test = LTER == "Finnish Environmental Institute" & 
                                    Discharge_File_Name == "Site28208_Q",
                                  # This ID is from a *very* close neighboring site
                                  yes = "2000029960",
                                  no = HYBAS_ID))

# And to make our lives easier, check out which continents we actually need
sort(unique(stringr::str_sub(sites_actual$HYBAS_ID, 1, 1)))
## 1 = Africa; 2 = Europe; 3 = Siberia; 4 = Asia; 5 = Australia
## 6 = South America; 7 = North America; 8 = Arctic (North America); 9 = Greenland 

# The missing IDs are Antarctica streams
sites_actual %>%
  dplyr::filter(nchar(stringr::str_sub(sites_actual$HYBAS_ID, 1, 1)) == 0) %>%
  dplyr::select(LTER) %>%
  sf::st_drop_geometry() %>%
  unique()

# Prepare only needed HydroSheds 'continents'
basin_needs <- rbind(africa, europe, oceania, north_am, arctic)
basin_needs$HYBAS_ID <- as.character(basin_needs$HYBAS_ID)

# Get area for our focal polygons
sites_actual$SUB_AREA <- basin_needs$SUB_AREA[match(sites_actual$HYBAS_ID, basin_needs$HYBAS_ID)]

# Check the object again
dplyr::glimpse(sites_actual)
## This object has polygons defined at the finest possible level
## We may want to visualize aggregated basins so let's go that direction now

# Drop the individual regional/continental objects
# Clean up environment to have less data stored as we move forward
rm(list = c("arctic", "asia", "oceania", "greenland", "north_am", 
            "south_am", "siberia", "africa", "europe"))

## ---------------------------------------------- ##
      # Load Modified HydroSHEDS Functions ----
## ---------------------------------------------- ##

# These are modified from someone's GitHub functions to accept non-S4 objects
# Link to originals here: https://rdrr.io/github/ECCC-MSC/Basin-Delineation/

# First function finds just the next upstream polygon(s)
find_next_up <- function(HYBAS, HYBAS.ID, ignore.endorheic = F){
  
  # Process sf object into a regular dataframe
  HYBAS_df <- HYBAS
  
  # Find next upstream polygon(s) as character
  upstream.ab <- HYBAS_df[HYBAS_df$NEXT_DOWN == HYBAS.ID, c("HYBAS_ID", "ENDO")]
  
  if (ignore.endorheic){
    upstream.ab <- upstream.ab[upstream.ab$ENDO != 2, ]
  }
  return(as.character(upstream.ab$HYBAS_ID))
}

# Second function iteratively runs through the first one to find all of the upstream polygons
find_all_up <- function(HYBAS, HYBAS.ID, ignore.endorheic = F, split = F){
  
  # make containers
  HYBAS.ID <- as.character(HYBAS.ID)
  HYBAS.ID.master <- list()
  
  #Get the possible upstream 'branches'
  direct.upstream <- find_next_up(HYBAS = HYBAS, HYBAS.ID = HYBAS.ID,
                                  ignore.endorheic = ignore.endorheic)
  
  # for each branch iterate upstream until only returning empty results
  for (i in direct.upstream){
    run <- T
    HYBAS.ID.list <- i
    sub.basins <- i # this is the object that gets passed to find_next_up in each iteration
    while (run){
      result.i <- unlist(lapply(sub.basins, find_next_up,
                                HYBAS = HYBAS, ignore.endorheic = ignore.endorheic))
      
      if (length(result.i) == 0){ run <- F } # Stopping criterion
      HYBAS.ID.list <- c(HYBAS.ID.list, result.i)
      sub.basins <- result.i
    }
    HYBAS.ID.master[[i]] <- HYBAS.ID.list
  }
  
  if (!split){ HYBAS.ID.master <- as.character(unlist(HYBAS.ID.master)) }
  
  return(HYBAS.ID.master)
}

## ---------------------------------------------- ##
          # Identify Upstream Polygons ----
## ---------------------------------------------- ##

# Because some sites fall into the same focal HydroSHEDS polygon,
# It makes sense to identify areas / polygons based on that focal polygon
# rather than stream name to save on computation time

# Drop geometry information for HydroSHEDS basins
basin_df <- basin_needs %>%
  sf::st_drop_geometry()

# Create an empty list
id_list <- list()

# Identify area for sites where we don't know it already
for(focal_poly in unique(sites_actual$HYBAS_ID)){
  # for(focal_poly in "7000073120"){
  
  # Create/identify name and path of file
  poly_file <- file.path(path, 'hydrosheds-basin-ids',
                         paste0(focal_poly, '_Upstream_IDs.csv'))
  
  # If we've already found this polygon's upstream polygons:
  if (fs::file_exists(poly_file) == TRUE) {
    
    # Read the CSV
    hydro_df <- read.csv(file = poly_file)
    
    # Add to the list
    id_list[[as.character(focal_poly)]] <- hydro_df
    
    # Message outcome
    message("Upstream HydroSheds IDs for HYBAS ID '", focal_poly, "' already identified.")
    
    # If we *don't* have the polygon, continue!
  } else {
    
    # Print start-up message
    message( "Processing for HYBAS ID '", focal_poly, "' begun at ", Sys.time())
    
    # Identify all upstream shapes
    fxn_out <- find_all_up(HYBAS = basin_df, HYBAS.ID = focal_poly)
    
    # Make a dataframe of this
    hydro_df <- data.frame(focal_poly = as.character(rep(focal_poly, (length(fxn_out) + 1))),
                           hybas_id = c(focal_poly, fxn_out))
    
    # Save this out
    write.csv(x = hydro_df, file = poly_file, na = '', row.names = F)
    
    # And add it to the list
    id_list[[as.character(focal_poly)]] <- hydro_df
    
    # Print finishing message
    message( "Processing for HYBAS ID '", focal_poly, "' finished at ", Sys.time())
    
  } # Close `else` clause
} # Close `loop`

# Unlist the list
hydro_out <- id_list %>%
  purrr::list_rbind(x = .) %>%
  dplyr::filter(!is.na(focal_poly))

# Check the structure
dplyr::glimpse(hydro_out)

## ---------------------------------------------- ##
                # Wrangle Output -----
## ---------------------------------------------- ##

# Pre-emptively resolve an error with 'invalid spherical geometry'
sf::sf_use_s2(F)

# change value to numeric to match across datasets
basin_needs$HYBAS_ID <- as.numeric(basin_needs$HYBAS_ID)

# Strip the polygons that correspond to those IDs - STOPPED HERE!
hydro_poly_df <- hydro_out %>%
  # Make the HydroBasins ID column have an identical name between the df and sf objects
  dplyr::rename(HYBAS_ID = hybas_id) %>%
  # Attach everything in the polygon variant
  ## Necessary because of some polygons are found in >1 uniqueID
  dplyr::left_join(basin_needs, by = 'HYBAS_ID') %>%
  # Within uniqueID...
  dplyr::group_by(focal_poly) %>%
  # ...sum sub-polygon areas and combine sub-polygon geometries
  dplyr::summarise(drainSqKm = sum(SUB_AREA, na.rm = TRUE),
                   geometry = sf::st_union(geometry)) %>%
  # Need to make the class officially sf again before continuing
  sf::st_as_sf() %>%
  # Then drop geometry
  sf::st_drop_geometry() %>%
  # Get a character version of the HYBAS ID and drop the old one
  dplyr::mutate(HYBAS_ID = as.character(focal_poly), 
                .before = dplyr::everything()) %>%
  dplyr::select(-focal_poly)

# Check structure
dplyr::glimpse(hydro_poly_df)

# Combine polygon IDs with the unknown areas dataframe
unk_actual <- sites_actual %>%
  # Drop old (and empty) drainage area column
  dplyr::select(-drainSqKm) %>%
  # Drop geometry
  sf::st_drop_geometry() %>%
  # Make HYBAS ID into a character
  dplyr::mutate(HYBAS_ID = as.character(HYBAS_ID)) %>%
  # Join on newly identified areas
  dplyr::left_join(y = hydro_poly_df, by = c("HYBAS_ID"))

# Glimpse this too
dplyr::glimpse(unk_actual)

# Then recombine the known and unknown areas files
ref_table_actual <- known_area %>%
  # Use bind_rows to get the correct column order back
  dplyr::bind_rows(unk_actual) %>%
  # Drop geometry
  sf::st_drop_geometry() %>%
  # Remove some unneeded columns
  dplyr::select(-uniqueID, -ixn, -HYBAS_ID, -SUB_AREA)

# Glimpse it
dplyr::glimpse(ref_table_actual)
# view(ref_table_actual)

# Make sure that no drainage areas are missing
nrow(filter(ref_table_actual, is.na(drainSqKm)))
nrow(filter(ref_table_actual, nchar(drainSqKm) == 0))
## Both above should return "0"

# Drop any that occur
ref_table_final <- ref_table_actual %>%
  dplyr::filter(!is.na(drainSqKm)) %>%
  dplyr::filter(drainSqKm != 0)

# How many areas do we gain by doing this operation?
sites_v1 %>% dplyr::filter(is.na(drainSqKm)) %>% nrow()
ref_table_final %>% dplyr::filter(is.na(drainSqKm)) %>% nrow()

# Define name/path of this file
out_name <- file.path(path, "WRTDS_Reference_Table_with_Areas_DO_NOT_EDIT.csv")

# Save this locally
write.csv(x = ref_table_final, na = "", row.names = F, file = out_name)

# Now upload this as well to the GoogleDrive
googledrive::drive_upload(media = out_name, overwrite = T,
                          path = as_id("https://drive.google.com/drive/u/0/folders/15FEoe2vu3OAqMQHqdQ9XKpFboR4DvS9M"))

# End ----
