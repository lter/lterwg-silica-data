## ------------------------------------------------------- ##
               # Google Drive Interactions
## ------------------------------------------------------- ##
# Written by:
## Nick J Lyon; 

# Purpose:
## Download needed inputs from Google Drive for a given script
## Upload outputs produced by a given script to Google Drive

## ---------------------------------------------- ##
# Housekeeping ----
## ---------------------------------------------- ##

# Read needed libraries
# install.packages("librarian")
librarian::shelf(tidyverse, googledrive)

# Authorize Google Drive manually (or do it on first interaction with GDrive later)
## googledrive::drive_auth()

# Clear environment
rm(list = ls())

# Make needed sub-folders
# dir.create(path = file.path(), showWarnings = F)

# Identify path to location of shared data
(path <- scicomptools::wd_loc(local = T, remote_path = file.path('/', "home", "shares", "lter-si", "si-watershed-extract")))

## ---------------------------------------------- ##
        # Reference Table - Download -----
## ---------------------------------------------- ##

# Identify reference table folder
ref_folder <- googledrive::as_id("https://drive.google.com/drive/u/0/folders/0AIPkWhVuXjqFUk9PVA")

# Identify reference table Google ID
ref_id <- googledrive::drive_ls(path = ref_folder) %>%
  dplyr::filter(name == "Site_Reference_Table")

# Check it out
ref_id

# Download ref table (overwriting previous downloads)
googledrive::drive_download(file = as_id(ref_id),
                            path = file.path(path, "Site_Reference_Table.xlsx"),
                            overwrite = T)

## ---------------------------------------------- ##
# WRTDS Step 1 (Find Areas) - Download -----
## ---------------------------------------------- ##







# End ----
