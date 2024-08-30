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
(path <- scicomptools::wd_loc(local = F, remote_path = file.path('/', "home", "shares", "lter-si", "si-watershed-extract")))

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
    # WRTDS Step 1 (Find Areas) - Upload -----
## ---------------------------------------------- ##

# Define the name/path of this output
wrtds_step1_out <- file.path(path, "WRTDS_Reference_Table_with_Areas_DO_NOT_EDIT.csv")

# If the file exists locally, upload it
if(file.exists(wrtds_step1_out)){
  
  # Upload it to the relevant folder
  googledrive::drive_upload(media = wrtds_step1_out, overwrite = T,
                            path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/15FEoe2vu3OAqMQHqdQ9XKpFboR4DvS9M"))
  
  # Otherwise tell the user to run the relevant script
} else { message("Output not found! Need to run `01-wrtds-step01_find-areas.R`") }

  





# End ----
