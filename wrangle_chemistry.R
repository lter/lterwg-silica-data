## ---------------------------------------------------------- ##
# Chemistry - Harmonization & Wrangling
## ---------------------------------------------------------- ##
# Script author(s): Nick J Lyon

# PURPOSE:
## Ingest all chemistry files and perform harmonization necessary for creation of 'master' file
## Wrangle the first version of the 'master' file into a tidy, analysis-ready format

## -------------------------------------------- ##
# Housekeeping ----
## -------------------------------------------- ##

# Load libraries
# install.packages("librarian")
librarian::shelf(googledrive, purrr, readxl, stringr, supportR, tidyverse)

# Clear environment
rm(list = ls())

# Create a folder for any needed data keys
dir.create(path = file.path("keys"), showWarnings = F)

# Create a folder for inputs & and outputs
dir.create(path = file.path("chem_raw"), showWarnings = F)
dir.create(path = file.path("chem_tidy"), showWarnings = F)

## -------------------------------------------- ##
# Data Acquisition ----
## -------------------------------------------- ##

# Identify and download the data key
googledrive::drive_ls(path = googledrive::as_id("https://drive.google.com/drive/folders/1hbkUsTdo4WAEUnlPReOUuXdeeXm92mg-")) %>%
  dplyr::filter(name == "SiSyn_Data_Key.xlsx") %>%
  googledrive::drive_download(file = .$id, path = file.path("keys", .$name), overwrite = T)

# Read in the key
key_v1 <- readxl::read_excel(path = file.path("keys", "SiSyn_Data_Key.xlsx")) %>%
  # Subset to only chemistry data
  dplyr::filter(Data_type == "chemistry")

# Check structure
dplyr::glimpse(key_v1)

# Identify the old master chemistry file
old_primary <- googledrive::drive_ls(path = googledrive::as_id("https://drive.google.com/drive/folders/1dTENIB5W2ClgW0z-8NbjqARiaGO2_A7W")) %>%
  dplyr::filter(name == "20221030_masterdata_chem.csv")

# Identify all raw chemistry files
chem_drive <- googledrive::drive_ls(path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1da8AkSvforPehp-gPJsmA7-pPAJ913mz")) %>%
  # Attach the old master chem file
  dplyr::bind_rows(old_primary)
  
# Check which files are in the data key but not the Drive and vice versa
supportR::diff_check(old = unique(key_v1$Raw_Filename), new = unique(chem_drive$name))

# Filter the Drive files to only those in the data key
chem_drive_actual <- chem_drive %>%
  dplyr::filter(name %in% key_v1$Raw_Filename)

# Download these files
purrr::walk2(.x = chem_drive_actual$id, .y = chem_drive_actual$name,
             .f = ~ googledrive::drive_download(file = .x, overwrite = T,
                                                path = file.path("chem_raw", .y)))

## -------------------------------------------- ##
# Data Harmonizing ----
## -------------------------------------------- ##












# End ----
