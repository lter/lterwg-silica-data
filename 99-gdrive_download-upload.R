# ---- ## ----
# google drive interactions
# ---- ## ----
# written by:
# ---- nick j lyon; ----

# purpose:
# ---- download needed inputs from google drive for a given script ----
# ---- upload outputs produced by a given script to google drive ----

# ---- ## ----
# ---- housekeeping ----
# ---- ## ----

# read needed libraries
# install.packages("librarian")
librarian::shelf(tidyverse, googledrive)

# authorize google drive manually (or do it on first interaction with gdrive later)
# ---- googledrive::drive_auth() ----

# clear environment
rm(list = ls())

# make needed sub-folders
# dir.create(path = file.path(), showWarnings = F)

# identify path to location of shared data
(path <- scicomptools::wd_loc(local = F, remote_path = file.path("/", "home", "shares", "lter-si", "si-watershed-extract")))

# ---- ## ----
# ---- reference table - download ----
# ---- ## ----

# identify reference table folder
ref_folder <- googledrive::as_id("https://drive.google.com/drive/u/0/folders/0AIPkWhVuXjqFUk9PVA")

# identify reference table google id
ref_id <- googledrive::drive_ls(path = ref_folder) %>%
  dplyr::filter(name == "Site_Reference_Table")

# check it out
ref_id

# download ref table (overwriting previous downloads)
googledrive::drive_download(file = as_id(ref_id),
  path = file.path(path, "Site_Reference_Table.xlsx"),
  overwrite = T)

# ---- ## ----
# ---- wrtds step 1 (find areas) - upload ----
# ---- ## ----

# define the name/path of this output
wrtds_step1_out <- file.path(path, "WRTDS_Reference_Table_with_Areas_DO_NOT_EDIT.csv")

# if the file exists locally, upload it
if (file.exists(wrtds_step1_out) == TRUE) {
  # upload it to the relevant folder
  googledrive::drive_upload(media = wrtds_step1_out, overwrite = T,
    path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/15FEoe2vu3OAqMQHqdQ9XKpFboR4DvS9M"))

  # otherwise tell the user to run the relevant script
} else {
  message("Output not found! Need to run `01-wrtds-step01_find-areas.R`")
}







# ---- end ----
