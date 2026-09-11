# ---- ## ----
# wrtds centralized workflow
# ---- ## ----
# WRTDS = Weighted Regressions on Time, Discharge, and Season
# ---- nick j lyon, kathi jo jankowski, keira johnson ----

# ---- ## ----
# ---- housekeeping ----
# ---- ## ----
# load libraries
# install.packages("librarian")
librarian::shelf(tidyverse, lubridate, EGRET, EGRETci, supportR, scicomptools, zoo, lter / HERON,
  tsibble, googledrive)

# clear environment
rm(list = ls())

# need to specify correct path for local versus server work
(path <- scicomptools::wd_loc(local = FALSE, remote_path = file.path("/", "home", "shares", "lter-si", "WRTDS")))

# create folders for the raw downloaded files (i.e., sources) & wrtds inputs (created by this script)
dir.create(path = file.path(path, "WRTDS Source Files"), showWarnings = F)
dir.create(path = file.path(path, "WRTDS Inputs"), showWarnings = F)

# define the names of the drive files we need
file_names <- c("WRTDS_Reference_Table_with_Areas_DO_NOT_EDIT.csv", # no.1 simplified ref table
  "Site_Reference_Table", # no.2 full ref table
  "20260106_masterdata_discharge.csv", # no.3 main discharge ## update this file with new discharge!!
  "20260105_masterdata_chem.csv", # no.4 main chemistry ## update this file with new chemistry!!
  "Data_Cropping_WRTDS", # no.5 data cropping for chemistry (si)
  "Discharge_Cropping_WRTDS")  # no.6 data cropping for discharge

# find those files' ids
ids <- googledrive::drive_ls(as_id("https://drive.google.com/drive/u/0/folders/15FEoe2vu3OAqMQHqdQ9XKpFboR4DvS9M")) %>%
  dplyr::bind_rows(googledrive::drive_ls(as_id("https://drive.google.com/drive/u/0/folders/1hbkUsTdo4WAEUnlPReOUuXdeeXm92mg-"))) %>%
  dplyr::bind_rows(googledrive::drive_ls(as_id("https://drive.google.com/drive/u/0/folders/1dTENIB5W2ClgW0z-8NbjqARiaGO2_A7W"))) %>%
  dplyr::bind_rows(googledrive::drive_ls(as_id("https://drive.google.com/drive/u/0/folders/0AIPkWhVuXjqFUk9PVA"))) %>%
  # ---- and filter out any extraneous files ----
  dplyr::filter(name %in% file_names)


# check that no file names have changed
for (file in file_names) {
  if (!file %in% ids$name) {
    message("File '", file, "' not found.")
  } else {
    message("File '", file, "' found!")
  }
}

# download the files we want
purrr::walk2(.x = ids$name, .y = ids$id,
  .f = ~ googledrive::drive_download(file = .y, overwrite = T,
    path = file.path(path, "WRTDS Source Files", .x)))

# read in each of these files
areas <- read.csv(file = file.path(path, "WRTDS Source Files", file_names[1]))
ref_v0 <- readxl::read_excel(path = file.path(path, "WRTDS Source Files",
  paste0(file_names[2], ".xlsx")))
disc_v0 <- read.csv(file = file.path(path, "WRTDS Source Files", file_names[3]))
chem_v0 <- read.csv(file = file.path(path, "WRTDS Source Files", file_names[4]))
chemcrop_v0 <- readxl::read_excel(path = file.path(path, "WRTDS Source Files",
  paste0(file_names[5], ".xlsx")))
discrop_v0 <- readxl::read_excel(path = file.path(path, "WRTDS Source Files",
  paste0(file_names[6], ".xlsx")))

# ---- ## ----
# ---- process raw files (v0 -> v1) ----
# ---- ## ----

# generate a complete reference table (areas + other info)
ref_table <- ref_v0 %>%
  # pare down to only some columns
  dplyr::select(LTER, Discharge_File_Name, Stream_Name, Use_WRTDS) %>%
  # standardize wrtds column
  dplyr::mutate(Use_WRTDS = tolower(Use_WRTDS)) %>%
  # drop non-unique rows
  dplyr::distinct() %>%
  # filter to only rivers where we *do* want to use wrtds
  dplyr::filter(Use_WRTDS == "yes") %>%
  # attach areas
  dplyr::left_join(y = dplyr::select(areas, LTER, Discharge_File_Name, Stream_Name, drainSqKm),
    by = c("LTER", "Discharge_File_Name", "Stream_Name")) %>%

  # generate a 'stream id' column that combines lter and chemistry stream name
  dplyr::mutate(Stream_ID = paste0(LTER, "__", Stream_Name),
    .before = dplyr::everything())

# should be no missing areas
ref_table %>%
  dplyr::filter(is.na(drainSqKm) | nchar(drainSqKm) == 0)

# not sure why i had to assign this by hand for gro obidos, i think because of "areas" dataset
# ref_table[91,6] <- 4701550

# check structure
dplyr::glimpse(ref_table)

# any *discharge* rivers not in reference table? or vice versa
# this list will reflect rivers that were removed based on "use_WRTDS" of "no"
# setdiff(x = unique(disc_v0$Discharge_File_Name),y = unique(ref_table$Discharge_File_Name))
setdiff(x = unique(ref_table$Discharge_File_Name), y = unique(disc_v0$Discharge_File_Name))

# wrangle discharge
# fixes issue with OSTEGLO_Q here
disc_v1 <- disc_v0 %>%
  # rename site column as it appears in the reference table
  dplyr::rename(Discharge_File_Name = Discharge_File_Name) %>%
  # fix any broken names (special characters from scandinavia)
  dplyr::mutate(Discharge_File_Name = gsub(pattern = "ØSTEGLO_Q", replacement = "OSTEGLO_Q",
    x = Discharge_File_Name)) %>%
  # dplyr::mutate(Stream_Name = dplyr::case_match(Stream_Name,
  #                                             "Kiiminkij 13010 4-tien s"~"Kiiminkij 13010 4tien s",
  #                                            .default = Stream_Name))
  # Attach the reference table object;
  # Master discharge file now has "Stream_Name" and "LTER" columns so removing before joining to avoid duplication
  dplyr::left_join(y = dplyr::select(ref_table, -drainSqKm, -Stream_Name, -LTER),
    by = c("Discharge_File_Name")) %>%
  # drop any rivers we don't want to use in wrtds
  dplyr::filter(Use_WRTDS == "yes") %>%
  dplyr::select(-Use_WRTDS) %>%
  # generate a 'stream id' column that combines lter and chemistry stream name
  dplyr::mutate(Stream_ID = paste0(LTER, "__", Stream_Name),
    .before = dplyr::everything())

# any rivers without a corresponding chemistry name?
disc_v1 %>%
  dplyr::filter(is.na(Stream_Name) | nchar(Stream_Name) == 0) %>%
  dplyr::pull(Discharge_File_Name) %>%
  unique()

# check structure
dplyr::glimpse(disc_v1)

# any *chemistry* rivers not in reference table?
# fyi -- renamed finnish streams in master chemistry so they all have actual names not site #'s
# set them to "no" in "Use_WRTDS" column in reference table

# Finnish Names don't read into R well, they say they are missing, but they are not, I adjusted the Finnish Stream names that # differ between chem and ref table when creating chem_v1 below
setdiff(x = unique(ref_table$Stream_Name), y = unique(chem_v0$Stream_Name))

# wrangle chemistry as well
chem_v1 <- chem_v0 %>%
  # pare down to only particular solutes that we're interested in
  dplyr::filter(variable %in% c("SRP", "PO4", "DSi", "NO3", "NOx", "NH4", "NHX")) %>%
  # fix issue with umr having nh4 and nhx for same streams
  dplyr::mutate(variable = case_when(LTER == "UMR" & variable == "NHX" ~ "NH4",
    .default = variable)) %>%
  # drop old lter column
  dplyr::select(-LTER) %>%
  # rename some finnish streams before joining
  dplyr::mutate(Stream_Name = dplyr::case_match(Stream_Name,
    "N<e4>rpi<f6>njoki mts 6761" ~  "Narpionjoki mts 6761",
    "Pyh<e4>joki Hourunk 11400" ~ "Pyhajoki Hourunk 11400",
    "Koskenkyl<e4>njoki 6030" ~ "Koskenkylanjoki 6030",
    # "simojoki as. 13500" ~ "simojoki as 13500",
    # "lestijoki 10800 8-tien s" ~ "lestijoki 10800 8tien s",
    # "porvoonjoki 11,5  6022" ~ "porvoonjoki 115  6022",
    # "Mustionjoki 4,9  15500"~"Mustionjoki 49  15500",
    # "Mustijoki 4,2  6010"~"Mustijoki 42  6010",
    # "Vantaa 4,2  6040"~"Vantaa 42  6040",
    .default = Stream_Name)) %>%
  # another option for renaming finnish streams
  # dplyr::mutate(Stream_Name = gsub(pattern = "[<]e4[>]", replacement = "a", x = Stream_Name)) %>%
  # dplyr::mutate(Stream_Name = gsub(pattern = "[<]f6[>]", replacement = "o", x = Stream_Name)) %>%
  # attach reference table information
  dplyr::left_join(y = dplyr::select(ref_table, -drainSqKm),
    by = c("Stream_Name")) %>%
  # drop any rivers we don't want to use in wrtds
  dplyr::filter(Use_WRTDS == "yes") %>%
  dplyr::select(-Use_WRTDS) %>%
  # generate a 'stream id' column that combines lter and chemistry stream name
  dplyr::mutate(Stream_ID = paste0(LTER, "__", Stream_Name),
    .before = dplyr::everything()) %>%
  # filter obidos data
  dplyr::filter((Dataset != "Amazon")) %>%
  dplyr::filter(!(Stream_Name == "Obidos" & date < "2000-01-01"))


# check to see if all names included in chemistry and ref table again after updating finnish names
setdiff(x = unique(ref_table$Stream_Name), y = unique(chem_v1$Stream_Name))

# any rivers without a corresponding chemistry name?
chem_v1 %>%
  dplyr::filter(is.na(Discharge_File_Name) | nchar(Discharge_File_Name) == 0) %>%
  dplyr::pull(Stream_Name) %>%
  unique()

# check structure
dplyr::glimpse(chem_v1)

# drop some objects we won't need again
rm(list = c("ids", "file", "file_names", "areas"))

# ---- ## ----
# ---- prep supporting files ----
# ---- ## ----

# wrangle a special minimum detection limit (mdl) object too
mdl_info <- ref_v0 %>%
  # drop unneeded columns
  dplyr::select(Stream_Name, dplyr::starts_with("MDL_")) %>%
  # pivot longer
  tidyr::pivot_longer(cols = dplyr::starts_with("MDL_"),
    names_to = "variable",
    values_to = "MDL") %>%
  # drop any nas that result from the pivoting
  dplyr::filter(!is.na(MDL)) %>%
  # clean up variable name
  dplyr::mutate(variable_simp = gsub("MDL\\_|\\_mgL", replacement = "", x = variable),
    .after = variable) %>%
  # drop duplicate rows
  dplyr::distinct() %>%
  # drop original variable column
  dplyr::select(-variable)

# check it
dplyr::glimpse(mdl_info)

# create the scaffold for what will become the "information" file required by wrtds
wrtds_info <- ref_table %>%
  # make empty columns to fill later
  dplyr::mutate(param.units = "mg/L",
    shortName = stringr::str_sub(string = Stream_Name, start = 1, end = 8),
    paramShortName = NA,
    constitAbbrev = NA,
    station.nm = paste0(LTER, "__", Stream_Name)) %>%
  # drop unwanted column(s)
  dplyr::select(-Use_WRTDS)

# check that out
dplyr::glimpse(wrtds_info)

# ---- ## ----
# ---- initial wrangling (v1 -> v2) ----
# ---- ## ----
# includes:
# ---- column name standardization ----
# ---- removal of unnecessary columns ----
# ---- removal of data without dates / values (i.e., discharge or solute values) ----
# ---- unit standardization (by conversion) ----

# wrangle the discharge data objects to standardize naming somewhat
disc_v2 <- disc_v1 %>%
  # convert date to true date format
  dplyr::mutate(Date = as.Date(Date, "%Y-%m-%d")) %>%
  # average through duplicate lter-stream-date combinations to get rid of them
  dplyr::group_by(dplyr::across(c(-Qcms))) %>%
  dplyr::summarize(Qcms = mean(Qcms, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # drop any nas in the discharge or date columns
  dplyr::filter(!is.na(Qcms) & !is.na(Date)) %>%
  # Drop pre-1982 (Oct. 1) discharge data for COLUMBIA_RIVER_AT_PORT_WESTWARD_Q
  # ---- keep all data for all other rivers ----
  dplyr::filter(Discharge_File_Name != "COLUMBIA_RIVER_AT_PORT_WESTWARD_Q" |
    (Discharge_File_Name == "COLUMBIA_RIVER_AT_PORT_WESTWARD_Q" &
      Date >= as.Date("1992-10-01")))

# take a look
dplyr::glimpse(disc_v2)

# check for lost/gained streams
supportR::diff_check(old = unique(disc_v1$Discharge_File_Name),
  new = unique(disc_v2$Discharge_File_Name))


# clean up the chemistry data
chem_v2 <- chem_v1 %>%
  # calculate the mg/l (from micro moles) for each of these chemicals
  dplyr::mutate(value_mgL = dplyr::case_when(
    # ---- phosphorous ----
    variable == "SRP" ~ (((value / 10^6) * 30.973762) * 10^3),
    variable == "PO4" ~ (((value / 10^6) * 30.973762) * 10^3),
    variable == "TP" ~ (((value / 10^6) * 30.973762) * 10^3),
    # ---- silica ----
    variable == "DSi" ~ (((value / 10^6) * 28.0855) * 10^3),
    # ---- nitrogen ----
    variable == "NOx" ~ (((value / 10^6) * 14.0067) * 10^3),
    variable == "NO3" ~ (((value / 10^6) * 14.0067) * 10^3),
    variable == "NH4" ~ (((value / 10^6) * 14.0067) * 10^3),
    variable == "TN" ~ (((value / 10^6) * 14.0067) * 10^3))) %>%
  # drop some unwanted columns
  dplyr::select(-Dataset, -Raw_Filename, -units, -value) %>%
  # rename some columns
  dplyr::rename(Date = date) %>%
  # convert date to true date format
  dplyr::mutate(Date = as.Date(Date, "%Y-%m-%d")) %>%
  # average through duplicate lter-stream-date-variable combinations to get rid of them
  dplyr::group_by(dplyr::across(c(-value_mgL))) %>%
  dplyr::summarize(value_mgL = mean(value_mgL, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # drop any nas in the value column
  dplyr::filter(!is.na(value_mgL)) %>%
  # keep all data from non-andrews (and) sites, but drop pre-1983 andrews data
  dplyr::filter(LTER != "AND" | (LTER == "AND" & lubridate::year(Date) > 1983)) %>%
  # create a simplified variable column
  dplyr::mutate(variable_simp = dplyr::case_when(
    variable == "SRP" ~ "P",
    variable == "PO4" ~ "P",
    variable == "NO3" ~ "NOx",
    TRUE ~ variable))  %>%
  # attach the minimum detection limit information where it is known
  dplyr::left_join(y = mdl_info, by = c("Stream_Name", "variable_simp")) %>%
  # using this, create a "remarks" column that indicates whether a value is below the mdl
  dplyr::mutate(remarks = dplyr::case_when(
    value_mgL < MDL ~ "<",
    value_mgL >= MDL ~ "",
    is.na(MDL) ~ ""),
  .after = Date) %>%
  # now we can safely drop the mdl information because we have what we need
  dplyr::select(-MDL) %>%
  # now let's make an "actual" variable column and ditch the others
  dplyr::mutate(variable_actual = ifelse(test = (variable == "SRP" | variable == "PO4"),
    yes = "P", no = variable), .after = variable) %>%
  dplyr::select(-variable, -variable_simp) %>%
  dplyr::rename(variable = variable_actual) %>%
  dplyr::filter(value_mgL >= 0)

# examine that as well
dplyr::glimpse(chem_v2)

# check for lost/gained streams
supportR::diff_check(old = unique(chem_v1$Stream_Name),
  new = unique(chem_v2$Stream_Name))

# ---- ## ----
# ---- crop datasets for qa (v2 -> v3) ----
# ---- ## ----

chemcrop <- chemcrop_v0 %>%
  # generate a 'stream id' column that combines lter and chemistry stream name
  dplyr::mutate(Stream_ID = paste0(LTER, "__", Site),
    .before = dplyr::everything()) %>%
  # drop unwanted columns
  dplyr::select(-dplyr::starts_with("BlankTime_"), -LTER, -Site) %>%
  # drop non-unique rows
  dplyr::distinct() %>%
  # drop uncropped streams
  dplyr::filter(!(Greater_Than == "NA" & Less_Than == "NA")) %>%
  # make years numeric; makes all "nas" in original dataset into real na
  dplyr::mutate(Greater_Than = suppressWarnings(as.numeric(Greater_Than)),
    Less_Than = suppressWarnings(as.numeric(Less_Than)))

dplyr::glimpse(chemcrop)

chem_v3 <- chem_v2 %>%
  left_join(chemcrop, by = c("Stream_ID", "variable")) %>%
  mutate(year = as.numeric(str_sub(Date, start = 1, end = 4))) %>%
  filter(
    # keep every river where there is no date cropping
    (is.na(Greater_Than) & is.na(Less_Than)) |
      # removes years before "Greater_Than" when there is no Less_Than condition
      ((!is.na(Greater_Than) & is.na(Less_Than)) & year >= Greater_Than) |
      # removes years after "Less_Than" when there is no Greater_Than condition
      ((is.na(Greater_Than) & !is.na(Less_Than)) & year <= Less_Than) |
      # removes years before Greater_Than and after Less_Than when years are between those
      ((!is.na(Greater_Than) & !is.na(Less_Than)) & year <= Less_Than & year >= Greater_Than)) %>%
  select(-year, -Less_Than, -Greater_Than)

glimpse(chem_v3)

# crop the discharge file!

# review gaps in discharge data
# identify time series with long gaps
disc_gaps <- disc_v2 %>%
  as_tsibble(index = Date, key = Stream_Name) %>%
  count_gaps(.full = FALSE) %>%
  filter(.n > 365)

discrop <- discrop_v0 %>%
  # generate a 'stream id' column that combines lter and chemistry stream name
  # dplyr::mutate(Stream_ID = paste0(LTER, "__", Site),
  #             .before = dplyr::everything()) %>%
  # drop unwanted columns
  dplyr::select(-dplyr::starts_with("Remove")) %>%
  # drop non-unique rows
  dplyr::distinct() %>%
  # drop uncropped streams
  dplyr::filter(!(Greater_Than == "NA" & Less_Than == "NA")) %>%
  # make years numeric; makes all "nas" in original dataset into real na
  dplyr::mutate(Greater_Than = suppressWarnings(as.numeric(Greater_Than)),
    Less_Than = suppressWarnings(as.numeric(Less_Than)))

dplyr::glimpse(discrop)

disc_v3 <- disc_v2 %>%
  left_join(discrop, by = c("Stream_ID")) %>%
  mutate(year = as.numeric(str_sub(Date, start = 1, end = 4))) %>%
  filter(
    # keep every river where there is no date cropping
    (is.na(Greater_Than) & is.na(Less_Than)) |
      # removes years before "Greater_Than" when there is no Less_Than condition
      ((!is.na(Greater_Than) & is.na(Less_Than)) & year >= Greater_Than) |
      # removes years after "Less_Than" when there is no Greater_Than condition
      ((is.na(Greater_Than) & !is.na(Less_Than)) & year <= Less_Than) |
      # removes years before Greater_Than and after Less_Than when years are between those
      ((!is.na(Greater_Than) & !is.na(Less_Than)) & year <= Less_Than & year >= Greater_Than)) %>%
  select(-year, -Less_Than, -Greater_Than)

glimpse(disc_v3)

# check that worked as expected
disc_v3 %>%
  filter(Stream_ID %in% discrop$Stream_ID) %>%
  ggplot(aes(Date, Qcms)) +
  geom_point() +
  facet_wrap(~Stream_ID, scales = "free")

# adding this to avoid having to update all disc objects below in case i made wrong decision
# disc_v3 <- disc_v2

# pick a river that should change and check that it did change accordingly
# and vice versa

# ---- ## ----
# ---- crop time series for wrtds (v3 -> v4) ----
# ---- ## ----
# wrtds runs best when there is discharge data *before* the first chemistry datapoint
# recommendations vary between "standard" (a few months) wrtds and "generalized flow normalization" (half the window width), so went with a couple of years
# similarly, we can't have more chemistry data than we have discharge data
# so we need to identify the min/max dates of discharge and chemistry (separately)
# ...to be able to use them to crop the actual data as wrtds requires

# identify earliest and latest chemical data at each site -
disc_lims <- chem_v3 %>%
  # make a new column of earliest days per stream (note we don't care which solute this applies to)
  dplyr::group_by(LTER, Stream_Name, Discharge_File_Name, variable) %>%
  dplyr::mutate(min_date = min(Date, na.rm = T)) %>%
  dplyr::mutate(max_date = max(Date, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # filter to only those dates
  dplyr::filter(Date == min_date) %>%
  # Pare down columns (drop date now that we have `min_date`)
  dplyr::select(LTER, Stream_Name, Discharge_File_Name, variable, min_date, max_date) %>%
  # subtract 1 years to crop the discharge data to 1 yrs per chemistry data
  dplyr::mutate(disc_start = (min_date - (1 * 365.25)) - 1) %>% # changed this to 1 years
  dplyr::mutate(disc_end = (max_date + (0.25 * 365))) %>%
  # keep only unique rows
  dplyr::distinct()

# check that
dplyr::glimpse(disc_lims)
head(disc_lims)

# identify min/max of discharge data
chem_lims <- disc_v3 %>%
  # group by stream and identify the first and last days of sampling
  dplyr::group_by(LTER, Stream_Name, Discharge_File_Name) %>%
  dplyr::summarize(min_date = min(Date, na.rm = T),
    max_date = max(Date, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # using the custom function supplied by the silica team, convert to hydro day
  dplyr::mutate(min_hydro = as.numeric(HERON::hydro_day(cal_date = min_date)),
    max_hydro = as.numeric(HERON::hydro_day(cal_date = max_date))) %>%
  # find difference between beginning of next water year and end of chem file
  dplyr::mutate(water_year_diff = 365 - max_hydro) %>%
  # keep only unique rows
  dplyr::distinct()

# look at that outcome
dplyr::glimpse(chem_lims)

disc_v4 <- disc_v3 %>%
  # left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Discharge_File_Name", "Stream_Name")) %>%
  # drop any years before the buffer suggested by wrtds (currently 1 year)
  dplyr::filter(Date > disc_start) %>%
  dplyr::filter(Date <= disc_end) %>%
  # reorder columns / rename q column / implicitly drop unwanted columns
  dplyr::select(Stream_ID, LTER, Discharge_File_Name, Stream_Name, Date, Q = Qcms) %>%
  # remove duplicate rows
  distinct()

# take another look
dplyr::glimpse(disc_v4)

# check for gained/lost streams
supportR::diff_check(old = unique(disc_v3$Discharge_File_Name),
  new = unique(disc_v4$Discharge_File_Name))

# check lost streams
disc_v3 %>%
  filter(Discharge_File_Name == "WalkerBranch_Q") %>%
  ggplot(aes(Date, Qcms)) +
  geom_point()


# check for unintentionally lost columns
supportR::diff_check(old = names(disc_v3), new = names(disc_v4))
# ---- change to discharge column name is fine ----
# ---- added "stream_id" column is purposeful ----

# now crop chemistry to the min and max dates of discharge
chem_v4 <- chem_v3 %>%
  # attach important discharge dates
  dplyr::left_join(y = chem_lims, by = c("LTER", "Discharge_File_Name", "Stream_Name")) %>%
  # use those to crop the dataframe
  dplyr::filter(Date > min_date & Date < max_date) %>%
  # reorder columns / implicitly drop unwanted columns
  dplyr::select(Stream_ID, LTER, Discharge_File_Name, Stream_Name, variable, Date, remarks, value_mgL)

# glimpse it
dplyr::glimpse(chem_v4)

# check for gained/lost streams
supportR::diff_check(old = unique(chem_v3$Stream_Name), new = unique(chem_v4$Stream_Name))
# ---- any streams lost here are lost because somehow *all* chemistry dates are outside of the allowed range defined by the min and max dates found in the discharge data ----
# ---- or possibly because the range limits identified from the discharge file were flawed ----

# check for unintentionally lost columns
supportR::diff_check(old = names(chem_v3), new = names(chem_v4))
# ---- should only gain stream id and lose nothing ----

# ---- ## ----
# ---- gap fill discharge data ----
# ---- ## ----

# read in wrtds input file here
disc_v5 <- disc_v4

site_names <- unique(disc_v5$Stream_ID)
# date_list = list()
Q_interp <- list()

i <- i

for (i in 1:length(site_names)) {

  print(i)

  # pull out one site
  Q_site <- subset(disc_v5, disc_v5$Stream_ID == site_names[i])

  # Q_site<-Q_site[,c("Stream_ID","Date","Q")]

  # remove all na from q
  Q_site <- Q_site[complete.cases(Q_site$Q), ]

  Q_site$Date <- as.Date(Q_site$Date)

  # determine if missing data by comparing complete
  # date range from min to max date to all dates in date columns
  date_range <- seq(from = min(Q_site$Date), to = max(Q_site$Date), by = 1)
  num_missing_days <- length(date_range[!date_range %in% Q_site$Date])

  # if no missing dates, skip rest of loop
  if (num_missing_days == 0) {

    Q_site$indicate <- "measured"

    Q_interp[[i]] <- Q_site

  } else {

    print(site_names[i])

    # create new dataframe with date range as dates
    alldates <- as.data.frame(date_range)
    colnames(alldates) <- "Date"
    alldates <- merge(alldates, Q_site, by = "Date", all.x = TRUE)
    alldates$indicate <- ifelse(is.na(alldates$Q), "interpolated", "measured")

    alldates$Stream_ID <- site_names[i]

    # ---- fill new data frame na values using na.approx #### ----
    Q_site_interp <- alldates
    Q_site_interp$Q <- na.approx(Q_site_interp$Q) # if Q column ends in NA, they will remain NA; rule=2 carries the last measured Q value if the values end in NA

    Q_interp[[i]] <- Q_site_interp

  }

}

# Q_interp_summary = ldply(date_list)
disc_v6 <- do.call(rbind, Q_interp)

# ---- !!! may need to fix - the interpolated sites lose information in the "lter", "discharge_file_name", and "stream_name" columns ----
# but we link with chemistry using "Stream_ID" so probably OK
glimpse(disc_v6)


# ---- remove sites with limited data ----
low_n <- chem_v4 |>
  dplyr::mutate(Stream_Element_ID = paste0(Stream_ID, "___", variable),
    .before = dplyr::everything()) %>%
  dplyr::group_by(Stream_Element_ID) |>
  dplyr::summarise(n = n()) |>
  filter(n < 45)

high_cens <- chem_v4 |>
  dplyr::mutate(Stream_Element_ID = paste0(Stream_ID, "___", variable),
    .before = dplyr::everything()) %>%
  mutate(remark_2 = ifelse(remarks == "<", 1, 0)) |>
  dplyr::group_by(Stream_Element_ID, remark_2) |>
  dplyr::summarise(cens_n = n()) |>
  pivot_wider(names_from = "remark_2", values_from = "cens_n")

colnames(high_cens) <- c("Stream_Element_ID", "Above_BDL", "Below_BDL")

# remove cases where censored values are greater than some proportion of total - here is 1/3
high_cens_2 <- high_cens |>
  mutate(test = case_when((Below_BDL) / (Below_BDL + Above_BDL) >= 0.33 ~ "remove",
    .default = "keep")) |>
  filter(test == "remove")

glimpse(high_cens_2)

# combine low n and streams with a lot of censored values
to_remove <- full_join(low_n, high_cens_2, by = "Stream_Element_ID") %>%
  separate(Stream_Element_ID, into = c("LTER", "Stream_Element"), sep = "__", remove = FALSE, extra = "merge") %>%
  separate(Stream_Element, into = c("Stream_Name", "Element"), sep = "___", remove = FALSE, extra = "merge")

# save to file for future reference
write.csv(x = to_remove, file = file.path(path, "streams_removed_before_WRTDS.csv"))

# now remove those streams from chemistry file
chem_v5 <- chem_v4 |>
  dplyr::mutate(Stream_Element_ID = paste0(Stream_ID, "___", variable),
    .before = dplyr::everything()) %>%
  filter(!Stream_Element_ID %in% to_remove$Stream_Element_ID)

# check how many removed
chem_v4 <- chem_v4 %>% dplyr::mutate(Stream_Element_ID = paste0(Stream_ID, "___", variable),
  .before = dplyr::everything())

# how many were removed?
length(unique(chem_v4$Stream_Element_ID)) - length(unique(chem_v5$Stream_Element_ID))

# check total number of streams remaining
length(unique(chem_v5$Stream_ID))


# ---- ## ----
# ---- final processing & export ----
# ---- ## ----
# identify streams in all three datasets (information, chemistry, and discharge)
incl_streams <- intersect(x = intersect(x = disc_v6$Stream_ID, y = chem_v5$Stream_ID),
  y = wrtds_info$Stream_ID)

# filter to only those streams & drop unneeded name columns
discharge <- disc_v6 %>%
  dplyr::filter(Stream_ID %in% incl_streams) %>%
  dplyr::select(-LTER, -Discharge_File_Name, -Stream_Name)

# final glimpse
dplyr::glimpse(discharge)

# check for gained/lost streams
supportR::diff_check(old = unique(disc_v6$Stream_ID), new = unique(discharge$Stream_ID))

# do the same for chemistry - need to update chem version
chemistry <- chem_v5 %>%
  dplyr::filter(Stream_ID %in% incl_streams) %>%
  dplyr::select(-LTER, -Discharge_File_Name, -Stream_Name) %>%
  # Make a column for Stream_ID + Chemical
  dplyr::mutate(Stream_Element_ID = paste0(Stream_ID, "___", variable),
    .before = dplyr::everything())

# check it
dplyr::glimpse(chemistry)

# check for gained/lost streams
supportR::diff_check(old = unique(chem_v4$Stream_ID), new = unique(chemistry$Stream_ID))

# and finally for information
information <- wrtds_info %>%
  dplyr::filter(Stream_ID %in% incl_streams) %>%
  dplyr::select(-LTER, -Discharge_File_Name, -Stream_Name) %>%
  dplyr::relocate(drainSqKm, .before = station.nm)

# final glimpse
dplyr::glimpse(information)

# check for gained/lost streams
supportR::diff_check(old = unique(wrtds_info$Stream_ID), new = unique(information$Stream_ID))

# write these final products out for posterity
write.csv(x = discharge, row.names = F, na = "",
  file = file.path(path, "WRTDS Inputs",
    "WRTDS-input_discharge.csv"))
write.csv(x = chemistry, row.names = F, na = "",
  file = file.path(path, "WRTDS Inputs",
    "WRTDS-input_chemistry.csv"))
write.csv(x = information, row.names = F, na = "",
  file = file.path(path, "WRTDS Inputs",
    "WRTDS-input_information.csv"))

# export them to google drive to in case anyone has other uses for them
# ---- name drive folder ----
tidy_dest <- googledrive::as_id("https://drive.google.com/drive/u/0/folders/1QEofxLdbWWLwkOTzNRhI6aorg7-2S3JE")
# ---- export to it ----
googledrive::drive_upload(path = tidy_dest, overwrite = T,
  media = file.path(path, "WRTDS Inputs",
    "WRTDS-input_discharge.csv"))
googledrive::drive_upload(path = tidy_dest, overwrite = T,
  media = file.path(path, "WRTDS Inputs",
    "WRTDS-input_chemistry.csv"))
googledrive::drive_upload(path = tidy_dest, overwrite = T,
  media = file.path(path, "WRTDS Inputs",
    "WRTDS-input_information.csv"))

# ---- ## ----
# ---- check - find dropped streams ----
# ---- ## ----

# we want to be super sure we didn't (somehow) drop any sites in the wrangling steps above
# data versions are as follows:
# ---- [disc/chem]_v0 = "raw" data (i.e., initial master files) ----
# ---- [disc/chem]_v1 = drop rivers not included for wrtds ----
# ---- [disc/chem]_v2 = coarse wrangling and averaging within date-stream- combos ----
# ---- [disc/chem]_v3 = cropping by date range (uses both discharge and chemistry) ----

# generate a 'sabotage check' to flag where rivers are dropped
sab_check <- ref_table %>%
  # pare down to needed columns only
  dplyr::select(Stream_ID, LTER, Discharge_File_Name, Stream_Name) %>%
  # filter out any streams found in the final data objects
  # ---- note that it shouldn't matter which final data object stream id is pulled from ----
  dplyr::filter(!Stream_ID %in% discharge$Stream_ID) %>%
  # identify *when* rivers were dropped
  dplyr::mutate(
    chem_v1 = ifelse(Stream_ID %in% unique(chem_v1$Stream_ID),
      yes = "found in ref table / had drainage area", no = NA),
    disc_v1 = ifelse(Stream_ID %in% unique(disc_v1$Stream_ID),
      yes = "found in ref table / had drainage area", no = NA),
    chem_v2 = ifelse(Stream_ID %in% unique(chem_v2$Stream_ID),
      yes = "had chemistry data/dates", no = NA),
    disc_v2 = ifelse(Stream_ID %in% unique(disc_v2$Stream_ID),
      yes = "had chemistry data/dates", no = NA),
    chem_v3 = ifelse(Stream_ID %in% unique(chem_v3$Stream_ID),
      yes = "survived time series cropping", no = NA),
    disc_v3 = ifelse(Stream_ID %in% unique(disc_v3$Stream_ID),
      yes = "survived time series cropping", no = NA))

# check structure
dplyr::glimpse(sab_check)
# ---- tibble::view(sab_check) ----

# make a file name
sab_file <- "WRTDS_Sabotage_Check_SITES.csv"

# export locally
write.csv(x = sab_check, na = "", row.names = F,
  file.path(path, "WRTDS Source Files", sab_file))

# Export it to GoogleDrive too
googledrive::drive_upload(media = file.path(path, "WRTDS Source Files", sab_file),
  name = sab_file,
  overwrite = T,
  path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1aJXFBt61bntXDQec9Ne0F2m5yjvA6TsK"))

# ---- ## ----
# ---- check - find dropped chemicals ----
# ---- ## ----
# we also want to be sure that included chemistry sites keep only chemicals
# above check would (correctly) give green light even if a given chem site lost all but one chemical's data

# # identify stream-element combinations for each data file (except main)
# c2_var <- chem_v2 %>%
#   # fix lter as we do in version 3 of the chem file
#   # standardize some lter names to match the lookup table
#   dplyr::mutate(LTER = dplyr::case_when(
#     LTER == "KRR(Julian)" ~ "KRR",
#     LTER == "LMP(Wymore)" ~ "LMP",
#     LTER == "NWQA" ~ "USGS",
#     LTER == "Sagehen(Sullivan)" ~ "Sagehen",
#     LTER == "UMR(Jankowski)" ~ "UMR",
#     TRUE ~ LTER)) %>%
#   dplyr::mutate(Stream_Element_ID = paste0(LTER, "__", Stream_Name, "_", variable)) %>%
#   dplyr::select(Stream_Name, Stream_Element_ID) %>%
#   unique() %>%
#   dplyr::mutate(in_c2 = 1)
# c3_var <- chem_v3 %>%
#   dplyr::mutate(Stream_Element_ID = paste0(LTER, "__", Stream_Name, "_", variable)) %>%
#   dplyr::select(Stream_Element_ID) %>%
#   unique() %>%
#   dplyr::mutate(in_c3 = 1)
# c4_var <- chem_v4 %>%
#   dplyr::mutate(Stream_Element_ID = paste0(LTER, "__", Stream_Name, "_", variable)) %>%
#   dplyr::select(Stream_Element_ID) %>%
#   unique() %>%
#   dplyr::mutate(in_c4 = 1)
# c5_var <- chemistry %>%
#   dplyr::select(Stream_Element_ID) %>%
#   unique() %>%
#   dplyr::mutate(in_c5 = 1)
#
# # bind these together to assemble the first pass at this check
# var_check_v0 <- c2_var %>%
#   dplyr::full_join(y = c3_var, by = "Stream_Element_ID") %>%
#   dplyr::full_join(y = c4_var, by = "Stream_Element_ID") %>%
#   dplyr::full_join(y = c5_var, by = "Stream_Element_ID")
#
# # drop any rows that aren't missing in any dataset
# var_check <- var_check_v0[ !complete.cases(var_check_v0), ] %>%
#   # drop any streams that are caught by the "sabotage check" above
#   dplyr::filter(!Stream_Name %in% sab_check$Stream_Name) %>%
#   # count how many datasets these streams are included in
#   dplyr::mutate(incl_data_count = rowSums(dplyr::across(dplyr::starts_with("in_")), na.rm = T)) %>%
#   # order by that column
#   dplyr::arrange(desc(incl_data_count)) %>%
#   # generate a rough "diagnosis" column from the included data count
#   dplyr::mutate(diagnosis = dplyr::case_when(
#     incl_data_count == 2 ~ "Dropped at date cropping step. Maybe dates are wrong for these elements?",
#   ), .before = in_c2)
#
# # take a look!
# dplyr::glimpse(var_check)
#
# # if there are any streams in the sabotage object, export a list for later diagnosis!
# if(nrow(var_check) > 0){
#
#   # make a file name
#   (var_file <- paste0("WRTDS_", Sys.Date(), "_sabotage_check_CHEMICALS.csv"))
#
#   # export locally
#   write.csv(x = var_check, na = "", row.names = F,
#             file.path(path, "WRTDS Source Files", var_file))
#
#   # Export it to GoogleDrive too
#   googledrive::drive_upload(media = file.path(path, "WRTDS Source Files", var_file),
#                             name = "WRTDS_Sabotage_Check_CHEMICALS.csv",
#                             overwrite = T,
#                             path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1aJXFBt61bntXDQec9Ne0F2m5yjvA6TsK"))
# }

# ---- end ----
