## ---------------------------------------------- ##
           # WRTDS Centralized Workflow
## ---------------------------------------------- ##
# WRTDS = Weighted Regressions on Time, Discharge, and Season
## Nick J Lyon

## ---------------------------------------------- ##
                # Housekeeping ----
## ---------------------------------------------- ##
# Load libraries
# install.packages("librarian")
librarian::shelf(tidyverse, lubridate, EGRET, EGRETci, supportR, scicomptools, zoo, lter/HERON)

# Clear environment
rm(list = ls())

# Need to specify correct path for local versus server work
(path <- scicomptools::wd_loc(local = FALSE, remote_path = file.path('/', "home", "shares", "lter-si", "WRTDS")))

# Create folders for the raw downloaded files (i.e., sources) & WRTDS inputs (created by this script)
dir.create(path = file.path(path, "WRTDS Source Files_Feb2024"), showWarnings = F)
dir.create(path = file.path(path, "WRTDS Inputs_Feb2024"), showWarnings = F)
dir.create(path = file.path(path, "WRTDS Inputs_data paper"), showWarnings=F)

# Define the names of the Drive files we need
file_names <- c("WRTDS_Reference_Table_with_Areas_DO_NOT_EDIT.csv", # No.1 Simplified ref table
                "Site_Reference_Table", # No.2 Full ref table
                "20240801_masterdata_discharge.csv", # No.3 Main discharge ## update this file with new discharge!!
                "20241003_masterdata_chem.csv", # No.4 Main chemistry ## update this file with new chemistry!!
                "Data_Cropping_WRTDS") # No.5 Data cropping for chemistry (Si) 

# Find those files' IDs
ids <- googledrive::drive_ls(as_id("https://drive.google.com/drive/u/0/folders/15FEoe2vu3OAqMQHqdQ9XKpFboR4DvS9M")) %>%
  dplyr::bind_rows(googledrive::drive_ls(as_id("https://drive.google.com/drive/u/0/folders/1hbkUsTdo4WAEUnlPReOUuXdeeXm92mg-"))) %>%
  dplyr::bind_rows(googledrive::drive_ls(as_id("https://drive.google.com/drive/u/0/folders/1dTENIB5W2ClgW0z-8NbjqARiaGO2_A7W")))%>%
  dplyr::bind_rows(googledrive::drive_ls(as_id("https://drive.google.com/drive/u/0/folders/0AIPkWhVuXjqFUk9PVA"))) %>%
  ## And filter out any extraneous files
  dplyr::filter(name %in% file_names)

# Check that no file names have changed
for(file in file_names){
  if(!file %in% ids$name){ message("File '", file, "' not found.") 
    } else { message("File '", file, "' found!") } }

# Download the files we want
purrr::walk2(.x = ids$name, .y = ids$id,
             .f = ~ googledrive::drive_download(file = .y, overwrite = T,
                                                path = file.path(path, "WRTDS Source Files", .x)))

# Read in each of these files
areas <- read.csv(file = file.path(path, "WRTDS Source Files", file_names[1])) 
ref_v0 <- readxl::read_excel(path = file.path(path, "WRTDS Source Files", 
                                               paste0(file_names[2], ".xlsx"))) 
disc_v0 <- read.csv(file = file.path(path, "WRTDS Source Files", file_names[3]))
chem_v0 <- read.csv(file = file.path(path, "WRTDS Source Files", file_names[4]))
chemcrop_v0 <- readxl::read_excel(path = file.path(path, "WRTDS Source Files", 
                                                   paste0(file_names[5], ".xlsx")))

## ---------------------------------------------- ##
        # Process Raw Files (v0 -> v1) ----
## ---------------------------------------------- ##

# Generate a complete reference table (areas + other info)
ref_table <- ref_v0 %>%
  # Pare down to only some columns
  dplyr::select(LTER, Discharge_File_Name, Stream_Name, Use_WRTDS) %>%
  # Standardize WRTDS column
  dplyr::mutate(Use_WRTDS = tolower(Use_WRTDS)) %>%
  # Drop non-unique rows
  dplyr::distinct() %>%
  # Attach areas
  dplyr::left_join(y = dplyr::select(areas, LTER, Discharge_File_Name, Stream_Name, drainSqKm),
                   by = c("LTER", "Discharge_File_Name", "Stream_Name")) %>%
  # Filter to only rivers where we *do* want to use WRTDS
  dplyr::filter(Use_WRTDS == "yes") %>%
  # Generate a 'stream ID' column that combines LTER and chemistry stream name
  dplyr::mutate(Stream_ID = paste0(LTER, "__", Stream_Name),
                .before = dplyr::everything())
  
# Should be no missing areas
ref_table %>%
  dplyr::filter(is.na(drainSqKm) | nchar(drainSqKm) == 0)

# Check structure
dplyr::glimpse(ref_table)

# Any *discharge* rivers not in reference table?
setdiff(x = unique(ref_table$Discharge_File_Name), y = unique(disc_v0$Discharge_File_Name))

# Wrangle discharge
disc_v1 <- disc_v0 %>%
  # Rename site column as it appears in the reference table
  dplyr::rename(Discharge_File_Name = Discharge_File_Name) %>%
  # Fix any broken names (special characters from Scandinavia)
  dplyr::mutate(Discharge_File_Name = gsub(pattern = "ØSTEGLO_Q", replacement = "OSTEGLO_Q",
                                           x = Discharge_File_Name)) %>%
  #dplyr::mutate(Stream_Name = dplyr::case_match(Stream_Name, 
   #                                             "Kiiminkij 13010 4-tien s"~"Kiiminkij 13010 4tien s",
    #                                            .default = Stream_Name))
  # Attach the reference table object; 
  # Master discharge file now has "Stream_Name" and "LTER" columns so removing before joining to avoid duplication
  dplyr::left_join(y = dplyr::select(ref_table, -drainSqKm, -Stream_Name,-LTER),
                   by = c("Discharge_File_Name")) %>%
  # Drop any rivers we don't want to use in WRTDS
  dplyr::filter(Use_WRTDS == "yes") %>%
  dplyr::select(-Use_WRTDS) %>% 
  # Generate a 'stream ID' column that combines LTER and chemistry stream name
  dplyr::mutate(Stream_ID = paste0(LTER, "__", Stream_Name),
                .before = dplyr::everything())

# Any rivers without a corresponding chemistry name?
disc_v1 %>%
  dplyr::filter(is.na(Stream_Name) | nchar(Stream_Name) == 0) %>%
  dplyr::pull(Discharge_File_Name) %>%
  unique()

# Check structure
dplyr::glimpse(disc_v1)

# Any *chemistry* rivers not in reference table?
# FYI -- Finnish Names don't read into R well, they say they are missing, but they are not, I adjusted the Finnish Stream names that # differ between chem and ref table when creating chem_v1 below
setdiff(x = unique(ref_table$Stream_Name), y = unique(chem_v0$Stream_Name))

# Wrangle chemistry as well
chem_v1 <- chem_v0 %>%
  # Pare down to only particular solutes that we're interested in
  dplyr::filter(variable %in% c("SRP", "PO4", "DSi", "NO3", "NOx", "NH4")) %>%
  # Drop old LTER column
  dplyr::select(-LTER) %>% 
  # rename some Finnish streams before joining
  dplyr::mutate(Stream_Name = dplyr::case_match(Stream_Name, 
                                                "N<e4>rpi<f6>njoki mts 6761" ~  "Narpionjoki mts 6761",
                                                "Pyh<e4>joki Hourunk 11400" ~ "Pyhajoki Hourunk 11400",
                                                "Koskenkyl<e4>njoki 6030" ~ "Koskenkylanjoki 6030",
                                                #"SIMOJOKI AS. 13500" ~ "SIMOJOKI AS 13500",
                                                #"Lestijoki 10800 8-tien s" ~ "Lestijoki 10800 8tien s",
                                                #"Porvoonjoki 11,5  6022" ~ "Porvoonjoki 115  6022",
                                                #"Mustionjoki 4,9  15500"~"Mustionjoki 49  15500",
                                                #"Mustijoki 4,2  6010"~"Mustijoki 42  6010",
                                                #"Vantaa 4,2  6040"~"Vantaa 42  6040",
                                                .default = Stream_Name)) %>% 
  # another option for renaming Finnish streams
  #dplyr::mutate(Stream_Name = gsub(pattern = "[<]e4[>]", replacement = "a", x = Stream_Name)) %>%
  #dplyr::mutate(Stream_Name = gsub(pattern = "[<]f6[>]", replacement = "o", x = Stream_Name)) %>%
  # Attach reference table information
  dplyr::left_join(y = dplyr::select(ref_table, -drainSqKm),
                   by = c("Stream_Name")) %>% 
  # Drop any rivers we don't want to use in WRTDS
  dplyr::filter(Use_WRTDS == "yes") %>%
  dplyr::select(-Use_WRTDS) %>% 
  # Generate a 'stream ID' column that combines LTER and chemistry stream name
  dplyr::mutate(Stream_ID = paste0(LTER, "__", Stream_Name),
                .before = dplyr::everything())


# check to see if all names included in chemistry and ref table again after updating Finnish names
setdiff(x = unique(ref_table$Stream_Name), y = unique(chem_v1$Stream_Name))

# Any rivers without a corresponding chemistry name?
chem_v1 %>%
  dplyr::filter(is.na(Discharge_File_Name) | nchar(Discharge_File_Name) == 0) %>%
  dplyr::pull(Stream_Name) %>%
  unique()

# Check structure
dplyr::glimpse(chem_v1)

# Drop some objects we won't need again
rm(list = c("ids", "file", "file_names", "areas"))

## ---------------------------------------------- ##
             # Prep Supporting Files ----
## ---------------------------------------------- ##

# Wrangle a special minimum detection limit (MDL) object too
mdl_info <- ref_v0 %>%
  # Drop unneeded columns
  dplyr::select(Stream_Name, dplyr::starts_with("MDL_")) %>%
  # Pivot longer
  tidyr::pivot_longer(cols = dplyr::starts_with("MDL_"),
                      names_to = "variable",
                      values_to = "MDL") %>%
  # Drop any NAs that result from the pivoting
  dplyr::filter(!is.na(MDL)) %>%
  # Clean up variable name
  dplyr::mutate(variable_simp = gsub("MDL\\_|\\_mgL", replacement = "", x = variable),
                .after = variable) %>% 
  # Drop duplicate rows
  dplyr::distinct() %>%
  # Drop original variable column
  dplyr::select(-variable)

# Check it
dplyr::glimpse(mdl_info)

# Create the scaffold for what will become the "information" file required by WRTDS
wrtds_info <- ref_table %>%
  # Make empty columns to fill later
  dplyr::mutate(param.units = "mg/L",
                shortName = stringr::str_sub(string = Stream_Name, start = 1, end = 8),
                paramShortName = NA,
                constitAbbrev = NA,
                station.nm = paste0(LTER, "__", Stream_Name)) %>%
  # Drop unwanted column(s)
  dplyr::select(-Use_WRTDS)

# Check that out
dplyr::glimpse(wrtds_info)

## ---------------------------------------------- ##
        # Initial Wrangling (v1 -> v2) ----
## ---------------------------------------------- ##
# Includes:
## Column name standardization
## Removal of unnecessary columns
## Removal of data without dates / values (i.e., discharge or solute values)
## Unit standardization (by conversion)

# Wrangle the discharge data objects to standardize naming somewhat
disc_v2 <- disc_v1 %>%
  # Convert date to true date format
  dplyr::mutate(Date = as.Date(Date, "%Y-%m-%d")) %>%
  # Average through duplicate LTER-stream-date combinations to get rid of them
  dplyr::group_by(dplyr::across(c(-Qcms))) %>%
  dplyr::summarize(Qcms = mean(Qcms, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # Drop any NAs in the discharge or date columns
  dplyr::filter(!is.na(Qcms) & !is.na(Date)) %>%
  # Drop pre-1982 (Oct. 1) discharge data for COLUMBIA_RIVER_AT_PORT_WESTWARD_Q
  ## Keep all data for all other rivers
  dplyr::filter(Discharge_File_Name != "COLUMBIA_RIVER_AT_PORT_WESTWARD_Q" |
                  (Discharge_File_Name == "COLUMBIA_RIVER_AT_PORT_WESTWARD_Q" &
                     Date >= as.Date("1992-10-01")))

# Take a look
dplyr::glimpse(disc_v2)

# Check for lost/gained streams
supportR::diff_check(old = unique(disc_v1$Discharge_File_Name),
                     new = unique(disc_v2$Discharge_File_Name))

# Clean up the chemistry data
chem_v2 <- chem_v1 %>%
  # Calculate the mg/L (from micro moles) for each of these chemicals
  dplyr::mutate(value_mgL = dplyr::case_when(
    ## Phosphorous
    variable == "SRP" ~ (((value / 10^6) * 30.973762) * 10^3),
    variable == "PO4" ~ (((value / 10^6) * 30.973762) * 10^3),
    variable == "TP" ~ (((value / 10^6) * 30.973762) * 10^3),
    ## Silica
    variable == "DSi" ~ (((value / 10^6) * 28.0855) * 10^3),
    ## Nitrogen
    variable == "NOx" ~ (((value / 10^6) * 14.0067) * 10^3),
    variable == "NO3" ~ (((value / 10^6) * 14.0067) * 10^3),
    variable == "NH4" ~ (((value / 10^6) * 14.0067) * 10^3),
    variable == "TN" ~ (((value / 10^6) * 14.0067) * 10^3))) %>% 
  # Drop some unwanted columns
  dplyr::select(-Dataset, -Raw_Filename, -units, -value) %>%
  # Rename some columns
  dplyr::rename(Date = date) %>% 
  # Convert date to true date format
  dplyr::mutate(Date = as.Date(Date, "%Y-%m-%d")) %>% 
  # Average through duplicate LTER-stream-date-variable combinations to get rid of them
  dplyr::group_by(dplyr::across(c(-value_mgL))) %>%
  dplyr::summarize(value_mgL = mean(value_mgL, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # Drop any NAs in the value column
  dplyr::filter(!is.na(value_mgL)) %>%
  # Keep all data from non-Andrews (AND) sites, but drop pre-1983 Andrews data
  dplyr::filter(LTER != "AND" | (LTER == "AND" & lubridate::year(Date) > 1983)) %>%
  # Create a simplified variable column
  dplyr::mutate(variable_simp = dplyr::case_when(
    variable == "SRP" ~ "P",
    variable == "PO4" ~ "P",
    variable == "NO3" ~ "NOx",
    TRUE ~ variable))  %>%
  # Attach the minimum detection limit information where it is known
  dplyr::left_join(y = mdl_info, by = c("Stream_Name", "variable_simp")) %>%
  # Using this, create a "remarks" column that indicates whether a value is below the MDL
  dplyr::mutate(remarks = dplyr::case_when(
    value_mgL < MDL ~ "<",
    value_mgL >= MDL ~ "",
    is.na(MDL) ~ ""),
    .after = Date) %>%
  # Now we can safely drop the MDL information because we have what we need
  dplyr::select(-MDL) %>%
  # Now let's make an "actual" variable column and ditch the others
  dplyr::mutate(variable_actual = ifelse(test = (variable == "SRP" | variable == "PO4"),
                                         yes = "P", no = variable), .after = variable) %>%
  dplyr::select(-variable, -variable_simp) %>%
  dplyr::rename(variable = variable_actual) %>% 
  dplyr::filter(value_mgL >=0)
  
# Examine that as well
dplyr::glimpse(chem_v2)

# Check for lost/gained streams
supportR::diff_check(old = unique(chem_v1$Stream_Name),
                     new = unique(chem_v2$Stream_Name))

## ---------------------------------------------- ##
# Crop Chemistry dataset for QA (v2 -> v3) ----
## ---------------------------------------------- ##

chemcrop <- chemcrop_v0 %>% 
  # Generate a 'stream ID' column that combines LTER and chemistry stream name
  dplyr::mutate(Stream_ID = paste0(LTER, "__", Site),
                .before = dplyr::everything()) %>% 
  # drop unwanted columns
  dplyr::select(-dplyr::starts_with("BlankTime_"),-LTER,-Site) %>% 
  # drop non-unique rows
  dplyr::distinct() %>% 
  # drop uncropped streams
  dplyr::filter(!(Greater_Than == "NA" & Less_Than == "NA")) %>% 
  # make years numeric; makes all "NAs" in original dataset into real NA
  dplyr::mutate(Greater_Than = suppressWarnings(as.numeric(Greater_Than)), 
                Less_Than = suppressWarnings(as.numeric(Less_Than)))
  
dplyr::glimpse(chemcrop)

chem_v3 <- chem_v2 %>% 
  left_join(chemcrop,by=c("Stream_ID","variable")) %>% 
  mutate(year = as.numeric(str_sub(Date,start=1,end=4))) %>% 
  filter(
    # keep every river where there is no date cropping
    (is.na(Greater_Than) & is.na(Less_Than)) |
      # removes years before "Greater_Than" when there is no Less_Than condition
      ((!is.na(Greater_Than) & is.na(Less_Than)) & year >= Greater_Than) |
      # removes years after "Less_Than" when there is no Greater_Than condition
      ((is.na(Greater_Than) & !is.na(Less_Than)) & year <= Less_Than) |
      # removes years before Greater_Than and after Less_Than when years are between those
      ((!is.na(Greater_Than) & !is.na(Less_Than)) & year <= Less_Than & year >= Greater_Than)) %>%
  select(-year,-Less_Than,-Greater_Than)
  
glimpse(chem_v3)

# do the same for discharge - I don't think we want to crop this because we do below
#disc_v3 <- disc_v2 %>% 
 # left_join(chemcrop,by=c("Stream_ID")) %>% 
  #mutate(year = as.numeric(str_sub(Date,start=1,end=4))) %>% 
  #filter(
   # (is.na(Greater_Than) & is.na(Less_Than)) |
    #  ((!is.na(Greater_Than) & is.na(Less_Than)) & year >= Greater_Than) |
     # ((is.na(Greater_Than) & !is.na(Less_Than)) & year <= Less_Than) |
      #((!is.na(Greater_Than) & !is.na(Less_Than)) & year <= Less_Than & year >= Greater_Than)) %>%
  #select(-year,-Less_Than,-Greater_Than)

# adding this to avoid having to update all disc objects below in case I made wrong decision
disc_v3 <- disc_v2

# pick a river that should change and check that it did change accordingly
# and vice versa

## ---------------------------------------------- ##
    # Crop Time Series for WRTDS (v3 -> v4) ----
## ---------------------------------------------- ##
# WRTDS runs best when there is discharge data *before* the first chemistry datapoint
# recommendations vary between "standard" (a few months) WRTDS and "generalized flow normalization" (half the window width), so went with a couple of years
# Similarly, we can't have more chemistry data than we have discharge data.
# So we need to identify the min/max dates of discharge and chemistry (separately)...
# ...to be able to use them to crop the actual data as WRTDS requires

# Identify earliest chemical data at each site -
## need to add MAX DATE!
disc_lims <- chem_v3 %>%
  # Make a new column of earliest days per stream (note we don't care which solute this applies to)
  dplyr::group_by(LTER, Stream_Name, Discharge_File_Name) %>%
  dplyr::mutate(min_date = min(Date, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # Filter to only those dates
  dplyr::filter(Date == min_date) %>%
  # Pare down columns (drop date now that we have `min_date`)
  dplyr::select(LTER, Stream_Name, Discharge_File_Name, min_date) %>%
  # Subtract 1 years to crop the discharge data to 1 yrs per chemistry data
  dplyr::mutate(disc_start = (min_date - (1 * 365.25)) - 1) %>% # changed this to 1 years
  # Keep only unique rows
  dplyr::distinct()

# Check that
dplyr::glimpse(disc_lims)

# Identify min/max of discharge data 
chem_lims <- disc_v3 %>%
  # Group by stream and identify the first and last days of sampling
  dplyr::group_by(LTER, Stream_Name, Discharge_File_Name) %>%
  dplyr::summarize(min_date = min(Date, na.rm = T),
                   max_date = max(Date, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # Using the custom function supplied by the Silica team, convert to hydro day
  dplyr::mutate(min_hydro = as.numeric(HERON::hydro_day(cal_date = min_date)),
                max_hydro = as.numeric(HERON::hydro_day(cal_date = max_date))) %>%
  # Find difference between beginning of next water year and end of chem file
  dplyr::mutate(water_year_diff = 365 - max_hydro) %>%
  # Keep only unique rows
  dplyr::distinct()

# Look at that outcome
dplyr::glimpse(chem_lims)

# Crop the discharge file!
disc_v4 <- disc_v3 %>%
  # Left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Discharge_File_Name", "Stream_Name")) %>%
  # Drop any years before the buffer suggested by WRTDS (currently 1 year)
  dplyr::filter(Date > disc_start) %>% 
  # Reorder columns / rename Q column / implicitly drop unwanted columns
  dplyr::select(Stream_ID, LTER, Discharge_File_Name, Stream_Name, Date, Q = Qcms)

# Take another look
dplyr::glimpse(disc_v4)

# Check for gained/lost streams
supportR::diff_check(old = unique(disc_v3$Discharge_File_Name),
                     new = unique(disc_v4$Discharge_File_Name))

# Check for unintentionally lost columns
supportR::diff_check(old = names(disc_v3), new = names(disc_v4))
## Change to discharge column name is fine
## Added "Stream_ID" column is purposeful

# Now crop chemistry to the min and max dates of discharge
chem_v4 <- chem_v3 %>%
  # Attach important discharge dates
  dplyr::left_join(y = chem_lims, by = c("LTER", "Discharge_File_Name", "Stream_Name")) %>%
  # Use those to crop the dataframe
  dplyr::filter(Date > min_date & Date < max_date) %>%
  # Reorder columns / implicitly drop unwanted columns
  dplyr::select(Stream_ID, LTER, Discharge_File_Name, Stream_Name, variable, Date, remarks, value_mgL)
  
# Glimpse it
dplyr::glimpse(chem_v4)

# Check for gained/lost streams
supportR::diff_check(old = unique(chem_v3$Stream_Name), new = unique(chem_v4$Stream_Name))
## Any streams lost here are lost because somehow *all* chemistry dates are outside of the allowed range defined by the min and max dates found in the discharge data
## Or possibly because the range limits identified from the discharge file were flawed...

# Check for unintentionally lost columns
supportR::diff_check(old = names(chem_v3), new = names(chem_v4))
## Should only gain Stream ID and lose nothing

## ---------------------------------------------- ##
# Gap Fill Discharge Data ---- 
## ---------------------------------------------- ##

#read in WRTDS input file here
disc_v5 <-disc_v4

site_names = unique(disc_v5$Stream_ID)
#date_list = list()
Q_interp = list()

i=i

for (i in 1:length(site_names)){
  
  print(i)
  
  #pull out one site
  Q_site = subset(disc_v5, disc_v5$Stream_ID==site_names[i])
  
  #Q_site<-Q_site[,c("Stream_ID","Date","Q")]
  
  #remove all NA from Q
  Q_site<-Q_site[complete.cases(Q_site$Q),]
  
  Q_site$Date<-as.Date(Q_site$Date)
  
  #determine if missing data by comparing complete 
  #date range from min to max date to all dates in date columns
  date_range <- seq(from=min(Q_site$Date), to=max(Q_site$Date), by = 1) 
  num_missing_days<-length(date_range[!date_range %in% Q_site$Date])
  
  #if no missing dates, skip rest of loop
  if(num_missing_days==0){
    
    Q_site$indicate<-"measured"
    
    Q_interp[[i]] = Q_site 
    
  } else{
    
    print(site_names[i])
    
    #create new dataframe with date range as dates
    alldates<-as.data.frame(date_range)
    colnames(alldates)<-"Date"
    alldates<-merge(alldates, Q_site, by="Date", all.x=TRUE)
    alldates$indicate<-ifelse(is.na(alldates$Q), "interpolated", "measured")
    
    alldates$Stream_ID<-site_names[i]
    
    #### fill new data frame NA values using na.approx ####
    Q_site_interp = alldates
    Q_site_interp$Q<-na.approx(Q_site_interp$Q) #if Q column ends in NA, they will remain NA; rule=2 carries the last measured Q value if the values end in NA
    
    Q_interp[[i]] = Q_site_interp 
    
  }
  
}

# Q_interp_summary = ldply(date_list)
disc_v6 = do.call(rbind, Q_interp)

##### !!! may need to fix - the interpolated sites lose information in the "LTER", "Discharge_File_Name", and "Stream_Name" columns
# but we link with chemistry using "Stream_ID" so probably OK
glimpse(disc_v6)

## ---------------------------------------------- ##
          # Final Processing & Export ----
## ---------------------------------------------- ##
# Identify streams in all three datasets (information, chemistry, and discharge)
incl_streams <- intersect(x = intersect(x = disc_v6$Stream_ID, y = chem_v4$Stream_ID),
                          y = wrtds_info$Stream_ID)

# Filter to only those streams & drop unneeded name columns
discharge <- disc_v6 %>%
  dplyr::filter(Stream_ID %in% incl_streams) %>%
  dplyr::select(-LTER, -Discharge_File_Name, -Stream_Name)

# Final glimpse
dplyr::glimpse(discharge)

# Check for gained/lost streams
supportR::diff_check(old = unique(disc_v6$Stream_ID), new = unique(discharge$Stream_ID))

# Do the same for chemistry
chemistry <- chem_v4 %>%
  dplyr::filter(Stream_ID %in% incl_streams) %>%
  dplyr::select(-LTER, -Discharge_File_Name, -Stream_Name) %>%
  # Make a column for Stream_ID + Chemical
  dplyr::mutate(Stream_Element_ID = paste0(Stream_ID, "_", variable),
                .before = dplyr::everything())

# Check it
dplyr::glimpse(chemistry)

# Check for gained/lost streams
supportR::diff_check(old = unique(chem_v4$Stream_ID), new = unique(chemistry$Stream_ID))

# And finally for information
information <- wrtds_info %>%
  dplyr::filter(Stream_ID %in% incl_streams) %>%
  dplyr::select(-LTER, -Discharge_File_Name, -Stream_Name) %>%
  dplyr::relocate(drainSqKm, .before = station.nm)

# Final glimpse
dplyr::glimpse(information)

# Check for gained/lost streams
supportR::diff_check(old = unique(wrtds_info$Stream_ID), new = unique(information$Stream_ID))

# Write these final products out for posterity
write.csv(x = discharge, row.names = F, na = "",
          file = file.path(path, "WRTDS Inputs_Feb2024",
                           "WRTDS-input_discharge.csv"))
write.csv(x = chemistry, row.names = F, na = "",
          file = file.path(path, "WRTDS Inputs_Feb2024",
                           "WRTDS-input_chemistry.csv"))
write.csv(x = information, row.names = F, na = "",
          file = file.path(path, "WRTDS Inputs_Feb2024",
                           "WRTDS-input_information.csv"))

# Export them to Google Drive to in case anyone has other uses for them
## Name Drive folder
tidy_dest <- googledrive::as_id("https://drive.google.com/drive/u/0/folders/1QEofxLdbWWLwkOTzNRhI6aorg7-2S3JE")
## Export to it
googledrive::drive_upload(path = tidy_dest, overwrite = T,
                          media = file.path(path, "WRTDS Inputs_Feb2024",
                                            "WRTDS-input_discharge.csv"))
googledrive::drive_upload(path = tidy_dest, overwrite = T,
                          media = file.path(path, "WRTDS Inputs_Feb2024",
                                            "WRTDS-input_chemistry.csv"))
googledrive::drive_upload(path = tidy_dest, overwrite = T,
                          media = file.path(path, "WRTDS Inputs_Feb2024",
                                            "WRTDS-input_information.csv"))

## ---------------------------------------------- ##
        # Check - Find Dropped Streams ----
## ---------------------------------------------- ##

# We want to be super sure we didn't (somehow) drop any sites in the wrangling steps above.
# Data versions are as follows:
## [disc/chem]_v0 = "Raw" data (i.e., initial master files)
## [disc/chem]_v1 = Drop rivers not included for WRTDS
## [disc/chem]_v2 = Coarse wrangling and averaging within date-stream- combos
## [disc/chem]_v3 = Cropping by date range (uses both discharge and chemistry)

# Generate a 'sabotage check' to flag where rivers are dropped
sab_check <- ref_table %>%
  # Pare down to needed columns only
  dplyr::select(Stream_ID, LTER, Discharge_File_Name, Stream_Name) %>%
  # Filter out any Streams found in the final data objects
  ## Note that it shouldn't matter which final data object stream ID is pulled from
  dplyr::filter(!Stream_ID %in% discharge$Stream_ID) %>% 
  # Identify *when* rivers were dropped
  dplyr::mutate(
    chem_v1 = ifelse(Stream_ID %in% unique(chem_v1$Stream_ID),
                     yes = 'found in ref table / had drainage area', no = NA),
    disc_v1 = ifelse(Stream_ID %in% unique(disc_v1$Stream_ID),
                     yes = 'found in ref table / had drainage area', no = NA),
    chem_v2 = ifelse(Stream_ID %in% unique(chem_v2$Stream_ID),
                     yes = 'had chemistry data/dates', no = NA),
    disc_v2 = ifelse(Stream_ID %in% unique(disc_v2$Stream_ID),
                     yes = 'had chemistry data/dates', no = NA),
    chem_v3 = ifelse(Stream_ID %in% unique(chem_v3$Stream_ID),
                     yes = 'survived time series cropping', no = NA),
    disc_v3 = ifelse(Stream_ID %in% unique(disc_v3$Stream_ID),
                     yes = 'survived time series cropping', no = NA) )
  
# Check structure
dplyr::glimpse(sab_check)
## tibble::view(sab_check)

# Make a file name
sab_file <- "WRTDS_Sabotage_Check_SITES.csv"

# Export locally
write.csv(x = sab_check, na = "", row.names = F,
          file.path(path, "WRTDS Source Files", sab_file))

# Export it to GoogleDrive too
googledrive::drive_upload(media = file.path(path, "WRTDS Source Files", sab_file),
                          name = sab_file,
                          overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1aJXFBt61bntXDQec9Ne0F2m5yjvA6TsK"))

## ---------------------------------------------- ##
      # Check - Find Dropped Chemicals ----
## ---------------------------------------------- ##
# We also want to be sure that included chemistry sites keep only chemicals
# Above check would (correctly) give green light even if a given chem site lost all but one chemical's data

# # Identify stream-element combinations for each data file (except main)
# c2_var <- chem_v2 %>%
#   # Fix LTER as we do in version 3 of the chem file
#   # Standardize some LTER names to match the lookup table
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
# # Bind these together to assemble the first pass at this check
# var_check_v0 <- c2_var %>%
#   dplyr::full_join(y = c3_var, by = "Stream_Element_ID") %>%
#   dplyr::full_join(y = c4_var, by = "Stream_Element_ID") %>%
#   dplyr::full_join(y = c5_var, by = "Stream_Element_ID")
# 
# # Drop any rows that aren't missing in any dataset
# var_check <- var_check_v0[ !complete.cases(var_check_v0), ] %>% 
#   # Drop any streams that are caught by the "sabotage check" above
#   dplyr::filter(!Stream_Name %in% sab_check$Stream_Name) %>%
#   # Count how many datasets these streams are included in
#   dplyr::mutate(incl_data_count = rowSums(dplyr::across(dplyr::starts_with("in_")), na.rm = T)) %>%
#   # Order by that column
#   dplyr::arrange(desc(incl_data_count)) %>%
#   # Generate a rough "diagnosis" column from the included data count
#   dplyr::mutate(diagnosis = dplyr::case_when(
#     incl_data_count == 2 ~ "Dropped at date cropping step. Maybe dates are wrong for these elements?",
#   ), .before = in_c2)
# 
# # Take a look!
# dplyr::glimpse(var_check)
# 
# # If there are any streams in the sabotage object, export a list for later diagnosis!
# if(nrow(var_check) > 0){
#   
#   # Make a file name
#   (var_file <- paste0("WRTDS_", Sys.Date(), "_sabotage_check_CHEMICALS.csv"))
#   
#   # Export locally
#   write.csv(x = var_check, na = "", row.names = F,
#             file.path(path, "WRTDS Source Files", var_file))
#   
#   # Export it to GoogleDrive too
#   googledrive::drive_upload(media = file.path(path, "WRTDS Source Files", var_file),
#                             name = "WRTDS_Sabotage_Check_CHEMICALS.csv",
#                             overwrite = T,
#                             path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1aJXFBt61bntXDQec9Ne0F2m5yjvA6TsK"))
# }

# End ----
