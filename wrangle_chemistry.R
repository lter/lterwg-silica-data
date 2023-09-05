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

# Silence group-wise summarization messages from `dplyr::summarize`
options(dplyr.summarise.inform = F)

# Create a folder for any needed data keys
dir.create(path = file.path("keys"), showWarnings = F)

# Create a folder for inputs & and outputs
dir.create(path = file.path("chem_raw"), showWarnings = F)
dir.create(path = file.path("tidy"), showWarnings = F)

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

# Identify and download the **units key** as well
googledrive::drive_ls(path = googledrive::as_id("https://drive.google.com/drive/folders/1hbkUsTdo4WAEUnlPReOUuXdeeXm92mg-")) %>%
  dplyr::filter(name == "SiSyn_Chem_Units_Key") %>%
  googledrive::drive_download(file = .$id, path = file.path("keys", .$name), 
                              overwrite = T, type = "csv")

# Read it in
units_key_v1 <- read.csv(file = file.path("keys", paste0("SiSyn_Chem_Units_Key", ".csv"))) %>%
  # Subset to only chemistry data
  dplyr::filter(Data_type == "chemistry")

# Check structure
dplyr::glimpse(units_key_v1)

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

# Process the data key object as needed
key <- key_v1 %>%
  # Drop unwanted columns
  dplyr::select(-Data_type, -Notes) %>%
  # Mangle raw column names as they will be by reading in the CSVs
  dplyr::mutate(Raw_Column_Name = gsub(pattern = " |\\+|\\(|\\)|\\/|\\-", replacement = ".",
                                       x = Raw_Column_Name))

# Re-check structure
dplyr::glimpse(key)

# Do the same for the units key
units_key <- units_key_v1 %>%
  dplyr::select(-Data_type, -Notes) %>%
  dplyr::mutate(Variable = tolower(Variable))

# Re-check structure
dplyr::glimpse(units_key)

# Identify all downloaded files
( raw_files <- dir(path = file.path("chem_raw")) )

# Make an empty list to store re-formatted raw data
df_list <- list()

# For each raw file...
for(j in 1:length(raw_files)){
  
  # Grab its name
  focal_raw <- raw_files[j]
  
  # Message procesing start
  message("Harmonizing '", focal_raw, "' (file ",  j, " of ", length(raw_files), ")")
  
  # Subset the key object a bit
  key_sub <- key %>%
    # Only this file's section
    dplyr::filter(Raw_Filename == focal_raw) %>%
    # And only columns that have a synonymized equivalent
    dplyr::filter(!is.na(Standardized_Column_Name) & nchar(Standardized_Column_Name) != 0)
  
  # Load in that file
  raw_df_v1 <- read.csv(file = file.path("chem_raw", focal_raw))
  
  # Process it to ready for integration with other raw files
  raw_df_v2 <- raw_df_v1 %>%
    # Create a row number column and a column for the original file
    dplyr::mutate(row_num = 1:nrow(.),
                  Raw_Filename = focal_raw,
                  .before = dplyr::everything()) %>%
    # Make all columns into character columns
    dplyr::mutate(dplyr::across(.cols = dplyr::everything(), .fns = as.character)) %>%
    # Now pivot everything into ultimate long format
    ## Note: if column class differs this step can't be done
    ## That is why we convert everything into characters in the previous step
    tidyr::pivot_longer(cols = -row_num:-Raw_Filename,
                        names_to = "Raw_Column_Name",
                        values_to = "values")
  
  # Identify any columns that are in the data key but (apparently) not in the data
  missing_cols <- setdiff(x = key_sub$Raw_Column_Name, y = unique(raw_df_v2$Raw_Column_Name))
  
  # If any are found, print a warning for whoever is running this
  if(length(missing_cols) != 0){
    message("Not all expected columns in '", focal_raw, "' are in *data* key!")
    message("Check (and fix if needed) raw columns: ", 
            paste0("'", missing_cols, "'", collapse = " & ")) }
  
  # Drop this object (if it exists) to avoid false warning with the next run of the loop
  if(exists("missing_cols") == T){ rm(list = "missing_cols") }
  
  # Integrate synonymized column names from key
  raw_df_v3 <- raw_df_v2 %>%
    # Attach revised column names
    dplyr::left_join(key_sub, by = c("Raw_Filename", "Raw_Column_Name")) %>%
    # Drop any columns that don't have a synonymized equivalent
    dplyr::filter(!is.na(Standardized_Column_Name)) %>%
    # Pick a standard 'not provided' entry for concentration units
    dplyr::mutate(Units = ifelse(nchar(Units) == 0, yes = NA, no = Units)) %>%
    # Handle concentration units characters that can't be in column names
    dplyr::mutate(units_fix = gsub(pattern = "\\/| |\\-", replacement = "_", x = Units)) %>%
    # Combine concentration units with column name (where conc units are provided)
    dplyr::mutate(names_actual = ifelse(test = !is.na(units_fix),
                                        yes = paste0(Standardized_Column_Name, "_", units_fix),
                                        no = Standardized_Column_Name)) %>%
    # Pare down to only needed columns (implicitly removes unspecified columns)
    dplyr::select(row_num, Dataset, Raw_Filename, names_actual, values) %>%
    # Pivot back to wide format with revised column names
    tidyr::pivot_wider(names_from = names_actual, values_from = values, values_fill = NA) %>%
    # Drop row number column
    dplyr::select(-row_num) %>%
    # Drop non-unique rows (there shouldn't be any but better safe than sorry)
    dplyr::distinct()
  
  # Do additional processing if the dataset is long format
  if(unique(key_sub$Dataset_orientation) == "long"){
    
    # Get just the relevant bit of the units key
    units_sub <- units_key %>%
      # Filter to only this site
      dplyr::filter(Raw_Filename == focal_raw) %>%
      # Make all units ready to become column headers
      dplyr::mutate(Units = gsub(pattern = "\\/| |\\-", replacement = "_", x = Units)) %>%
      # Rename variable column & tweak so it's ready to be a column name
      dplyr::mutate(solute = gsub(pattern = "\\/| |\\-", replacement = "_", x = Variable)) %>%
      dplyr::select(-Variable)
    
    # Needed wrangling
    raw_df_v4 <- raw_df_v3 %>%
      # Make solute column lowercase to increase chances of pattern match
      dplyr::mutate(solute = tolower(solute)) %>%
      # Make some other specific changes (conditionally)
      dplyr::mutate(solute = dplyr::case_when(
        ## 20221030_masterdata_chem.csv
        Dataset == "Master_2022" & solute == "daily.avg.q.(discharge)" ~ "daily_q",
        Dataset == "Master_2022" & solute == "instantaneous.q.(discharge)" ~ "instant_q",
        Dataset == "Master_2022" & solute == "spec.cond" ~ "specific_conductivity",
        Dataset == "Master_2022" & solute == "temp.c" ~ "temp",
        Dataset == "Master_2022" & solute == "suspended.chl" ~ "suspended_chl",
        ## Chem_HYBAM.csv
        Dataset == "HYBAM" & solute == "si" ~ "dsi",
        # If no conditional provided, no mismatch found so keep regular solute column
        TRUE ~ solute)) %>%
      # Make the amount column truly numeric
      dplyr::mutate(amount = as.numeric(amount)) %>%
      # Group by everything and get an average
      ## In case any solute is measured more than once in a day
      dplyr::group_by(dplyr::across(c(-amount))) %>%
      dplyr::summarize(amount = mean(amount, na.rm = T)) %>%
      dplyr::ungroup() %>%
      # Make amount back into a character (needed for later)
      dplyr::mutate(amount = as.character(amount))
    
    # Identify whether any units mismatch the data key units
    mismatch_chem <- setdiff(x = unique(units_sub$solute), 
                             y = unique(raw_df_v4$solute))
    
    # If any mismatches are found, print a warning for whoever is running this
    if(length(mismatch_chem) != 0){
      message("Solutes found in *Units* key that are absent from '", focal_raw, "'!")
      message("Fix the following solutes: ", 
              paste0("'", mismatch_chem, "'", collapse = " & ")) }
    
    # Drop this object (if it exists) to avoid false warning with the next run of the loop
    if(exists("mismatch_chem") == T){ rm(list = "mismatch_chem") }
    
    # Wrangling to get long format data into wide format
    raw_df <- raw_df_v4 %>%
      # Drop built-in units column if one exists
      dplyr::select(-dplyr::ends_with("solute_units")) %>%
      # Attach units from key
      dplyr::left_join(y = units_sub, by = c("Dataset", "Raw_Filename", "solute")) %>%
      # Attach solute and solute units into one column
      dplyr::mutate(solute_actual = paste0(solute, "_", Units)) %>%
      # Drop unwanted columns
      dplyr::select(-solute, -Units) %>%
      # Reshape wider
      tidyr::pivot_wider(names_from = solute_actual, values_from = amount, values_fill = NA)
    
    } else { raw_df <- raw_df_v3 }
  
  # Add to list
  df_list[[focal_raw]] <- raw_df
  
} # Close loop

# Unlist the list we just generated
tidy_v0 <- df_list %>%
  purrr::list_rbind(x = .)

# Check that out
dplyr::glimpse(tidy_v0)

# Clean up environment (i.e., drop everything prior to this object)
rm(list = setdiff(ls(), c("key", "tidy_v0")))

## -------------------------------------------- ##
              # Big-Picture Checks ----
## -------------------------------------------- ##

# Begin by taking a look at our 'version 0' data object's structure for macro issues
dplyr::glimpse(tidy_v0)

# Any unit-less columns?
tidy_v0 %>%
  dplyr::select(dplyr::ends_with("_NA")) %>%
  names()

# Any missing dataset-level info?
tidy_v0 %>%
  dplyr::filter(is.na(Dataset) | is.na(Raw_Filename) | is.na(LTER)) %>%
  dplyr::select(Dataset, Raw_Filename, LTER) %>%
  dplyr::distinct()

# Any missing stream names?
tidy_v0 %>%
  dplyr::select(Dataset, Raw_Filename, Stream_Name) %>%
  dplyr::distinct() %>%
  dplyr::filter(is.na(Stream_Name))
  
# Do needed wrangling
tidy_v1b <- tidy_v0 %>%
  # Fix missing LTER information
  dplyr::mutate(LTER = dplyr::case_when(
    is.na(LTER) & !is.na(Dataset) ~ Dataset,
    T ~ LTER)) %>%
  # Fill in missing stream names
  dplyr::mutate(Stream_Name = dplyr::case_when(
    Raw_Filename == "NigerRiver.csv" & is.na(Stream_Name) ~ "Niger",
    Raw_Filename == "MCM_Chem_clean.csv" ~ "McMurdo",
    Raw_Filename == "ElbeRiverChem.csv" ~ "Elbe",
    T ~ Stream_Name)) %>%
  # Drop any unitless columns that we don't want to retain
  dplyr::select(-dplyr::ends_with("_NA")) %>%
  # Also drop 'dataset orientation' column
  dplyr::select(-Dataset_orientation)
  
# Check what was lost/gained
supportR::diff_check(old = names(tidy_v0), new = names(tidy_v1b))

# Repeat earlier checks to make sure everything is fixed
## Missing dataset-level info?
tidy_v1b %>%
  dplyr::filter(is.na(Dataset) | is.na(Raw_Filename) | is.na(LTER)) %>%
  dplyr::select(Dataset, Raw_Filename, LTER) %>%
  dplyr::distinct()

## Missing stream names?
tidy_v1b %>%
  dplyr::select(Dataset, Raw_Filename, Stream_Name) %>%
  dplyr::distinct() %>%
  dplyr::filter(is.na(Stream_Name))

# Finally, remove any columns that are entirely NA (possibly due to structure of data key)
tidy_v1c <- tidy_v1b %>%
  # Also drop any columns that don't include any values
  dplyr::select(dplyr::where(~ !(all(is.na(.)) | all(. == ""))))

# See if that actually removed anything
supportR::diff_check(old = names(tidy_v1b), new = names(tidy_v1c))

# Check the structure of the remaining data
dplyr::glimpse(tidy_v1c)

## -------------------------------------------- ##
               # Numeric Checks ----
## -------------------------------------------- ##

# Before we can do units conversions we need to do numeric checks (quality control)
tidy_v2a <- tidy_v1c %>%
  # Add a row number column
  dplyr::mutate(row_num = 1:nrow(.), .before = dplyr::everything()) %>%
  # Reshape the data to get all numeric columns into long format
  tidyr::pivot_longer(cols = -row_num:-date,
                      names_to = "solute",
                      values_to = "measurement")

# Check for any malformed numbers
supportR::num_check(data = tidy_v2a, col = "measurement")

# Check for unreasonable numbers too
range(suppressWarnings(as.numeric(tidy_v2a$measurement)), na.rm = T)

# Having identified any unreasonable / malformed numbers, we can now fix them
tidy_v2b <- tidy_v2a %>%
  dplyr::mutate(measurement = dplyr::case_when(
    ## Malformed numbers / non-numbers
    measurement %in% unique(key$NA_indicator) ~ NA,
    measurement == "NaN" ~ NA,
    measurement == "N/A" ~ NA,
    measurement == "<LOD" ~ NA,
    ## Unreasonable numbers
    ### None yet!
    # If not broken, leave alone!
    T ~ measurement)) %>%
  # Filter out all NAs (don't worry we'll get them back momentarily)
  dplyr::filter(!is.na(measurement))

# Re-check for malformed numbers / unreasonable numbers
supportR::num_check(data = tidy_v2b, col = "measurement")
range(suppressWarnings(as.numeric(tidy_v2b$measurement)), na.rm = T)

# Now change class of measurements to numeric and re-claim wide format
tidy_v2c <- tidy_v2b %>%
  # Make measurement column numeric
  dplyr::mutate(measure_actual = as.numeric(measurement)) %>%
  dplyr::select(-measurement) %>%
  # Sort solutes (likely to make unit conversions easier because like columns will group together)
  dplyr::arrange(solute) %>%
  # Re-claim original format
  tidyr::pivot_wider(names_from = solute, values_from = measure_actual) %>%
  # Then drop row number column
  dplyr::select(-row_num)

# Re-check structure
dplyr::glimpse(tidy_v2c)

## -------------------------------------------- ##
# Column Name Standardization ----
## -------------------------------------------- ##

# Check current columns in data (not site info columns)
tidy_v2c %>%
  dplyr::select(-Dataset:-date) %>%
  names()

# Check structure of pH column variants
tidy_v2c %>%
  dplyr::select(dplyr::contains("pH")) %>%
  summary()

# With partially-tided columns, we can now fix any name standardization issues
tidy_v3a <- tidy_v2c %>%
  # Many variants on pH column naming
  ## Combine into one
  dplyr::mutate(ph_actual = dplyr::coalesce(pH, ph, pH_pH, ph_SU), .after = date) %>%
  ## Delete old columns
  dplyr::select(-pH, -pH_pH, -ph, -ph_SU) %>%
  ## Rename new one simply
  dplyr::rename(pH = ph_actual)

# Did that fix the pH issues?
summary(tidy_v3a$pH) ## Yes! Many fewer NAs

# Fix some of the element columns too
tidy_v3b <- tidy_v3a %>%
  # Alkalinity (uEq/L)
  dplyr::mutate(alka_actual = dplyr::coalesce(alkalinity_uEq_l, alkalinity_ueq_L),
                .after = alkalinity_ueq_L) %>%
  dplyr::select(-alkalinity_uEq_l, -alkalinity_ueq_L) %>%
  dplyr::rename(alkalinity_ueq_L = alka_actual) %>%
  # Conductivity (uS/cm)
  dplyr::mutate(cond_actual = dplyr::coalesce(cond_uS_cm, conductivity_uS_cm), 
                .after = cond_uS_cm) %>%
  dplyr::select(-cond_uS_cm, -conductivity_uS_cm) %>%
  dplyr::rename(conductivity_uS_cm = cond_actual) %>%
  # Na (mg/L)
  dplyr::mutate(na_actual = dplyr::coalesce(na_mg_L, Na_mg_L), .after = na_mg_L) %>%
  dplyr::select(-na_mg_L, -Na_mg_L) %>%
  dplyr::rename(na_mg_L = na_actual) %>%
  # TSS (mg/L)
  dplyr::mutate(tss_actual = dplyr::coalesce(tss_mg_L, TSS_mg_L), .after = tss_mg_L) %>%
  dplyr::select(-tss_mg_L, -TSS_mg_L) %>%
  dplyr::rename(tss_mg_L = tss_actual) %>%
  # VSS (mg/L)
  dplyr::mutate(vss_actual = dplyr::coalesce(vss_mg_L, VSS_mg_L), .after = vss_mg_L) %>%
  dplyr::select(-vss_mg_L, -VSS_mg_L) %>%
  dplyr::rename(vss_mg_L = vss_actual)
  
# Re-check remaining columns
tidy_v3b %>%
  dplyr::select(-Dataset:-date) %>%
  names()

## -------------------------------------------- ##
              # Unit Conversions ----
## -------------------------------------------- ##

# Check what units are in the data
tidy_v3b %>%
  dplyr::select(-Dataset:-date) %>%
  names()

# Define any needed molecular weights here
Al_mw <- 26.981539
Br_mw <- 79.904
Ca_mw <- 40.078
Cl_mw <- 35.453

# Need to do unit conversions to get each metric into a single, desired unit
tidy_v4 <- tidy_v3b %>%
  # Aluminum
  dplyr::mutate(al_uM = al_mg_L * Al_mw, .after = al_mg_L) %>%
  dplyr::select(-al_mg_L) %>%
  # Bromine
  dplyr::mutate(br_uM = br_mg_L * Br_mw, .after = br_mg_L) %>%
  dplyr::select(-br_mg_L) %>%
  # Calcium
  dplyr::mutate(ca_uM = ifelse(test = (is.na(ca_uM) == T),
                               yes = (ca_mg_L * Ca_mw),
                               no = ca_uM), .after = ca_mg_L) %>%
  dplyr::select(-ca_mg_L) %>%
  # Chlorine
  dplyr::mutate(cl_uM = ifelse(test = (is.na(cl_uM) == T),
                               yes = (cl_mg_L * Cl_mw),
                               no = cl_uM), .after = cl_mg_L) %>%
  dplyr::select(-cl_mg_L)
  




# Re-check names and structure
## Names
tidy_v4 %>%
  dplyr::select(-Dataset:-date) %>%
  names()

## Structure
dplyr::glimpse(tidy_v4)

## -------------------------------------------- ##
                  # Export ----
## -------------------------------------------- ##

# Create one final tidy object
tidy_final <- tidy_v3

# Check structure
dplyr::glimpse(tidy_final)

# Generate a date-stamped file name for this file
( chem_filename <- paste0("tidy_chemistry_", Sys.Date(), ".csv") )

# Export locally
write.csv(x = tidy_final, file = file.path("tidy", chem_filename), na = '', row.names = F)

# Export to Drive
# googledrive::drive_upload(media = file.path("tidy", chem_filename), overwrite = T,
#                           path = googledrive::as_id(""))

# End ----
