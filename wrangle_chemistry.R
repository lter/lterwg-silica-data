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

# Identify all raw chemistry files
chem_drive <- googledrive::drive_ls(path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1da8AkSvforPehp-gPJsmA7-pPAJ913mz"))
  
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
      dplyr::select(-Variable) %>%
      # Also drop Dataset column
      dplyr::select(-Dataset)
    
    # Needed wrangling
    raw_df_v4 <- raw_df_v3 %>%
      # Make solute column lowercase to increase chances of pattern match
      dplyr::mutate(solute = tolower(gsub(pattern = "\\/| |\\-", 
                                          replacement = "_", x = solute))) %>%
      # Make some other specific changes (conditionally)
      dplyr::mutate(solute = dplyr::case_when(
        ## Canada_WQ_dat.csv
        # Dataset == "Canada" ~ gsub(pattern = " ", replacement = "_", x = solute),
        ## NT_NSW_Chem_Cleaned.csv
        # Dataset == "Australia" ~ gsub(pattern = "\\/| |\\-", replacement = "_", x = solute),
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
      dplyr::left_join(y = units_sub, by = c("Raw_Filename", "solute")) %>%
      # Filter to only solutes included specifically in units key
      dplyr::filter(solute %in% unique(units_sub$solute)) %>% 
      # Attach solute and solute units into one column
      dplyr::mutate(solute_actual = paste0(solute, "_", Units)) %>%
      # Drop unwanted columns
      dplyr::select(-solute, -Units) %>%
      # Reshape wider averaging within groups if duplicate values are found
      tidyr::pivot_wider(names_from = solute_actual, values_from = amount, values_fill = NA, 
                         values_fn = ~ as.character(mean(x = as.numeric(.x), na.rm = T)))
    
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
  # Fix missing dataset info using the reverse operation
  dplyr::mutate(Dataset = dplyr::case_when(
    is.na(Dataset) & !is.na(LTER) ~ LTER,
    T ~ Dataset)) %>%
  # Fill in missing stream names - added "Stream_Name" to original datasets 1/2/24
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
    # Catalina Jemez has many versions of -9999
    measurement == -9999.00 ~ "NA",
    measurement == -9999.000 ~ "NA",
    measurement == -9999.0000 ~ "NA",
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
  dplyr::mutate(ph_actual = dplyr::coalesce(pH, ph, pH_pH, ph_SU, ph_), .after = date) %>%
  ## Delete old columns
  dplyr::select(-pH, -pH_pH, -ph, -ph_SU, -ph_) %>%
  ## Rename new one simply
  dplyr::rename(pH = ph_actual)

# Did that fix the pH issues?
summary(tidy_v3a$pH) ## Yes! Many fewer NAs

### NEED TO COMBINE CHLOROPHYLL COLUMNS ###

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
  names() %>% sort()

# Now handle the Canadian solutes (they use un-abbreviated names) 
tidy_v3c <- tidy_v3b %>%
  ## DOC
  dplyr::mutate(doc_actual = dplyr::coalesce(doc_mg_L, carbon_dissolved_organic_mg_L)) %>%
  dplyr::select(-doc_mg_L, -carbon_dissolved_organic_mg_L) %>%
  dplyr::rename(doc_mg_L = doc_actual) %>%
  ## TDN
  dplyr::rename(tdn_mg_L = nitrogen_total_dissolved_mg_N_L) %>%
  ## TN
  dplyr::mutate(tn_actual = dplyr::coalesce(tn_mg_N_L, nitrogen_total_mg_N_L)) %>%
  dplyr::select(-tn_mg_N_L, -nitrogen_total_mg_N_L) %>%
  dplyr::rename(tn_mg_L = tn_actual) %>%
  ## NH4
  dplyr::mutate(nh4_actual = dplyr::coalesce(nh4_mg_NH4_L, ammonia_dissolved_mg_NH4_L)) %>%
  dplyr::select(-nh4_mg_NH4_L, -ammonia_dissolved_mg_NH4_L) %>%
  dplyr::rename(nh4_mg_NH4_L = nh4_actual) %>%
  ## NO3
  dplyr::mutate(no3_actual = dplyr::coalesce(no3_mg_NO3_L, dissolved_nitrogen_nitrate_mg_NO3_L)) %>%
  dplyr::select(-no3_mg_NO3_L, -dissolved_nitrogen_nitrate_mg_NO3_L) %>%
  dplyr::rename(no3_mg_NO3_L = no3_actual) %>%
  ## TP
  dplyr::rename(tp_mg_L = phosphorus_total_mg_P_L) %>%
  ## TDP
  dplyr::rename(tdp_mg_L = phosphorus_total_dissolved_mg_P_L) %>%
  ## DSi
  dplyr::mutate(dsi_actual = dplyr::coalesce(dsi_mg_Si_L, silicon_extractable_mg_Si_L)) %>%
  dplyr::select(-dsi_mg_Si_L, -silicon_extractable_mg_Si_L) %>%
  dplyr::rename(dsi_mg_L = dsi_actual)

# Check again
tidy_v3c %>%
  dplyr::select(-Dataset:-date) %>%
  names() %>% sort()

## -------------------------------------------- ##
              # Unit Conversions ----
## -------------------------------------------- ##

# Check what units are in the data
tidy_v3c %>%
  dplyr::select(-Dataset:-date) %>%
  names() %>% sort()

# First, do any non-elemental unit conversions
tidy_v4a <- tidy_v3c %>%
  # Alkalinity (uM)
  ## Pretty sure 1 uEq/L == 1 uM
  dplyr::mutate(alkalinity_uM = dplyr::coalesce(alkalinity_uM, alkalinity_ueq_L)) %>%
  dplyr::select(-alkalinity_ueq_L) %>%
  # Conductivity (uS/cm)
  dplyr::mutate(conductivity_uS_cm = ifelse(test = (is.na(conductivity_uS_cm) == T),
                                            yes = (conductivity_mS_m * 10^3 * 0.01),
                                            no = conductivity_uS_cm)) %>%
  dplyr::select(-conductivity_mS_m) %>%
  # Turbidity
  dplyr::mutate(turbidity_NTU = dplyr::coalesce(turbidity_NTU, turb_NTU)) %>%
  dplyr::select(-turb_NTU) %>% 
  # Relocate all of these columns to the left of the elemental columns
  dplyr::relocate(temp_C, alkalinity_uM, conductivity_uS_cm, 
                  specific_conductivity_uS_cm, suspended_chl_ug_L, turbidity_NTU,
                  tds_mg_L, dplyr::starts_with("tss_"), dplyr::starts_with("vss_"),
                  .after = pH) %>%
  # Also moving discharge columns to left
  dplyr::relocate(dplyr::contains("_q"), .before = pH)
  
# Re-check remaining columns
tidy_v4a %>%
  dplyr::select(-Dataset:-vss_mg_L) %>%
  names() %>% sort()

# Define any needed molecular weights here
Al_mw <- 26.981539
Br_mw <- 79.904
C_mw <- 12.011
Ca_mw <- 40.078
Cl_mw <- 35.453
F_mw <- 18.998403
Fe_mw <- 55.845
H_mw <- 1.00784
K_mw <- 39.0983
Li_mw <- 6.941
Mg_mw <- 24.305
Mn_mw <- 54.938044
N_mw <- 14.0067
Na_mw <- 22.989769
O_mw <- 15.999
P_mw <- 30.973762
S_mw <- 32.065
Si_mw <- 28.0855
Sr_mw <- 87.62

# Calculate any needed molecules' molecular weights here
HCO3_mw <- H_mw + C_mw + (O_mw * 3)
NH3_mw <- N_mw + (H_mw * 3)
NH4_mw <- N_mw + (H_mw * 4)
NO2_mw <- N_mw + (O_mw * 2)
NO3_mw <- N_mw + (O_mw * 3)
PO4_mw <- P_mw + (O_mw * 4)
SO4_mw <- S_mw + (O_mw * 4)

# Need to do unit conversions to get each metric into a single, desired unit
tidy_v4b <- tidy_v4a %>%
  # Aluminum
  dplyr::mutate(al_uM = (al_mg_L / Al_mw) * 10^3, 
                .after = al_mg_L) %>%
  dplyr::select(-al_mg_L) %>%
  # Bromine
  dplyr::mutate(br_uM = (br_mg_L / Br_mw) * 10^3, 
                .after = br_mg_L) %>%
  dplyr::select(-br_mg_L) %>%
  # Calcium
  dplyr::mutate(ca_uM = ifelse(test = (is.na(ca_uM) == T),
                               yes = ((ca_mg_L / Ca_mw) * 10^3),
                               no = ca_uM), .after = ca_mg_L) %>%
  dplyr::select(-ca_mg_L) %>%
  # Chlorine
  dplyr::mutate(cl_uM = ifelse(test = (is.na(cl_uM) == T),
                               yes = ((cl_mg_L / Cl_mw) * 10^3),
                               no = cl_uM), .after = cl_mg_L) %>%
  dplyr::select(-cl_mg_L) %>%
  # Dissolved Organic Carbon
  dplyr::mutate(doc_uM = dplyr::case_when(
    !is.na(doc_uM) ~ doc_uM,
    is.na(doc_uM) & !is.na(doc_mg_C_L) ~ (doc_mg_C_L / C_mw) * 10^3,
    is.na(doc_uM) & !is.na(doc_mg_L) ~ (doc_mg_L / C_mw) * 10^3,
    T ~ NA)) %>%
  dplyr::select(-doc_mg_C_L, -doc_mg_L) %>%
  # Dissolved Oxygen
  dplyr::mutate(do_uM = (do_mg_O2_L / (O_mw * 2)) * 10^3, 
                .after = do_mg_O2_L) %>%
  dplyr::select(-do_mg_O2_L) %>%
  # Silica (!)
  dplyr::mutate(dsi_uM = dplyr::case_when(
    !is.na(dsi_uM) ~ dsi_uM,
    is.na(dsi_uM) & !is.na(dsi_mg_L) ~ (dsi_mg_L / Si_mw) * 10^3,
    is.na(dsi_uM) & !is.na(dsi_mg_SiO2_L) ~ (dsi_mg_SiO2_L / (Si_mw + (O_mw * 2))) * 10^3,
    is.na(dsi_uM) & !is.na(dsi_mg_L) ~ (dsi_mg_L / Si_mw) * 10^3,
    T ~ NA)) %>%
  dplyr::select(-dsi_mg_L, -dsi_mg_SiO2_L, -dsi_mg_L) %>%
  # Fluorine
  dplyr::mutate(f_uM = ifelse(test = (is.na(f_uM) == T),
                              yes = (f_mg_L / F_mw) * 10^3,
                              no = f_uM)) %>%
  dplyr::select(-f_mg_L) %>%
  # Iron
  dplyr::mutate(fe_uM = (fe_mg_L / Fe_mw) * 10^3, .after = fe_mg_L) %>%
  dplyr::select(-fe_mg_L) %>%
  # Bicarbonate (HCO3)
  dplyr::mutate(hco3_uM = ifelse(test = (is.na(hco3_uM) == T),
                                 yes = (hco3_mg_L / HCO3_mw) * 10^3,
                                 no = hco3_uM)) %>%
  dplyr::select(-hco3_mg_L) %>%
  # Potassium
  dplyr::mutate(k_uM = ifelse(test = (is.na(k_uM) == T),
                              yes = (k_mg_L / K_mw) * 10^3,
                              no = k_uM)) %>%
  dplyr::select(-k_mg_L) %>%
  # Lithium
  dplyr::mutate(li_uM = (li_mg_L / Li_mw) * 10^3, .after = li_mg_L) %>%
  dplyr::select(-li_mg_L) %>%
  # Magensium
  dplyr::mutate(mg_uM = ifelse(test = (is.na(mg_uM) == T),
                               yes = (mg_mg_L / Mg_mw) * 10^3,
                               no = mg_uM)) %>%
  dplyr::select(-mg_mg_L) %>%
  # Manganese
  dplyr::mutate(mn_uM = (mn_ug_L / Mn_mw), .after = mn_ug_L) %>%
  dplyr::select(-mn_ug_L) %>%
  # Sodium
  dplyr::mutate(na_uM = ifelse(test = (is.na(na_uM) == T),
                              yes = (na_mg_L / Na_mw) * 10^3,
                              no = na_uM)) %>%
  dplyr::select(-na_mg_L) %>%
  # Ammonia (NH3)
  dplyr::mutate(nh3_uM = dplyr::case_when(
    !is.na(nh3_mg_NH3_N_L) ~ (nh3_mg_NH3_N_L / N_mw) * 10^3,
    T ~ NA), .after = nh3_mg_NH3_N_L) %>%
  dplyr::select(-nh3_mg_NH3_N_L) %>%
  # Ammonium (NH4)
  dplyr::mutate(nh4_uM = dplyr::case_when(
    !is.na(nh4_uM) ~ nh4_uM,
    is.na(nh4_uM) & !is.na(nh4_mg_NH4_N_L) ~ (nh4_mg_NH4_N_L / N_mw) * 10^3,
    is.na(nh4_uM) & !is.na(nh4_ug_NH4_N_L) ~ nh4_ug_NH4_N_L / N_mw,
    T ~ NA)) %>%
  dplyr::select(-nh4_mg_NH4_N_L, -nh4_ug_NH4_N_L) %>%
  # Ammoni__ (NHx)
  dplyr::mutate(nhx_uM = (nhx_mg_NH4_N_L / N_mw) * 10^3, 
                .after = nhx_mg_NH4_N_L) %>%
  dplyr::select(-nhx_mg_NH4_N_L) %>%
  # Nitrate (NO3)
  dplyr::mutate(no3_uM = dplyr::case_when( 
    !is.na(no3_uM) ~ no3_uM,
    is.na(no3_uM) & !is.na(no3_mg_NO3_L) ~ (no3_mg_NO3_L / NO3_mw) * 10^3,
    is.na(no3_uM) & !is.na(no3_ug_NO3_N_L) ~ (no3_ug_NO3_N_L / NO3_mw),
    T ~ NA)) %>%
  dplyr::select(-no3_mg_NO3_L, -no3_ug_NO3_N_L) %>%
  # Nitr__ (NOx)
  dplyr::mutate(nox_uM = dplyr::case_when(
    !is.na(nox_uM) ~ nox_uM,
    is.na(nox_uM) & !is.na(nox_mg_NO3_N_L) ~ (nox_mg_NO3_N_L / N_mw) * 10^3,
    is.na(nox_uM) & !is.na(nox_ug_NO3_N_L) ~ nox_ug_NO3_N_L / N_mw,
    T ~ NA)) %>%
  dplyr::select(-nox_mg_NO3_N_L, -nox_ug_NO3_N_L) %>%
  # Phosphate (PO4)
  dplyr::mutate(po4_uM = dplyr::case_when(
    !is.na(po4_uM) ~ po4_uM,
    is.na(po4_uM) & !is.na(po4_mg_PO4_L) ~ (po4_mg_PO4_L / PO4_mw) * 10^3,
    is.na(po4_uM) & !is.na(po4_mg_PO4_P_L) ~ (po4_mg_PO4_P_L / P_mw) * 10^3,
    is.na(po4_uM) & !is.na(po4_ug_PO4_P_L) ~ (po4_ug_PO4_P_L / P_mw),
    T ~ NA)) %>%
  dplyr::select(-po4_mg_PO4_L, -po4_mg_PO4_P_L, -po4_ug_PO4_P_L) %>%
  # Sulfate (SO4)
  dplyr::mutate(so4_uM = dplyr::case_when(
    !is.na(so4_uM) ~ so4_uM,
    !is.na(so4_um) ~ so4_um,
    is.na(so4_uM) & !is.na(so4_mg_SO4_L) ~ (so4_mg_SO4_L / SO4_mw) * 10^3,
    T ~ NA)) %>%
  dplyr::select(-so4_um, -so4_mg_SO4_L) %>%
  # Strontium (Sr)
  dplyr::mutate(sr_uM = (sr_mg_L / Sr_mw) * 10^3,
                .before = sr_mg_L) %>%
  dplyr::select(-sr_mg_L) %>%
  # Soluble Reactive Phosphorus (SRP)
  dplyr::mutate(srp_uM = dplyr::case_when(
    !is.na(srp_uM) ~ srp_uM,
    is.na(srp_uM) & !is.na(srp_mg_P_L) ~ (srp_mg_P_L / P_mw) * 10^3,
    is.na(srp_uM) & !is.na(srp_ug_P_L) ~ (srp_ug_P_L / P_mw),
    T ~ NA)) %>%
  dplyr::select(-srp_mg_P_L, -srp_ug_P_L) %>%
  # Total Dissolved Nitrogen (TDN)
  dplyr::mutate(tdn_uM = dplyr::case_when(
    !is.na(tdn_uM) ~ tdn_uM,
    is.na(tdn_uM) & !is.na(tdn_mg_L) ~ (tdn_mg_L / N_mw) * 10^3,
    T ~ NA)) %>%
  dplyr::select(-tdn_mg_L) %>%
  # total Kjeldahl nitrogen (TKN)
  dplyr::mutate(tkn_uM = (tkn_mg_N_L / N_mw) * 10^3, 
                .after = tkn_mg_N_L) %>%
  dplyr::select(-tkn_mg_N_L) %>%
  # Total Nitrogen (TN)
  dplyr::mutate(tn_uM = ifelse(test = is.na(tn_uM) == T,
                               yes = (tn_mg_L / N_mw) * 10^3,
                               no = tn_uM)) %>%
  dplyr::select(-tn_mg_L) %>%
  # Total Organic Carbon (TOC)
  dplyr::mutate(toc_uM = dplyr::case_when(
    !is.na(toc_uM) ~ toc_uM,
    is.na(toc_uM) & !is.na(toc_mg_C_L) ~ (toc_mg_C_L / C_mw) * 10^3,
    is.na(toc_uM) & !is.na(toc_mg_L) ~ (toc_mg_L / C_mw) * 10^3,
    T ~ NA)) %>%
  dplyr::select(-toc_mg_C_L, -toc_mg_L) %>%
  # Total Phosphorus (TP)
  dplyr::mutate(tp_uM = dplyr::case_when(
    !is.na(tp_uM) ~ tp_uM,
    is.na(tp_uM) & !is.na(tp_mg_P_L) ~ tp_mg_P_L / P_mw * 10^3,
    is.na(tp_uM) & !is.na(tp_mg_L) ~ tp_mg_L / P_mw * 10^3,
    T ~ NA)) %>%
  dplyr::select(-tp_mg_P_L, -tp_mg_L)

# Re-check names
tidy_v4b %>%
  dplyr::select(-Dataset:-vss_mg_L) %>%
  names() %>% 
  sort()

# Examine full structure too
dplyr::glimpse(tidy_v4b)

## -------------------------------------------- ##
            # Clarification Tweaks ----
## -------------------------------------------- ##

# Some of these column names are heavily abbreviated
# Can expand them to make them a little more intuitive
tidy_v5 <- tidy_v4b %>%
  # And consensus was that TSS & SPM are equivalent
  dplyr::mutate(spm_actual = dplyr::coalesce(spm_mg_L, tss_mg_L)) %>%
  dplyr::select(-spm_mg_L, -tss_mg_L) %>%
  # Expand column names to be more descriptive
  dplyr::rename(
    # DO
    dissolved_oxygen_uM = do_uM,
    # DOC/DIC
    dissolved_org_c_uM = doc_uM,
    dissolved_inorg_c_uM = dic_uM,
    # DON/DIN
    dissolved_org_n_uM = don_uM,
    dissolved_inorg_n_uM = din_uM,
    # SRP
    soluble_reactive_p_uM = srp_uM,
    # SPM / TSS
    susp_partic_matter_uM = spm_actual,
    # SSC
    susp_sediment_conc_mg_L = ssc_mg_L,
    # Suspended chlorophyll
    susp_chl_ug_L = suspended_chl_ug_L,
    # TDS
    tot_dissolved_solids_mg_L = tds_mg_L,
    # TDN
    tot_dissolved_n_uM = tdn_uM,
    # TN
    tot_n_uM = tn_uM,
    # TKN
    tot_kjeldahl_n_uM = tkn_uM,
    # TOC
    tot_org_c_uM = toc_uM,
    # TP
    tot_p_uM = tp_uM,
    # VSS
    volatile_susp_solids_mg_L = vss_mg_L) %>%
  # Reorder these slightly
  dplyr::relocate(dplyr::starts_with("susp_"), dplyr::starts_with("tot_"),
                  dplyr::starts_with("dissolved_"), soluble_reactive_p_uM, 
                  .after = volatile_susp_solids_mg_L)
  
# Re-check structure
dplyr::glimpse(tidy_v5)

## -------------------------------------------- ##
            # Change Data Shape ----
## -------------------------------------------- ##

# Data need to be in long format so we'll do that here
tidy_v6 <- tidy_v5 %>%
  # Drop discharge columns
  dplyr::select(-dplyr::contains("_q_")) %>%
  # Flip variables into long format
  tidyr::pivot_longer(cols = -Dataset:-date,
                      names_to = "vars_raw", values_to = "value") %>%
  # Filter out all the NAs that introduces
  dplyr::filter(!is.na(value)) %>%
  # Create a standard character between variable name and units
  dplyr::mutate(vars_std = gsub(pattern = "_uM", replacement = "--uM", x = vars_raw)) %>%
  dplyr::mutate(vars_std = gsub(pattern = "_uS", replacement = "--uS", x = vars_std)) %>%
  dplyr::mutate(vars_std = gsub(pattern = "_ug", replacement = "--ug", x = vars_std)) %>%
  dplyr::mutate(vars_std = gsub(pattern = "_mg", replacement = "--mg", x = vars_std)) %>%
  dplyr::mutate(vars_std = gsub(pattern = "_C", replacement = "--C", x = vars_std)) %>%
  dplyr::mutate(vars_std = gsub(pattern = "_NTU", replacement = "--NTU", x = vars_std)) %>%
  # Separate solute/units into different columns
  tidyr::separate_wider_delim(cols = vars_std, delim = "--", too_few = "align_start",
                              names = c("vars_only", "units_only")) %>%
  # Process the units formatting slightly
  dplyr::mutate(units = gsub(pattern = "_", replacement = "/", x = units_only)) %>%
  # Now wrangle the variable names
  dplyr::mutate(variable = dplyr::case_when(
    ## Fix pH
    vars_only == "ph" ~ "pH",
    ## Fix Silica!
    vars_only == "dsi" ~ "DSi",
    ## If one or two letters (i.e., is an element), make the first capital
    nchar(vars_only) == 1 ~ toupper(vars_only),
    nchar(vars_only) == 2 ~ stringr::str_to_title(vars_only),
    ## If there's an X or a number (i.e., is a molecule), make the whole thing uppercase
    nchar(vars_only) < 4 & 
      stringr::str_detect(string = vars_only, pattern = "x") ~ toupper(vars_only),
    stringr::str_detect(string = vars_only, pattern = "[[:digit:]]") ~ toupper(vars_only),
    ## Handle idiosyncratic fixes
    vars_only == "chla" ~ "chlorophyll a",
    vars_only == "dissolved_inorg_c" ~ "dissolved inorg C",
    vars_only == "dissolved_inorg_n" ~ "dissolved inorg N",
    vars_only == "dissolved_org_c" ~ "dissolved org C",
    vars_only == "dissolved_org_n" ~ "dissolved org N",
    vars_only == "soluble_reactive_p" ~ "SRP",
    vars_only == "tot_dissolved_n" ~ "tot dissolved N",
    vars_only == "tot_kjeldahl_n" ~ "tot kjeldahl N",
    vars_only == "tot_n" ~ "TN",
    vars_only == "tot_org_c" ~ "tot org C",
    vars_only == "tot_p" ~ "TP",
    ## Otherwise just swap underscores for spaces
    T ~ gsub(pattern = "_", replacement = " ", x = vars_only)),
    .before = units) %>%
  # Fix pernicious issues with a few oddballs
  dplyr::mutate(variable = gsub(pattern = "Ph", replacement = "pH", x = variable),
                variable = gsub(pattern = "Ec", replacement = "EC", x = variable),
                variable = gsub(pattern = "NOX", replacement = "NOx", x = variable)) %>% 
  # Drop columns we made to get here
  dplyr::select(-vars_raw, -dplyr::ends_with("_only")) %>%
  # Move value to the end
  dplyr::relocate(value, .after = dplyr::everything())

# Happy with resulting variables/units?
sort(unique(tidy_v6$variable))
sort(unique(tidy_v6$units))

# Check structure
dplyr::glimpse(tidy_v6)

## -------------------------------------------- ##
              # Handle Outliers ----
## -------------------------------------------- ##

# Now we need to identify / handle outlier values
tidy_v7a <- tidy_v6 %>%
  # Group by everything except value and date
  dplyr::group_by(Dataset, Raw_Filename, LTER, Stream_Name, variable, units) %>%
  # Calculate some needed metrics
  dplyr::mutate(mean_value = mean(value, na.rm = T),
                sd_value = sd(value, na.rm = T),
                outlier_thresh = (abs(mean_value) + abs(sd_value * 2)) ) %>%
  # Identify outliers
  dplyr::mutate(is_outlier = ifelse(test = abs(value) > outlier_thresh,
                                    yes = TRUE, no = FALSE)) %>%
  # Ungroup
  dplyr::ungroup()
  
# Identify percent outliers
(nrow(dplyr::filter(tidy_v7a, is_outlier == T)) / nrow(tidy_v6)) * 100

# Handle outliers
tidy_v7b <- tidy_v7a %>%
  # Currently handling outliers by removing them
  dplyr::filter(is_outlier == FALSE | is.na(is_outlier)) %>%
  # Then drop the columns we needed for this filtering step
  dplyr::select(-mean_value, -sd_value, -outlier_thresh, -is_outlier)

# Did that work? Should lose some rows
nrow(tidy_v7a) - nrow(tidy_v7b)

# Glimpse it
dplyr::glimpse(tidy_v7b)

## -------------------------------------------- ##
                # Wrangle Dates ----
## -------------------------------------------- ##

# Check out current dates
sort(unique(tidy_v7b$date))

# Try to standardize date formatting a bit
tidy_v8a <- tidy_v7b %>%
  # Remove time stamps where present
  dplyr::mutate(date_v2 = gsub(pattern = " [[:digit:]]{1,2}\\:[[:digit:]]{1,2}",
                               replacement = "", x = date),
                .after = date) %>%
  # Standardize to only using slashes between numbers
  dplyr::mutate(date_v3 = gsub(pattern = "\\_|\\-", replacement = "/", x = date_v2),
                .after = date_v2) %>%
  # Remove bizarre trailing ":00" on some dates
  dplyr::mutate(date_v4 = gsub(pattern = "\\:00", replacement = "", x = date_v3),
                .after = date_v3) %>%
  # Rename original date column
  dplyr::rename(date_v1 = date)

# Look at general date format per LTER
tidy_v8a %>%
  dplyr::group_by(Raw_Filename) %>%
  dplyr::summarize(dates = paste(unique(date_v4), collapse = "; ")) %>%
  tidyr::pivot_wider(names_from = Raw_Filename, values_from = dates) %>%
  dplyr::glimpse()

# Identify format for each file name based on **human eye/judgement**
tidy_v8b <- tidy_v8a %>%
  dplyr::mutate(date_format = dplyr::case_when(
    Raw_Filename == "20221030_masterdata_chem_V2.csv" ~ "ymd",
    Raw_Filename == "Australia_MurrayBasin_PJulian_071723.csv" ~ "ymd",
    Raw_Filename == "CAMELS_USGS_N_P.csv" ~ "ymd",
    Raw_Filename == "CAMREX_filled_template.csv" ~ "mdy",
    Raw_Filename == "Canada_WQ_dat.csv" ~ "ymd",
    Raw_Filename == "Chem_Cameroon.csv" ~ "mdy",
    Raw_Filename == "Chem_HYBAM.csv" ~ "mdy",
    Raw_Filename == "ElbeRiverChem.csv" ~ "dmy",
    Raw_Filename == "Krycklan_NP.csv" ~ "mdy",
    Raw_Filename == "MCM_Chem_clean.csv" ~ "mdy",
    Raw_Filename == "MurrayDarlingChem_Merged.csv" ~ "ymd",
    Raw_Filename == "NIVA_Water_chemistry.csv" ~ "mdy",
    Raw_Filename == "NT_NSW_Chem_Cleaned.csv" ~ "dmy",
    Raw_Filename == "NigerRiver.csv" ~ "mdy",
    Raw_Filename == "SiSyn_DataTemplate_Sweden_102423.csv" ~ "mdy",
    Raw_Filename == "UK_Si.csv" ~ "dmy",
    Raw_Filename == "UMR_si_new_sites.csv" ~ "ymd",
    Raw_Filename == "UMR_si_update_existing_sites.csv" ~ "ymd",
    Raw_Filename == "USGS_NWQA_Chemistry_MissRiverSites.csv" ~ "ymd",
    Raw_Filename == "NEON_Chem.csv" ~"ymd",
    Raw_Filename == "WalkerBranch_Chem.csv" ~"ymd",
    Raw_Filename == "CatalinaJemez_chemistry_2009-2019_V2.csv"~ "mdy",
    # Raw_Filename == "" ~ "",
    T ~ "UNKNOWN"))

# Check remaining date formats
tidy_v8b %>%
  dplyr::group_by(date_format) %>%
  dplyr::summarize(files = paste(unique(Raw_Filename), collapse = "; "))

# Let's do the last (hopefully) little bit of date wrangling
tidy_v8c <- tidy_v8b %>%
  # Break apart the date information
  tidyr::separate_wider_delim(cols = date_v4, delim = "/", names = c("date1", "date2", "date3"),
                              too_few = "align_start", too_many = "merge", cols_remove = F) %>%
  # If year is two-digits, fill up to 4
  dplyr::mutate(date3 = dplyr::case_when(
    # Anything after '24 is definitely "19__"
    date_format == "mdy" & nchar(date3) == 2 & as.numeric(date3) >= 24 ~ paste0("19", date3),
    # Less than '24 is likely early 2000s (rather than really early 1900s)
    date_format == "mdy"  & nchar(date3) == 2 & as.numeric(date3) < 24 ~ paste0("20", date3),
    date_format == "dmy" & nchar(date3) == 2 & as.numeric(date3) >= 24 ~ paste0("19", date3),
    # Less than '24 is likely early 2000s (rather than really early 1900s)
    date_format == "dmy"  & nchar(date3) == 2 & as.numeric(date3) < 24 ~ paste0("20", date3),
    T ~ date3)) %>%
  # Do the same fixes for the other date format
  dplyr::mutate(date1 = dplyr::case_when(
    date_format == "ymd" & nchar(date1) == 2 & as.numeric(date1) >= 24 ~ paste0("19", date1),
    date_format == "ymd" & nchar(date1) == 2 & as.numeric(date1) < 24 ~ paste0("20", date1),
    T ~ date1)) %>%
  # Make the three components into numbers
  dplyr::mutate(dplyr::across(.cols = date1:date3,
                              .fns = as.numeric))

# Any non 4-digit years remaining?
tidy_v8c %>%
  dplyr::filter((date_format == "ymd" & nchar(date1) != 4) | 
                  (date_format == "mdy" & nchar(date3) != 4)) %>%
  dplyr::glimpse()


# Use formats identified above to determine day, month, and year
tidy_v8d <- tidy_v8c %>% 
  dplyr::mutate(
    ## Days
    day = dplyr::case_when(
      date_format == "ymd" ~ date3,
      date_format == "mdy" ~ date2,
      date_format == "dmy" ~ date1,
      T ~ NA),
    ## Months
    month = dplyr::case_when(
      date_format == "ymd" ~ date2,
      date_format == "mdy" ~ date1,
      date_format == "dmy" ~ date2,
      T ~ NA),
    ## Years
    year = dplyr::case_when(
      date_format == "ymd" ~ date1,
      date_format == "mdy" ~ date3,
      date_format == "dmy" ~ date3,
      T ~ NA),
    .after = date_v4)

# How's that look?
dplyr::glimpse(tidy_v8d)

# Assemble back into a real date
tidy_v8e <- tidy_v8d %>% 
  dplyr::mutate(date_v5 = paste(day, month, year, sep = "-"),
                .after = date_v4) %>%
  # Make it truly a date
  dplyr::mutate(date_actual = as.Date(x = date_v5, format = "%d-%m-%Y"),
                .after = date_v5)

# Glimpse it
dplyr::glimpse(tidy_v8e)

# Check date range quickly
range(tidy_v8e$date_actual, na.rm = T)

# Final (actually this time) date wrangling
tidy_v8f <- tidy_v8e %>%
  # Rename final date column
  dplyr::rename(date = date_actual) %>%
  # Drop all intermediary columns
  dplyr::select(-date1, -date2, -date3, -day, -month, -year,
                -dplyr::starts_with("date_"))

# Check structure
dplyr::glimpse(tidy_v8f)

## -------------------------------------------- ##
                  # Export ----
## -------------------------------------------- ##

# Create one final tidy object
tidy_final <- tidy_v8f

# Check structure
dplyr::glimpse(tidy_final)

# Grab today's date
date <- gsub(pattern = "-", replacement = "", x = Sys.Date())

# Generate a date-stamped file name for this file
( chem_filename <- paste0(date, "_masterdata_chem.csv") )

# Export locally
write.csv(x = tidy_final, file = file.path("tidy", chem_filename), na = '', row.names = F)

# Export to Drive
googledrive::drive_upload(media = file.path("tidy", chem_filename), overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1dTENIB5W2ClgW0z-8NbjqARiaGO2_A7W"))

# End ----
