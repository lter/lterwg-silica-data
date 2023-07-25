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

# Process the data key object as needed
key <- key_v1 %>%
  # Drop unwanted columns
  dplyr::select(-Data_type, -Notes)

# Re-check structure
dplyr::glimpse(key)

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
    dplyr::filter(!is.na(Combined_Column_Name) & nchar(Combined_Column_Name) != 0)
  
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
    message("Not all expected columns in '", focal_raw, "' are in data key!")
    message("Check (and fix if needed) raw columns: ", 
            paste0("'", missing_cols, "'", collapse = " & ")) }
  
  # Drop this object (if it exists) to avoid false warning with the next run of the loop
  if(exists("missing_cols") == T){ rm(list = "missing_cols") }
  
  # Integrate synonymized column names from key
  raw_df_v3 <- raw_df_v2 %>%
    # Attach revised column names
    dplyr::left_join(key_sub, by = c("Raw_Filename", "Raw_Column_Name")) %>%
    # Drop any columns that don't have a synonymized equivalent
    dplyr::filter(!is.na(Combined_Column_Name)) %>%
    # Pick a standard 'not provided' entry for concentration units
    dplyr::mutate(Concentration_Units = ifelse(nchar(Concentration_Units) == 0,
                                               yes = NA, no = Concentration_Units)) %>%
    # Handle concentration units characters that can't be in column names
    dplyr::mutate(conc_actual = gsub(pattern = "\\/| |\\-", replacement = "_", 
                                     x = Concentration_Units)) %>%
    # Combine concentration units with column name (where conc units are provided)
    dplyr::mutate(names_actual = ifelse(!is.na(conc_actual),
                                        yes = paste0(Combined_Column_Name, "_", conc_actual),
                                        no = Combined_Column_Name)) %>%
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
    
    # Grab just the unit information of the key sub object
    units_key <- key_sub %>%
      # Drop unwanted columns
      dplyr::select(dplyr::ends_with("_unit")) %>%
      # Flip to long format
      tidyr::pivot_longer(cols = dplyr::everything(), 
                          names_to = "solute", 
                          values_to = "units") %>%
      # Drop duplicate rows / rows without units
      dplyr::filter(!is.na(units)) %>%
      dplyr::distinct() %>%
      # Drop "unit" bit of column
      dplyr::mutate(solute = gsub(pattern = "_unit", replacement = "", x = solute)) %>%
      # Make all units ready to become column headers
      dplyr::mutate(units = gsub(pattern = "\\/| |\\-", replacement = "_", x = units))
      
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
    mismatch_chem <- setdiff(x = unique(units_key$solute), 
                             y = unique(raw_df_v4$solute))
    
    # If any mismatches are found, print a warning for whoever is running this
    if(length(mismatch_chem) != 0){
      message("Solutes found in key that are absent from '", focal_raw, "'!")
      message("Fix the following solutes: ", 
              paste0("'", mismatch_chem, "'", collapse = " & ")) }
    
    # Drop this object (if it exists) to avoid false warning with the next run of the loop
    if(exists("mismatch_chem") == T){ rm(list = "mismatch_chem") }
    
    # Wrangling to get long format data into wide format
    raw_df <- raw_df_v4 %>%
      # Drop built-in units column if one exists
      dplyr::select(-dplyr::ends_with("solute_units")) %>%
      # Attach units from key
      dplyr::left_join(y = units_key, by = c("solute")) %>%
      # Attach solute and solute units into one column
      dplyr::mutate(solute_actual = paste0(solute, "_", units)) %>%
      # Drop unwanted columns
      dplyr::select(-solute, -units) %>%
      # Reshape wider
      tidyr::pivot_wider(names_from = solute_actual, values_from = amount, values_fill = NA)
    
    } else { raw_df <- raw_df_v3 }
  
  # Add to list
  df_list[[focal_raw]] <- raw_df
  
} # Close loop

# Unlist the list we just generated
tidy_v0 <- df_list %>%
  purrr::list_rbind()

# Check that out
dplyr::glimpse(tidy_v0)

# Clean up environment (i.e., drop everything prior to this object)
rm(list = setdiff(ls(), "tidy_v0"))






# End ----
