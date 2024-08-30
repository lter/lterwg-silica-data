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
librarian::shelf(tidyverse, googledrive, scicomptools)

# Clear environment
rm(list = ls())

# If working on server, need to specify correct path
(path <- scicomptools::wd_loc(local = FALSE, remote_path = file.path('/', "home", "shares", "lter-si", "WRTDS")))

# Create a new folder for saving temporary results
dir.create(path = file.path(path, "WRTDS Results_Feb2024"), showWarnings = F)
dir.create(path = file.path(path, "WRTDS Bootstrap Results_Feb2024"), showWarnings = F)

# Download the reference table object
googledrive::drive_ls(googledrive::as_id("https://drive.google.com/drive/u/0/folders/15FEoe2vu3OAqMQHqdQ9XKpFboR4DvS9M"), pattern = "WRTDS_Reference_Table_with_Areas_DO_NOT_EDIT.csv") %>%
  googledrive::drive_download(file = googledrive::as_id(.), overwrite = T,
                              path = file.path(path, "WRTDS Source Files", "WRTDS_Reference_Table_with_Areas_DO_NOT_EDIT.csv"))

# Read that file in
ref_table <- read.csv(file = file.path(path, "WRTDS Source Files", 
                                       "WRTDS_Reference_Table_with_Areas_DO_NOT_EDIT.csv")) %>%
  # Pare down to only needed columns
  dplyr::select(LTER, stream = Stream_Name, drainSqKm)

# Check it out
dplyr::glimpse(ref_table)

# Define the GoogleDrive URL to upload flat results files
## Original destination
dest_url <- googledrive::as_id("https://drive.google.com/drive/u/0/folders/1V5EqmOlWA8U9NWfiBcWdqEH9aRAP-zCk")

# Check current contents of this folder
googledrive::drive_ls(path = dest_url)

# Identify complete rivers for typical workflow
done_rivers <- data.frame("file" = dir(path = file.path(path, "WRTDS Loop Diagnostic_Feb2024"))) %>%
  # Drop the file suffix part of the file name 
  dplyr::mutate(river = gsub(pattern = "\\_Loop\\_Diagnostic.csv", replacement = "", x = file)) %>%
  # Pull out just that column
  dplyr::pull(river)

# Do the same for the bootstrap results
done_boots <- data.frame("file" = dir(path = file.path(path, "WRTDS Bootstrap Diagnostic"))) %>%
  dplyr::mutate(river = gsub(pattern = "\\_Boot\\_Loop\\_Diagnostic.csv", replacement = "", x = file)) %>%
  dplyr::pull(river)

## ---------------------------------------------- ##
            # Identify WRTDS Outputs ----
## ---------------------------------------------- ##

# List all files in "WRTDS Outputs"
wrtds_outs_v0 <- dir(path = file.path(path, "WRTDS Outputs_Feb2024"))

# Do some useful processing of that object
wrtds_outs <- data.frame("file_name" = wrtds_outs_v0) %>%
  # Split LTER off the file name
  tidyr::separate(col = file_name, into = c("LTER", "other_content"),
                  sep = "__", remove = FALSE, fill = "right", extra = "merge") %>%
  # Separate the remaining content further
  tidyr::separate(col = other_content, into = c("stream", "chemical", "data_type"),
                  sep = "_", remove = TRUE, fill = "right", extra = "merge") %>%
  # Recreate the "Stream_Element_ID" column
  dplyr::mutate(Stream_Element_ID = paste0(LTER, "__", stream, "_", chemical)) %>%
  # Remove the PDFs of exploratory graphs
  dplyr::filter(data_type != "WRTDS_GFN_output.pdf") %>%
  # Remove unwanted chemicals that we have data for
  dplyr::filter(!chemical %in% c("TN", "TP")) %>%
  # Keep only rivers that finish the whole workflow!
  dplyr::filter(Stream_Element_ID %in% done_rivers)

# Glimpse it
dplyr::glimpse(wrtds_outs)

# Create an empty list
out_list <- list()

# Define the types of output file suffixes that are allowed
(out_types <- unique(wrtds_outs$data_type))

# For each data type...
for(type in out_types){

  # Return processing message
  message("Processing ", type, " outputs")
  
  # Identify all files of that type
  file_set <- wrtds_outs %>%
    dplyr::filter(data_type == type) %>%
    dplyr::pull(var = file_name)
  
  # Make a counter set to 1
  k <- 1
  
  # Make an empty list
  sub_list <- list()
  
  # Read them all in!
  for(file in file_set){
   
    # Read in CSV and add it to the list
    datum <- read.csv(file = file.path(path, "WRTDS Outputs_Feb2024", file))
    
    # Add it to the list
    sub_list[[paste0(type, "_", k)]] <- datum %>%
      # Add a column for the name of the file
      dplyr::mutate(file_name = file, .before = dplyr::everything())
    
    # Advance counter
    k <- k + 1
  }
  
  # Once all files of that type are retrieved, unlist the sub_list!
  type_df <- sub_list %>%
    # Actual unlisting of the list
    purrr::list_rbind(x = .) %>%
    # Bring in other desired columns
    dplyr::left_join(y = wrtds_outs, by = "file_name") %>%
    # Drop the redundant data_type column
    dplyr::select(-data_type) %>%
    # Relocate other joined columns to front
    dplyr::relocate(Stream_Element_ID, LTER, stream, chemical,
                    .after = file_name) %>%
    # Drop file_name and stream_element_ID
    dplyr::select(-file_name, -Stream_Element_ID) %>%
    # Condense Finnish site synonym names
    ## A given site has one name for silica and a diff name for all other chemicals
    dplyr::mutate(stream = dplyr::case_when(
      stream == "Site 1069" ~ "Mustionjoki 4,9  15500",
      stream == "Site 11310" ~ "Virojoki 006 3020",
      stream == "Site 11523" ~ "Kymijoki Ahvenkoski 001",
      stream == "Site 11532" ~ "Kymijoki Kokonkoski 014",
      stream == "Site 11564" ~ "Kymij Huruksela 033 5600",
      stream == "Site 227" ~ "Koskenkylanjoki 6030",
      stream == "Site 26534" ~ "Lapuanjoki 9900",
      stream == "Site 26740" ~ "Perhonjoki 10600",
      stream == "Site 26935" ~ "Lestijoki 10800 8-tien s",
      stream == "Site 27095" ~ "Kalajoki 11000",
      stream == "Site 27697" ~ "Pyhajoki Hourunk 11400",
      stream == "Site 27880" ~ "Siikajoki 8-tien s 11600",
      stream == "Site 28208" ~ "Oulujoki 13000",
      stream == "Site 28414" ~ "Kiiminkij 13010 4-tien s",
      stream == "Site 28639" ~ "Iijoki Raasakan voimal",
      stream == "Site 36177" ~ "SIMOJOKI AS. 13500",
      stream == "Site 397" ~ "Porvoonjoki 11,5  6022",
      stream == "Site 39892" ~ "KEMIJOKI ISOHAARA 14000",
      stream == "Site 39974" ~ "TORNIONJ KUKKOLA 14310",
      stream == "Site 4081" ~ "Myllykanava vp 9100",
      stream == "Site 4381" ~ "Skatila vp 9600",
      stream == "Site 567" ~ "Mustijoki 4,2  6010",
      stream == "Site 605" ~ "Vantaa 4,2  6040",
      stream == "Site 69038" ~ "Narpionjoki mts 6761",
      TRUE ~ stream))
  
  # Add this dataframe to the output list
  out_list[[type]] <- type_df
  
  # Completion message
  message("Completed processing ", type, " outputs") }

# Check the structure of the whole output list
str(out_list)
names(out_list)

# Clear environment of everything but the filepath, destination URL, out_list, & ref_table
rm(list = setdiff(ls(), c("path", "dest_url", "out_list", "ref_table",
                          "wrtds_outs", "wrtds_outs_v0", "done_rivers", "done_boots")))

## ---------------------------------------------- ##
           # Process WRTDS - Trends ----
## ---------------------------------------------- ##

# Handle trends table
trends_table <- out_list[["TrendsTable_GFN_WRTDS.csv"]]
  
# Glimpse this
dplyr::glimpse(trends_table)

## ---------------------------------------------- ##
          # Process WRTDS - Flux Bias ----
## ---------------------------------------------- ##

# Handle trends table
flux_stats <- out_list[["FluxBias_WRTDS.csv"]]

# Glimpse this
dplyr::glimpse(flux_stats)

## ---------------------------------------------- ##
        # Process WRTDS - Daily WRTDS & Kalman ----
## ---------------------------------------------- ##

# GFN output
gfn <- out_list[["GFN_WRTDS.csv"]] %>%
  # Attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream")) %>%
  # Calculate some additional columns
  dplyr::mutate(Yield = FluxDay / drainSqKm,
                FNYield = FNFlux / drainSqKm) %>% 
  dplyr::rename(Stream_Name = stream)

# Glimpse
dplyr::glimpse(gfn)

# Handle primary Kalman output
kalm_main <- out_list[["Kalman_WRTDS.csv"]] %>%
  # Attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream")) %>%
  # Calculate some additional columns
  dplyr::mutate(Yield = FluxDay / drainSqKm,
                FNYield = FNFlux / drainSqKm)%>% 
  dplyr::rename(Stream_Name = stream)


# Glimpse it
dplyr::glimpse(kalm_main)

## ---------------------------------------------- ##
         # Process WRTDS - Error Stats ----
## ---------------------------------------------- ##

# Error statistics
error_stats <- out_list[["ErrorStats_WRTDS.csv"]]

# Glimpse it
dplyr::glimpse(error_stats)

## ---------------------------------------------- ##
      # Process WRTDS - Monthly Results ----
## ---------------------------------------------- ##

# Monthly information
monthly <- out_list[["Monthly_GFN_WRTDS.csv"]] %>%
  # Attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream")) %>%
  # Compute season of each month
  dplyr::mutate(season = dplyr::case_when(
    !LTER %in% c("LUQ", "MCM") & Month %in% 1:3 ~ "winter",
    !LTER %in% c("LUQ", "MCM") & Month %in% 4:6 ~ "freshet",
    !LTER %in% c("LUQ", "MCM") & Month %in% 7:9 ~ "growing season",
    !LTER %in% c("LUQ", "MCM") & Month %in% 10:12 ~ "fall",
    LTER == "MCM" & Month %in% 12 ~ "freshet",
    LTER == "MCM" & Month %in% 1 ~ "growing season",
    LTER == "MCM" & Month %in% 2 ~ "fall",
    LTER == "MCM" & Month %in% 3:11 ~ "winter",
    TRUE ~ ""), .after = Month) %>%
  # Rename columns to be more explicit about starting units
  dplyr::rename(Discharge_cms = Q,
                Conc_mgL = Conc,
                FNConc_mgL = FNConc,
                Flux_10_6kg_yr = Flux,
                FNFlux_10_6kg_yr = FNFlux) %>%
  # Do some unit conversions
  dplyr::mutate(
    Conc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (Conc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (Conc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (Conc_mgL / 30.9) * 1000),
    FNConc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNConc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNConc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (FNConc_mgL / 30.9) * 1000),
    Flux_10_6kmol_yr = dplyr::case_when(
      chemical %in% c("DSi") ~ (Flux_10_6kg_yr / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (Flux_10_6kg_yr / 14),
      chemical %in% c("P", "TP") ~ (Flux_10_6kg_yr / 30.9)),
    FNFlux_10_6kmol_yr = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNFlux_10_6kg_yr / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNFlux_10_6kg_yr / 14),
      chemical %in% c("P", "TP") ~ (FNFlux_10_6kg_yr / 30.9)) ) %>%
  # Move area to the left
  dplyr::relocate(drainSqKm, .after = stream) %>%
  # Calculate ratios of different chemicals
  ## Pivot longer to get various responses into a column
  tidyr::pivot_longer(cols = Discharge_cms:FNFlux_10_6kmol_yr,
                      names_to = "response_types",
                      values_to = "response_values") %>%
  # Handle "duplicate" values for sites that break across a year so have two values for one year
  ## Only relevant to the McMurdo sites where we altered period of analysis
  dplyr::group_by(LTER, stream, drainSqKm, chemical, Month, season,
                  Year, nDays, DecYear, response_types) %>%
  dplyr::summarize(response_values = mean(response_values, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  ## Pivot back wider but with chemicals as columns
  tidyr::pivot_wider(names_from = chemical,
                     values_from = response_values) %>%
  ## Calculate DIN (DIN = NOx <or> NO3 + NH4)
  dplyr::mutate(DIN = dplyr::case_when(
    ### NOx is preferred for calculating DIN because it is NO3 + NOx
    !is.na(NOx) & !is.na(NH4) ~ (NOx + NH4),
    !is.na(NO3) & !is.na(NH4) ~ (NO3 + NH4))) %>%
  ## Calculate ratios
  dplyr::mutate(Si_to_DIN = ifelse(test = (!is.na(DSi) & !is.na(DIN)),
                                   yes = (DSi / DIN), no = NA),
                Si_to_P = ifelse(test = (!is.na(DSi) & !is.na(P)),
                                   yes = (DSi / P), no = NA)) %>%
  ## Pivot back long
  tidyr::pivot_longer(cols = DSi:Si_to_P,
                      names_to = "chemical",
                      values_to = "response_values") %>%
  ## Drop NAs this pivot introduces
  dplyr::filter(!is.na(response_values)) %>%
  ## Pivot back wide *again* using the original column names
  tidyr::pivot_wider(names_from = response_types,
                     values_from = response_values) %>%
  ## Fix the ratio specification now that they're not column names
  dplyr::mutate(
   chemical = gsub(pattern = "_to_", replacement = ":", x = chemical),
   .before = dplyr::everything()) %>%
  # Reorder column names
  dplyr::select(LTER:chemical, Discharge_cms,
                dplyr::ends_with("Conc_mgL"), dplyr::ends_with("Conc_uM"),
                dplyr::ends_with("Flux_10_6kg_yr"), dplyr::ends_with("Flux_10_6kmol_yr")) %>%
  # Calculate yield for both units
  dplyr::mutate(Yield = Flux_10_6kg_yr / drainSqKm,
                FNYield = FNFlux_10_6kg_yr / drainSqKm,
                Yield_10_6kmol_yr_km2 = Flux_10_6kmol_yr / drainSqKm,
                FNYield_10_6kmol_yr_km2 = FNFlux_10_6kmol_yr / drainSqKm)%>% 
  dplyr::rename(Stream_Name = stream)

# Check it out
dplyr::glimpse(monthly)

## ---------------------------------------------- ##
      # Process WRTDS - Monthly Kalman ----
## ---------------------------------------------- ##

# Monthly information
kalman_monthly <- out_list[["Monthly_Kalman_WRTDS.csv"]] %>%
  # Attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream")) %>%
  # Compute season of each month
  dplyr::mutate(season = dplyr::case_when(
    !LTER %in% c("LUQ", "MCM") & Month %in% 1:3 ~ "winter",
    !LTER %in% c("LUQ", "MCM") & Month %in% 4:6 ~ "freshet",
    !LTER %in% c("LUQ", "MCM") & Month %in% 7:9 ~ "growing season",
    !LTER %in% c("LUQ", "MCM") & Month %in% 10:12 ~ "fall",
    LTER == "MCM" & Month %in% 12 ~ "freshet",
    LTER == "MCM" & Month %in% 1 ~ "growing season",
    LTER == "MCM" & Month %in% 2 ~ "fall",
    LTER == "MCM" & Month %in% 3:11 ~ "winter",
    TRUE ~ ""), .after = Month) %>%
  # Rename columns to be more explicit about starting units
  dplyr::rename(Discharge_cms = Q,
                Conc_mgL = Conc,
                FNConc_mgL = FNConc,
                Flux_10_6kg_yr = Flux,
                FNFlux_10_6kg_yr = FNFlux) %>%
  # Do some unit conversions
  dplyr::mutate(
    Conc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (Conc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (Conc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (Conc_mgL / 30.9) * 1000),
    FNConc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNConc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNConc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (FNConc_mgL / 30.9) * 1000),
    Flux_10_6kmol_yr = dplyr::case_when(
      chemical %in% c("DSi") ~ (Flux_10_6kg_yr / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (Flux_10_6kg_yr / 14),
      chemical %in% c("P", "TP") ~ (Flux_10_6kg_yr / 30.9)),
    FNFlux_10_6kmol_yr = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNFlux_10_6kg_yr / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNFlux_10_6kg_yr / 14),
      chemical %in% c("P", "TP") ~ (FNFlux_10_6kg_yr / 30.9)) ) %>%
  # Move area to the left
  dplyr::relocate(drainSqKm, .after = stream) %>%
  # Calculate ratios of different chemicals
  ## Pivot longer to get various responses into a column
  tidyr::pivot_longer(cols = Discharge_cms:FNFlux_10_6kmol_yr,
                      names_to = "response_types",
                      values_to = "response_values") %>%
  # Handle "duplicate" values for sites that break across a year so have two values for one year
  ## Only relevant to the McMurdo sites where we altered period of analysis
  dplyr::group_by(LTER, stream, drainSqKm, chemical, Month, season,
                  Year, nDays, DecYear, response_types) %>%
  dplyr::summarize(response_values = mean(response_values, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  ## Pivot back wider but with chemicals as columns
  tidyr::pivot_wider(names_from = chemical,
                     values_from = response_values) %>%
  ## Calculate DIN (DIN = NOx <or> NO3 + NH4)
  dplyr::mutate(DIN = dplyr::case_when(
    ### NOx is preferred for calculating DIN because it is NO3 + NOx
    !is.na(NOx) & !is.na(NH4) ~ (NOx + NH4),
    !is.na(NO3) & !is.na(NH4) ~ (NO3 + NH4))) %>%
  ## Calculate ratios
  dplyr::mutate(Si_to_DIN = ifelse(test = (!is.na(DSi) & !is.na(DIN)),
                                   yes = (DSi / DIN), no = NA),
                Si_to_P = ifelse(test = (!is.na(DSi) & !is.na(P)),
                                 yes = (DSi / P), no = NA)) %>%
  ## Pivot back long
  tidyr::pivot_longer(cols = DSi:Si_to_P,
                      names_to = "chemical",
                      values_to = "response_values") %>%
  ## Drop NAs this pivot introduces
  dplyr::filter(!is.na(response_values)) %>%
  ## Pivot back wide *again* using the original column names
  tidyr::pivot_wider(names_from = response_types,
                     values_from = response_values) %>%
  ## Fix the ratio specification now that they're not column names
  dplyr::mutate(
    chemical = gsub(pattern = "_to_", replacement = ":", x = chemical),
    .before = dplyr::everything()) %>%
  # Reorder column names
  dplyr::select(LTER:chemical, Discharge_cms,
                dplyr::ends_with("Conc_mgL"), dplyr::ends_with("Conc_uM"),
                dplyr::ends_with("Flux_10_6kg_yr"), dplyr::ends_with("Flux_10_6kmol_yr")) %>%
  # Calculate yield for both units
  dplyr::mutate(Yield = Flux_10_6kg_yr / drainSqKm,
                FNYield = FNFlux_10_6kg_yr / drainSqKm,
                Yield_10_6kmol_yr_km2 = Flux_10_6kmol_yr / drainSqKm,
                FNYield_10_6kmol_yr_km2 = FNFlux_10_6kmol_yr / drainSqKm)%>% 
  dplyr::rename(Stream_Name = stream)

# Check it out
dplyr::glimpse(kalman_monthly)

## ---------------------------------------------- ##
      # Process WRTDS - Annual Results ----
## ---------------------------------------------- ##

# Results table
results_table <- out_list[["ResultsTable_GFN_WRTDS.csv"]] %>%
  # Rename some columns
  dplyr::rename(Discharge_cms = Discharge..cms.,
                Conc_mgL = Conc..mg.L.,
                FNConc_mgL = FN.Conc..mg.L.,
                Flux_10_6kg_yr = Flux..10.6kg.yr.,
                FNFlux_10_6kg_yr = FN.Flux..10.6kg.yr.) %>%
  # Attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream")) %>%
  # Do some unit conversions
  dplyr::mutate(
    Conc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (Conc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (Conc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (Conc_mgL / 30.9) * 1000),
    FNConc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNConc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNConc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (FNConc_mgL / 30.9) * 1000),
    Flux_10_6kmol_yr = dplyr::case_when(
      chemical %in% c("DSi") ~ (Flux_10_6kg_yr / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (Flux_10_6kg_yr / 14),
      chemical %in% c("P", "TP") ~ (Flux_10_6kg_yr / 30.9)),
    FNFlux_10_6kmol_yr = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNFlux_10_6kg_yr / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNFlux_10_6kg_yr / 14),
      chemical %in% c("P", "TP") ~ (FNFlux_10_6kg_yr / 30.9)) ) %>%
  # Calculate ratios of different chemicals
  ## Move area to the left
  dplyr::relocate(drainSqKm, .after = stream) %>%
  ## Pivot longer to get various responses into a column
  tidyr::pivot_longer(cols = Discharge_cms:FNFlux_10_6kmol_yr,
                      names_to = "response_types",
                      values_to = "response_values") %>%
  # Handle "duplicate" values for sites that break across a year so have two values for one year
  ## Only relevant to the McMurdo sites where we altered period of analysis
  dplyr::group_by(LTER, stream, drainSqKm, chemical, Year, response_types) %>%
  dplyr::summarize(response_values = mean(response_values, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  ## Pivot back wider but with chemicals as columns
  tidyr::pivot_wider(names_from = chemical,
                     values_from = response_values) %>%
  ## Calculate DIN (DIN = NOx <or> NO3 + NH4)
  dplyr::mutate(DIN = dplyr::case_when(
    ### NOx is preferred for calculating DIN because it is NO3 + NOx
    !is.na(NOx) & !is.na(NH4) ~ (NOx + NH4),
    !is.na(NO3) & !is.na(NH4) ~ (NO3 + NH4))) %>%
  ## Calculate ratios
  dplyr::mutate(Si_to_DIN = ifelse(test = (!is.na(DSi) & !is.na(DIN)),
                                   yes = (DSi / DIN), no = NA),
                Si_to_P = ifelse(test = (!is.na(DSi) & !is.na(P)),
                                   yes = (DSi / P), no = NA)) %>%
  ## Pivot back long
  tidyr::pivot_longer(cols = DSi:Si_to_P,
                      names_to = "chemical",
                      values_to = "response_values") %>%
  ## Drop NAs this pivot introduces
  dplyr::filter(!is.na(response_values)) %>%
  ## Pivot back wide *again* using the original column names
  tidyr::pivot_wider(names_from = response_types,
                     values_from = response_values) %>%
  ## Fix the ratio specification now that they're not column names
  dplyr::mutate(
   chemical = gsub(pattern = "_to_", replacement = ":", x = chemical),
   .before = dplyr::everything()) %>%
  # Reorder column names
  dplyr::select(LTER:chemical, Discharge_cms,
                dplyr::ends_with("Conc_mgL"), dplyr::ends_with("Conc_uM"),
                dplyr::ends_with("Flux_10_6kg_yr"), dplyr::ends_with("Flux_10_6kmol_yr")) %>%
  # Calculate yield for both units
  dplyr::mutate(Yield = Flux_10_6kg_yr / drainSqKm,
                FNYield = FNFlux_10_6kg_yr / drainSqKm,
                Yield_10_6kmol_yr_km2 = Flux_10_6kmol_yr / drainSqKm,
                FNYield_10_6kmol_yr_km2 = FNFlux_10_6kmol_yr / drainSqKm)%>% 
  dplyr::rename(Stream_Name = stream)
  
# Glimpse this as well
dplyr::glimpse(results_table)

## ---------------------------------------------- ##
        # Process WRTDS - Annual Kalman ----
## ---------------------------------------------- ##

# Results table
kalman_annual <- out_list[["ResultsTable_Kalman_WRTDS.csv"]] %>%
  # Attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream"))%>% 
  dplyr::rename(Stream_Name = stream)

# Glimpse this as well
dplyr::glimpse(kalman_annual)



## ---------------------------------------------- ##
            # Export WRTDS Outputs ----
## ---------------------------------------------- ##

# Combine processed files into a list
export_list <- list("WRTDS_trends.csv" = trends_table,
                    "WRTDS_flux_bias.csv" = flux_stats,
                    "WRTDS_error_stats.csv" = error_stats,
                    ## Daily
                    "WRTDS_daily.csv" = gfn,
                    "WRTDS_kalman_daily.csv" = kalm_main,
                    ## Monthly
                    "WRTDS_monthly.csv" = monthly,
                    "WRTDS_kalman_monthly.csv" = kalman_monthly,
                    ## Yearly
                    "WRTDS_annual.csv" = results_table,
                    "WRTDS_kalman_annual.csv" = kalman_annual)

# Loop across the list to export locally and to GoogleDrive
## Note that the "GFN_WRTDS.csv" file is *huge* so it takes a few seconds to upload
for(name in names(export_list)){
  
  # Rip out that dataframe
  datum <- export_list[[name]]
  
  # Define name for this file
  report_file <- file.path(path, "WRTDS Results_Feb2024", paste0("Full_Results_", name))
  
  # Write this CSV out
  write.csv(x = datum, na = "", row.names = F, file = report_file)
  
  # Upload that object to GoogleDrive
  googledrive::drive_upload(media = report_file, overwrite = T, path = dest_url) }

## ---------------------------------------------- ##
            # Export PDF Reports ----
## ---------------------------------------------- ##

# The "step 3" script also creates a PDF for every site
# We want to make those available outside of the server for later exploration and use

# Identify all PDFs
# Do some useful processing of that object
pdf_outs <- data.frame("file_name" = wrtds_outs_v0) %>%
  # Split LTER off the file name
  tidyr::separate(col = file_name, into = c("LTER", "other_content"),
                  sep = "__", remove = FALSE, fill = "right", extra = "merge") %>%
  # Separate the remaining content further
  tidyr::separate(col = other_content, into = c("stream", "chemical", "data_type"),
                  sep = "_", remove = TRUE, fill = "right", extra = "merge") %>%
  # Recreate the "Stream_Element_ID" column
  dplyr::mutate(Stream_Element_ID = paste0(LTER, "__", stream, "_", chemical)) %>%
  # Remove the PDFs of exploratory graphs
  dplyr::filter(data_type == "WRTDS_GFN_output.pdf") %>%
  # Remove unwanted chemicals that we have data for
  dplyr::filter(!chemical %in% c("TN", "TP")) %>%
  # Keep only rivers that finish the whole workflow!
  dplyr::filter(Stream_Element_ID %in% done_rivers)

# Glimpse it
dplyr::glimpse(pdf_outs)

# Identify PDF folder
## Standard output destination
pdf_url <- googledrive::as_id("https://drive.google.com/drive/folders/1sqgNj0OPrquEe2_IyKn8Bplb_VFoPg7X")

# Identify PDFs already in GoogleDrive
drive_pdfs <- googledrive::drive_ls(path = pdf_url)

# Use that to identify new PDFs!
new_pdfs <- setdiff(pdf_outs$file_name, drive_pdfs$name)

# Loop across these PDFs and put them into GoogleDrive
for(report in unique(pdf_outs$file_name)){
## (^^^) Upload *all* PDFs regardless of whether they're in the Drive
## (vvv) Upload only *new* PDFs
#for(report in new_pdfs){

  # Send that report to a GoogleDrive folder
  googledrive::drive_upload(media = file.path(path, "WRTDS Outputs_Feb2024", report),
                            overwrite = T, path = pdf_url) }

# Clear environment of everything but the filepath, destination URL, and ref_table
rm(list = setdiff(ls(), c("path", "dest_url", "ref_table", "done_rivers", "done_boots")))

## ---------------------------------------------- ##
         # Identify Bootstrap Outputs ----
## ---------------------------------------------- ##

# List all files in "WRTDS Outputs"
boot_outs_v0 <- dir(path = file.path(path, "WRTDS Bootstrap Outputs"))

# Do some useful processing of that object
boot_outs <- data.frame("file_name" = boot_outs_v0) %>%
  # Split LTER off the file name
  tidyr::separate(col = file_name, into = c("LTER", "other_content"),
                  sep = "__", remove = FALSE, fill = "right", extra = "merge") %>%
  # Separate the remaining content further
  tidyr::separate(col = other_content, into = c("stream", "chemical", "data_type"),
                  sep = "_", remove = TRUE, fill = "right", extra = "merge") %>%
  # Recreate the "Stream_Element_ID" column
  dplyr::mutate(Stream_Element_ID = paste0(LTER, "__", stream, "_", chemical)) %>%
  # Keep only rivers that finish the whole workflow!
  dplyr::filter(Stream_Element_ID %in% done_boots)

# Glimpse it
dplyr::glimpse(boot_outs)

# Create an empty list
boot_out_list <- list()

# Define the types of output file suffixes that are allowed
(boot_out_types <- unique(boot_outs$data_type))

# For each data type...
for(type in boot_out_types){
  
  # Return processing message
  message("Processing ", type, " outputs")
  
  # Identify all files of that type
  file_set <- boot_outs %>%
    dplyr::filter(data_type == type) %>%
    dplyr::pull(var = file_name)
  
  # Make a counter set to 1
  k <- 1
  
  # Make an empty list
  boot_sub_list <- list()
  
  # Read them all in!
  for(file in file_set){
    
    # Read in CSV and add it to the list
    boot_datum <- read.csv(file = file.path(path, "WRTDS Bootstrap Outputs", file))
    
    # Add it to the list
    boot_sub_list[[paste0(type, "_", k)]] <- boot_datum %>%
      # Add a column for the name of the file
      dplyr::mutate(file_name = file, .before = dplyr::everything())
    
    # Advance counter
    k <- k + 1
  }
  
  # Once all files of that type are retrieved, unlist the sub_list!
  boot_type_df <- boot_sub_list %>%
    # Actual unlisting of the list
    purrr::list_rbind(x = .) %>%
    # Bring in other desired columns
    dplyr::left_join(y = boot_outs, by = "file_name") %>%
    # Drop the redundant data_type column
    dplyr::select(-data_type) %>%
    # Relocate other joined columns to front
    dplyr::relocate(Stream_Element_ID, LTER, stream, chemical,
                    .after = file_name)
  
  # Add this dataframe to the output list
  boot_out_list[[type]] <- boot_type_df
  
  # Completion message
  message("Completed processing ", type, " outputs")
}

# Check the structure of the whole output list
str(boot_out_list)
names(boot_out_list)

# Clear environment of everything but the filepath, destination URL, boot_out_list, & ref_table
rm(list = setdiff(ls(), c("path", "dest_url", "boot_out_list", "ref_table",
                          "done_rivers", "done_boots")))

## ---------------------------------------------- ##
          # Process Bootstrap Outputs ----
## ---------------------------------------------- ##

# Bootstraps
boots_gfn <- boot_out_list[["EGRETCi_GFN_bootstraps.csv"]]

# Glimpse it
dplyr::glimpse(boots_gfn)

# Grab trends
boots_trends <- boot_out_list[["EGRETCi_GFN_Trend.csv"]]

# Glimpse it
dplyr::glimpse(boots_trends)

# Grab final output: pairs
boots_pairs <- boot_out_list[["ListPairs_GFN_WRTDS.csv"]]

# Glimpse it
dplyr::glimpse(boots_pairs)

## ---------------------------------------------- ##
        # Export Bootstrap Outputs ----
## ---------------------------------------------- ##

# Combine processed files into a list
boot_export_list <- list("WRTDS_EGRETCi_bootstraps.csv" = boots_gfn,
                         "WRTDS_EGRETCi_trends.csv" = boots_trends,
                         "WRTDS_GFN.csv" = boots_pairs)

# Loop across the list to export locally and to GoogleDrive
## Note that the "GFN_WRTDS.csv" file is *huge* so it takes a few seconds to upload
for(name in names(boot_export_list)){
  
  # Rip out that dataframe
  boot_datum <- boot_export_list[[name]]
  
  # Define name for this file
  boot_report_file <- file.path(path, "WRTDS Bootstrap Results", paste0("Bootstrap_Full_Results_", name))
  
  # Write this CSV out
  write.csv(x = boot_datum, na = "", row.names = F, file = boot_report_file)
  
  # Upload that object to GoogleDrive
  googledrive::drive_upload(media = boot_report_file, overwrite = T, path = dest_url) }

# End ----

## ---------------------------------------------- ##
# Crop WRTDS Outputs ----
## ---------------------------------------------- ##

## temporary step to accommodate for leading discharge data ## 

## Daily Data
daily_wrtds_v1 <- gfn %>%
  # Left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Stream_Name")) %>%
  # Drop any years before the one year buffer suggested by WRTDS
  dplyr::filter(Date > disc_start) %>% 
  # Reorder columns / rename Q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name,-min_date,-disc_start)

daily_kalman_v1 <- kalm_main %>%
  # Left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Stream_Name")) %>%
  # Drop any years before the one year buffer suggested by WRTDS
  dplyr::filter(Date > disc_start) %>% 
  # Reorder columns / rename Q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name,-min_date,-disc_start)


## Monthly Data
monthly_v1 <- monthly %>%
  # Left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER","Stream_Name")) %>%
  # Drop any years before the one year buffer suggested by WRTDS
  dplyr::filter(Year > year(disc_start)) %>% 
  # Reorder columns / rename Q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name,-min_date,-disc_start)

monthly_kalman_v1 <- kalman_monthly %>%
  # Left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Stream_Name")) %>%
  # Drop any years before the one year buffer suggested by WRTDS
  dplyr::filter(Year > year(disc_start)) %>% 
  # Reorder columns / rename Q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name,-min_date,-disc_start)

## Annual Data ##

annual_wrtds_v1 <- results_table %>% 
  # Left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Stream_Name")) %>%
  # Drop any years before the one year buffer suggested by WRTDS
  dplyr::filter(Year > year(disc_start)) %>% 
  # Reorder columns / rename Q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name,-min_date,-disc_start)

annual_kalman_v1 <- kalman_annual %>% 
  # Left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Stream_Name")) %>%
  # Drop any years before the one year buffer suggested by WRTDS
  dplyr::filter(DecYear > year(disc_start)) %>% 
  # Reorder columns / rename Q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name,-min_date,-disc_start)


