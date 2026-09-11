# ---- ## ----
# wrtds centralized workflow
# ---- ## ----
# WRTDS = Weighted Regressions on Time, Discharge, and Season
# ---- nick j lyon, kathi jo jankowski ----

# ---- ## ----
# ---- housekeeping ----
# ---- ## ----
# load libraries
# install.packages("librarian")
librarian::shelf(tidyverse, googledrive, scicomptools)

# clear environment
rm(list = ls())

# if working on server, need to specify correct path
(path <- scicomptools::wd_loc(local = FALSE, remote_path = file.path("/", "home", "shares", "lter-si", "WRTDS")))

# create a new folder for saving temporary results
dir.create(path = file.path(path, "WRTDS Results_2025"), showWarnings = F)
dir.create(path = file.path(path, "WRTDS Bootstrap Results"), showWarnings = F)

# download the reference table object
googledrive::drive_ls(googledrive::as_id("https://drive.google.com/drive/u/0/folders/15FEoe2vu3OAqMQHqdQ9XKpFboR4DvS9M"), pattern = "WRTDS_Reference_Table_with_Areas_DO_NOT_EDIT.csv") %>%
  googledrive::drive_download(file = googledrive::as_id(.), overwrite = T,
    path = file.path(path, "WRTDS Source Files", "WRTDS_Reference_Table_with_Areas_DO_NOT_EDIT.csv"))

# read that file in
ref_table <- read.csv(file = file.path(path, "WRTDS Source Files",
  "WRTDS_Reference_Table_with_Areas_DO_NOT_EDIT.csv")) %>%
  # pare down to only needed columns
  dplyr::select(LTER, stream = Stream_Name, drainSqKm) %>%
  mutate(stream = case_when(stream == "OR_low" ~ "ORlow",
    stream == "MG_WEIR" ~ "MGWEIR",
    .default = stream))

# check it out
dplyr::glimpse(ref_table)

# Define the GoogleDrive URL to upload flat results files
# ---- original destination ----
dest_url <- googledrive::as_id("https://drive.google.com/drive/u/1/folders/1V5EqmOlWA8U9NWfiBcWdqEH9aRAP-zCk")

# check current contents of this folder
googledrive::drive_ls(path = dest_url)

# identify complete rivers for typical workflow
done_rivers <- data.frame("file" = dir(path = file.path(path, "WRTDS Loop Diagnostic_2025"))) %>%
  # drop the file suffix part of the file name
  dplyr::mutate(river = gsub(pattern = "\\_Loop\\_Diagnostic.csv", replacement = "", x = file)) %>%
  # pull out just that column
  dplyr::pull(river)

# do the same for the bootstrap results
done_boots <- data.frame("file" = dir(path = file.path(path, "WRTDS Bootstrap Diagnostic"))) %>%
  dplyr::mutate(river = gsub(pattern = "\\_Boot\\_Loop\\_Diagnostic.csv", replacement = "", x = file)) %>%
  dplyr::pull(river)

# ---- ## ----
# ---- identify wrtds outputs ----
# ---- ## ----

# list all files in "wrtds outputs"
wrtds_outs_v0 <- dir(path = file.path(path, "WRTDS Outputs"))

# remove files with wrong name for catalina jemez, need to delete from server
remove_streams_v1 <- "MG_WEIR"
# remove items that match the pattern
wrtds_outs_v1 <- str_subset(wrtds_outs_v0, pattern = remove_streams_v1, negate = TRUE)
remove_streams_v2 <- "OR_low"
# remove items that match the pattern
wrtds_outs_v2 <- str_subset(wrtds_outs_v1, pattern = remove_streams_v2, negate = TRUE)



# do some useful processing of that object
wrtds_outs <- data.frame("file_name" = wrtds_outs_v2) %>%
  # split lter off the file name
  tidyr::separate(col = file_name, into = c("LTER", "other_content"),
    sep = "__", remove = FALSE, fill = "right", extra = "merge") %>%
  # separate the remaining content further
  tidyr::separate(col = other_content, into = c("stream", "chemical", "data_type"),
    sep = "_", remove = TRUE, fill = "right", extra = "merge") %>%
  # Recreate the "Stream_Element_ID" column
  dplyr::mutate(Stream_Element_ID = paste0(LTER, "__", stream, "_", chemical)) %>%
  # remove the pdfs of exploratory graphs
  dplyr::filter(data_type != "WRTDS_output.pdf") %>%
  dplyr::filter(data_type != "WRTDS_kalman_output.pdf") %>%
  # remove unwanted chemicals that we have data for
  dplyr::filter(!chemical %in% c("TN", "TP")) %>%
  # keep only rivers that finish the whole workflow!
  dplyr::filter(Stream_Element_ID %in% done_rivers)

# glimpse it
dplyr::glimpse(wrtds_outs)

# quantify how many of each element there are
wrtds_outs %>%
  group_by(Stream_Element_ID, chemical, stream) %>%
  dplyr::summarise(n = n()) %>%
  aggregate(stream ~ chemical, FUN = length)

# create an empty list
out_list <- list()

# define the types of output file suffixes that are allowed
(out_types <- unique(wrtds_outs$data_type))

# for each data type
for (type in out_types) {
  # return processing message
  message("Processing ", type, " outputs")

  # identify all files of that type
  file_set <- wrtds_outs %>%
    dplyr::filter(data_type == type) %>%
    dplyr::pull(var = file_name)

  # make a counter set to 1
  k <- 1

  # make an empty list
  sub_list <- list()

  # read them all in!
  for (file in file_set) {
    # read in csv and add it to the list
    datum <- read.csv(file = file.path(path, "WRTDS Outputs", file))

    # add it to the list
    sub_list[[paste0(type, "_", k)]] <- datum %>%
      # add a column for the name of the file
      dplyr::mutate(file_name = file, .before = dplyr::everything())

    # advance counter
    k <- k + 1
  }

  # Once all files of that type are retrieved, unlist the sub_list!
  type_df <- sub_list %>%
    # actual unlisting of the list
    purrr::list_rbind(x = .) %>%
    # bring in other desired columns
    dplyr::left_join(y = wrtds_outs, by = "file_name") %>%
    # Drop the redundant data_type column
    dplyr::select(-data_type) %>%
    # relocate other joined columns to front
    dplyr::relocate(Stream_Element_ID, LTER, stream, chemical,
      .after = file_name) %>%
    # Drop file_name and stream_element_ID
    dplyr::select(-file_name, -Stream_Element_ID) %>%
    # condense finnish site synonym names
    # ---- a given site has one name for silica and a diff name for all other chemicals ----
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

  # add this dataframe to the output list
  out_list[[type]] <- type_df

  # completion message
  message("Completed processing ", type, " outputs")
}

# check the structure of the whole output list
str(out_list)
names(out_list)

# Clear environment of everything but the filepath, destination URL, out_list, & ref_table
rm(list = setdiff(ls(), c("path", "dest_url", "out_list", "ref_table",
  "wrtds_outs", "wrtds_outs_v0", "wrtds_outs_v1", "done_rivers", "done_boots")))

# ---- ## ----
# ---- process wrtds - trends ----
# ---- ## ----

# handle trends table
trends_table <- out_list[["TrendsTable_GFN_WRTDS.csv"]]

# glimpse this
dplyr::glimpse(trends_table)

# ---- ## ----
# ---- process wrtds - flux bias ----
# ---- ## ----

# handle trends table
flux_stats <- out_list[["FluxBias_WRTDS.csv"]]

# glimpse this
dplyr::glimpse(flux_stats)

# ---- ## ----
# ---- process wrtds - daily wrtds & kalman ----
# ---- ## ----

# have to remove certain months from these streams that didn't get properly removed in analysis
mcm_months <- seq(3, 11, 1)
martinelli_months <- c(10, 11, 12, 1, 2, 3, 4)
saddle_months <- c(8, 9, 10, 11, 12, 1, 2, 3, 4)

# gfn output
gfn_daily <- out_list[["GFN_WRTDS.csv"]] %>%
  # rename some columns for clarity
  dplyr::rename(Discharge_cms = Q,
    Conc_mgL = ConcDay,
    FNConc_mgL = FNConc,
    Flux_kg_day = FluxDay,
    FNFlux_kg_day = FNFlux,
    Year = DecYear) %>%
  # adjust year to not have decimal places
  dplyr::mutate(Year = round(Year)) %>%
  # need to remove "winter" months for mcm & nwt since estimation is bad
  dplyr::filter(!(LTER == "MCM" & Month %in% mcm_months)) %>%
  dplyr::filter(!(stream == "MARTINELLI" & Month %in% martinelli_months)) %>%
  dplyr::filter(!(stream == "SADDLE STREAM 007" & Month %in% saddle_months)) %>%
  # attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream")) %>%
  # calculate some additional columns
  dplyr::mutate(Yield_kg_day_km2 = Flux_kg_day / drainSqKm,
    FNYield_kg_day_km2 = FNFlux_kg_day / drainSqKm) %>%
  # dplyr::select(-GenFlux,-GenConc) %>% # this might be temporary
  dplyr::rename(Stream_Name = stream)

# glimpse
dplyr::glimpse(gfn_daily)

# handle primary kalman output
kalman_daily <- out_list[["Kalman_WRTDS.csv"]] %>%
  # rename some columns for clarity
  dplyr::rename(Discharge_cms = Q,
    Conc_mgL = ConcDay,
    GenConc_mgL = GenConc,
    FNConc_mgL = FNConc,
    Flux_kg_day = FluxDay,
    GenFlux_kg_day = GenFlux,
    FNFlux_kg_day = FNFlux,
    Year = DecYear) %>%
  # adjust year to not have decimal places
  dplyr::mutate(Year = round(Year)) %>%
  # need to remove "winter" months for mcm & nwt since estimation is bad
  dplyr::filter(!(LTER == "MCM" & Month %in% mcm_months)) %>%
  dplyr::filter(!(stream == "MARTINELLI" & Month %in% martinelli_months)) %>%
  dplyr::filter(!(stream == "SADDLE STREAM 007" & Month %in% saddle_months)) %>%
  # attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream")) %>%
  # calculate some additional columns
  dplyr::mutate(GenYield_kg_day_km2 = GenFlux_kg_day / drainSqKm,
    FNYield_kg_day_km2 = FNFlux_kg_day / drainSqKm) %>%
  dplyr::rename(Stream_Name = stream)

# glimpse it
dplyr::glimpse(kalman_daily)


# ---- ## ----
# ---- process wrtds - error stats ----
# ---- ## ----

# error statistics
error_stats <- out_list[["ErrorStats_WRTDS.csv"]]

# glimpse it
dplyr::glimpse(error_stats)

# ---- ## ----
# ---- process wrtds - kalman error stats ----
# ---- ## ----

# error statistics
kalman_error_stats <- out_list[["ErrorStats_kalman_WRTDS.csv"]]

# glimpse it
dplyr::glimpse(kalman_error_stats)

# ---- ## ----
# ---- process wrtds - monthly results ----
# ---- ## ----

# monthly information
monthly <- out_list[["Monthly_GFN_WRTDS.csv"]] %>%
  # attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream")) %>%
  # compute season of each month
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
  # need to remove "winter" months for mcm & nwt since estimation is bad
  dplyr::filter(!(LTER == "MCM" & season == "winter")) %>%
  dplyr::filter(!(stream == "MARTINELLI" & Month %in% martinelli_months)) %>%
  dplyr::filter(!(stream == "SADDLE STREAM 007" & Month %in% saddle_months)) %>%
  # rename columns to be more explicit about starting units
  dplyr::rename(Discharge_cms = Q,
    Conc_mgL = Conc,
    FNConc_mgL = FNConc,
    Flux_kg_day = Flux,
    FNFlux_kg_day = FNFlux) %>%
  # do some unit conversions
  dplyr::mutate(
    Conc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (Conc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (Conc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (Conc_mgL / 30.9) * 1000),
    FNConc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNConc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNConc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (FNConc_mgL / 30.9) * 1000),
    Flux_kmol_day = dplyr::case_when(
      chemical %in% c("DSi") ~ (Flux_kg_day / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (Flux_kg_day / 14),
      chemical %in% c("P", "TP") ~ (Flux_kg_day / 30.9)),
    FNFlux_kmol_day = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNFlux_kg_day / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNFlux_kg_day / 14),
      chemical %in% c("P", "TP") ~ (FNFlux_kg_day / 30.9))) %>%
  # move area to the left
  dplyr::relocate(drainSqKm, .after = stream) %>%
  # calculate ratios of different chemicals
  # ---- pivot longer to get various responses into a column ----
  tidyr::pivot_longer(cols = Discharge_cms:FNFlux_kmol_day,
    names_to = "response_types",
    values_to = "response_values") %>%
  # handle "duplicate" values for sites that break across a year so have two values for one year
  # ---- only relevant to the mcmurdo sites where we altered period of analysis ----
  dplyr::group_by(LTER, stream, drainSqKm, chemical, Month, season,
    Year, nDays, DecYear, response_types) %>%
  dplyr::summarize(response_values = mean(response_values, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  # ---- pivot back wider but with chemicals as columns ----
  tidyr::pivot_wider(names_from = chemical,
    values_from = response_values) %>%
  # ---- calculate din (din = nox <or> no3 + nh4) ----
  dplyr::mutate(DIN = dplyr::case_when(
    # ---- nox is preferred for calculating din because it is no3 + nox ----
    !is.na(NOx) & !is.na(NH4) ~ (NOx + NH4),
    !is.na(NO3) & !is.na(NH4) ~ (NO3 + NH4))) %>%
  # ---- calculate ratios ----
  dplyr::mutate(Si_to_DIN = ifelse(test = (!is.na(DSi) & !is.na(DIN)),
    yes = (DSi / DIN), no = NA),
  Si_to_P = ifelse(test = (!is.na(DSi) & !is.na(P)),
    yes = (DSi / P), no = NA)) %>%
  # ---- pivot back long ----
  tidyr::pivot_longer(cols = DSi:Si_to_P,
    names_to = "chemical",
    values_to = "response_values") %>%
  # ---- drop nas this pivot introduces ----
  dplyr::filter(!is.na(response_values)) %>%
  # ---- pivot back wide *again* using the original column names ----
  tidyr::pivot_wider(names_from = response_types,
    values_from = response_values) %>%
  # ---- fix the ratio specification now that they're not column names ----
  dplyr::mutate(
    chemical = gsub(pattern = "_to_", replacement = ":", x = chemical),
    .before = dplyr::everything()) %>%
  # reorder column names
  dplyr::select(LTER:chemical, Discharge_cms,
    dplyr::ends_with("Conc_mgL"), dplyr::ends_with("Conc_uM"),
    dplyr::ends_with("Flux_kg_day"), dplyr::ends_with("Flux_kmol_day")) %>%
  # calculate yield for both units
  dplyr::mutate(Yield_kg_day_km2 = Flux_kg_day / drainSqKm,
    FNYield_kg_day_km2 = FNFlux_kg_day / drainSqKm,
    Yield_kmol_day_km2 = Flux_kmol_day / drainSqKm,
    FNYield_kmol_day_km2 = FNFlux_kmol_day / drainSqKm) %>%
  dplyr::rename(Stream_Name = stream)

# check it out
dplyr::glimpse(monthly)

# ---- ## ----
# ---- process wrtds - monthly kalman ----
# ---- ## ----

# monthly information
kalman_monthly <- out_list[["Monthly_Kalman_WRTDS.csv"]] %>%
  # attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream")) %>%
  # compute season of each month
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
  # ---- remove winter months for nwt and mcm ----
  dplyr::filter(!(LTER == "MCM" & season == "winter")) %>%
  dplyr::filter(!(stream == "MARTINELLI" & Month %in% martinelli_months)) %>%
  dplyr::filter(!(stream == "SADDLE STREAM 007" & Month %in% saddle_months)) %>%
  # rename columns to be more explicit about starting units
  dplyr::rename(Discharge_cms = Q,
    GenConc_mgL = GenConc, # using kalman estimate here
    FNConc_mgL = FNConc,
    GenFlux_kg_day = GenFlux, # using kalman estimate here
    FNFlux_kg_day = FNFlux) %>%
  # do some unit conversions
  dplyr::mutate(
    GenConc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (GenConc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (GenConc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (GenConc_mgL / 30.9) * 1000),
    FNConc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNConc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNConc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (FNConc_mgL / 30.9) * 1000),
    GenFlux_kmol_day = dplyr::case_when(
      chemical %in% c("DSi") ~ (GenFlux_kg_day / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (GenFlux_kg_day / 14),
      chemical %in% c("P", "TP") ~ (GenFlux_kg_day / 30.9)),
    FNFlux_kmol_day = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNFlux_kg_day / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNFlux_kg_day / 14),
      chemical %in% c("P", "TP") ~ (FNFlux_kg_day / 30.9))) %>%
  # move area to the left
  dplyr::relocate(drainSqKm, .after = stream) %>%
  # calculate ratios of different chemicals
  # ---- pivot longer to get various responses into a column ----
  tidyr::pivot_longer(cols = Discharge_cms:FNFlux_kmol_day,
    names_to = "response_types",
    values_to = "response_values") %>%
  # handle "duplicate" values for sites that break across a year so have two values for one year
  # ---- only relevant to the mcmurdo sites where we altered period of analysis ----
  dplyr::group_by(LTER, stream, drainSqKm, chemical, Month, season,
    Year, nDays, DecYear, response_types) %>%
  dplyr::summarize(response_values = mean(response_values, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  # ---- pivot back wider but with chemicals as columns ----
  tidyr::pivot_wider(names_from = chemical,
    values_from = response_values) %>%
  # ---- calculate din (din = nox <or> no3 + nh4) ----
  dplyr::mutate(DIN = dplyr::case_when(
    # ---- nox is preferred for calculating din because it is no3 + nox ----
    !is.na(NOx) & !is.na(NH4) ~ (NOx + NH4),
    !is.na(NO3) & !is.na(NH4) ~ (NO3 + NH4))) %>%
  # ---- calculate ratios ----
  dplyr::mutate(Si_to_DIN = ifelse(test = (!is.na(DSi) & !is.na(DIN)),
    yes = (DSi / DIN), no = NA),
  Si_to_P = ifelse(test = (!is.na(DSi) & !is.na(P)),
    yes = (DSi / P), no = NA)) %>%
  # ---- pivot back long ----
  tidyr::pivot_longer(cols = DSi:Si_to_P,
    names_to = "chemical",
    values_to = "response_values") %>%
  # ---- drop nas this pivot introduces ----
  dplyr::filter(!is.na(response_values)) %>%
  # ---- pivot back wide *again* using the original column names ----
  tidyr::pivot_wider(names_from = response_types,
    values_from = response_values) %>%
  # ---- fix the ratio specification now that they're not column names ----
  dplyr::mutate(
    chemical = gsub(pattern = "_to_", replacement = ":", x = chemical),
    .before = dplyr::everything()) %>%
  # reorder column names
  dplyr::select(LTER:chemical, Discharge_cms,
    dplyr::ends_with("Conc_mgL"), dplyr::ends_with("Conc_uM"),
    dplyr::ends_with("Flux_kg_day"), dplyr::ends_with("Flux_kmol_day")) %>%
  # calculate yield for both units
  dplyr::mutate(GenYield_kg_day_km2 = GenFlux_kg_day / drainSqKm,
    FNYield_kg_day_km2 = FNFlux_kg_day / drainSqKm,
    GenYield_kmol_day_km2 = GenFlux_kmol_day / drainSqKm,
    FNYield_kmol_day_km2 = FNFlux_kmol_day / drainSqKm) %>%
  dplyr::rename(Stream_Name = stream)

# check it out
dplyr::glimpse(kalman_monthly)

# ---- ## ----
# ---- process wrtds - annual results ----
# ---- ## ----

# results table
results_table <- out_list[["ResultsTable_GFN_WRTDS.csv"]] %>%
  # rename some columns
  dplyr::rename(Discharge_cms = Discharge..cms.,
    Conc_mgL = Conc..mg.L.,
    FNConc_mgL = FN.Conc..mg.L.,
    Flux_10_6kg_yr = Flux..10.6kg.yr.,
    FNFlux_10_6kg_yr = FN.Flux..10.6kg.yr.) %>%
  # attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream")) %>%
  # do some unit conversions
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
      chemical %in% c("P", "TP") ~ (FNFlux_10_6kg_yr / 30.9))) %>%
  # calculate ratios of different chemicals
  # ---- move area to the left ----
  dplyr::relocate(drainSqKm, .after = stream) %>%
  # ---- pivot longer to get various responses into a column ----
  tidyr::pivot_longer(cols = Discharge_cms:FNFlux_10_6kmol_yr,
    names_to = "response_types",
    values_to = "response_values") %>%
  # handle "duplicate" values for sites that break across a year so have two values for one year
  # ---- only relevant to the mcmurdo sites where we altered period of analysis ----
  dplyr::group_by(LTER, stream, drainSqKm, chemical, Year, response_types) %>%
  dplyr::summarize(response_values = mean(response_values, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  # ---- pivot back wider but with chemicals as columns ----
  tidyr::pivot_wider(names_from = chemical,
    values_from = response_values) %>%
  # ---- calculate din (din = nox <or> no3 + nh4) ----
  dplyr::mutate(DIN = dplyr::case_when(
    # ---- nox is preferred for calculating din because it is no3 + nox ----
    !is.na(NOx) & !is.na(NH4) ~ (NOx + NH4),
    !is.na(NO3) & !is.na(NH4) ~ (NO3 + NH4))) %>%
  # ---- calculate ratios ----
  dplyr::mutate(Si_to_DIN = ifelse(test = (!is.na(DSi) & !is.na(DIN)),
    yes = (DSi / DIN), no = NA),
  Si_to_P = ifelse(test = (!is.na(DSi) & !is.na(P)),
    yes = (DSi / P), no = NA)) %>%
  # ---- pivot back long ----
  tidyr::pivot_longer(cols = DSi:Si_to_P,
    names_to = "chemical",
    values_to = "response_values") %>%
  # ---- drop nas this pivot introduces ----
  dplyr::filter(!is.na(response_values)) %>%
  # ---- pivot back wide *again* using the original column names ----
  tidyr::pivot_wider(names_from = response_types,
    values_from = response_values) %>%
  # ---- fix the ratio specification now that they're not column names ----
  dplyr::mutate(
    chemical = gsub(pattern = "_to_", replacement = ":", x = chemical),
    .before = dplyr::everything()) %>%
  # reorder column names
  dplyr::select(LTER:chemical, Discharge_cms,
    dplyr::ends_with("Conc_mgL"), dplyr::ends_with("Conc_uM"),
    dplyr::ends_with("Flux_10_6kg_yr"), dplyr::ends_with("Flux_10_6kmol_yr")) %>%
  # calculate yield for both units
  dplyr::mutate(Yield_10_6kg_yr_km2 = Flux_10_6kg_yr / drainSqKm,
    FNYield_10_6kg_yr_km2 = FNFlux_10_6kg_yr / drainSqKm,
    Yield_10_6kmol_yr_km2 = Flux_10_6kmol_yr / drainSqKm,
    FNYield_10_6kmol_yr_km2 = FNFlux_10_6kmol_yr / drainSqKm) %>%
  dplyr::rename(Stream_Name = stream)

# glimpse this as well
dplyr::glimpse(results_table)

# ---- ## ----
# ---- process wrtds - annual kalman ----
# ---- ## ----

# results table
kalman_annual <- out_list[["ResultsTable_Kalman_WRTDS.csv"]] %>%
  # rename some columns for clarity
  dplyr::rename(Discharge_cms = Discharge..cms.,
    GenConc_mgL = GenConc..mg.L.,
    FNConc_mgL = FN.Conc..mg.L.,
    GenFlux_10_6kg_yr = GenFlux..10.6kg.yr.,
    FNFlux_10_6kg_yr = FN.Flux..10.6kg.yr.) %>%
  dplyr::mutate(Year = round(Year)) %>%
  # attach basin area
  dplyr::left_join(y = ref_table, by = c("LTER", "stream")) %>%
  # do some unit conversions
  dplyr::mutate(
    GenConc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (GenConc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (GenConc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (GenConc_mgL / 30.9) * 1000),
    FNConc_uM = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNConc_mgL / 28) * 1000,
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNConc_mgL / 14) * 1000,
      chemical %in% c("P", "TP") ~ (FNConc_mgL / 30.9) * 1000),
    GenFlux_10_6kmol_yr = dplyr::case_when(
      chemical %in% c("DSi") ~ (GenFlux_10_6kg_yr / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (GenFlux_10_6kg_yr / 14),
      chemical %in% c("P", "TP") ~ (GenFlux_10_6kg_yr / 30.9)),
    FNFlux_10_6kmol_yr = dplyr::case_when(
      chemical %in% c("DSi") ~ (FNFlux_10_6kg_yr / 28),
      chemical %in% c("NOx", "NH4", "NO3", "TN") ~ (FNFlux_10_6kg_yr / 14),
      chemical %in% c("P", "TP") ~ (FNFlux_10_6kg_yr / 30.9))) %>%
  # calculate ratios of different chemicals
  # ---- move area to the left ----
  dplyr::relocate(drainSqKm, .after = stream) %>%
  # ---- pivot longer to get various responses into a column ----
  tidyr::pivot_longer(cols = Discharge_cms:FNFlux_10_6kmol_yr,
    names_to = "response_types",
    values_to = "response_values") %>%
  # handle "duplicate" values for sites that break across a year so have two values for one year
  # ---- only relevant to the mcmurdo sites where we altered period of analysis ----
  dplyr::group_by(LTER, stream, drainSqKm, chemical, Year, response_types) %>%
  dplyr::summarize(response_values = mean(response_values, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  # ---- pivot back wider but with chemicals as columns ----
  tidyr::pivot_wider(names_from = chemical,
    values_from = response_values) %>%
  # ---- calculate din (din = nox <or> no3 + nh4) ----
  dplyr::mutate(DIN = dplyr::case_when(
    # ---- nox is preferred for calculating din because it is no3 + nox ----
    !is.na(NOx) & !is.na(NH4) ~ (NOx + NH4),
    !is.na(NO3) & !is.na(NH4) ~ (NO3 + NH4))) %>%
  # ---- calculate ratios ----
  dplyr::mutate(Si_to_DIN = ifelse(test = (!is.na(DSi) & !is.na(DIN)),
    yes = (DSi / DIN), no = NA),
  Si_to_P = ifelse(test = (!is.na(DSi) & !is.na(P)),
    yes = (DSi / P), no = NA)) %>%
  # ---- pivot back long ----
  tidyr::pivot_longer(cols = DSi:Si_to_P,
    names_to = "chemical",
    values_to = "response_values") %>%
  # ---- drop nas this pivot introduces ----
  dplyr::filter(!is.na(response_values)) %>%
  # ---- pivot back wide *again* using the original column names ----
  tidyr::pivot_wider(names_from = response_types,
    values_from = response_values) %>%
  # ---- fix the ratio specification now that they're not column names ----
  dplyr::mutate(
    chemical = gsub(pattern = "_to_", replacement = ":", x = chemical),
    .before = dplyr::everything()) %>%
  # reorder column names
  dplyr::select(LTER:chemical, Discharge_cms,
    dplyr::ends_with("Conc_mgL"), dplyr::ends_with("Conc_uM"),
    dplyr::ends_with("Flux_10_6kg_yr"), dplyr::ends_with("Flux_10_6kmol_yr")) %>%
  # calculate yield for both units
  dplyr::mutate(GenYield_10_6kg_yr_km2 = GenFlux_10_6kg_yr / drainSqKm,
    FNYield_10_6kg_yr_km2 = FNFlux_10_6kg_yr / drainSqKm,
    GenYield_10_6kmol_yr_km2 = GenFlux_10_6kmol_yr / drainSqKm,
    FNYield_10_6kmol_yr_km2 = FNFlux_10_6kmol_yr / drainSqKm) %>%
  dplyr::rename(Stream_Name = stream)

# glimpse this as well
dplyr::glimpse(kalman_annual)


# ---- ## ----
# ---- export wrtds outputs ----
# ---- ## ----

# combine processed files into a list
export_list <- list("WRTDS_trends.csv" = trends_table,
  "WRTDS_flux_bias.csv" = flux_stats,
  "WRTDS_error_stats.csv" = error_stats,
  "WRTDS_kalman_error_stats.csv" = kalman_error_stats,
  # ---- daily ----
  "WRTDS_daily.csv" = gfn_daily,
  "WRTDS_kalman_daily.csv" = kalman_daily,
  # ---- monthly ----
  "WRTDS_monthly.csv" = monthly,
  "WRTDS_kalman_monthly.csv" = kalman_monthly,
  # ---- yearly ----
  "WRTDS_annual.csv" = results_table,
  "WRTDS_kalman_annual.csv" = kalman_annual)

dest_url <- "https://drive.google.com/drive/u/1/folders/1YuRcqIKqjup3rRIc-riYJb70VK43uxAd"

# Loop across the list to export locally and to GoogleDrive
# ---- note that the "gfn_wrtds.csv" file is *huge* so it takes a few seconds to upload ----
for (name in names(export_list)) {
  # rip out that dataframe
  datum <- export_list[[name]]

  # define name for this file
  report_file <- file.path(path, "WRTDS Results_2025", paste0("Full_Results_", name))

  # write this csv out
  write.csv(x = datum, na = "", row.names = F, file = report_file)

  # Upload that object to GoogleDrive
  googledrive::drive_upload(media = report_file, overwrite = T, path = dest_url)
}

# ---- ## ----
# ---- export pdf reports ----
# ---- ## ----

# the "step 3" script also creates a pdf for every site
# we want to make those available outside of the server for later exploration and use

# identify all pdfs
# do some useful processing of that object
pdf_outs <- data.frame("file_name" = wrtds_outs_v0) %>%
  # split lter off the file name
  tidyr::separate(col = file_name, into = c("LTER", "other_content"),
    sep = "__", remove = FALSE, fill = "right", extra = "merge") %>%
  # separate the remaining content further
  tidyr::separate(col = other_content, into = c("stream", "chemical", "data_type"),
    sep = "_", remove = TRUE, fill = "right", extra = "merge") %>%
  # Recreate the "Stream_Element_ID" column
  dplyr::mutate(Stream_Element_ID = paste0(LTER, "__", stream, "___", chemical)) %>%
  # remove the pdfs of exploratory graphs
  dplyr::filter(data_type == "WRTDS_output.pdf") %>%
  # remove unwanted chemicals that we have data for
  dplyr::filter(!chemical %in% c("TN", "TP")) %>%
  # keep only rivers that finish the whole workflow!
  dplyr::filter(Stream_Element_ID %in% done_rivers)

# glimpse it
dplyr::glimpse(pdf_outs)

# kalman pdf outs
kalman_pdf_outs <- data.frame("file_name" = wrtds_outs_v0) %>%
  # split lter off the file name
  tidyr::separate(col = file_name, into = c("LTER", "other_content"),
    sep = "__", remove = FALSE, fill = "right", extra = "merge") %>%
  # separate the remaining content further
  tidyr::separate(col = other_content, into = c("stream", "chemical", "data_type"),
    sep = "_", remove = TRUE, fill = "right", extra = "merge") %>%
  # Recreate the "Stream_Element_ID" column
  dplyr::mutate(Stream_Element_ID = paste0(LTER, "__", stream, "___", chemical)) %>%
  # remove the pdfs of exploratory graphs
  dplyr::filter(data_type == "WRTDS_kalman_output.pdf") %>%
  # remove unwanted chemicals that we have data for
  dplyr::filter(!chemical %in% c("TN", "TP")) %>%
  # keep only rivers that finish the whole workflow!
  dplyr::filter(Stream_Element_ID %in% done_rivers)

# glimpse it
dplyr::glimpse(kalman_pdf_outs)


# identify pdf folder
# ---- standard output destination ----
pdf_url <- googledrive::as_id("https://drive.google.com/drive/folders/1Sx0A5C8nk53ft2Ip28q4PipnjYYwi3D7")
kalman_pdf_url <- googledrive::as_id("https://drive.google.com/drive/folders/1n2M0n6UU7_lQrU94gvSzhUXVKl9Qg3V4")

# Identify PDFs already in GoogleDrive
drive_pdfs <- googledrive::drive_ls(path = pdf_url)

# use that to identify new pdfs!
new_pdfs <- setdiff(pdf_outs$file_name, drive_pdfs$name)

# Loop across these PDFs and put them into GoogleDrive
for (report in unique(pdf_outs$file_name)) {
  # ---- (^^^) upload *all* pdfs regardless of whether they're in the drive ----
  # ---- (vvv) upload only *new* pdfs ----
  # for(report in new_pdfs){

  # Send that report to a GoogleDrive folder
  googledrive::drive_upload(media = file.path(path, "WRTDS Outputs_2025", report),
    overwrite = T, path = pdf_url)
}

# Clear environment of everything but the filepath, destination URL, and ref_table
rm(list = setdiff(ls(), c("path", "dest_url", "ref_table", "done_rivers", "done_boots")))

# ---- ## ----
# ---- identify bootstrap outputs ----
# ---- ## ----

# list all files in "wrtds outputs"
boot_outs_v0 <- dir(path = file.path(path, "WRTDS Bootstrap Outputs"))

# do some useful processing of that object
boot_outs <- data.frame("file_name" = boot_outs_v0) %>%
  # split lter off the file name
  tidyr::separate(col = file_name, into = c("LTER", "other_content"),
    sep = "__", remove = FALSE, fill = "right", extra = "merge") %>%
  # separate the remaining content further
  tidyr::separate(col = other_content, into = c("stream", "chemical", "data_type"),
    sep = "_", remove = TRUE, fill = "right", extra = "merge") %>%
  # Recreate the "Stream_Element_ID" column
  dplyr::mutate(Stream_Element_ID = paste0(LTER, "__", stream, "___", chemical)) %>%
  # keep only rivers that finish the whole workflow!
  dplyr::filter(Stream_Element_ID %in% done_boots)

# glimpse it
dplyr::glimpse(boot_outs)

# create an empty list
boot_out_list <- list()

# define the types of output file suffixes that are allowed
(boot_out_types <- unique(boot_outs$data_type))

# for each data type
for (type in boot_out_types) {
  # return processing message
  message("Processing ", type, " outputs")

  # identify all files of that type
  file_set <- boot_outs %>%
    dplyr::filter(data_type == type) %>%
    dplyr::pull(var = file_name)

  # make a counter set to 1
  k <- 1

  # make an empty list
  boot_sub_list <- list()

  # read them all in!
  for (file in file_set) {
    # read in csv and add it to the list
    boot_datum <- read.csv(file = file.path(path, "WRTDS Bootstrap Outputs", file))

    # add it to the list
    boot_sub_list[[paste0(type, "_", k)]] <- boot_datum %>%
      # add a column for the name of the file
      dplyr::mutate(file_name = file, .before = dplyr::everything())

    # advance counter
    k <- k + 1
  }

  # Once all files of that type are retrieved, unlist the sub_list!
  boot_type_df <- boot_sub_list %>%
    # actual unlisting of the list
    purrr::list_rbind(x = .) %>%
    # bring in other desired columns
    dplyr::left_join(y = boot_outs, by = "file_name") %>%
    # Drop the redundant data_type column
    dplyr::select(-data_type) %>%
    # relocate other joined columns to front
    dplyr::relocate(Stream_Element_ID, LTER, stream, chemical,
      .after = file_name)

  # add this dataframe to the output list
  boot_out_list[[type]] <- boot_type_df

  # completion message
  message("Completed processing ", type, " outputs")
}

# check the structure of the whole output list
str(boot_out_list)
names(boot_out_list)

# Clear environment of everything but the filepath, destination URL, boot_out_list, & ref_table
rm(list = setdiff(ls(), c("path", "dest_url", "boot_out_list", "ref_table",
  "done_rivers", "done_boots")))

# ---- ## ----
# ---- process bootstrap outputs ----
# ---- ## ----

# bootstraps
boots_gfn <- boot_out_list[["EGRETCi_GFN_bootstraps.csv"]]

# glimpse it
dplyr::glimpse(boots_gfn)

# grab trends
boots_trends <- boot_out_list[["EGRETCi_GFN_Trend.csv"]]

# glimpse it
dplyr::glimpse(boots_trends)

# grab final output: pairs
boots_pairs <- boot_out_list[["ListPairs_GFN_WRTDS.csv"]]

# glimpse it
dplyr::glimpse(boots_pairs)

# ---- ## ----
# ---- export bootstrap outputs ----
# ---- ## ----

# combine processed files into a list
boot_export_list <- list("WRTDS_EGRETCi_bootstraps.csv" = boots_gfn,
  "WRTDS_EGRETCi_trends.csv" = boots_trends,
  "WRTDS_GFN.csv" = boots_pairs)

# Loop across the list to export locally and to GoogleDrive
# ---- note that the "gfn_wrtds.csv" file is *huge* so it takes a few seconds to upload ----
for (name in names(boot_export_list)) {
  # rip out that dataframe
  boot_datum <- boot_export_list[[name]]

  # define name for this file
  boot_report_file <- file.path(path, "WRTDS Bootstrap Results", paste0("Bootstrap_Full_Results_", name))

  # write this csv out
  write.csv(x = boot_datum, na = "", row.names = F, file = boot_report_file)

  # Upload that object to GoogleDrive
  googledrive::drive_upload(media = boot_report_file, overwrite = T, path = dest_url)
}

# ---- end ----

# ---- ## ----
# ---- crop wrtds outputs ----
# ---- ## ----

# ---- temporary step to accommodate for leading discharge data ## ----

# ---- daily data ----
daily_wrtds_v1 <- gfn %>%
  # left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Stream_Name")) %>%
  # drop any years before the one year buffer suggested by wrtds
  dplyr::filter(Date > disc_start) %>%
  # reorder columns / rename q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name, -min_date, -disc_start)

daily_kalman_v1 <- kalm_main %>%
  # left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Stream_Name")) %>%
  # drop any years before the one year buffer suggested by wrtds
  dplyr::filter(Date > disc_start) %>%
  # reorder columns / rename q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name, -min_date, -disc_start)


# ---- monthly data ----
monthly_v1 <- monthly %>%
  # left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Stream_Name")) %>%
  # drop any years before the one year buffer suggested by wrtds
  dplyr::filter(Year > year(disc_start)) %>%
  # reorder columns / rename q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name, -min_date, -disc_start)

monthly_kalman_v1 <- kalman_monthly %>%
  # left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Stream_Name")) %>%
  # drop any years before the one year buffer suggested by wrtds
  dplyr::filter(Year > year(disc_start)) %>%
  # reorder columns / rename q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name, -min_date, -disc_start)

# ---- annual data ## ----

annual_wrtds_v1 <- results_table %>%
  # left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Stream_Name")) %>%
  # drop any years before the one year buffer suggested by wrtds
  dplyr::filter(Year > year(disc_start)) %>%
  # reorder columns / rename q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name, -min_date, -disc_start)

annual_kalman_v1 <- kalman_annual %>%
  # left join on the start date from the chemistry data
  dplyr::left_join(y = disc_lims, by = c("LTER", "Stream_Name")) %>%
  # drop any years before the one year buffer suggested by wrtds
  dplyr::filter(DecYear > year(disc_start)) %>%
  # reorder columns / rename q column / implicitly drop unwanted columns
  dplyr::select(-Discharge_File_Name, -min_date, -disc_start)
