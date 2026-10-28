# install.packages("googledrive")

# load libraries
require(tidyverse)
require(googledrive)
require(stringr)
require(lubridate)
require(reshape)
require(gtools)
require(plyr)
require(dplyr)
require(tidyr)
require(data.table)
require(dataRetrieval)
library(readxl)
library(supportR)

rm(list = ls())

# load the daily discharge preparation before changing directories
source("discharge_functions.R")

# select the finalized master chemistry csv, not an earlier drive copy
# enter its local path here or choose it when prompted
chemistry_file <- ""
if (!nzchar(chemistry_file)) chemistry_file <- file.choose()

# keep the dated discharge-only usgs download separate from the older mixed file
shingle_source_file <- normalizePath(file.path("discharge-inputs",
  "20261027_USGS_05288705_daily_discharge.json"), mustWork = FALSE)
# download the saved source once if it is not already here
if (!file.exists(shingle_source_file)) {
  dir.create(dirname(shingle_source_file), recursive = TRUE, showWarnings = FALSE)
  googledrive::drive_download(googledrive::as_id("1n0JHBLq1Oi6zH3CPa3UxlWjMY542y4KN"),
    path = shingle_source_file, overwrite = FALSE)
}

# leave shared outputs alone while checking the discharge changes
upload_outputs <- FALSE

# saved original-source records fill only the checked gaps, without replacing existing flows
discharge_additions_file <- file.path("discharge-inputs", "20261027_original_discharge_additions.rds")
if (!file.exists(discharge_additions_file)) {
  dir.create(dirname(discharge_additions_file), recursive = TRUE, showWarnings = FALSE)
  googledrive::drive_download(googledrive::as_id("1kYR7-tWCEPO2tKj5Mcl2V1BtX2JytkUv"),
    path = discharge_additions_file, overwrite = FALSE)
}
discharge_additions <- readRDS(discharge_additions_file)

# Set up paths and import data --------------------------------------------

# set up path to store files
(path <- scicomptools::wd_loc(local = FALSE, remote_path = file.path('/', "home", "shares", "lter-si", "WRTDS")))

# create new folder for discharge data
dir.create(path = file.path(path, "discharge"), showWarnings = F)

# create new path for storing discharge folders and files
(path2 <- scicomptools::wd_loc(local = FALSE, remote_path = file.path('/', "home", "shares", "lter-si", "WRTDS","discharge")))

dir.create(path = file.path(path2,"discharge_raw"), showWarnings = F)
dir.create(path = file.path(path2,"discharge_tidy"), showWarnings = F)

### read in reference table - you might need to change this link
#ref_table_link<-"https://docs.google.com/spreadsheets/d/11t9YYTzN_T12VAQhHuY5TpVjGS50ymNmKznJK4rKTIU"
#ref_table_folder = drive_get(as_id(ref_table_link))
#ref_table<-drive_download(ref_table_folder$drive_resource, overwrite = T)
#ref_table <- drive_download(file = ref_table_folder$id, path = file.path(path2,"Site_Reference_Table"), overwrite = T)
#QLog<-read_xlsx(ref_table$local_path)

# read the same live reference tab used by the chemistry workflow
ref_table <- as.data.frame(googlesheets4::read_sheet(
  "11t9YYTzN_T12VAQhHuY5TpVjGS50ymNmKznJK4rKTIU",
  sheet = "Site_Reference_Table_LTER_V3", col_types = "c"
))
ref_table <- discharge_site_reference(ref_table, chemistry_file)

QLog<-ref_table

# Identify and Download files ----------------------------------------------------------

### Create list of files to download and data download
#get folder URL from google drive with discharge data - "Discharge_files"
#only use files ending in "_Q_WRTDS.csv"
folder_url = "https://drive.google.com/drive/u/1/folders/1mw0rYbqpMO4VIXfTH5Ry9USzlR4tbvfb"

#get ID of folder
folder = drive_get(as_id(folder_url))

#get list of csv files from folder
csv_files = drive_ls(folder, type="csv")

# list of files to not include in the download - duplicates or not using
csv_files_remove <- c( "andrsn_h1_Q.csv",                                      
                       "canada_f1_Q.csv" ,                                     
                       "common_c1_Q.csv"  ,                                    
                       "Congo_Q.csv",                                          
                       "crescent_f8_Q.csv",                                    
                       "delta_f10_Q.csv",                                       
                       "Francisco_Q.csv",                                      
                       "green_f9_Q.csv",                                       
                       "harnish_f7_Q.csv",                                     
                       "lawson_b3_Q.csv",                                      
                       "MCM_Andersen Creek at H1_fill_Q_update.csv",           
                       "MCM_Lawson Creek at B3_fill_Q_update.csv",             
                       "MCM_Onyx River at Lake Vanda Weir_fill_Q_update.csv",  
                       "MCM_Onyx River at Lower Wright Weir_fill_Q_update.csv",
                       "NigerRiver_Q.csv",                                     
                       "onyx_lwright_Q.csv",                                   
                       "onyx_vnda_Q.csv",                                      
                       "priscu_b1_Q.csv",                                      
                       "vguerard_f6_Q.csv")

required_files <- unique(ref_table$Discharge_File_Name)
required_files <- required_files[!is.na(required_files) & nzchar(trimws(required_files))]
required_files <- setdiff(paste0(required_files, ".csv"), csv_files_remove)
missing_files <- setdiff(required_files, csv_files$name)
if (length(missing_files)) stop("missing discharge files for finalized chemistry sites: ",
  paste(missing_files, collapse = ", "))
csv_files_download <- csv_files %>% filter(name %in% required_files)
if (anyDuplicated(csv_files_download$name)) stop("multiple drive files share a required discharge filename")


# Download files from Google Drive to store locally
# check working directory where files will be stored locally; separate folder within project folder

#setwd("~/Documents/Work/Silica/Data/Discharge/discharge_raw")
setwd("//home/shares/lter-si/WRTDS/discharge/discharge_raw")

# download each file to the working directory; files are saved locally
for (i in 1:length(csv_files_download$drive_resource)) {
  drive_download(csv_files_download$drive_resource[i],  overwrite=T)
}


# Prep loop and check match with reference table --------------------------

# use only files selected for this run, never stale files in the local folder
discharge_files <- csv_files_download$name
if (!all(file.exists(file.path(path2, "discharge_raw", discharge_files)))) {
  stop("a selected discharge file was not downloaded")
}
if ("BK01.0M_Q" %in% ref_table$Discharge_File_Name &&
    !"BK01.0M_Q.csv" %in% discharge_files) {
  stop("BK01.0M_Q is mapped but was not downloaded for this run")
}

# Check for whether all the streams have representatives in the Site Reference Table?
check_files <- as.data.frame(discharge_files) %>% 
  # remove csv for matching
  mutate(Discharge_File_Name_nocsv = substr(discharge_files,start=1,stop=nchar(discharge_files)-4)) %>% 
  # filter out those that are NOT in the Site Reference Table
  filter(!Discharge_File_Name_nocsv  %in% ref_table$Discharge_File_Name)

discharge_files_v2 <- as.data.frame(discharge_files) %>% 
  # remove csv for matching
  mutate(Discharge_File_Name_nocsv = substr(discharge_files,start=1,stop=nchar(discharge_files)-4)) %>% 
  # filter out those that are NOT in the Site Reference Table
  filter(Discharge_File_Name_nocsv  %in% ref_table$Discharge_File_Name)

head(discharge_files_v2)

discharge_files_v3 <- discharge_files_v2$discharge_files

# Combine discharge files with loop ---------------------------------------

# set working directory where discharge files stored locally
setwd("//home/shares/lter-si/WRTDS/discharge/discharge_raw")

#create list to store output from for loop
data_list = list()

# Create lists for discharge and dates
# this is where you add different names for discharge and date columns
#you will need to add more to incorporate new data

DischargeList<-c("MEAN_Q", "Discharge", "InstantQ", "Q_m3sec", "discharge", "Q", 
                 "Q_cms","Flow","var", "Value", "valeur",
                 "AVG_DISCHARGE","dailyQ","Discharge.m3.s.","Discharge(m3/s)", 
                 "Mean Daily", "Mean_Daily_Discharge","mean_daily_Q", "Daily_Mean_Q",
                 "CC_Q_cms","Qcms", "Discharge.m3.s.")
DateList<-c("Date", "dateTime", "dates", "date", "datetime", "DATE_TIME",
            "Sampling Date", "Dates","DateTime")


# prepare daily discharge without changing the source files

discharge_files <- discharge_files_v3
source_cache <- new.env(parent = emptyenv())


for (i in seq_along(discharge_files)) {
  file_name <- discharge_files[i]
  file_name_nocsv <- tools::file_path_sans_ext(file_name)
  original <- data.table::fread(file.path(path2, "discharge_raw", file_name),
    showProgress = FALSE)
  original <- restore_original_discharge(original, file_name_nocsv, ref_table, source_cache,
    shingle_source_file = shingle_source_file)
  data_list[[i]] <- prepare_daily_discharge(
    original, file_name_nocsv, ref_table, DischargeList, DateList
  )
  data_list[[i]] <- add_verified_discharge(data_list[[i]], discharge_additions)
}

### Create data frame from list - use bind_rows to concatenate each new discharge file
# daily values retain the discharge filename and preparation flags
disc_v1 = bind_rows(data_list)


# QA/QC of combined file --------------------------------------------------

# preserve shared gauges without duplicating the same file-to-site mapping
disc_v2 <- attach_discharge_sites(disc_v1, ref_table)

# checking files where dates got converted to NA
disc_v2 %>% 
  filter(is.na(Date)) %>% 
  group_by(Discharge_File_Name) %>% 
  dplyr::summarise(n=n())

# remove missing dates
disc_v3 <- disc_v2 %>% 
  filter(!is.na(Date))
  
# screen discharge values for NA and unreasonable numbers
# Check for any non numeric values in discharge column
supportR::num_check(data = disc_v3, col = "Qcms")

range(suppressWarnings(as.numeric(disc_v3$Qcms)), na.rm = T)

# look at negative discharge values 
disc_v3 %>% 
  filter(Qcms <0) %>% 
  group_by(Discharge_File_Name) %>% 
  dplyr::summarise(n=n())

# plotting to see what they look like
disc_v3 %>%
  filter(Qcms<0) %>% 
  ggplot(aes(Qcms))+
  geom_histogram()+
  facet_wrap(~Discharge_File_Name, scales="free")

# addressing negative values
# keep all values above zero, replace -999999 with NA, replace negative with 0
disc_v4 = disc_v3 %>% 
  mutate(Qcms = case_when(
   Qcms >= 0 ~ Qcms,
   # covers the NA indicators like -9999
   Qcms <= 0 ~ NA,
   .default=Qcms))

# check again
disc_v4 %>% 
  filter(Qcms <0) %>% 
  group_by(Discharge_File_Name) %>% 
  dplyr::summarise(n=n()) %>% 
  filter()

## Check date formats
# Look at general date format per discharge file
disc_v4 %>%
  dplyr::group_by(Discharge_File_Name) %>%
  dplyr::summarize(dates = paste(unique(Date), collapse = "; ")) %>%
  tidyr::pivot_wider(names_from = Discharge_File_Name, values_from = dates) %>%
  dplyr::glimpse()

# Plot discharge data and save to file for review -------------------------


# plot all discharge and save to file
p = ggplot(data = disc_v4, aes(x = Date, y = Qcms)) + 
  geom_point()

plots = disc_v4 %>%
  group_by(Discharge_File_Name) %>%
  do(plots = p %+% . + facet_wrap(~Discharge_File_Name))

setwd("//home/shares/lter-si/WRTDS/discharge")
pdf()
plots$plots
dev.off()


# Export Data to file and Google Drive ------------------------------------

#change date to reflect new file creation
setwd("//home/shares/lter-si/WRTDS/discharge/discharge_tidy")

# Grab today's date
date <- gsub(pattern = "-", replacement = "", x = Sys.Date())

# Generate a date-stamped file name for this file
( disc_filename <- paste0(date, "_masterdata_discharge.csv") )

# write locally
write.csv(x = disc_v4, file = file.path(path2,"discharge_tidy", disc_filename), na = '', row.names = F)

# write to google drive
if (upload_outputs) googledrive::drive_upload(media = file.path(path2,"discharge_tidy", disc_filename), overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1hbkUsTdo4WAEUnlPReOUuXdeeXm92mg-"))

########################################################################

# Generate a date-stamped file name for this file
( filename <- paste0(date, "_check_discharge_file_names.csv") )

# write locally
write.csv(x = check_files, file = file.path(path2,"discharge_tidy", filename), na = '', row.names = F)

# write to google drive
if (upload_outputs) googledrive::drive_upload(media = file.path(path2,"discharge_tidy", filename), overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1hbkUsTdo4WAEUnlPReOUuXdeeXm92mg-"))



### reviewing files
# older source-editing examples below are not part of the master discharge run
if (FALSE) {
filter(all_discharge_save, DischargeFileName == "AL02.3M_Q")


##########################################################################################
### clean up Australia files to combine into larger dataset - and save them to file 
## they have leading rows that need to be skipped and weird time/date stamp
# Murray Darling
setwd("C:/Users/kjankowski/OneDrive - DOI/Documents/Projects/SilicaSynthesis/Data/Discharge/Discharge_forAnalysis")
filedir <- setwd("C:/Users/kjankowski/OneDrive - DOI/Documents/Projects/SilicaSynthesis/Data/Discharge/Discharge_forAnalysis")

# Loading data files 
file_names <- dir(filedir)

# list of annual result files
MD_files <- file_names[file_names %like% "MD_"]
AUS_files <- file_names[file_names %like% "AUS_"]

# import as dataframe with file names 
# from this post: https://stackoverflow.com/questions/11433432/how-to-import-multiple-csv-files-at-once
MD_dat <-
  list.files(pattern = "\\MD_",
             full.names = T) %>%
  map_df(~read_plus(.))

# reformat file names 
MD_dat$filename = gsub("./", "", as.character(MD_dat$filename))
MD_dat$`#Timestamp`= as.Date(MD_dat$`#Timestamp`)
colnames(MD_dat)=c("Date","Discharge","Quality_Code","Interpolation_Type","Filename")

# checking data - totally missing Q data for two sites
MD_dat %>% 
  ggplot(aes(Date,Discharge))+
  geom_point()+
  facet_wrap(~Filename)

AUS_dat <-
  list.files(pattern = "\\AUS_",
             full.names = T) %>%
  map_df(~read_plus(.))

# reformat file names 
AUS_dat$filename = gsub("./", "", as.character(AUS_dat$filename))
AUS_dat$`#Timestamp`= as.Date(AUS_dat$`#Timestamp`)
colnames(AUS_dat)=c("Date","Discharge","Quality_Code","Interpolation_Type","Filename")

AUS_dat %>% 
  ggplot(aes(Date,Discharge))+
  geom_point()+
  facet_wrap(~Filename, scales="free")

# Export and OVERWRITE originally downloaded files with modified Australia files - be CAREFUL!
setwd("C:/Users/kjankowski/OneDrive - DOI/Documents/Projects/SilicaSynthesis/Data/Discharge/Discharge_forAnalysis")
MD_dat %>% 
  group_by(Filename) %>% 
  group_walk(~ write_csv(.x, paste0(.y$Filename)))

AUS_dat %>% 
  group_by(Filename) %>% 
  group_walk(~ write_csv(.x, paste0(.y$Filename)))



# allQ<-read.csv("UpdatedAll_Q_master_10262022.csv")
# 
# 
# GRO_min<-all_discharge %>%
#   filter(LTER=="GRO") %>%
#   dplyr::group_by(DischargeFileName) %>%
#   slice_min(Date)
# 
# #which sites are included in master discharge file? Different from input files?
# discharge_sites = data.frame("site.name"=unique(all_discharge$site.name))
# WRTDS_sites = data.frame("site"=unique(WRTDS_discharge$site))
# 
# #long term sites
# Data_years_streams_WRTDS = read_csv("L:/GitHub/SiSyn/Merge Site Discharge/Data_years_streams_WRTDS.csv") #download directly from "https://drive.google.com/drive/folders/1q92ee9nKct_nCJ3NVD2-tm8KCuRBfm2U"
# longterm_list = data.frame(LTER=Data_years_streams_WRTDS$LTER,
#                            site.name=Data_years_streams_WRTDS$Stream.Site)
# #are all sites in long term site list in all_discharge?
# longterm_check = merge(discharge_sites,longterm_list, by="site.name", all=T)
# 
# #merge long-term list with all_discharge to add LTER name
# all_discharge_longterm = merge(all_discharge, longterm_list, all=T)
# 
# #write master discharge file to .csv
# setwd("L:/GitHub/SiSyn/Merge Site Discharge")
# write.csv(all_discharge_longterm, file="WRTDS_discharge_allsites_11Aug21.csv")
}
