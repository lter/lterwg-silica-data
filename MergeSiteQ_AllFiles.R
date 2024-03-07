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

# file for raw discharge data files
setwd("C:/Users/kjankowski/OneDrive - DOI/Documents/Projects/SilicaSynthesis/Data/Discharge")
dir.create(path = file.path("discharge_raw"), showWarnings = F)
dir.create(path = file.path("discharge_tidy"), showWarnings = F)

#read in reference table - you might need to change this link
ref_table_link<-"https://docs.google.com/spreadsheets/d/11t9YYTzN_T12VAQhHuY5TpVjGS50ymNmKznJK4rKTIU/edit?usp=sharing"

ref_table_folder = drive_get(as_id(ref_table_link))

ref_table<-drive_download(ref_table_folder$drive_resource, overwrite = T)

QLog<-read_xlsx(ref_table$local_path)


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

csv_files_download <- csv_files %>% 
  filter(!name %in% csv_files_remove)

#######################################################################################
#### Download files from Google Drive to store locally
# check working directory where files will be stored locally; separate folder within project folder

setwd("C:/Users/kjankowski/OneDrive - DOI/Documents/Projects/SilicaSynthesis/Data/Discharge/discharge_raw")

# download each file to the working directory; files are saved locally
for (i in 1:length(csv_files_download$drive_resource)) {
  drive_download(csv_files_download$drive_resource[i],  overwrite=T)
}


###############################################################################################
### Prep for loop to concatentate discharge files

# get list of files downloaded
discharge_files = list.files(path="discharge_raw", pattern = ".csv")

# remove all "Master Q" or other unneeded files
discharge_files<-discharge_files[!(discharge_files %like% "Discharge_master")]
remove_these<-setdiff(csv_files$name,discharge_files)
discharge_files<-discharge_files[!(discharge_files %in% remove_these)]

# set working directory where discharge files stored locally
setwd("discharge_raw")

#create list to store output from for loop
data_list = list()

# Create lists for discharge and dates
# this is where you add different names for discharge and date columns
#you will need to add more to incorporate new data

DischargeList<-c("MEAN_Q", "Discharge", "InstantQ", "Q_m3sec", "discharge", "Q", 
                 "Q_cms","Flow","var", "Value", "valeur",
                 "AVG_DISCHARGE","dailyQ")
DateList<-c("Date", "dateTime", "dates", "date", "datetime", "DATE_TIME",
            "Sampling Date", "Dates")

i=i

#loop through each discharge file
#rename columns, convert units, keep only important columns

for (i in 1:length(discharge_files)) {
  file_name_nocsv<-substr(discharge_files[i],start=1,stop=nchar(discharge_files[i])-4)
  file_name = discharge_files[i]
  d = fread(file_name, sep=",", tz="")
  names(d)[which(colnames(d) %in% DischargeList)]<-"Q"
  names(d)[which(colnames(d) %in% DateList)]<-"Date"
  d<-d[,c("Q", "Date")]
  d$Discharge_File_Name<-file_name_nocsv
  ref_site<-subset(QLog, QLog$Discharge_File_Name==file_name_nocsv)
  ref_site<-ref_site[1,]
  d$Units<-ref_site$Units
  #d$LTER = LTER_name
  
  #convert all Q file units to CMS
  d$Qcms<-ifelse(d$Units=="cms", d$Q, 
                 ifelse(d$Units=="cfs", d$Q*0.0283,
                        ifelse(d$Units=="Ls", d$Q*0.001,
                               ifelse(d$Units=="cmd", d$Q*1.15741e-5, ""))))
  
  d<-d[,c("Qcms", "Date", "Discharge_File_Name")]
  
  #convert date to date format
  if(is.Date(d$Date)){

    d$Date<-d$Date

  } else{

    #d<-date_format_guess(d,"Date", groups = TRUE, group_col = "LTER")
    #d <- date_format_guess(data=d,date_col="Date", groups = DischargeFileName)
    
    format<-"%m/%d/%Y"
    d$Date<-as.IDate(d$Date, format)
    
    #x = d %>% mutate(Date = as.character(Date)) %>% 
     # date_format_guess(date_col="Date", groups = DischargeFileName) %>% 
      #case_when(date_format_guess == "year/month/day" ~ as.Date(Date, ""))

  }

  print(is(d$Date)) # check date format as list is created
  data_list[[i]] = d
}

### Create data frame from list - use bind_rows to concatenate each new discharge file
# should have 3 columns: date, site, discharge
disc_v1 = bind_rows(data_list)

#####################################################
## checks on discharge, dates
# checking files where dates got converted to NA
na_dates = filter(disc_v1, is.na(Date))

# checking which files have negative discharge values
neg_Q = filter(disc_v1, Qcms<0)

# plotting to see what they look like
neg_Q %>% 
  ggplot(aes(Qcms))+
  geom_histogram()+
  facet_wrap(~Discharge_File_Name, scales="free")

# addressing negative values
# keep all values above zero, replace -999999 with NA, replace negative with 0
disc_v2 = disc_v1 %>% 
  mutate(Qcms = case_when(
   Qcms >= 0 ~ Qcms,
   Qcms == -999999 ~ NA,
   Qcms < 0 ~ 0))

# check
neg_Q <- filter(disc_v2, Qcms<0)

## plot to see what new data look like
disc_v2 %>% 
  filter(Discharge_File_Name == "Rio Purus_Q") %>%  
  ggplot(aes(Date,Qcms)) +
  geom_point()

## append the Stream Name to the file before saving ## 
name_table = QLog %>%
  select(LTER,Stream_Name,Discharge_File_Name)

disc_v3 <- disc_v2 %>% 
  left_join(y=name_table,by="Discharge_File_Name")

#check this
glimpse(disc_v3)

#check the many-to-many warnings
name_table %>% filter(Discharge_File_Name=="ARC_Imnavait_fill_Q")
disc_v2 %>% filter(Discharge_File_Name=="AAGEVEG_Q") %>% pull(Discharge_File_Name) %>% unique()
#pull() makes a column and returns it as a vector

#the number of rows should be the same, to make check discharge is added for multi-alias'd sites
disc_v2 %>% filter(Discharge_File_Name=="AAGEVEG_Q") %>% nrow()
disc_v3 %>% filter(Discharge_File_Name=="AAGEVEG_Q") %>% nrow()

####
# plot all discharge and save to file
p = ggplot(data = disc_v3, aes(x = Date, y = Qcms)) + 
  geom_point()

plots = disc_v3 %>%
  group_by(Discharge_File_Name) %>%
  do(plots = p %+% . + facet_wrap(~Discharge_File_Name))

setwd("C:/Users/kjankowski/OneDrive - DOI/Documents/Projects/SilicaSynthesis/Data/Discharge")
pdf()
plots$plots
dev.off()

#change date to reflect new file creation
setwd('../discharge_tidy')
write.csv(disc_v3, "20240201_masterdata_discharge.csv", row.names=FALSE)



########################################################################
### reviewing files
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
