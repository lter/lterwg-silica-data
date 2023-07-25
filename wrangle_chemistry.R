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
librarian::shelf(googledrive, tidyverse, supportR)

# Clear environment
rm(list = ls())

# Create a folder for inputs & and outputs
dir.create(path = file.path("chem_ins"), showWarnings = F)
dir.create(path = file.path("chem_outs"), showWarnings = F)

## -------------------------------------------- ##

## -------------------------------------------- ##


# End ----
