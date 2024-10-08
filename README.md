# From poles to tropics: A multi-biome synthesis investigating the controls on river Si exports

- Primary Investigators: Joanna Carey & Kathi Jo Jankowski
- [Project Summary](https://lternet.edu/working-groups/river-si-exports/)
- [Participant Information](https://www.nceas.ucsb.edu/projects/12816)

## Script Explanations

### Data Harmonizing (`00`)

As with many synthesis projects, this work involves "harmonizing" many separate data files (i.e., rendering them comparable then combining them).

- `00-harmonize_chemistry.R`: **Combines individual _chemistry_ files (river / stream gage level) into a single file.** This is done with virtually no filtering to minimize data loss from this process
    - _When to Use:_ you want to integrate new _chemistry_ data into the 'master' file

- `00-harmonize_discharge.R`: **Combines individual _discharge_ files (river / stream gage level) into a single file.** This is done with virtually no filtering to minimize data loss from this process
    - _When to Use:_ you want to integrate new _discharge_ data into the 'master' file

### WRTDS (`01`)

WRTDS (Weighted Regressions on Time, Discharge, and Season) is a group of workflows necessary to process the harmonized discharge and chemistry data (see `00`) into data that are analysis-ready for a larger suite of further analyses. It does require several 'steps' be taken--in order--and these are preserved as separate scripts for ease of maintenance.

- `01-wrtds-step01_find-areas.R`: **Identifies the drainage basin area (in km^2) for all rivers.** Uses either the expert-provided area in the [reference table GoogleSheet](https://docs.google.com/spreadsheets/d/11t9YYTzN_T12VAQhHuY5TpVjGS50ymNmKznJK4rKTIU/edit#gid=357814834) or calculates it from DEM data.
    - _When to Use:_ you want to add new rivers to the WRTDS workflow

- `01-wrtds-step02_wrangling.R`: **Does all pre-WRTDS wrangling to (1) master chemistry, (2) master discharge, and (3) reference table files.** Includes a "sabotage check" looking for any sites dropped by that wrangling.
    - _When to Use:_ one of the three input files has been updated

- `01-wrtds-step03_analysis.R`: **Actually runs WRTDS.**
    - _When to Use:_ you've tweaked the WRTDS workflow and/or want to update results

- `01-wrtds-step03b_bootstrap.R`: **Runs the 'bootstrap' variant of WRTDS.** _This script is optional_ and is separate from `step03` because (A) it takes _much_ longer to run and (B) we don't run it as often as the 'main' WRTDS analysis script
    - _When to Use:_ you've tweaked the _bootstrap_ workflow and/or want to update its results

- `01-wrtds-step04_results-report.R`: **Creates single results output files for each type of WRTDS output.**
    - _When to Use:_ you want to generate new summary files

### Data Down/Upload (`99`) - _INTERNAL USE ONLY_

This group makes extensive use of Google Drive for convenient storing/sharing of the outputs of these scripts. However, such interactions do require access to the groups' Shared Google Drive and thus include these operations directly in the code functionally makes the code only run-able by members of the group. To solve this, **all Google Drive interactions are isolated into a standalone script**. Only a member of this working group can run the script so all others must run the 'actual' workflow scripts (in order) to ensure they have the relevant inputs for each.

- `99-gdrive_download-upload.R`: **Downloads inputs and uploads outputs from/to the Shared Google Drive.**
    - _When to Use:_ you want to skip 'early' steps in the workflow by downloading their most recent outputs from the Drive

## Related Repositories

This working group has several repositories. All are linked and described (briefly) below.

- [lter/**lterwg-silica-data**](https://github.com/lter/lterwg-silica-data) - Primary data wrangling / tidying repository for "master" data files
- [lsethna/**NCEAS_SiSyn_CQ**](https://github.com/lsethna/NCEAS_SiSyn_CQ) - Examples concentration (C) and discharge (Q) relationships for a wide range of solutes
- [lter/**lterwg-silica-spatial**](https://github.com/lter/lterwg-silica-spatial) - Extracts spatial and climatic information from within watershed shapefiles
- [njlyon0/**lter_silica-high-latitude**](https://github.com/njlyon0/lter_silica-high-latitude) - Performs analysis and visualization for the high latitude manuscript
- [SwampThingPaul/**SiSyn**](https://github.com/SwampThingPaul/SiSyn) - Original repository for this working group. Performs many functions from data wrangling through analysis and figure creation
