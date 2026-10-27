# From poles to tropics: A multi-biome synthesis investigating the controls on river Si exports

- Primary Investigators: Joanna Carey & Kathi Jo Jankowski
- [Project Summary](https://lternet.edu/working-groups/river-si-exports/)
- [Participant Information](https://www.nceas.ucsb.edu/projects/12816)

## Script Explanations

### Data Harmonizing (`00`)

These scripts put chemistry files into the same format and combine them into master chemistry.

- `00-harmonize_chemistry.R`: Kathi Jo's original workflow, kept unchanged for reference

- `00-harmonize_chemistry_sidney_review-only.R`: comparison copy with Sidney's updates commented beside Kathi Jo's code; use it for review, not to build the master

- `run_full_chemistry_workflow.R`: open this file in RStudio and click **Source** to run the updated workflow

- `00-harmonize_chemistry_sidney.R`: combines the prepared files, converts units, calculates daily averages, and adds flags; the runner calls it automatically

- `chemistry_functions.R`: functions shared by the scripts; it is loaded automatically

- `chemistry_rules.R`: the names, conversion factors, dates, and exclusions used in the workflow; check here to review those choices

#### Running the Updated Chemistry Workflow

Keep the files together in the downloaded or cloned repository folder. Open `run_full_chemistry_workflow.R` in RStudio and click **Source**. Choose where to save the results and sign in to Google when prompted. There are no paths to edit or files to download by hand.

The full validation passed on October 26 at 9:46 p.m. Pacific, including the additional GEMS chemistry and the agreed Canada Si conversion. The live keys and lab notes now match the copies used for that run. Thirteen cells were updated; the Site Reference Table already matched and was left alone. The code still needs final review and publication before Kathi Jo can pull it from GitHub. The older `_SAB` files on Drive are from the earlier version, which filled only Si gaps.

The GEMS additions include 65,510 non-Si daily measurements. Of those, 106 daily averages exclude below-limit samples and 15 exclude above-limit samples; their flags say so. Estimated-value flags also survive daily averaging. Another 249 additions use the shallowest sample depth and carry that method flag. These flags apply only to the affected results, not every row in the network.

The updated chemistry package needs nine R files and this README:

```text
run_full_chemistry_workflow.R
00-harmonize_chemistry_sidney.R
chemistry_functions.R
chemistry_rules.R
README.md
chemistry-inputs/
  prepare_chemistry.R
  prepare_eccc_gems_danube.R
  prepare_other_sources.R
  prepare_usgs_legacy.R
  prepare_qualifier_inputs.R
```

Only the runner needs to be opened and run manually. `prepare_chemistry.R` shows the preparation order and combines the prepared files. `prepare_eccc_gems_danube.R` builds the combined ECCC, GEMS, and Danube files from the original downloads. `prepare_other_sources.R` handles the other datasets' unit corrections, dates, names, and method choices. It loads `prepare_usgs_legacy.R` for the USGS TN/TDN/TDS repairs and corrections to the older combined master. The existing repair order is unchanged; neither file needs to be run separately. `prepare_qualifier_inputs.R` preserves reported limit signs and thresholds. Each source has a named section you can find through RStudio's section menu. Kathi Jo's original script and the optional `review-only` copy are for reference; neither is needed for this run.

The split passed full background validation on October 26 at 11:11 p.m. Pacific. All 77 prepared files, the final master CSV, and the metadata CSV are byte-for-byte identical to the successful run before the split. The master still has 4,686,420 measurements at 1,170 sites. All 236 raw chemistry files and the other inputs stayed unchanged. Uploading was off during validation.

The script installs missing R packages, downloads the shared inputs and live keys, checks the setup, and runs master chemistry. Each run creates a new dated folder in the location you choose. Existing local files are left alone. No Box download is needed. After the checks pass, the finished master and its notes file are saved locally and uploaded to the [shared master-data folder](https://drive.google.com/drive/folders/1dTENIB5W2ClgW0z-8NbjqARiaGO2_A7W), the same destination used by Kathi Jo's original script. The inputs are not uploaded or changed.

#### What passed

The validated master has 4,686,420 measurements at 1,170 sites, including 296,848 Si measurements. It keeps 58,820 `<` signs, 43 `>` signs, all 121 repaired daily notes, three estimated-value flags, and 249 sample-depth flags from the GEMS additions.

The agreed Canada conversion changes 4,215 final Si values by a factor of 2.139307 compared with the earlier SiO2 assumption. Those rows keep the filtered ICP-OES flag. Every other chemistry row is unchanged from the preceding GEMS run, and the other 76 prepared files are byte-for-byte identical. Date notes, method notes, row flags, and local keys passed their checks. All 236 raw chemistry files stayed unchanged. Uploading was off during validation; Kathi Jo's independent run and comparison are still needed.

Earlier checks on October 22–25 confirmed that the readability changes, nine-file layout, and move to a 14-column master with separate notes did not change the chemistry results. The October 26 checks also confirmed the Seine Si selection and GEMS additions. The comparison treats blank CSV units and limit signs as equivalent to missing values in R; it does not change the saved measurements.

#### Source choices and flags

The Seine Si check matches the saved Hub'eau download by station, date, and rounded value. It excludes 652 source rows confirmed as unfiltered, removing 482 daily Si values. Older results without a confirmed sample fraction stay with `Si filtration not confirmed` in `Method_Flag`. A match to both filtered and unfiltered records gets the same flag. Of the 8,203 retained daily Si values, 7,273 have that flag and 930 have a confirmed fraction. The raw file is unchanged.

In the lab workbook's `Workflow rules` tab, `Apply_To` says whether a rule describes an existing sample flag (`source flag only`) or adds a flag to a network over specified dates (`network and dates`). Naming a network in a source-only rule does not flag all its results. Only the Hubbard Brook Si, HYBAM Si, and Guadeloupe conductivity rules add flags across their listed networks and dates. Each final method flag is checked against its allowed variables, network, and dates.

Method labels, explanations, allowed variables, and expected counts now come directly from that tab. The code no longer rewrites the Canada or GEMS notes, adds a missing reactive-Si rule, or changes the sample-depth rule after reading the workbook. Missing required rules produce an error asking for a refreshed workbook. The full background validation passed on October 26: all 77 prepared files, the master CSV, and the metadata CSV were byte-for-byte identical to the successful split run. All input files stayed unchanged, and uploading was off. The 25 rules in the live shared sheet also matched the validated workbook.

The preparation scripts handle the USGS TDS/TN repair, reported limit signs, chemical forms and units, alternate site names, date corrections and inferences, and field-versus-lab choices. Downloaded chemistry files stay unchanged. Reported outliers remain for the later standardized review.

Daily averages are calculated after converting units and checking dates. When a day has detected and below-limit results, only the detected values are averaged; `Lab_Flag` says the below-limit samples were left out. If all results are below the limit, the average reported threshold is kept with `<`. A result reported with `>` keeps that sign and is flagged as above the reported limit, not above an MDL. If the same source has both `<` and `>` for one site, date, variable, and unit, the run stops with an error so those results can be reviewed.

GEMS defines `~` as an estimated value. Preparation retains the numeric measurement and sets `Lab_Flag` to `estimated value`; it does not treat the estimate as a detection threshold. If an estimated measurement contributes to a daily mean, the estimate flag is retained alongside any note about excluded below-limit samples.

#### Output files

The master has 14 columns: source and site names, date, variable, unit, value, limit sign, and flags. `Site_Flag` repeats on every row for that site. `Lab_Flag`, `Method_Flag`, and `Date_Flag` describe the samples used for that daily result. `Sample_Flag` combines those sample-level flags in one field.

Long explanations are saved once in `YYYYMMDD_masterdata_chem_metadata.csv` under `chemistry-run/master/metadata`. It opens in Excel or R. `Note_Type` tells you whether a row contains a date note or method note. Fields that do not apply are blank.

To add those notes back to the master in R:

- For date notes, keep rows where `Note_Type == "date"` and match on `Dataset`, `Raw_Filename`, `LTER`, `Stream_Name`, `date`, and `Date_Flag`
- For method notes, keep rows where `Note_Type == "method"` and match the full `Method_Flag` text, including combined flags
- Keep only the matching columns and `Date_Note` or `Method_Note` for each join, and read `date` the same way in both files

These matches restore the explanations without adding chemistry rows or requiring new site or sample IDs.

Both outputs are CSVs. Uploading was tested with the earlier saved `_SAB` files, and their names, sizes, folder locations, and checksums were verified. Those older files do not contain the final GEMS additions or Canada conversion. The final validated master has not been uploaded.

Canada VMV 109113 uses the agreed Si basis because the original method metadata identify silicon. The workflow converts mg Si/L to uM using silicon's atomic mass instead of the earlier SiO2 assumption. These results keep the `soluble reactive Si, filtered ICP-OES` flag. GEMS records labeled SiO2-Rea still use the SiO2 conversion and keep their reactive-Si flag. The live keys and lab notes match the validated copies.

#### Questions still open

Sakleshpur (GEMS station IND02168) has two published locations about 1.34 km apart. The stored point matches an older CWC register; GEMS and newer CWC records use the other point. We left the coordinates unchanged and flagged the difference in the Site Reference Table. Six other Indian coordinate differences also match the older CWC register and are documented there.

We have not confirmed every historical lab method or limit against its original source. The lab workbook marks 37 older limit-summary rows as needing more checks: three obsolete Canada entries (`anc_ueql`, `NHX`, and suspended solids), 12 other Canada entries, and 22 entries for other networks. They are for reference only and do not assign limits to master-chemistry rows. A published method or limit does not establish which limit applied to each sample.

East River DOC is confirmed as NPOC measured after field filtration through 0.45 micrometer Millipore filters in both the [2020 ESS-DIVE methods](https://data.ess-dive.lbl.gov/catalog/d1/mn/v2/object/ess-dive-3e2f8b427ee0700-20201008T064242876) and the [current methods](https://data.ess-dive.lbl.gov/catalog/d1/mn/v2/object/ess-dive-3e2f8b427ee0700-20260902T141747295). The `reported as NPOC` flag describes that method. The separate instrument-specific detection limits for East River metals still need checking.

Alpine ammonium still needs clarification. The [current USGS metadata](https://data.usgs.gov/datacatalog/metadata/USGS.6bd9cfc6-3308-4e0c-ab3a-ce6349d91043.xml) define `nh4_mgl` as ammonium-ion concentration, and the [2003-2009 QA report](https://pubs.usgs.gov/of/2011/1137/pdf/OF11-1137.pdf) uses that basis too. Table 9 of the [2010-2019 QA report](https://pubs.usgs.gov/tm/01/d9/tm1d9.pdf) instead labels the detection limit as NH4-N. That does not tell us whether the stored values changed basis. The 3,037 uncertain results remain excluded: 1,509 before 2003, 496 in 2003-2009, and 1,032 from 2010 onward.

The GEMS archive README and [submission guide](https://gemstat.org/wp-content/uploads/downloads/GEMStat_Data_Submissions_Guide_for_Data_Providers.pdf) define `<` as below a detection or quantification limit. Its use on some ordinary Italian physical measurements is still unclear: one example is pH 7.9 with `<`, no defined method, a default `Fair` rating, and no explanation. Those values stay flagged as uncertain rather than being treated as confirmed detection limits. We also still need to confirm whether RMWQMP soluble Si is reported as Si or SiO2. Results without a usable numeric detection threshold are not assigned an invented value.

The GEMS overlap check covers 49 confirmed ECCC sites and five USGS sites. Direct results, including censored results with numeric thresholds, take priority on the same date. Filtered reactive Si from GEMS adds 57 missing Si days: one at Winnipeg River at Pointe du Bois, 25 at Sacramento, 17 at Dismal, and 14 at St. Lawrence. Each addition keeps its source, limit sign, and `reactive Si, filtered (GEMS)` flag. The unconfirmed Missouri result from May 28, 1991 and seven possible new sites are not included.

For the other mapped chemistry variables, GEMS fills gaps only where ECCC or USGS lacks that variable and unit on that date. The additions keep their below-limit, above-limit, estimated-value, and sample-depth flags. Passing validation confirms that these rules work as written; it does not resolve the scientific questions listed above.

#### Shared files

The runner downloads these automatically. The links are here if you want to check an input:

| Input | Shared location | Local filename or folder |
|---|---|---|
| Source chemistry | [Chemistry_Files on Drive](https://drive.google.com/drive/folders/1da8AkSvforPehp-gPJsmA7-pPAJ913mz) | `Chemistry_Files` |
| Data Key | [SiSyn_Data_Key](https://docs.google.com/spreadsheets/d/1gKe7zdMtFHeBZJPfzu5l4jjtvP0AFsfrdCreB7OZvkU/edit) | `keys/SiSyn_Data_Key.csv` |
| Units Key | [SiSyn_Chem_Units_Key](https://docs.google.com/spreadsheets/d/15KDcbCAs4ZzZWoPN2g4MpgOMnYQFaxtAuZnQzwAAONI/edit) | `keys/SiSyn_Chem_Units_Key.csv` |
| Site reference | [Site_Reference_Table](https://docs.google.com/spreadsheets/d/11t9YYTzN_T12VAQhHuY5TpVjGS50ymNmKznJK4rKTIU/edit) | `keys/Site_Reference_Table.csv` |
| Lab and method metadata | [Lab metadata workbook](https://docs.google.com/spreadsheets/d/1PshqCYF2hAR9WhpgN65KLBm0qiqcBmihwkGdSesxI3Q/edit) | `evidence/Chemistry_detection_limits_and_methods.xlsx` |
| Original source downloads and supporting records | [Chemistry_workflow_support_20261022.zip](https://drive.google.com/file/d/1jtWKpxjj6iI9KvQZRWCo6EUZBf8ObU1o/view) inside `Chemistry_Files` | `providers` and `evidence` subfolders after unpacking |

The supporting ZIP contains data only. Keep the scripts in this repository, including the `chemistry-inputs` folder; do not copy just the runner on its own.

The setup check confirms that the required scripts, keys, original downloads, supporting files, R packages, and `unzip` command are available. It checks source file counts and versions but does not create chemistry output. The full run then checks the chemistry results and metadata.

The current source release contains exactly 236 CSVs: 76 main files and 160 GEMS site files. The fixed counts are intentional for this run. The generated `Canada_ECCC_AllSites_chemistry.csv` is not a raw input; preparation builds it locally.

Chemistry starts automatically after the setup check passes. The workflow creates 77 temporary working inputs under `chemistry-run/run-inputs` in the new dated folder, writes one master chemistry CSV under `chemistry-run/master/tidy`, and saves its notes under `chemistry-run/master/metadata`. Keep RStudio open and the computer awake until the run finishes. Downloads and uploads require an internet connection.

If a script fails, the error shows its full log location under `chemistry-run/logs`. Successful temporary logs are removed. Upload starts only after the chemistry run and input checks pass, with the notes uploaded before the master. If either filename already exists on Drive, neither upload starts. Existing Drive files are not overwritten. Uploads are separate operations; if the master upload fails after the notes upload, the notes may already be on Drive. Both local files remain available.

For a background run, supply the five folder paths in this order: chemistry files, original source downloads, keys, supporting records, and a new output folder. That option skips the folder picker, downloads, and uploads. Calls to `run_chemistry()` also leave uploading off unless `upload = TRUE` is supplied. Clicking **Source** in RStudio turns uploading on automatically.

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
