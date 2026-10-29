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

- `chemistry_functions.R`: shared functions for reading selected columns, parsing limit signs, averaging daily results, adding flags and notes, and checking inputs and outputs; it is loaded automatically

- `chemistry_rules.R`: the reviewed name matches, conversion factors, overlap choices, inferred dates, exact exclusions, and expected counts used by the code; lab-method labels and explanations come from the shared lab workbook instead

#### Running the Updated Chemistry Workflow

Keep the files together in the downloaded or cloned repository folder. Open `run_full_chemistry_workflow.R` in RStudio and click **Source**. Choose where to save the results and sign in to Google when prompted. There are no paths to edit or files to download by hand.

The full chemistry validation passed on October 26, including the additional GEMS chemistry and the agreed Canada Si conversion. The shared keys and lab rules matched the validated copies. That chemistry code was committed and pushed on October 27 as `4c15c54`, `Finalize master chemistry workflow, source checks, and metadata`. The October 29 local rerun and comparison also passed with the half-limit averaging rule described below. The new outputs remain local. The older `_SAB` chemistry files on Drive are from the earlier version, which filled only Si gaps; they are not the final validated master.

The GEMS additions include 65,510 non-Si daily measurements. The earlier averaging rule excluded below-limit samples from 106 mixed daily averages; those averages now include the samples at half their reported limits. Above-limit samples remain excluded from 15 mixed daily averages, as recorded in their flags. Estimated-value flags also survive daily averaging. Another 249 additions use the shallowest sample depth and carry that method flag. These flags apply only to the affected results, not every row in the network.

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

Only the runner needs to be opened and run manually. The other scripts are called automatically:

- `run_full_chemistry_workflow.R` downloads the chemistry files, supporting ZIP, and live keys; checks that the required files and packages are present; runs preparation and master chemistry; and uploads the two finished CSVs after the checks pass. Failed runs keep their logs.
- `00-harmonize_chemistry_sidney.R` reads the 77 prepared inputs using the Data Key, standardizes variables and units, matches sites, calculates daily results, and writes the 14-column master and separate notes CSV.
- `chemistry_functions.R` contains the repeated reading, reshaping, conversion, flag, and checking functions. It also reads the lab workbook's `Workflow rules` tab, checks which variables each flag can describe, and checks that the original inputs have not changed.
- `chemistry_rules.R` keeps the code's reviewed choices together, including source overlaps, chemical forms, date inferences, and expected counts. It does not replace the Data Key, Units Key, Site Reference Table, or lab workbook.
- `chemistry-inputs/prepare_chemistry.R` controls the preparation order, prepares the Amazon files, selects sources where files overlap, adds GEMS results only where the preferred direct source lacks a matching result, and collects the 77 inputs for master chemistry.
- `chemistry-inputs/prepare_eccc_gems_danube.R` reads the original ECCC, GEMS, and Danube downloads. It keeps parameter and method information, applies the checked chemical forms and units, preserves limit signs, and builds their combined chemistry files and local key updates.
- `chemistry-inputs/prepare_other_sources.R` handles the remaining datasets' names, units, sample fractions, date decisions, and field-versus-lab choices. Its named source sections include the Seine fraction checks and the flagged Catalina and Cameroon date inferences.
- `chemistry-inputs/prepare_usgs_legacy.R` is loaded by `prepare_other_sources.R`. It separates the confirmed TDS-as-TN records, rebuilds nitrogen using the original USGS parameter identities, and restores checked TDS values in mg/L without a nitrogen conversion. It also handles the reviewed corrections to the older combined master.
- `chemistry-inputs/prepare_qualifier_inputs.R` recovers limit signs and numeric thresholds from source-specific fields and documented codes. It checks that these changes reach the master parser and leaves unrelated source columns unchanged.

Each source has a named section you can find through RStudio's section menu. Kathi Jo's original script and the optional `review-only` copy are for reference; neither is needed for this run. None of the preparation scripts needs to be run separately.

The split passed full background validation on October 26 at 11:11 p.m. Pacific. All 77 prepared files, the final master CSV, and the metadata CSV are byte-for-byte identical to the successful run before the split. The master still has 4,686,420 measurements at 1,170 sites. All 236 raw chemistry files and the other inputs stayed unchanged. Uploading was off during validation.

The script installs missing R packages, downloads the shared inputs and live keys, checks the setup, and runs master chemistry. Each run creates a new dated folder in the location you choose. Existing local files are left alone. No Box download is needed. After the checks pass, the finished master and its notes file are saved locally and uploaded to the [shared master-data folder](https://drive.google.com/drive/folders/1dTENIB5W2ClgW0z-8NbjqARiaGO2_A7W), the same destination used by Kathi Jo's original script. The inputs are not uploaded or changed.

#### What passed

The validated master has 4,686,420 measurements at 1,170 sites, including 296,848 Si measurements. It keeps 58,820 `<` signs, 43 `>` signs, all 121 repaired daily notes, three estimated-value flags, and 249 sample-depth flags from the GEMS additions.

The agreed Canada conversion changes 4,215 final Si values by a factor of 2.139307 compared with the earlier SiO2 assumption. Those rows keep the filtered ICP-OES flag. Every other chemistry row is unchanged from the preceding GEMS run, and the other 76 prepared files are byte-for-byte identical. Date notes, method notes, row flags, and local keys passed their checks. All 236 raw chemistry files stayed unchanged. Uploading was off during validation; Kathi Jo's independent run and comparison are still needed.

Earlier checks on October 22–25 confirmed that the readability changes, nine-file layout, and move to a 14-column master with separate notes did not change the chemistry results. The October 26 checks also confirmed the Seine Si selection and GEMS additions. The comparison treats blank CSV units and limit signs as equivalent to missing values in R; it does not change the saved measurements.

#### Source choices and flags

The Seine Si check matches the saved Hub'eau download by station, date, and rounded value. It excludes 652 source rows confirmed as unfiltered, removing 482 daily Si values. Older results without a confirmed sample fraction stay with `Si filtration not confirmed` in `Method_Flag`. A match to both filtered and unfiltered records gets the same flag. Of the 8,203 retained daily Si values, 7,273 have that flag and 930 have a confirmed fraction. The raw file is unchanged.

In the lab workbook's `Workflow rules` tab, `Apply_To` says whether a rule describes an existing sample flag (`source flag only`) or adds a flag to a network over specified dates (`network and dates`). Naming a network in a source-only rule does not flag all its results. Only the Hubbard Brook Si, HYBAM Si, and Guadeloupe conductivity rules add flags across their listed networks and dates. Each final method flag is checked against its allowed variables, network, and dates.

Method labels, explanations, allowed variables, and expected counts now come directly from that tab. The code no longer rewrites the Canada or GEMS notes, adds a missing reactive-Si rule, or changes the sample-depth rule after reading the workbook. Missing required rules produce an error asking for a refreshed workbook. The full background validation passed on October 26: all 77 prepared files, the master CSV, and the metadata CSV were byte-for-byte identical to the successful split run. All input files stayed unchanged, and uploading was off. The 25 rules in the shared sheet matched the workbook used in that run. The October 28 focused checks passed after removing the resolved Murray-Darling reporting-basis flag; the current lookup has 24 rules. The October 29 local run rebuilt the master and metadata using that lookup without replacing the Drive exports.

The preparation scripts handle the USGS TDS/TN repair, reported limit signs, chemical forms and units, alternate site names, date corrections and inferences, and field-versus-lab choices. Downloaded chemistry files stay unchanged. Reported outliers remain for the later standardized review.

Daily averages include confirmed below-limit samples at half their reported threshold: `<2` contributes `1`. On affected days, each retained sample has equal weight, including days when every sample is below its limit. Days without below-limit samples keep the previous grouping and daily averages. `Lab_Flag` records, for example, `half reported limit used for 2 of 5 observations in daily average`. The total counts observations used for that solute at that site on that day, after source, method, and depth selection; missing values and excluded results do not count. The calculated estimate does not keep `<`, because it is no longer the original threshold. Earlier preparation steps apply the same rule and carry both counts forward without halving the result again. The raw files keep their original values and signs. Uncertain Italian GEMS physical-variable flags are not interpreted as confirmed limits and are not halved.

Greater-than handling is unchanged: detected values take priority on mixed days, and all-greater-than days retain the average threshold with `>`. A group containing both confirmed `<` and `>` results stops for review. Focused regression tests and full local validation passed on October 29. The master has 4,686,420 measurements at 1,170 sites. Half-limit flags cover 59,918 daily values, with 62,211 substituted observations out of 63,782 observations on those days. Independent calculations confirmed the averages and both counts; unaffected measurements stayed unchanged. All 77 prepared files were present, with only the 10 expected files changed. All 314 input files, including 236 raw chemistry files, stayed unchanged. Uploading was disabled, and WRTDS files stayed unchanged.

The metadata comparison also passed after review of 11 shortened method notes. They preserve the conversion bases, filtration qualifications, source priorities, and site/date rules, and match the saved lab workbook. Date notes stayed unchanged. The resolved Murray-Darling method flag was removed from 1,752 results and its metadata note was removed. The new metadata has 210 rows.

GEMS defines `~` as an estimated value. Preparation retains the numeric measurement and sets `Lab_Flag` to `estimated value`; it does not treat the estimate as a detection threshold. If an estimated measurement contributes to a daily mean, the estimate flag is retained alongside any half-limit substitution count.

#### Undated September chemistry

Checked the 57 rows without dates in the September 25 export against the original sample records and October 29 chemistry. Of these, 52 were recovered with dates, two conductivity entries were replaced by readings corrected to 25 °C, and three Rio Negro entries were excluded because their sampling date is missing. October 29 has no rows without dates.

| Source | September rows | Outcome in October 29 |
| --- | ---: | --- |
| Cameroon: Mbalmayo, Olama, Pont So'o | 23 | Retained on February 27, 2006; confirmed against the original provider workbooks |
| Catalina–Jemez: MGWEIR | 10 | Date inferred as April 19, 2018 from paired sample AZ_S1792 and the sampling notes; all ten measurements from AZ_S1793 have an inferred-date flag |
| Guadeloupe: Petit Bras-David | 10 | Nine measurements retained on September 28, 2007; uncorrected conductivity replaced by the 25 °C reading, from 67.8 to 69.2 uS/cm |
| Guadeloupe: Vieux-Habitants, Savanne-Beauséjour | 11 | Ten averages traced to individual dated measurements from 2006–2010; the conductivity average replaced by corrected readings on the original dates |
| Amazon: Rio Negro | 3 | CAMREX record V1=144 has no Sampling.Date; its alkalinity, total organic carbon, and dissolved organic carbon entries remain excluded |

The Vieux-Habitants rows combine measurements from ten dates between May 6, 2006 and October 5, 2010. The original dates are present, but the sampling hours are blank. The September potassium and sulfate averages match when repeated identical values are counted once. October keeps the measurements on their individual dates.

The 57 September rows represent 136 original measurements: 128 kept with dates, five uncorrected conductivity measurements replaced, and three excluded because the date is missing. Dates were checked using sample records, not concentration matches alone. Raw files, chemistry outputs, and Drive files were not changed.

#### Output files

The master has 14 columns: source and site names, date, variable, unit, value, limit sign, and flags. `Site_Flag` repeats on every row for that site. `Lab_Flag`, `Method_Flag`, and `Date_Flag` describe the samples used for that daily result. `Sample_Flag` combines those sample-level flags in one field.

Long explanations are saved once in `YYYYMMDD_masterdata_chem_metadata.csv` under `chemistry-run/master/metadata`. It opens in Excel or R. `Note_Type` tells you whether a row contains a date note or method note. Fields that do not apply are blank.

To add those notes back to the master in R:

- For date notes, keep rows where `Note_Type == "date"` and match on `Dataset`, `Raw_Filename`, `LTER`, `Stream_Name`, `date`, and `Date_Flag`
- For method notes, keep rows where `Note_Type == "method"` and match the full `Method_Flag` text, including combined flags
- Keep only the matching columns and `Date_Note` or `Method_Note` for each join, and read `date` the same way in both files

These matches restore the explanations without adding chemistry rows or requiring new site or sample IDs.

Both outputs are CSVs. Uploading was tested with the earlier saved `_SAB` files, and their names, sizes, folder locations, and checksums were verified. Those older files do not contain the final GEMS additions or Canada conversion. The final validated master has not been uploaded.

Canada VMV 109113 uses the agreed Si basis because the original method metadata identify silicon. The workflow converts mg Si/L to uM using silicon's atomic mass instead of the earlier SiO2 assumption. These results keep the `soluble reactive Si, filtered ICP-OES` flag. GEMS records labeled SiO2-Rea still use the SiO2 conversion and keep their reactive-Si flag. The chemistry mappings and lab rules matched the validated copies. Later discharge and spatial reference updates are described below.

ColoradoAlpine ammonium is reported as NH4, not NH4-N. The Units Key uses mg NH4/L and the workflow converts it using ammonium's molecular mass for all years. The 3,037 source results already contribute 2,195 daily NH4 values at the retained river sites in the validated master; the earlier note saying they were excluded was incorrect. No duplicate records need to be added.

Murray-Darling silica uses the confirmed SiO2 mass basis. Its existing conversion stays unchanged. The reporting-basis uncertainty flag and note are removed, including the RMWQMP rule for the 1,752 Silica - Soluble records. Other method, lab, date, and site flags are retained. Generic source Si records without an identified sample fraction are still excluded; confirming the mass basis does not establish that those samples were dissolved.

The October 28 focused checks matched all 2,195 retained Alpine daily NH4 values and all 1,752 RMWQMP Si values to the saved master. Live keys and lab notes were read back and verified, with unrelated cells unchanged. The active preparation code and its commented review copy agree. No chemistry was rerun during that check. The October 29 full local run and comparison subsequently passed with the same measurement and site totals and the RMWQMP flag removed. Existing Drive exports were not replaced and retain the earlier flags.

#### Questions still open

Sakleshpur (GEMS station IND02168) has two published locations about 1.34 km apart. The stored point matches an older CWC register; GEMS and newer CWC records use the other point. We left the coordinates unchanged and flagged the difference in the Site Reference Table. Six other Indian coordinate differences also match the older CWC register and are documented there.

We have not confirmed every historical lab method or limit against its original source. The lab workbook marks 37 older limit-summary rows as needing more checks: three obsolete Canada entries (`anc_ueql`, `NHX`, and suspended solids), 12 other Canada entries, and 22 entries for other networks. They are for reference only and do not assign limits to master-chemistry rows. A published method or limit does not establish which limit applied to each sample.

East River DOC is confirmed as NPOC measured after field filtration through 0.45 micrometer Millipore filters in both the [2020 ESS-DIVE methods](https://data.ess-dive.lbl.gov/catalog/d1/mn/v2/object/ess-dive-3e2f8b427ee0700-20201008T064242876) and the [current methods](https://data.ess-dive.lbl.gov/catalog/d1/mn/v2/object/ess-dive-3e2f8b427ee0700-20260902T141747295). The `reported as NPOC` flag describes that method. The separate instrument-specific detection limits for East River metals still need checking.

The GEMS archive README and [submission guide](https://gemstat.org/wp-content/uploads/downloads/GEMStat_Data_Submissions_Guide_for_Data_Providers.pdf) define `<` as below a detection or quantification limit. The October 28 check of the original `GFQA_v3.zip` confirmed 2,976 such flags on physical measurements at 48 Italian stations: 742 pH, 744 temperature, 742 conductivity, 744 oxygen saturation, and four dissolved-oxygen results. Examples include pH 8.0 and conductivity 441 uS/cm at ITA00205 on January 23, 2018. Every affected record uses method code 0 and has no remark explaining the flag. These values remain unchanged and flagged as uncertain; they are not halved. The records came through the European Environment Agency, but the archive does not establish why these flags were applied. This still needs clarification from EEA or GEMStat.

The Italian dissolved-silica flags are different: all 33 flagged SiO2-Dis records at 17 stations report `<1 mg SiO2/L`. These are consistent with a reported threshold, so the new rule uses 0.5 mg SiO2/L before conversion and counts the substituted observations in the daily flag. Method code 0 does not establish whether this is an MDL or a quantification limit, so the flag says `reported limit`. Results without a usable numeric threshold are not assigned an invented value.

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

- `00-harmonize_discharge.R`: combines individual discharge files into daily values and matches them to sites using the reference table
    - _When to Use:_ you want to integrate new _discharge_ data into the 'master' file

- `discharge_functions.R`: loaded by the discharge script; checks site-to-file matches against the finalized chemistry sites, reads dates and fractional-second timestamps, removes repeated copies of the same reading, converts flow units, and adds `Q_Flag`. It also contains the original-source corrections and saved USGS additions described below. `attach_discharge_sites()` keeps shared gauges assigned to each matching chemistry site. `join_chemistry_discharge()` joins by site and date without dropping chemistry rows, then calls `apply_walk_discharge_rule()` for the solute-specific Walker Branch check. These functions do not run automatically just because the file is opened.

The discharge preparation uses `discharge_functions.R` before joining files to chemistry sites. Choose the finalized master chemistry CSV at the start, or enter its path in `chemistry_file`. That file determines which sites and discharge files are required. The current 1,170 chemistry sites require 776 available discharge files. Five absent East River filenames belong to sites outside this chemistry release, so they do not block it. Shared gauges remain assigned to every matching chemistry site. Missing required files stop the run instead of silently dropping sites.

Blank times are allowed for daily records. Rows with neither a date nor a value are skipped. A missing result beside a measured result is not treated as a conflict. Distinct timed readings are averaged without removing equal flows at different times. Identical input copies are removed and recorded in `Q_Flag`. Other conflicting readings still stop the run. The four saved PIE, NE14 Outlet, and Andrews Creek inputs already contain `Qcms` in m³/s. Missing local unit entries are filled accordingly, and a conflicting unit label stops the run rather than converting these values twice. Live sheets are not changed by this step.

The original-source checks run during preparation, without overwriting the downloaded inputs:

- Shingle Creek's saved `Q` column contains water temperatures alongside discharge. Preparation reads the [dated original USGS download on Drive](https://drive.google.com/file/d/1n0JHBLq1Oi6zH3CPa3UxlWjMY542y4KN/view), `discharge-inputs/20261027_USGS_05288705_daily_discharge.json`. The runner downloads this fixed file only if the local copy is missing. It does not request a new USGS series each run. The JSON keeps the original source information and estimated and provisional flags. The reader checks station 05288705, [discharge parameter 00060 and daily-mean statistic 00003](https://nwis.waterservices.usgs.gov/docs/dv-service/daily-values-service-details/). It never averages temperature with flow. Set `shingle_source_file` to the saved download if it is stored elsewhere. The file is data, excluded from Git. The older mixed CSV is unchanged. The uploaded source file was downloaded and checked against the local copy on October 27; their bytes match exactly.
- LUQ Q1, Q2, and Q3 each contain 153 duplicated dates from January 1 through June 2, 2006. The [archived Bisley daily data and header](https://www.hydroshare.org/resource/8fb0c17c23da423b8240396342bde0eb/) identify the original daily record. Preparation keeps that record and removes the extra row. The runoff-derived values from January 3, 1987 through June 2, 2006 use the historical conversion areas: Q1 0.067, Q2 0.0634, and Q3 0.35 km². The existing conversion used 0.07, 0.07, and 0.28 km². Correcting that conversion preserves the saved rounding and adds a flag; it does not read the spatial area from `drainSqKm`. Each site has 7,091 historical daily records, including 22 verified interpolated gaps. All later values from June 9, 2006 onward are unchanged. Six transition estimates each for Q2 and Q3 retain their existing interpolation weights with the corrected runoff endpoint. Q1's transition estimates are unchanged because its endpoint is zero. The [later Forest Service release](https://doi.org/10.2737/RDS-2022-0074) documents the cfs records beginning in June 2006; these records do not receive an area conversion.
- Krycklan Site 7 matches Kallkälsbäcken C7 at 64.251301, 19.7779. Its saved file has lost the hourly times. Preparation uses the station's [fixed published daily series](https://meta.fieldsites.se/objects/adVF13uWj9IlUHOhej0T_EFk), reported in m³/s and local UTC+1 dates, for dates already in the saved file. It does not invent hourly timestamps. Where that series has no value, legacy daily values are retained and flagged. Interpolated legacy values keep an interpolation flag. The published daily series differs from the older input, so this changes values as well as resolving timing. SITES data are licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

The historical LUQ areas also match Table 1 of [Schaefer et al. (2000)](https://research.fs.usda.gov/treesearch/download/30121.pdf): 6.7, 6.34, and 35 hectares. The live Site Reference Table now records those conversion areas and original discharge sources in `CQ_Notes` and `q_source_URL`. Its spatial areas remain 0.07, 0.07, and 0.28 km². Their citation now points to [Wymore et al. (2017), Table 1](https://doi.org/10.1002/2016WR020016), rather than the mapped boundaries. `Spatial_Notes` separately describes the mapped areas. Twelve cells were updated and read back; all other cells stayed unchanged.

All 776 required discharge files now pass the saved-data checks. The earlier failures, including the seven PIE unit conflicts, have been resolved against the original sources. The LUQ checks confirmed the historical conversion areas, unchanged later discharge, and separation from spatial drainage areas. Output uploading remains off in the discharge script, and the older source-file editing examples at its end are disabled. These discharge changes remain local and have not been committed or pushed.

The seven PIE files have now been compared with the original sources. All already contain m³/s: five USGS files match 731 checked daily values each after multiplying cfs by 0.028316846592. Cart Creek matches 168 daily means from [PIE 111.5](https://portal.edirepository.org/nis/mapbrowse?packageid=knb-lter-pie.111.5), using `Discharge` in L/s divided by 1,000, not the older rating-curve column. All 122 YSI-Ipswich dates match daily `Flow` means from [PIE 114.5](https://portal.edirepository.org/nis/mapbrowse?packageid=knb-lter-pie.114.5), also divided by 1,000. The local reference copy uses `cms` for these seven files so they are not converted twice. Other unexpected unit conflicts still stop the run.

The source checks also identified three wrong file assignments in the reference table. Preparation assigns `PIE_Parker_Q` to Parker River at Byfield, `PIE_IpswichMid_Q` to Ipswich at Middleton, and `PIE_YSIIR_Q` to Ipswich at North Reading. Sawmill Brook and IR-97 no longer inherit discharge from those different stations. Site names, chemistry, spatial areas, and raw files stay unchanged. The live reference table was updated and read back on October 27: 61 cells across nine PIE sites now contain the checked assignments, saved-file units, gauge details, distances, and source notes. All other cells were unchanged. The live file assignments and units match the validated preparation.

The complete saved-data joins passed again after the USGS and Walker additions and the four-site ERA5 update. They preserve all 4,686,420 chemistry measurements across 1,170 sites without multiplying rows or changing chemistry values or flags. All 130 missing-spatial notes are retained. The current discharge totals are given below; spatial coverage and the successful sync are described in the spatial section. A final combined dataset has not yet been exported.

The calculated `WALK` chemistry needs a row-level discharge check after the chemistry and discharge tables are joined. `apply_walk_discharge_rule()` uses the existing Data Key, prepared Walker chemistry input, and original historical daily discharge from [ORNL](https://doi.org/10.3334/CDIAC/ORNLSFA.007). It assigns East Fork plus West Fork daily discharge only when both forks contributed to that solute's concentration and both daily flows are available. `Q_Flag` identifies calculated combined flow and retains any original daily flow code. One-fork or unverified concentrations, and dates missing a required flow, keep discharge missing with an explanatory flag. The function changes only `Qcms` and `Q_Flag` on `WALK` rows; it does not modify chemistry or the separate fork records. This rule belongs in the combined-data join, not the variable-free master discharge export. It has not yet been used to export a combined dataset.

The local `discharge-inputs/20261027_original_discharge_additions.rds` stores the checked original USGS downloads and Walker daily records. `add_verified_discharge()` fills only missing saved discharge for Sacramento River, San Joaquin River, and Maumee River, preserving existing usable flows and their flags. The original-source check found 18,042 additional chemistry measurements with matching discharge. Added values use the existing cfs conversion and carry source and estimated/provisional flags where reported. The discharge preparation calls this helper before combining files.

The same checked [original-source input bundle](https://drive.google.com/file/d/1kYR7-tWCEPO2tKj5Mcl2V1BtX2JytkUv/view) is saved with the shared discharge inputs. Its downloaded bytes were verified against the local file. The discharge workflow downloads this saved copy only when the local copy is missing; it does not retrieve a changing replacement from the original websites. The live reference table includes source notes for these three USGS sites, the calculated WALK rule, and Ferguson's corrected boundary. No scripts or combined outputs were uploaded.

`join_chemistry_discharge()` joins the finalized chemistry to prepared daily discharge without changing chemistry rows, then applies the `WALK` rule. Pass the prepared Walker chemistry input, the saved discharge additions, and the Data Key from the same chemistry run. The WALK validation assigned calculated combined flow to 18,178 measurements, left 6,505 one-fork or unverified measurements without flow, and left 89 without a required daily flow. The complete saved-data validation passed with both sets of additions: 3,593,358 chemistry measurements have matching discharge, and 1,093,062 do not. All 4,686,420 chemistry measurements across 1,170 sites remain unchanged, including their existing flags. No combined output has been uploaded.

Quiock has 19 records on April 26, 2020 with seconds recorded as 58.39. Preparation preserves those fractional seconds instead of truncating them to 58. Decimal seconds in separate `Time` fields and combined date/time fields are also retained. The earlier targeted validation passed, including 23,374 exact matches to the original Theia timestamps after dividing the saved values by 3,600. The source files stayed unchanged.

Quiock's saved export contains values in m3/hour despite its m3/s column heading. On October 27, 2026, 515 exact UTC timestamps from beginning, middle, and end samples of the [saved export](https://drive.google.com/file/d/10W_hM0RO0ykT7epCak8SgaoZy7wD9btk/view) were compared with the [original Theia observations](https://in-situ.theia-land.fr/api-data/visualize/timeserie/OBSE/OBSE_DAT_HYDRO_GUADELOUPE.GHYBDQCK/OBSE_7fde6bf4-c131-381a-bb74-21f7a7b82fe0). All 253 matches in the 2015 sample, six in 2018, and 256 in 2025 agreed after dividing the saved values by 3,600, within the portal's six-decimal rounding. For example, at 2015-01-14 11:19 UTC, the saved 76.786388 becomes 0.021329552 m3/s, matching the portal's 0.021330. This was a sample comparison, not a check of every record. The reference unit stays `cmh`, the division by 3,600 stays in preparation, and the temporary Quiock stop has been removed. No raw values or shared-sheet entries were changed.

### Spatial data and the combined dataset

#### Current coverage and shared files

The spatial site list comes from finalized master chemistry, not a separate station list. The October 28 saved-data validation passed with 1,170 sites. ERA5-Land values are present at 1,040 sites; 130 sites still lack spatial data and retain their missing-data notes. Those sites stay in the dataset rather than being dropped.

PIE sites `12`, `15`, and `20`, and Western Australia's `Ferguson River - Dowdells Rd Bridge`, now each have all six annual ERA5-Land variables for 2000-2025. All 26 Earth Engine tasks completed, and all 104 site-year records and 624 values were checked against the merged data. The six variables are precipitation, temperature, evapotranspiration, potential evaporation, snow-cover fraction, and snow-water equivalent. The other 1,166 sites were unchanged by this merge.

The current files were synced and verified on October 28:

- [Spatial predictors CSV](https://drive.google.com/file/d/17Dbv-jV2E_aDg311Xqrm6DoyKSITL5ZA/view): `spatial_predictors_glass_climate_era5_land_modis_glc_20261028.csv`
- [Site metadata CSV](https://drive.google.com/file/d/1bavvkMZIrEkFBLWXlRujBXVAbBHBWYvb/view): `site_metadata_20261028.csv`

Matching copies are in `SiSyn/spatial-data-extractions/spatial-data` on Box. Both Drive downloads matched the local files by checksum. The Box copies matched too, and the Box sync client confirmed both uploads with no conflicts. The existing Drive file links were kept. Older dated releases were left untouched.

The predictor CSV keeps the site names, coordinates, drainage area, site flag, and predictor values. The metadata CSV contains source links, watershed details, discharge information, coverage, and longer notes. Human impacts and distance to coast are not included in this release. The land-cover columns cover 1985-2022, not 1900 onward.

#### Ferguson's boundary and remaining predictors

Ferguson now uses 25 connected upstream catchments from [BoM Geofabric](https://hosting.wsapi.cloud.bom.gov.au/arcgis/rest/services/ahgf/Geofabric_V3x_All_Products/FeatureServer/7), covering 104.973609 km² above station 611026. The older HydroBASINS polygon covered about 224.7 km². That original file is retained, but its predictors are not reused.

Ferguson's ERA5 results were extracted using the corrected boundary and are included in the shared spatial CSV. The Site Reference Table's watershed source link and extraction note match the metadata on Drive and Box. The source-link correction changed no spatial values or other reference-table cells.

Ferguson still needs the following predictors extracted using the corrected boundary:

- Land cover and long-term climate
- MODIS evapotranspiration, NPP, greenup, and snow
- Elevation, slope, geology, soils, and permafrost

Those fields remain blank with an explanatory note. They must not be filled from the older watershed's results.

#### What the spatial scripts do

These scripts belong to the separate spatial workflow checkout, not the chemistry runner:

- `workflow/build_watershed_collection.R` reads the site table and watershed files. It selects a verified replacement boundary before an older shapefile when one is available, including Ferguson's corrected boundary.
- `workflow/gee/build_gee_vector_payloads.R` prepares the selected watershed polygons for Earth Engine and checks site coverage, file size, and any geometry changes. It does not start extraction tasks.
- `workflow/gee/era5_land/run_safe_era5_land_exports.py` checks the selected sites, years, and products, skips existing results, and submits ERA5 tasks only with an approved quota check. Its default is a check without submitting tasks.
- `workflow/gee/era5_land/finish_missing_era5_land_assets.py` runs the missing exports in checked batches outside RStudio and waits for their results. It does not need an open chat to continue.
- `workflow/gee/era5_land/download_annual_era5_land_assets.R` downloads completed annual tables and checks for duplicate site-year matches, missing or infinite values, required variables, and complete requested coverage.
- `workflow/release/recover_saved_spatial_results.R` aligns saved spatial results to the chemistry sites, recovers reviewed missing results, keeps site flags, and removes predictors from replaced boundaries. `merge_annual_era5_results()` adds the checked annual updates after that removal, so Ferguson's new ERA5 values are retained while its other old predictors stay excluded.
- `workflow/release/build_spatial_dataset_files.R` writes separate product CSVs and one site metadata CSV. Its `--era5-updates` option reads the downloaded annual-results RDS when building the updated files. The RDS is an input to the local workflow; the shared predictor and metadata files are CSVs.

#### What remains before the final combined export

The saved chemistry, daily discharge, and spatial joins have passed. There are 3,593,358 chemistry measurements with matching discharge and 1,093,062 without usable matching flow. Missing discharge or spatial values do not remove chemistry rows.

The final assembly still needs to write the combined dataset and check the exported file. Match discharge by `LTER`, `Stream_Name`, and date; annual predictors by site and year; and static predictors by site. Keep chemistry and discharge flags on each row and longer explanations in metadata. Use year-matched predictor columns rather than repeating every year's columns on every chemistry row. Do not extend ERA5 beyond 2000-2025 or fill Ferguson's remaining predictors with old values.

Chemistry has not been rerun during this work. The updated discharge and spatial code and these documentation edits have not been committed or pushed.

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
