# Chemistry changelog

Earlier notes come from [Biogeochemical Dataset](https://docs.google.com/document/d/1DZT1mv-xkJx-bFy4MkP904MfxCjAjvrNjJXhI-lRfVg/edit). They describe changes made at the time, not necessarily the current workflow. Newest updates are at the bottom of each history section.

## Original master dataset column notes

- `Dataset` - name of research network or LTER
- `Raw_filename.csv` - original file name stored in [Chemistry_Files](https://drive.google.com/drive/folders/1da8AkSvforPehp-gPJsmA7-pPAJ913mz)
- `LTER` - name of research network
- `Stream_Name` - name of site
- `Date` - date of sample
- `Variable` - name of solute
- `Units` - units of concentration of solute
- `Value` - concentration of solute

## Master chemistry updates

Master chemistry datasets created using the updated data wrangling workflow, beginning October 2023.

### 20231030

- First file created by Nick Lyon using the new workflow: `20231030_masterdata_chem`

### 20231109

- Corrected unit conversions for N and P (Keira Johnson)

### 20240102

- Corrected date formats and some NA value formats

### 20240129

- Corrected unit conversions (Kathi Jo Jankowski)
- Updated outlier threshold to 4x standard deviation

### 20240130

- Removed a line that renamed all sites in `MCM_Chem_clean` to McMurdo

### 20240307

- Added USGS geogenic solutes

### 20240520

- Fixed Walker Branch unit issue; corrected Walker Branch N and P units
- Removed data from rivers that had both NOx and NO3: Krycklan streams, Biscuit Brook, Cane Creek, Merced River, Turkey River, James River, Corral Gulch, and Flat Brook
- Made these changes in `wrangle_chemistry.R`; original data files still have both

### 20240621

- Removed NO3 from more rivers that had both NOx and NO3, keeping NOx: Choptank River, Green River, North Slyamore Creek, SOPCHOPPY RIVER, Sopchoppy River, Vallecito Creek, West Clear Creek, and Smith Creek
- Hard-coded this choice in `wrangle_chemistry.R`; original data files still have both
- Removed SRP from sites with duplicate SRP and PO4: all Krycklan sites, MG_WEIR, and OR_low

### 20240624

- Corrected Si units for Australian sites from mg Si/L to mg SiO2/L in the Data Key

### 20241003

- Added other, non-Si solutes from the United Kingdom

### 20250602

- Added geogenic data from Norway and Finland and added the Loch Vale dataset
- Updated column name matching, including `fe_mgl_mg_L`, because of variable names in the Loch Vale dataset
- Updated Finnish names to match for all solutes, rather than using Site XXXX for DSi and Finnish names for other solutes
- Noted that these name changes could affect other processing

### 20250908

- Added geogenic data for Krycklan and Swedish government sites
- Added new data from French CZOs (Izeron and Guadalupe) and the Seine River
- Updated column names, unit conversions, and Data Key entries for several datasets
- Noted additional Finnish sites with only geogenic data that were not in the original dataset
- Noted that Swedish government sites had two LTER names that still needed resolving

### 20251008

- Added Seine River and Swedish geogenic data
- Updated unit conversions; noted unusually high Seine Ca, Mg, Cl, and SO4 values and possible problems with conversions or original units
- Keira checked Finnish geogenic, Swedish geogenic, Yzeron, Guadeloupe, and Seine data; Kathi Jo remade master chemistry
- Merged two Swedish geogenic/cation files and matched names to the existing Sweden names in the reference table
- Changed Swedish Goverment to Sweden in the reference table to address the two LTER names
- Added base geogenic solutes (Ca, Mg, K, and SO4) to the Seine dataset as a separate file
- Noted that the new Seine long-format file used additional solute labels and mg S/L for SO4; these needed adding to the master chemistry workflow

### 20251021

- Added GRO Obidos dataset

### 20251024

- Fixed GRO Obidos dates

### 20251123

- Added Western Australia sites
- Added updated East River SFA sites and Como Creek data
- Removed the old Coal Creek datasets by removing DOE SFA East River and East River SFA from the original master
- Replaced them with Keira's updated East River SFA files, which include more sites and more recent data

### 20260105

- Changed Catalina-Jemez stream names from MG_WEIR and OR_low to MGWEIR and ORlow

### 20260720

- Fixed SOPCHOPPY RIVER name issues; all should use SOPCHOPPY RIVER without duplicated solutes across datasets
- Mainly removed duplicates from the original master chemistry dataset and kept CAMELS data
- See [Dataset Tasks and Notes](https://docs.google.com/document/d/1uwTVn7coWQmRcRKRV7GBqzvTGOAmCT38-TQ20PJU4mc/edit) for the original note
- Removed Boulder Creek CZO sites with codes matching `FDUP|Fdup|DUP|BLANK|Blank|FLBK|FIELDBLANK|Duplicate|FBLK|FB`
- Noted that ARC sites still needed attention

### 20260903

- Addressed duplicated site names across CAMELS, USGS geogenics, and master chemistry; see [Deduplication Records](https://drive.google.com/drive/folders/1AHKQsHHahj-rV1a5FZ1dEv9ENgbOYnca)
- Removed sites absent from the [Site Reference Table](https://docs.google.com/spreadsheets/d/11t9YYTzN_T12VAQhHuY5TpVjGS50ymNmKznJK4rKTIU/edit), following a review of site identity, data coverage, and stream status
- Added a section to remove duplicate rows, approximately 12,000 rows
- Added sites from Switzerland, the Netherlands, Belgium, Italy, Japan, and India using GEMS, Danube, and CAMELS datasets

### 20260922

- Added non-Si solutes from GEMS, which were missing from the previous version
- Updated many Chemistry_Files inputs and changed date handling to detect formats automatically

### 20260925

- Removed BK01.0M from master chemistry

### 20261029

Workflow updates by Sidney Bush since the September notes:

- Added `run_full_chemistry_workflow.R` to run the updated workflow by opening it in RStudio and clicking Source. It downloads the shared inputs and keys, prepares 77 chemistry files, builds master chemistry, and uploads the two CSV outputs only after checks pass
- Kept Kathi Jo's original script unchanged. Added a separate active workflow and a review-only script with Sidney's replacement code commented beside the original sections
- Split preparation into named sections and shared functions. Method labels and notes now come from the shared lab workbook rather than duplicate rules in the code
- Kept downloaded raw files unchanged. Unit conversions, date inferences, flags, and exclusions are applied in the workflow. Reported outliers remain for the later standardized review
- Repaired the legacy USGS TDS/TN mix-up using original USGS parameter identities. Kept legitimate nitrogen measurements and restored TDS in mg/L without the nitrogen conversion. Two dates without a confirmed original-source match remain excluded
- Corrected chemical-form and unit mappings, including nitrate reported as N versus NO3, ammonium, phosphorus, Si versus SiO2, and alkalinity
- Used mg Si/L for Canada's filtered ICP-OES silicon measurements and kept their method flag. Confirmed SiO2 for Murray-Darling soluble silica and NH4 for ColoradoAlpine ammonium; removed the related uncertainty flags
- Separated Seine sediment calcium and isotope measurements from water concentrations. Converted sulfate using its reported S or SO4 basis. Excluded confirmed unfiltered Si; retained older Si with undocumented filtration and flagged it `Si filtration not confirmed`
- Matched alternate site names while keeping the main site names used by Kathi Jo where no error was confirmed. Combined overlapping GEMS, ECCC, and USGS data during preparation, keeping direct ECCC or USGS results when both sources have the same site, date, and variable
- Added 57 filtered reactive-Si dates and 65,510 other daily chemistry results from GEMS where the preferred sources lacked results. Only Si labeled reactive receives the reactive-Si flag; GEMS Si is not all reactive
- Excluded confirmed total or unfiltered Si. Kept the original reporting labels and conversion bases for dissolved Si and dissolved SiO2
- Used field pH and conductivity when field and lab results overlap, retaining lab-only dates with method flags. Used total alkalinity before bicarbonate alkalinity for overlapping Murray-Darling results, flagging bicarbonate-only dates
- Retained Catalina sample AZ_S1793 with April 19, 2018 inferred from paired sample AZ_S1792 and the sampling notes and sequence
- Changed six Cameroon dates from June 16, 2021 to June 16, 2022 within the 2022 sequence: Mbalmayo, Messam, Nsimi outlet, Nsimi spring, Olama, and Pont So'o. Both the Catalina and Cameroon decisions have date-inference flags and separate notes; raw dates are unchanged
- Recovered four Guadeloupe sampling events from original Theia dates. Measurements still lacking a defensible date remain excluded by the workflow. Corrected the CAMREX Julian-day offset without filling undocumented dates
- Preserved reported limit signs and estimated-value information. GEMS `~` values remain numeric results with an estimated-value flag, including when they contribute to a daily average
- Kept the master to 14 columns, including site and sample flags. Moved longer date and method explanations to a separate readable metadata CSV rather than repeating them on every measurement

Latest averaging change:

- Included confirmed below-limit results in daily averages at half the reported threshold: `<2` contributes `1`. Each affected sample has equal weight, including days when every result is below its reported limit
- Added flags such as `half reported limit used for 2 of 5 observations in daily average`. The total counts observations used for that solute at that site on that day, after source, method, and depth selection. Missing values and excluded results do not count. The calculated daily estimate does not keep `<`; raw results retain their original signs and thresholds
- Carried substituted and total observation counts through preparation and daily averaging without halving an already calculated result again. Days without below-limit results keep the previous grouping and averages. Greater-than handling is unchanged
- Did not halve Italian GEMS physical measurements whose `<` flags have an unconfirmed meaning. Their values remain unchanged and flagged. Those flags still need clarification from the original source
- Fixed repeat-sample handling after comparison found changes on days without below-limit results. Focused tests passed for the corrected grouping, half-limit averages, and sample counts

Validation status:

- The October 26 workflow passed full validation with 4,686,420 measurements at 1,170 sites. All 77 prepared files and both output CSVs matched the successful run before the script split; the raw files stayed unchanged
- The October 29 full local rerun and saved-baseline comparison passed with the half-limit averaging rule: 4,686,420 measurements at 1,170 sites. Independent calculations confirmed the daily averages and flags, including 62,211 substituted observations out of 63,782 observations across 59,918 affected daily values. Unaffected measurements stayed unchanged
- All 77 prepared files were present; only the 10 expected files changed. All 314 input files, including the 236 raw chemistry files, stayed unchanged. WRTDS files stayed unchanged, and uploading was disabled
- Reviewed 11 shortened method notes against the earlier metadata and saved lab workbook. Conversion bases, filtration qualifications, source priorities, and site/date rules stayed consistent. Date notes stayed unchanged. Removed the resolved Murray-Darling method flag from 1,752 results and its metadata note; the new metadata has 210 rows
- The October 29 averaging validation compares the new local outputs with the successful October 26 local baseline. The separate saved-export comparison reviewed October 26 `_SAB` against October 28 exports and October 28 against September 25. Drive exports and earlier local outputs were not overwritten

September rows without dates:

- Checked all 57 September 25 rows against the original sample records and October 29 chemistry. Recovered 52 with dates, replaced two conductivity entries with readings corrected to 25 °C, and kept three Rio Negro entries excluded because CAMREX record V1=144 has no sampling date
- Confirmed February 27, 2006 for the 23 Cameroon measurements at Mbalmayo, Olama, and Pont So'o using the provider workbooks. Inferred April 19, 2018 for the ten MGWEIR measurements from sample AZ_S1793 using paired sample AZ_S1792 and the sampling notes; these measurements keep the inferred-date flag
- Confirmed September 28, 2007 for the ten Petit Bras-David entries. The 11 Vieux-Habitants entries combine measurements from ten dates in 2006–2010; the original dates are present, but the sampling hours are blank. The September potassium and sulfate averages match when repeated identical values are counted once. October keeps the measurements on their individual dates
- The 57 export rows represent 136 original measurements: 128 kept with dates, five conductivity measurements replaced, and three excluded. Saved one 57-row CSV with the source records, original values, recovered dates, October values, and reasons for exclusions. Raw files, chemistry outputs, and Drive files were not changed

## Original master datasets created by Paul Julian

- 2020-10-15: initial file (`20201015_masterdata.csv`)
- 2020-11-11: fixed an error merging chlorophyll data (`20201111_masterdata.csv`)
- 2021-02-24: added HBR and corrected Coal Creek data (`20210224_masterdata.csv`)
- 2021-03-04: updated and combined Sagehen data (`20210304_masterdata.csv`)
- 2021-04-21: fixed HBR NOx conversion (`20210421_masterdata.csv`)
- 2021-05-24: fixed NWT NOx and SRP conversion (`20210524_masterdata.csv`)
- 2021-08-04: added GRO (`2020804_masterdata.csv`, filename as recorded in the original notes)
- 2021-09-07: updated and added GRO high-frequency sampling (`20210907_masterdata.csv`)
