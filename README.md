# From poles to tropics: A multi-biome synthesis investigating the controls on river Si exports

- Primary Investigators: Joanna Carey & Kathi Jo Jankowski
- [Project Summary](https://lternet.edu/working-groups/river-si-exports/)
- [Participant Information](https://www.nceas.ucsb.edu/projects/12816)

## Script Explanations

Start with the runner for the work you need. Keep the source inputs and release
checks together in the dated run folder so a later run can use the same inputs.

| Work | Entry point |
|---|---|
| Build master chemistry and its notes | [`run_full_chemistry_workflow.R`](run_full_chemistry_workflow.R) |
| Prepare watersheds and run AppEEARS or the local spatial extraction | [`lterwg-silica-spatial`](https://github.com/lter/lterwg-silica-spatial) |
| Run Earth Engine and build the checked spatial tables | [`data-workflow_spatial`](https://github.com/global-river-chem/data-workflow_spatial) |
| Join the checked chemistry, discharge, and spatial inputs | [`combine_chemistry_spatial.R`](combine_chemistry_spatial.R) |

### Data Harmonizing (`00`)

These scripts put chemistry files into the same format and combine them into master chemistry.

- `00-harmonize_chemistry.R`: Kathi Jo's original workflow, kept unchanged for reference

- `00-harmonize_chemistry_sidney_review-only.R`: comparison copy with Sidney's updates commented beside Kathi Jo's code; use it for review, not to build the master

- `run_full_chemistry_workflow.R`: open this file in RStudio and click **Source** to run the updated workflow

- `00-harmonize_chemistry_sidney.R`: combines the prepared files, converts units, calculates daily averages, and adds flags; the runner calls it automatically

- `chemistry_functions.R`: shared functions for reading selected columns, parsing limit signs, averaging daily results, adding flags and notes, and checking inputs and outputs; it is loaded automatically

- `chemistry_rules.R`: the reviewed name matches, conversion factors, overlap choices, inferred dates, exact exclusions, and expected counts used by the code; lab-method labels and explanations come from the shared lab workbook instead

#### Running the Updated Chemistry Workflow

Keep the files together in the repository folder. The RStudio entry point is
`run_full_chemistry_workflow.R`. Open it, click **Source**, and choose where to
save the run. Use the local command below for a rerun from saved inputs.

The full chemistry validation passed on October 26, including the additional GEMS chemistry and the agreed Canada Si conversion. The shared keys and lab rules matched the validated copies. That chemistry code was committed and pushed on October 27 as `4c15c54`, `Finalize master chemistry workflow, source checks, and metadata`. The October 29 local rerun and comparison also passed with the half-limit averaging rule described below. The October 29 chemistry and metadata were uploaded to Drive and verified against the local files. The September 25, October 26 `_SAB`, and October 28 exports and their available matching metadata were archived without deleting them.

The GEMS additions include 65,510 non-Si daily measurements. The earlier averaging rule excluded below-limit samples from 106 mixed daily averages; those averages now include the samples at half their reported limits. Above-limit samples remain excluded from 15 mixed daily averages, as recorded in their flags. Estimated-value flags also survive daily averaging. Another 249 additions use the shallowest sample depth and carry that method flag. These flags apply only to the affected results, not every row in the network.

The updated chemistry package needs ten R files and this README:

```text
run_full_chemistry_workflow.R
00-harmonize_chemistry_sidney.R
chemistry_functions.R
chemistry_rules.R
recover_bnz_chemistry.R
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
- `recover_bnz_chemistry.R` adds the checked BNZ source rows and their method notes. The chemistry runner and the DSi recovery workflow both use this file at its current path.
- `chemistry-inputs/prepare_chemistry.R` controls the preparation order, prepares the Amazon files, selects sources where files overlap, adds GEMS results only where the preferred direct source lacks a matching result, and collects the 77 inputs for master chemistry.
- `chemistry-inputs/prepare_eccc_gems_danube.R` reads the original ECCC, GEMS, and Danube downloads. It keeps parameter and method information, applies the checked chemical forms and units, preserves limit signs, and builds their combined chemistry files and local key updates.
- `chemistry-inputs/prepare_other_sources.R` handles the remaining datasets' names, units, sample fractions, date decisions, and field-versus-lab choices. Its named source sections include the Seine fraction checks and the flagged Catalina and Cameroon date inferences.
- `chemistry-inputs/prepare_usgs_legacy.R` is loaded by `prepare_other_sources.R`. It separates the confirmed TDS-as-TN records, rebuilds nitrogen using the original USGS parameter identities, and restores checked TDS values in mg/L without a nitrogen conversion. It also handles the reviewed corrections to the older combined master.
- `chemistry-inputs/prepare_qualifier_inputs.R` recovers limit signs and numeric thresholds from source-specific fields and documented codes. It checks that these changes reach the master parser and leaves unrelated source columns unchanged.

Each source has a named section you can find through RStudio's section menu. Kathi Jo's original script and the optional `review-only` copy are for reference; neither is needed for this run. None of the preparation scripts needs to be run separately.

The October 26 validation matched all 77 prepared inputs, the master CSV, and the metadata CSV byte for byte to the run before the script split. That release contained 4,686,420 measurements at 1,170 sites.

The RStudio runner installs missing packages and reads the shared source files,
supporting ZIP, and live keys. The November recovery also requires two saved
snapshots in the evidence folder:

| Evidence file | MD5 of the checked November input |
|---|---|
| `chemistry_gap_sources_20261108.rds` | `3d68f34d70196423c7874a9bbde37788` |
| `chemistry_portal_sources_20261108.rds` | `d5d45c99f05c5b26a4b69fd5558b272e` |

The saved October 22 supporting ZIP predates these inputs. The runner checks
for them before downloading the 236 source files.
For a rerun with the November evidence, pass the saved input folders and a new
work folder:

```sh
Rscript run_full_chemistry_workflow.R SOURCE_DIR PROVIDER_DIR KEY_DIR EVIDENCE_DIR NEW_WORK_DIR
```

#### November 8 chemistry release

The checked source master has 4,987,150 records at 1,229 sites. The published
split uses exact `LTER` and `Stream_Name` keys for the data and the notes.

| File | Coverage | MD5 |
|---|---|---|
| [Stream chemistry](https://drive.google.com/file/d/1H4bhBl3dtSQLT3Ngc_ah4DhSoitIF9Bv/view) | 4,949,677 records, 1,216 sites | `8d1524ed81c4671b08242e31a69821fb` |
| [Stream notes](https://drive.google.com/file/d/1om7Tfgw-DN8JQ_b8iGIl03r4mZq2n5aB/view) | 1,224 notes | `4f5623c1bd289ba560275d298bd5aacc` |
| [Non-stream chemistry](https://drive.google.com/file/d/1NyGnOtrcSHRQVvAxhoneMI-Av7Ox29Jd/view) | 37,473 records, 13 sites | `476418160d3270eed1d9d40bc9f09088` |
| [Non-stream notes](https://drive.google.com/file/d/1wmhkYNrIGgRuNZ6aKjwI7PewxkWsdbcx/view) | 3 notes | `d2042c1207606998a4252f2cdb553701` |

The non-stream folder holds ten ARC soil-water points and three Seine estuary
stations. The two data files together account for every record in the checked
source master, MD5 `5b25d793583d5497fa66ebe18b5f805e`. The master records
daily chemistry. To verify sampling events, trace results to the original
sample IDs or source sample records.

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

Long explanations are saved in `YYYYMMDD_masterdata_chem_metadata.csv` under `chemistry-run/master/metadata`. It opens in Excel or R. `Note_Type` tells you whether a row contains a date note or method note. Each method note keeps its source, site, and variable, with one row per source, site, variable, and full method flag rather than one row per daily measurement. Method-note dates are blank; the flags on the master rows identify the affected dates. Fields that do not apply are blank.

To add those notes back to the master in R:

- For date notes, keep rows where `Note_Type == "date"` and match on `Dataset`, `Raw_Filename`, `LTER`, `Stream_Name`, `date`, and `Date_Flag`
- For method notes, keep rows where `Note_Type == "method"` and match on `Dataset`, `Raw_Filename`, `LTER`, `Stream_Name`, `variable`, and the full `Method_Flag` text, including combined flags
- Keep only the matching columns and `Date_Note` or `Method_Note` for each join, and read `date` the same way in both files

These matches restore the explanations without adding chemistry rows or requiring new site or sample IDs.

Both outputs are CSVs. Uploading was tested with the earlier saved `_SAB` files, and their names, sizes, folder locations, and checksums were verified. Those older files do not contain the final GEMS additions or Canada conversion. The final validated master has not been uploaded.

Canada VMV 109113 uses the agreed Si basis because the original method metadata identify silicon. The workflow converts mg Si/L to uM using silicon's atomic mass instead of the earlier SiO2 assumption. These results keep the `soluble reactive Si, filtered ICP-OES` flag. GEMS records labeled SiO2-Rea still use the SiO2 conversion and keep their reactive-Si flag. The chemistry mappings and lab rules matched the validated copies. Later discharge and spatial reference updates are described below.

ColoradoAlpine ammonium is reported as NH4, not NH4-N. The Units Key uses mg NH4/L and the workflow converts it using ammonium's molecular mass for all years. The 3,037 source results already contribute 2,195 daily NH4 values at the retained river sites in the validated master; the earlier note saying they were excluded was incorrect. No duplicate records need to be added.

The 1,752 provider-confirmed Silica - Soluble records at Yarrawonga and Burtundy use the confirmed SiO2 mass basis and keep their existing conversion. Generic source records labeled Si in mg/L at the 25 approved Australian sites are retained using the agreed elemental-Si assumption. These records carry both `Si basis assumed as Si` and `Si sample fraction not documented` in `Method_Flag`. Original concentrations are unchanged; the working units key records the assumption before conversion to uM.

All seven EastRiverSFA sites with silicon are retained using the source's elemental-Si basis in ug Si/L. Their silicon records carry `Si sample fraction not documented`. Berville-sur-Mer 1, Honfleur 1, and Tancarville 1 are retained as Seine estuary stations with a site flag. The existing Seine sample-fraction checks still apply: confirmed unfiltered silicon is excluded, and uncertain fraction records keep `Si filtration not confirmed`. Other site and sample flags remain.

At the 17 approved Danube sites, source code `2.3.10` explicitly reports silica as SiO2 in mg/L. The workflow converts those values using the SiO2 molecular mass and adds `Si sample fraction not documented`. It does not add an elemental-Si basis assumption. Documented dissolved-silicon code `2.3.11` retains priority when both source codes occur on the same day.

The chemistry master checked on November 9 contains 4,987,150 measurements at 1,229 sites. It includes the 52 approved restorations checked on November 7: 25 Australian, 17 Danube, seven EastRiverSFA, and three Seine estuary sites. It also restores three Chilean sites and Japan's Kawabe River at Yanase with recovered silica and sample-fraction uncertainty flags, and adds BNZ stations CB, PC and PJ with the approved silica sample-fraction flags. McMurdo nitrogen, BNZ chloride, and nitrate from three matched Guadeloupe river stations are included. Source quality and below-limit flags are retained. The checked spatial snapshot described below matches all 1,229 site keys, with unfinished fields blank; the full combined-data release still awaits its remaining source and watershed checks.

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

All 776 required discharge files now pass the saved-data checks. The earlier failures, including the seven PIE unit conflicts, have been resolved against the original sources. The LUQ checks confirmed the historical conversion areas, unchanged later discharge, and separation from spatial drainage areas. Output uploading remains off in the discharge script, and the older source-file editing examples at its end are disabled. The discharge changes were committed in `4e94fcf`.

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

The next spatial release has two versions, both matched to the finalized chemistry site list. Each version retains every approved site and records missing or inapplicable predictors in its metadata. Product years extend only through each source's latest available complete year.

| Version | Climate, ET, and snow | Other products |
|---|---|---|
| Expanded GlASS | Original GlASS sources and definitions: GPCP precipitation, GHCN-CAMS temperature, MODIS ET, and MODIS snow | Original GlASS productivity, greenup, land cover, terrain, geology, soils, and permafrost products, plus human impacts and river distance to coast |
| ERA5-Land | ERA5-Land precipitation, temperature, actual ET, snow cover, and SWE; original PET/PEV excluded | BESS GPP, MODIS NPP and first greenup, land cover and checked static predictors, plus human impacts and river distance to coast |

Both versions use the same checked human-impact and routing results where applicable. Each has its own site metadata, column dictionary, and source coverage notes. The archived GlASS file is a reference input; the expanded GlASS version is a separate output.

#### Current coverage and shared files

The November 9 checked partial release has 1,229 site keys in both spatial
tables. Use the same keys, `LTER` and `Stream_Name`, to join either table to the
published stream chemistry subset. Join annual fields by chemistry year as well.

| File | Contents |
|---|---|
| [GlASS spatial](https://drive.google.com/file/d/1MHL5VEFGRm8eWKcO0EhEN3UE1kFxsaj4/view) | Original GlASS climate and MODIS definitions, with shared drivers |
| [ERA5-Land spatial](https://drive.google.com/file/d/19oVEPjvyFnNJ5dMmZiISzHHamFi3Es2j/view) | Checked ERA5-Land values and shared drivers |
| [Site metadata](https://drive.google.com/file/d/1OSkLENmbrFTbhJ1oD_EO68uAfUbNz3cu/view) | Coverage, source notes, watershed checks, and river-distance flags |
| [Column dictionary](https://drive.google.com/file/d/1Lvd-_v48yOKjsnGnFFhkQQ0DKE9KhgTd/view) | Source names, units, years, and output fields for each version |
| [Release notes](https://drive.google.com/file/d/1pmQ-eWtJ2RbljM0oLVZNzD9YccA_pU8o/view) | Coverage and checks for the current partial release |

The first recovery added 5,514 GlASS values and 1,127 shared terrain, geology,
and permafrost values across 87 sites. The later check reused 2,604 annual
ERA5-Land values from matching watersheds at 12 sites. Available McMurdo
observations carry product-specific source and quality flags. Fill-only land
cover and unresolved fields stay blank. The earlier 1,170-site snapshot is in
the archive. Drive readback matched the checked release files.

River distance starts at the chemistry point projected onto the matched
HydroRIVERS reach. The remaining fraction of `LENGTH_KM` is added to `DIST_DN_KM`.
`coast_match_snap_km` records the gap to the reach separately. Use the adjusted
distance and its quality fields. The reach-end fields are kept for comparison.
The checked release has ocean distances for 959 sites and outlet distances for
962 sites.

| Service | Current files | Archived files |
|---|---|---|
| Google Drive | [Updated dataset](https://drive.google.com/drive/folders/1dh2uxghBGCeYZZZiBIuA407yzNAcR-9O) | [Archive](https://drive.google.com/drive/folders/1ELbbxbed1YZr0oM61e89_Aia6uSZ6c6r) |
| Box | [Updated dataset](https://app.box.com/folder/435176209940) | [archive](https://app.box.com/folder/409143582863) |

The nested **Supporting files** folder holds `20261102_updated_harmonized_spatial_gpp_quality.csv` and `ferguson_611026_verified.gpkg`. Match the GPP quality file by `LTER`, `Stream_Name`, and `year`. Match annual spatial values to chemistry by site and observation year; static values match by site. Unavailable years stay blank.

The Box path is `SiSyn/spatial-data-extractions/spatial-data/Updated dataset/`. Its archive holds the older October 15 and October 28 predictor CSVs, the October 28 metadata CSV, and `release-20260820/` and `release-20260917/`. The Drive archive holds earlier products and **Chemistry and spatial snapshot (2026-11-03)**, which retains the older combined CSV and metadata. Archived files were moved without deletion. Both Archives also retain the superseded November 2 wide CSV, metadata, and column dictionary with their original checksums. The full-release builder resolves those unchanged archived rebuild inputs automatically.

Box's `raw/`, `current-updated-release/`, `harmonized/current/`, and `shiny-app-inputs/` remain in place as workflow or app inputs. Use the current CSV folder above for the downloadable release.

#### Ferguson's boundary and remaining predictors

Ferguson now uses 25 connected upstream catchments from [BoM Geofabric](https://hosting.wsapi.cloud.bom.gov.au/arcgis/rest/services/ahgf/Geofabric_V3x_All_Products/FeatureServer/7), covering 104.973609 km² above station 611026. The older HydroBASINS polygon covered about 224.7 km². That original file is retained, but its predictors are not reused.

Ferguson's ERA5 results were extracted using the corrected boundary and are included in the shared spatial CSV. The Site Reference Table's watershed source link and extraction note match the metadata on Drive and Box. The source-link correction changed no spatial values or other reference-table cells.

The local October 29 combined export also includes Ferguson's corrected-boundary land cover, MODIS, long-term climate, elevation, slope, geology, and soils. The source checks passed. Permafrost stays missing because the legacy Northern Hemisphere product does not cover Ferguson. The shared October 28 spatial files contained only the earlier ERA5 update for this site and are now archived. The current wide release is linked above.

#### What the spatial scripts do

These scripts belong to the separate spatial workflow checkout, not the chemistry runner:

- `workflow/build_watershed_collection.R` reads the site table and watershed files. It selects a verified replacement boundary before an older shapefile when one is available, including Ferguson's corrected boundary.
- `workflow/gee/build_gee_vector_payloads.R` prepares the selected watershed polygons for Earth Engine and checks site coverage, file size, and any geometry changes. It does not start extraction tasks.
- `workflow/gee/era5_land/run_safe_era5_land_exports.py` checks the selected sites, years, and products, skips existing results, and submits ERA5 tasks only with an approved quota check. Its default is a check without submitting tasks.
- `workflow/gee/era5_land/finish_missing_era5_land_assets.py` runs the missing exports in checked batches outside RStudio and waits for their results. It does not need an open chat to continue.
- `workflow/gee/era5_land/download_annual_era5_land_assets.R` downloads completed annual tables and checks for duplicate site-year matches, missing or infinite values, required variables, and complete requested coverage.
- `workflow/release/recover_saved_spatial_results.R` aligns saved spatial results to the chemistry sites, recovers reviewed missing results, keeps site flags, and removes predictors from replaced boundaries. `merge_annual_era5_results()` adds the checked annual updates after that removal, so Ferguson's new ERA5 values are retained while its other old predictors stay excluded.
- `workflow/release/build_spatial_dataset_files.R` writes separate product CSVs and one site metadata CSV. Its `--era5-updates` option reads the downloaded annual-results RDS when building the updated files. The RDS is an input to the local workflow; the shared predictor and metadata files are CSVs.

#### Updated harmonized spatial export

The November 2 harmonized spatial CSV follows the earlier wide format: one row per site, separate columns for each year of an annual predictor, and one column per static predictor. It combines the saved tables for the Updated dataset, including BESS GPP, MODIS NPP and first greenup (Cycle 0), ERA5-Land, land cover, human impacts, river distances, and static watershed predictors. All 1,170 sites are included. The 130 sites without spatial coverage stay in the file.

For Ferguson, the initial wide export filled 298 blank fields using the checked October 29 results for the corrected watershed. These included land cover for observed source years, elevation, slope, lithology, and soils. The ERA5-Land version omits soil columns; the GlASS version retains original soils. Legacy permafrost stays blank because the source does not cover Ferguson.

The ERA5-Land wide CSV uses shorter headers such as `gpp_1982`, `greenup_c0_2022`, and `ocean_dist_km`. It keeps `LTER` and `Stream_Name` as the site keys and keeps the year in each annual header. The column dictionary, `20261102_updated_harmonized_spatial_column_dictionary.csv`, gives the original name, definition, units, source, and year where applicable, and identifies which file holds each field.

The retained November 2 rebuild input, `20261102_updated_harmonized_spatial.csv`, has **1,170 sites and 831 columns**, including annual GPP totals and CAMREX NPP gap-filling fields. Earlier local builds had 819 columns in the base Cycle 0 version or 844 columns in the CAMREX version with soils. GPP valid days, watershed coverage, flags, and notes are in `20261102_updated_harmonized_spatial_gpp_quality.csv` under **Supporting files**. It has 47,970 rows, one for each site and year from 1982–2022, keyed by `LTER`, `Stream_Name`, and `year`. Every reported BESS annual total passed the full calendar year and 80% watershed coverage checks.

The data rows matched byte for byte after renaming the headers. After moving the quality fields, the readback matched every retained value and reconstructed all 164 moved columns, including blanks, from the quality CSV. The site metadata file has one row per site in the same order, and its checksum matched before and after the update.

To rebuild the four wide release files from saved inputs, run:

```sh
Rscript build_wide_spatial_release.R PRODUCT_DIR PROVIDER_RDS FERGUSON_LAND_INPUT LAND_CLASSES_CSV COLUMN_DICTIONARY_CSV OUTPUT_DIR [CAMREX_NPP_RDS]
```

Use the verified `annual-release-20261102` product folder, Ferguson's checked `provider-results.rds`, and the release column dictionary. For the land-cover input, use `completed-extractions.rds` with the saved `GLC_FCS30D_full_to_simple_class_translation.csv`. If that cache is unavailable, use the validated wide spatial CSV with its matching dictionary and pass `-` for the class file. The builder reads only Ferguson's land-cover fields from that CSV. Choose a new output directory.

Pass the verified `updated-annual-camrex-npp.rds` as the last argument to rebuild the CAMREX release. The build matches the six sites by network and stream name, checks coordinates, and fills 150 blank annual NPP values after converting kilograms to grams. It also adds the 150 matching gap-filling percentages and their dictionary entries.

The current shared wide export reports both MODIS NPP and BESS GPP in g C/m²/year. The build multiplies the saved MODIS NPP values in kg C/m²/year by 1,000 and records the output units in the column dictionary. BESS GPP is already in grams. Blank values and zeros are preserved. An earlier November 2 version used kilograms for NPP.

The expanded build accepts the checked nine-product RDS and the existing Updated dataset folder as its baseline. It preserves earlier checked values, including Ferguson's land and terrain results and the supporting GPP quality fields. It rejects unexplained changes to earlier non-climate measurements. Use a new output directory:

```sh
Rscript build_wide_spatial_release.R PRODUCT_RDS BASELINE_DIR - - COLUMN_DICTIONARY_CSV OUTPUT_DIR - OUTPUT_PREFIX
```

The RDS branch includes five ERA5-Land fields for 1984–2025: precipitation, temperature, actual evapotranspiration, snow cover, and SWE. It also includes first greenup (Cycle 0) and the 26 observed land-cover map years: 1985, 1990, 1995, and 2000–2022. Soil columns remain excluded. The build converts the named kilogram NPP source fields to grams once and preserves the existing GPP units. An earlier November 8 check covered 1,222 sites, including 1,098 complete 42-year climate and snow histories. The full release now targets the repaired 1,229-site chemistry list and all twelve retained ERA5-Land climate fields; those earlier counts are not final coverage totals.

The full replacement release is pending the remaining extraction, watershed,
and coverage checks. MODIS uses the AppEEARS geographic workflow. The site
metadata records unresolved watershed identities and the source and quality
limits on retained McMurdo observations. Final assembly checks earlier validated
values against the saved release and records each accepted repair.

Updated builds exclude the original ERA5-Land potential-evaporation field (PET/PEV), including when rebuilding from older inputs that contain it. It is an open-water estimate as described in the [ECMWF ERA5-Land documentation](https://confluence.ecmwf.int/pages/viewpage.action?pageId=293729169). Archived source files remain available for reference. Neither replacement version includes reference evapotranspiration. The three retained non-snow ERA5-Land fields record their aggregation method in the site metadata; snow cover and SWE use annual maxima of daily watershed averages.

#### Combined export

The November 2 updated dataset uses the October 29 combined export as its base.
It adds annual BESSv2 GPP, LandScan population, dam, fertilizer, and wastewater
summaries, and HydroRIVERS distances to the outlet and ocean. The saved human
impact and routing results matched 881 sites by watershed name and drainage area.
Population matches by chemistry year. Ocean distance stays blank for closed basins.

The saved BESS extraction covers 1,040 sites from 1982–2022. Annual GPP sums
monthly mean daily rates weighted by calendar days and is reported in g C/m²/year.
Totals require all 12 months and at least 80% watershed coverage. The export keeps
valid-day counts, area fractions, and source-zero flags. It also keeps MODIS NPP
and both saved greenup cycles as separate fields. Future dated rebuilds keep
only first greenup (Cycle 0). The six annual ERA5-Land fields
replace the overlapping legacy climate, evapotranspiration, and snow columns.

The saved chemistry, daily discharge, and spatial joins have passed. There are 3,593,358 chemistry measurements with matching discharge and 1,093,062 without usable matching flow. Missing discharge or spatial values do not remove chemistry rows.

`combine_chemistry_spatial.R` assembles a base export from finalized chemistry, checked daily discharge, and a site-level spatial table. To reproduce the November 2 update from the saved October 29 base, run:

```sh
Rscript build_updated_combined.R BASE_CSV BASE_METADATA ANNUAL_GPP_RDS SAVED_SPATIAL_CSV OUTPUT_DIR
```

Use the October 29 combined CSV and its metadata, the saved BESS `annual-gpp.rds`, and the saved `final_spatial_era5_modis_human_point_1196_20260916.csv`. Choose a new local output directory. Add `--preflight` to check the inputs and output columns before writing.

The full combined releases are being prepared for the repaired 1,229-site
chemistry master. In `write_combined_chemistry()`, select
`spatial_version = "original_glass"` for the expanded original products or
`spatial_version = "updated"` for ERA5-Land with actual evapotranspiration,
BESS GPP, MODIS NPP, and first greenup. Both versions include human impacts
and downstream river distances where supported. The writer excludes
potential evaporation and reference evapotranspiration. It still applies the
older McMurdo vegetation and land-cover mask. Before building the next
combined release, update and check the writer against the product-specific
McMurdo flags in the checked partial spatial files.

Both spatial versions add the remaining distance within the matched river segment to the HydroRIVERS reach-outlet distance. The GlASS fields are `downstream_to_ocean_km` and `downstream_to_outlet_km`; the ERA5-Land file uses `ocean_dist_km` and `outlet_dist_km`. They estimate distance along the mapped river from the chemistry site's snapped position. The ocean field ends at the ocean. The outlet field ends at the basin's downstream endpoint, either the ocean or an inland endpoint such as a closed lake. The original values are retained as `downstream_to_ocean_reach_outlet_km` and `downstream_to_outlet_reach_end_km`. Both estimates exclude the gap between the site coordinate and the river line. `coast_match_snap_km` stores the current gap; the older snap columns retain the previous values. New matches have a 10 km acceptance limit, but a short snap does not establish that the correct tributary was selected. Match and direction flags accompany the estimates, and unresolved or unsupported cases stay blank. Inland-draining basins have no ocean-distance value. The shared partial snapshot retains 959 ocean and 962 basin-outlet distances; no checked distance has yet been merged for the 59 appended rows.

Discharge matches by `LTER`, `Stream_Name`, and date; annual predictors match by site and chemistry year; static predictors match by site. The combined CSV keeps the chemistry row order, values, and flags, and adds `Qcms`, `Q_Flag`, `year`, and the matched predictors. Monthly predictor columns retain the source climatologies. Annual columns outside the available source years stay blank. The separate metadata CSV keeps the existing chemistry and site notes and records each spatial source column, its output name, and its matching year. Both CSVs are read back and checked before the combined file is marked complete.

The October 29 combined export passed full readback checks: 4,686,420 chemistry measurements at 1,170 sites, with 3,593,358 matching discharge values and 1,093,062 without usable discharge. Chemistry values, flags, and row order stayed unchanged. Ferguson's corrected-boundary MODIS, observed-year land cover, long-term climate, elevation, slope, geology, and soils are included. Permafrost remains missing because the legacy source does not cover the site. The update changed only Ferguson's elevation, slope, and soil fields. A later dated combined snapshot is retained in the Drive archive described above.

The November 2 readback checked every row against the October 29 base and the saved spatial inputs. All 4,686,420 chemistry measurements, 3,593,358 discharge matches, and 1,170 sites carried through, including the 130 sites without spatial data. The local outputs are `20261102_updated_combined.csv` (121 columns) and `20261102_updated_combined_metadata.csv` (2,116 rows).

The November 3 dated rebuild keeps first greenup (Cycle 0) only. Full readback against the saved October 29 base and spatial inputs confirmed 4,686,420 chemistry rows, 1,170 sites, and 3,593,358 discharge matches. The combined CSV has 120 columns and the metadata has 2,092 rows. Cycle 0 is present in the CSV and its metadata mappings.

The checked pair is saved locally in `~/.local/share/sisyn-releases/20261103-dated-combined-cycle0/`. Both SHA-256 checksums matched after the move. Shiny loads separate prepared chemistry and concentration–discharge RDS files and a spatial RDS for site summaries. The dated CSV and metadata are a separate release.

The earlier discharge and spatial changes were committed in `4e94fcf`. This assembly work does not rerun chemistry or change WRTDS, raw files, or shared files.

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
