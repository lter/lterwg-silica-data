# ---- gems silicon gap filling ----
# use only reviewed matches to sites already in the chemistry dataset
gems_usgs_si_sites <- tibble::tribble(
  ~gems_station, ~usgs_station,
  "USA00004", "06934500",
  "USA00008", "01463500",
  "USA00011", "11447650",
  "USA00013", "06775900",
  "USA00021", "04264331"
)
gems_filtered_reactive_methods <- c("COL-HB", "COL-HB-AA-F", "VMV_3628")
gems_reactive_si_flag <- "reactive Si, filtered (GEMS)"
gems_si_exclusions <- tibble::tribble(
  ~gems_station, ~date, ~reason,
  "USA00004", "1991-05-28", "no matching result in the reviewed USGS download"
)
gems_si_expected_days <- c(Canada = 1L, USGS = 56L)

# the original catalogue identifies the sample fractions and reported chemical forms
# total metals, total sulfate, and total reactive phosphorus are not substitutes
gems_gap_parameters <- function() {
  tibble::tribble(
    ~source_entry, ~parameter_code, ~provider_units, ~variable, ~units, ~conversion, ~limit_is_clear,
    "Aluminium.csv", "Al-Dis", "mg/l", "Al", "uM", 1000 / atomic_mass[["Al"]], TRUE,
    "Calcium.csv", "Ca-Dis", "mg/l", "Ca", "uM", 1000 / atomic_mass[["Ca"]], TRUE,
    "Chloride.csv", "Cl-Dis", "mg/l", "Cl", "uM", 1000 / atomic_mass[["Cl"]], TRUE,
    "Pigment.csv", "Chl-a", "mg/l", "chlorophyll a", "ug/L", 1000, TRUE,
    "Carbon.csv", "DOC", "mg/l", "dissolved org C", "uM", 1000 / atomic_mass[["C"]], TRUE,
    "Carbon.csv", "TOC", "mg/l", "tot org C", "uM", 1000 / atomic_mass[["C"]], TRUE,
    "Fluoride.csv", "F-Dis", "mg/l", "F", "uM", 1000 / atomic_mass[["F"]], TRUE,
    "Iron.csv", "Fe-Dis", "mg/l", "Fe", "uM", 1000 / atomic_mass[["Fe"]], TRUE,
    "Bicarbonate.csv", "HCO3", "mg/l", "HCO3", "uM", 1000 / molecule_mass[["HCO3"]], TRUE,
    "Potassium.csv", "K-Dis", "mg/l", "K", "uM", 1000 / atomic_mass[["K"]], TRUE,
    "Magnesium.csv", "Mg-Dis", "mg/l", "Mg", "uM", 1000 / atomic_mass[["Mg"]], TRUE,
    "Manganese.csv", "Mn-Dis", "mg/l", "Mn", "uM", 1000 / atomic_mass[["Mn"]], TRUE,
    "Sodium.csv", "Na-Dis", "mg/l", "Na", "uM", 1000 / atomic_mass[["Na"]], TRUE,
    "Other_Nitrogen.csv", "NH4N", "mg/l", "NHX", "uM", 1000 / atomic_mass[["N"]], TRUE,
    "Other_Nitrogen.csv", "TN", "mg/l", "TN", "uM", 1000 / atomic_mass[["N"]], TRUE,
    "Other_Nitrogen.csv", "TKN", "mg/l", "tot kjeldahl N", "uM", 1000 / atomic_mass[["N"]], TRUE,
    "Oxidized_Nitrogen.csv", "NO2N", "mg/l", "NO2", "uM", 1000 / atomic_mass[["N"]], TRUE,
    "Oxidized_Nitrogen.csv", "NO3N", "mg/l", "NO3", "uM", 1000 / atomic_mass[["N"]], TRUE,
    "Oxidized_Nitrogen.csv", "NOxN", "mg/l", "NOx", "uM", 1000 / atomic_mass[["N"]], TRUE,
    "Phosphorus.csv", "TP", "mg/l", "TP", "uM", 1000 / atomic_mass[["P"]], TRUE,
    "Phosphorus.csv", "TDP", "mg/l", "tdp", "uM", 1000 / atomic_mass[["P"]], TRUE,
    "Phosphorus.csv", "DRP", "mg/l", "SRP", "uM", 1000 / atomic_mass[["P"]], TRUE,
    "Sulfur.csv", "SO4-Dis", "mg/l", "SO4", "uM", 1000 / molecule_mass[["SO4"]], TRUE,
    "Alkalinity.csv", "Alk-Tot", "mg/l", "alkalinity", "ueq/L", 2000 / 100.0869, TRUE,
    "Water.csv", "TSS", "mg/l", "susp partic matter", "mg/L", 1, TRUE,
    "Water.csv", "TDS", "mg/l", "tot dissolved solids", "mg/L", 1, TRUE,
    "Zinc.csv", "Zn-Dis", "mg/l", "Zn", "uM", 1000 / atomic_mass[["Zn"]], TRUE,
    "Dissolved_Gas.csv", "O2-Dis", "mg/l", "dissolved oxygen", "uM", 1000 / (2 * atomic_mass[["O"]]), FALSE,
    "Dissolved_Gas.csv", "O2-Dis-Sat", "%", "do percent", "percent", 1, FALSE,
    "Temperature.csv", "TEMP", "°C", "temp", "C", 1, FALSE,
    "Electrical_Conductance.csv", "EC", "µS/cm", "conductivity", "uS/cm", 1, FALSE,
    "pH.csv", "pH", "---", "pH", NA_character_, 1, FALSE,
    "Optical.csv", "TURB", "NTU", "turbidity", "NTU", 1, FALSE
  )
}

# ---- raw labels ----
deduplicated_filename <- "Deduplicated_Sites_AllSites_chemistry.csv"
deduplicated_raw_aliases <- tibble::tribble(
  ~solute, ~Units,
  "ca_mgl", "mg_L",
  "cl_mgl", "mg_L",
  "sio2_mgl", "mg_SiO2_L",
  "k_mgl", "mg_L",
  "mg_mgl", "mg_L",
  "nh4_mgl", "mg_NH4_L",
  "na_mgl", "mg_L",
  "po4_mgl", "mg_PO4_L",
  "so4_mgl", "mg_SO4_L",
  "tp_ugl", "ug_P_L",
  "doc_mgl", "mg_C_L",
  "no3_mgl", "mg_NO3_L",
  "al_ugl", "ug_L",
  "f_mgl", "mg_L",
  "fe_ugl", "ug_L",
  "mn_ugl", "ug_L",
  "anc_ueql", "ueq_L",
  "temp_c", "C",
  "tdn_mgl", "mg_N_L"
) |>
  dplyr::mutate(Raw_Filename = deduplicated_filename) |>
  dplyr::select(Raw_Filename, solute, Units)
fixed_missing_tokens <- c(
  "", "NA", "NaN", "N/A", "n/a", "<LOD", "wrong filtering",
  "no filter", "nd"
)
source_review_columns <- list(
  "EastRiverSFA_chem.csv" = c("solute", "value"),
  "CatalinaJemez_chemistry_2009-2019_V2.csv" = c("SampleCode", "SiteCode", "DateTime")
)

# ---- unit label aliases ----
unit_label_aliases <- c(
  "na" = NA_character_,
  "-" = "",
  "/" = "",
  "mg/l" = "mg/L",
  "milligrams/litre" = "mg/L",
  "milligramsperliter" = "mg/L",
  "mg(s)/l" = "mg S/L",
  "mg(so4)/l" = "mg SO4/L",
  "ug/l" = "ug/L",
  "micrograms/litre" = "ug/L",
  "microsiemens/cm" = "uS/cm",
  "microsiemens cm" = "uS/cm",
  "microsiemenspercentimeter" = "uS/cm",
  "deg c" = "C",
  "oc" = "C",
  "degrees celsius" = "C",
  "%" = "percent",
  "pcu" = "PCU",
  "fnu" = "FNU",
  "ntu" = "NTU",
  "ueq/l" = "ueq/L"
)

# ---- variable names and preferred columns ----
# the order below controls which equivalent column is used first
# converting between reported chemical forms is separate from matching names
ph_columns <- c(
  "pH", "ph", "pH_pH", "ph_SU", "ph_", "fldph", "fldph_", "labph", "labph_"
)
conductivity_columns <- c(
  "conduct_uS_cm", "conductivity_uS_cm", "cond_uscm_uS_cm",
  "fldcond_uscm_uS_cm", "labcond_uscm_uS_cm", "conductivity_us_cm",
  "ec_uS_cm", "specificconductance_uS_cm", "conductivity_at_25°c_uS_cm"
)
merge_rules <- list(
  alkalinity_ueq_L = c("alkalinity_ueq_L", "alkalinity_uEq_l"),
  anc_ueq_L = c("anc_ueq_L", "anc_ueql_ueq_L", "anc_ueq_l_ueq_L"),
  do_percent = c("do_percent", "do_pct_percent", "do_percent_percent", "do_percent_%"),
  colour_PCU = c("colour_PCU", "colour_pcu", "colour_pcu_", "colour_pcu_PCU"),
  do_uM = c("do_uM", "dissolved_oxygen_uM"),
  don_uM = c("don_uM", "dissolved_org_n_uM"),
  din_uM = c("din_uM", "dissolved_inorg_n_uM"),
  srp_uM = c("srp_uM", "soluble_reactive_p_uM"),
  tdn_uM = c("tdn_uM", "tot_dissolved_n_uM"),
  tn_uM = c("tn_uM", "tot_n_uM"),
  tkn_uM = c("tkn_uM", "tot_kjeldahl_n_uM"),
  tp_uM = c("tp_uM", "tot_p_uM"),
  al_ug_L = c("al_ug_L", "al_ugl_ug_L"),
  fe_ug_L = c("fe_ug_L", "fe_ugl_ug_L"),
  nh4_mg_NH4_L = c(
    "nh4_mg_NH4_L", "nh4_mgl_mg_NH4_L"
  ),
  tdn_mg_N_L = c("tdn_mg_N_L", "tdn_mgl_mg_N_L", "tdn_mg_L"),
  chla_ug_L = c(
    "chl_a_ug_L", "chla_ug_L", "suspended_chl_ug_L",
    "chla_ugl_ug_L", "chlorophyll_a_ug_L"
  ),
  do_mg_L = c("do_mg_L", "do_mg_O2_L"),
  ca_mg_L = c("ca_mgl_mg_L", "ca_mg_L", "calcium_mg_L"),
  ca_ug_L = c("ca_ug_L", "calcium_ug_L"),
  cl_mg_L = c("cl_mgl_mg_L", "cl_mg_L"),
  cl_uM = c("chloride_uM", "cl_uM"),
  doc_mg_C_L = c(
    "doc_mgl_mg_C_L", "doc_mg_C_L", "carbon_dissolved_organic_mg_C_L"
  ),
  doc_uM = c("dissolved_org_c_uM", "doc_uM"),
  dic_mg_C_L = "dic_mg_C_L",
  dic_uM = c("dic_uM", "dissolved_inorg_c_uM"),
  toc_mg_C_L = c("toc_mgl_mg_C_L", "toc_mg_C_L"),
  f_mg_L = c("f_mgl_mg_L", "f_mg_L"),
  hco3_mg_L = c("hco3_mgl_mg_L", "hco3_mg_L"),
  k_mg_L = c("k_mgl_mg_L", "k_mg_L", "potassium_mg_L"),
  mg_mg_L = c("mg_mg_L", "mg_mgl_mg_L", "magnesium_mg_L"),
  mg_ug_L = c("magnesium_ug_L", "mg_ug_L"),
  mn_ug_L = c("mn_ugl_ug_L", "mn_ug_L"),
  na_mg_L = c("na_mgl_mg_L", "na_mg_L", "na._mg_L"),
  nh4_mg_NH4_N_L = c(
    "nh4_mgl_mg_NH4_N_L", "nh4_mg_NH4_N_L",
    "nh4___n_mg_NH4_N_L"
  ),
  nh4_ug_NH4_N_L = "nh4_ug_NH4_N_L",
  nhx_mg_N_L = c(
    "nhx_mg_N_L", "ammonia_n_mg_N_L", "ammonia_dissolved_mg_N_L"
  ),
  nhx_ug_N_L = "nh4_n_ug_NH4_N_L",
  no3_mg_NO3_L = c("no3_mgl_mg_NO3_L", "no3_mg_NO3_L"),
  no3_uM = c("no3_uM", "nitrate_uM"),
  # keep combined nitrate plus nitrite as nox while using its reported n basis
  nox_mg_NO3_N_L = c(
    "nox_mg_NO3_N_L", "nox_mg_N_L", "no3+no2___n_mg_NO3_N_L"
  ),
  no3_mg_NO3_N_L = c(
    "no3_mgl_mg_NO3_N_L", "no3_mg_NO3_N_L",
    "dissolved_nitrogen_nitrate_mg_NO3_N_L"
  ),
  no2_mg_NO2_N_L = c("no2_mgl_mg_NO2_N_L", "no2_mg_NO2_N_L"),
  po4_mg_PO4_L = c("po4_mg_PO4_L", "po4_mgl_mg_PO4_L"),
  po4_uM = c("phosphate_uM", "po4_uM"),
  po4_mg_P_L = c("po4_mg_PO4_P_L", "po4_mg_P_L", "ortho___p_mg_PO4_P_L"),
  ssc_mg_L = c("ssc_mg_L", "susp_sediment_conc_mg_L"),
  dsi_mg_SiO2_L = c(
    "si_mg_SiO2_L", "sio2_mgl_mg_SiO2_L", "dsi_mg_SiO2_L",
    "sio2_mg_SiO2_L", "sio2_reactive_dissolved_mgl_mg_SiO2_L"
  ),
  so4_mg_SO4_L = c("so4_mg_SO4_L", "so4_mgl_mg_SO4_L"),
  so4_ug_SO4_L = "so4_ug_SO4_L",
  so4_uM = c("so4_uM", "sulfate_uM"),
  temp_C = c("temp_C", "temp_c_C", "water_temp_C", "twater_(°c)_C"),
  toc_uM = c("toc_uM", "tot_org_c_uM"),
  tp_ug_P_L = c("tp_ug_L", "tp_ugl_ug_P_L"),
  turbidity_NTU = c("turbidity_NTU", "turb_NTU"),
  tds_mg_L = c("tds_mg_L", "tot_dissolved_solids_mg_L"),
  tss_mg_L = c("tss___dry_mass_mg_L", "tss_mg_L"),
  vss_mg_L = c("volatile_susp_solids_mg_L", "vss_mg_L"),
  tdn_mg_L = "nitrogen_total_dissolved_mg_N_L",
  tn_mg_L = c("tn_mg_N_L", "nitrogen_total_mg_N_L", "ntot_mgl_mg_N_L"),
  tp_mg_L = "phosphorus_total_mg_P_L",
  tdp_mg_L = "phosphorus_total_dissolved_mg_P_L",
  dsi_mg_Si_L = c(
    "dsi_mg_Si_L", "si_mg_Si_L", "silicon_extractable_mg_Si_L"
  ),
  dsi_ug_Si_L = c(
    "dsi_ug_Si_L", "si_ug_Si_L", "silicon_ug_Si_L"
  )
)
optional_measurement_columns <- c("don_mg_L", "so4_mg_S_L", "zn_mg_L")
variable_names <- c(
  ph = "pH", dsi = "DSi", do = "dissolved oxygen", chla = "chlorophyll a",
  co2 = "CO2", dic = "dissolved inorg C", din = "dissolved inorg N",
  doc = "dissolved org C", don = "dissolved org N", srp = "SRP",
  ssc = "susp sediment conc", tdn = "tot dissolved N",
  tds = "tot dissolved solids", tkn = "tot kjeldahl N", tn = "TN",
  toc = "tot org C", tp = "TP", vss = "volatile susp solids"
)

# ---- unit conversions ----
atomic_mass <- c(
  Al = 26.981539, Br = 79.904, C = 12.011, Ca = 40.078, Cl = 35.453,
  F = 18.998403, Fe = 55.845, H = 1.00784, K = 39.0983, Li = 6.941,
  Mg = 24.305, Mn = 54.938044, N = 14.0067, Na = 22.989769,
  O = 15.999, P = 30.973762, S = 32.065, Si = 28.0855,
  Sr = 87.62, Zn = 65.38
)
molecule_mass <- c(
  HCO3 = atomic_mass[["H"]] + atomic_mass[["C"]] + (atomic_mass[["O"]] * 3),
  NH3 = atomic_mass[["N"]] + (atomic_mass[["H"]] * 3),
  NH4 = atomic_mass[["N"]] + (atomic_mass[["H"]] * 4),
  NO2 = atomic_mass[["N"]] + (atomic_mass[["O"]] * 2),
  NO3 = atomic_mass[["N"]] + (atomic_mass[["O"]] * 3),
  PO4 = atomic_mass[["P"]] + (atomic_mass[["O"]] * 4),
  SO4 = atomic_mass[["S"]] + (atomic_mass[["O"]] * 4),
  SiO2 = atomic_mass[["Si"]] + (atomic_mass[["O"]] * 2)
)
conversion_rules <- tibble::tribble(
  ~target, ~source, ~operation, ~amount,
  "alkalinity_ueq_L", "alkalinity_uM", "keep", NA_real_,
  "alkalinity_ueq_L", "alkalinity_meq_L", "multiply", 1000,
  "alkalinity_ueq_L", "alkalinity_ug_L", "divide", 50.04345,
  "alkalinity_ueq_L", "alkalinity_mg_CaCO3_L", "multiply_1000_then_divide", 50.04345,
  "conductivity_uS_cm", "conductivity_mS_m", "multiply", 10,
  "al_uM", "al_mg_L", "divide_then_1000", atomic_mass[["Al"]],
  "al_uM", "al_ug_L", "divide", atomic_mass[["Al"]],
  "br_uM", "br_mg_L", "divide_then_1000", atomic_mass[["Br"]],
  "ca_uM", "ca_mg_L", "divide_then_1000", atomic_mass[["Ca"]],
  "ca_uM", "ca_ug_L", "divide", atomic_mass[["Ca"]],
  "cl_uM", "cl_mg_L", "divide_then_1000", atomic_mass[["Cl"]],
  "cl_uM", "cl_ug_L", "divide", atomic_mass[["Cl"]],
  "doc_uM", "doc_mg_C_L", "divide_then_1000", atomic_mass[["C"]],
  "doc_uM", "doc_ug_C_L", "divide", atomic_mass[["C"]],
  "dic_uM", "dic_mg_C_L", "divide_then_1000", atomic_mass[["C"]],
  "do_uM", "do_mg_L", "divide_then_1000", atomic_mass[["O"]] * 2,
  "dsi_uM", "dsi_mg_Si_L", "divide_then_1000", atomic_mass[["Si"]],
  "dsi_uM", "dsi_ug_Si_L", "divide", atomic_mass[["Si"]],
  "dsi_uM", "dsi_mg_SiO2_L", "divide_then_1000", molecule_mass[["SiO2"]],
  "dsi_uM", "sio2_ug_SiO2_L", "divide", molecule_mass[["SiO2"]],
  "f_uM", "f_mg_L", "divide_then_1000", atomic_mass[["F"]],
  "fe_uM", "fe_mg_L", "divide_then_1000", atomic_mass[["Fe"]],
  "fe_uM", "fe_ug_L", "divide", atomic_mass[["Fe"]],
  "hco3_uM", "hco3_mg_L", "divide_then_1000", molecule_mass[["HCO3"]],
  "k_uM", "k_mg_L", "divide_then_1000", atomic_mass[["K"]],
  "k_uM", "k_ug_L", "divide", atomic_mass[["K"]],
  "k_uM", "potassium_ug_L", "divide", atomic_mass[["K"]],
  "li_uM", "li_mg_L", "divide_then_1000", atomic_mass[["Li"]],
  "mg_uM", "mg_mg_L", "divide_then_1000", atomic_mass[["Mg"]],
  "mg_uM", "mg_ug_L", "divide", atomic_mass[["Mg"]],
  "mn_uM", "mn_mg_L", "divide_then_1000", atomic_mass[["Mn"]],
  "mn_uM", "mn_ug_L", "divide", atomic_mass[["Mn"]],
  "na_uM", "na_mg_L", "divide_then_1000", atomic_mass[["Na"]],
  "na_uM", "na_ug_L", "divide", atomic_mass[["Na"]],
  "na_uM", "sodium_ug_L", "divide", atomic_mass[["Na"]],
  "nh3_uM", "nh3_mg_NH3_N_L", "divide_then_1000", atomic_mass[["N"]],
  "nh3_uM", "nh3_mg_NH3_L", "divide_then_1000", molecule_mass[["NH3"]],
  "nh4_uM", "nh4_mg_NH4_N_L", "divide_then_1000", atomic_mass[["N"]],
  "nh4_uM", "nh4_ug_NH4_N_L", "divide", atomic_mass[["N"]],
  "nh4_uM", "nh4_mg_NH4_L", "divide_then_1000", molecule_mass[["NH4"]],
  "nhx_uM", "nhx_mg_NH4_N_L", "divide_then_1000", atomic_mass[["N"]],
  "nhx_uM", "nhx_mg_N_L", "divide_then_1000", atomic_mass[["N"]],
  "nhx_uM", "nhx_ug_N_L", "divide", atomic_mass[["N"]],
  "no3_uM", "no3_mg_NO3_L", "divide_then_1000", molecule_mass[["NO3"]],
  "no3_uM", "no3_ug_NO3_L", "divide", molecule_mass[["NO3"]],
  "no3_uM", "no3_mg_NO3_N_L", "divide_then_1000", atomic_mass[["N"]],
  "no3_uM", "no3_mg_N_L", "divide_then_1000", atomic_mass[["N"]],
  "no3_uM", "no3_ug_NO3_N_L", "divide", atomic_mass[["N"]],
  "nox_uM", "nox_mg_NO3_N_L", "divide_then_1000", atomic_mass[["N"]],
  "nox_uM", "nox_ug_NO3_N_L", "divide", atomic_mass[["N"]],
  "no2_uM", "no2_mg_NO2_N_L", "divide_then_1000", atomic_mass[["N"]],
  "po4_uM", "po4_mg_PO4_L", "divide_then_1000", molecule_mass[["PO4"]],
  "po4_uM", "po4_mg_P_L", "divide_then_1000", atomic_mass[["P"]],
  "po4_uM", "po4_p_ug_PO4_P_L", "divide", atomic_mass[["P"]],
  "po4_uM", "po4_ug_PO4_P_L", "divide", atomic_mass[["P"]],
  "so4_uM", "so4_ug_SO4_L", "divide", molecule_mass[["SO4"]],
  "so4_uM", "so4_ug_S_L", "divide", atomic_mass[["S"]],
  "so4_uM", "so4_mg_SO4_L", "divide_then_1000", molecule_mass[["SO4"]],
  "so4_uM", "so4_mg_S_L", "divide_then_1000", atomic_mass[["S"]],
  "sr_uM", "sr_mg_L", "divide_then_1000", atomic_mass[["Sr"]],
  "sr_uM", "sr_ug_L", "divide", atomic_mass[["Sr"]],
  "srp_uM", "srp_mg_P_L", "divide_then_1000", atomic_mass[["P"]],
  "srp_uM", "srp_ug_P_L", "divide", atomic_mass[["P"]],
  "tdn_uM", "tdn_mg_L", "divide_then_1000", atomic_mass[["N"]],
  "tdn_uM", "tdn_mg_N_L", "divide_then_1000", atomic_mass[["N"]],
  "tdn_uM", "tdn_ug_N_L", "divide", atomic_mass[["N"]],
  "tkn_uM", "tkn_mg_N_L", "divide_then_1000", atomic_mass[["N"]],
  "tn_uM", "tn_mg_L", "divide_then_1000", atomic_mass[["N"]],
  "tn_uM", "tn_ug_L", "divide", atomic_mass[["N"]],
  "tn_uM", "tn_ug_N_L", "divide", atomic_mass[["N"]],
  "don_uM", "don_mg_L", "divide_then_1000", atomic_mass[["N"]],
  "toc_uM", "toc_mg_C_L", "divide_then_1000", atomic_mass[["C"]],
  "tp_uM", "tp_mg_P_L", "divide_then_1000", atomic_mass[["P"]],
  "tp_uM", "tp_mg_L", "divide_then_1000", atomic_mass[["P"]],
  "tp_uM", "tp_ug_P_L", "divide", atomic_mass[["P"]],
  "tdp_uM", "tdp_mg_P_L", "divide_then_1000", atomic_mass[["P"]],
  "tdp_uM", "tdp_mg_L", "divide_then_1000", atomic_mass[["P"]],
  "tdp_uM", "tdp_ug_P_L", "divide", atomic_mass[["P"]],
  "zn_uM", "zn_mg_L", "divide_then_1000", atomic_mass[["Zn"]],
  "zn_uM", "zn_ug_L", "divide", atomic_mass[["Zn"]]
)

# ---- reviewed overlap choices ----
drop_no3_lters <- "Krycklan"
drop_nox_streams <- "Biscuit Brook"
drop_no3_streams <- c(
  "Frio River",
  "Cane Creek",
  "Merced River",
  "Turkey River",
  "James River",
  "Corral Gulch",
  "Flat Brook",
  "Choptank River",
  "Green River",
  "Smith Creek",
  "Sopchoppy River"
)
drop_camels_no3_streams <- c(
  "North Slyamore Creek",
  "Vallecito Creek",
  "West Clear Creek"
)
drop_srp_lters <- "Krycklan"
drop_srp_streams <- c("MGWEIR", "ORlow")

# prefer the better documented source for the same site, date, variable, and unit
preferred_source_rules <- tibble::tribble(
  ~preferred_file, ~superseded_file, ~variables, ~expected_overlap,
  paste0(
    "Canada__Canada__ALSEK_RIVER_ABOVE_BATES_RIVER_IN_",
    "KLUANE_NATIONAL_PARK__b577690976a4.csv"
  ),
  "Canada_WQ_dat.csv",
  "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 570L,
  paste0(
    "Canada__Canada__BEAVER_RIVER_ABOVE_HIGHWAY_1_IN_GLACIER_",
    "NATIONAL_PARK__8db5e916b53d.csv"
  ),
  "Canada_WQ_dat.csv",
  "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 1661L,
  paste0(
    "Canada__Canada__KICKING_HORSE_RIVER_AT_FIELD_IN_YOHO_",
    "NATIONAL_PARK__78eed20cf9d6.csv"
  ),
  "Canada_WQ_dat.csv",
  "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 2058L,
  paste0(
    "Canada__Canada__KOOTENAY_RIVER_ABOVE_HIGHWAY_93_IN_",
    "KOOTENAY_NATIONAL_PARK__7a1cbddf7523.csv"
  ),
  "Canada_WQ_dat.csv",
  "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 2313L,
  "Canada__Canada__LIARD_RIVER_AT_UPPER_CROSSING__47916feca4a8.csv",
  "Canada_WQ_dat.csv",
  "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 1279L,
  "Canada__Canada__SKEENA_RIVER_AT_USK__6692e2a39888.csv",
  "Canada_WQ_dat.csv",
  "NHX;NO3;TN;TP;dissolved org C;tdp;tot dissolved N", 2317L,
  "MD__MD__Lock_5__092e7030faa8.csv",
  "MurrayDarlingChem_Merged.csv", NA_character_, 16882L,
  "MD__MD__Merbein__9df18bcf9b24.csv",
  "MurrayDarlingChem_Merged.csv", NA_character_, 15025L,
  "USGS_geogenic_solutes.csv", "20221030_masterdata_chem_V2.csv",
  "dissolved org C", 14285L,
  "MCM_Chem_clean.csv", "20221030_masterdata_chem_V2.csv",
  "alkalinity;Ca;Cl;DSi;K;Mg;Na;SO4", 7612L,
  "USGS__USGS__SUSQUEHANNA_RIVER__4db2531edf53.csv",
  "20221030_masterdata_chem_V2.csv",
  "dissolved org C;DSi;NHX;NOx;PO4;tot dissolved N;TP", 6171L,
  "USGS_geogenic_solutes.csv",
  "USGS__USGS__SUSQUEHANNA_RIVER__4db2531edf53.csv",
  "Ca;Cl;dissolved org C;K;Mg;Na;SO4", 3831L,
  "Swedish_Geo_Updated_10062025.csv", "SiSyn_DataTemplate_Sweden_102423.csv",
  "Cl", 3418L,
  "USGS_geogenic_solutes.csv",
  "USGS__USGS__Choptank_River__da9275d27028.csv",
  "Ca;Cl;dissolved org C;K;Mg;Na;SO4", 2457L,
  "EastRiverSFA_chem.csv", "20221030_masterdata_chem_V2.csv",
  "Cl;NO3;PO4;SO4;tot dissolved N", 0L,
  "USGS_geogenic_solutes.csv",
  "USGS__USGS__Frio_River__4e8764089639.csv",
  "Ca;Cl;dissolved org C;K;Mg;Na;SO4", 2044L,
  "CAMELS_USGS_N_P.csv",
  "USGS__USGS__Choptank_River__da9275d27028.csv",
  "NO3;NOx;PO4", 1769L,
  "USGS__USGS__Mississippi_River_above_Vicksburg__d2360d259841.csv",
  "USGS_NWQA_Chemistry_MissRiverSites.csv", NA_character_, 1453L,
  "Sweden__Sweden__Nykopingsan_Spanga__2cdeeea43708.csv",
  "SiSyn_DataTemplate_Sweden_102423.csv",
  "Cl;DSi;NH4;NOx;PO4;tot org C", 939L,
  "Sweden__Sweden__Ljungan_Skallboleforsen__c0d37822b53a.csv",
  "SiSyn_DataTemplate_Sweden_102423.csv",
  "Cl;DSi;NH4;NOx;PO4;tot org C", 929L,
  "Sweden__Sweden__Orekilsalven_Munkedal__96dcdaa96ada.csv",
  "SiSyn_DataTemplate_Sweden_102423.csv",
  "Cl;DSi;NH4;NOx;PO4;tot org C", 926L,
  "UK_Si.csv", "UK__UK__BLACKWATER_AT_LANGFORD__e4ddc46f9d7c.csv",
  "DSi", 847L,
  "UK_Si.csv", "UK__UK__CHELMER_AT_RUSHES_LOCK__f7addd85ee9b.csv",
  "DSi", 847L,
  "USGS__USGS__Choptank_River__da9275d27028.csv",
  "20221030_masterdata_chem_V2.csv", "DSi", 726L,
  "Krycklan_NP.csv", "20221030_masterdata_chem_V2.csv", "NH4", 403L,
  "USGS_geogenic_solutes.csv", "USGS_NWQA_Chemistry_MissRiverSites.csv",
  "dissolved org C", 400L,
  "CAMELS_USGS_N_P.csv", "USGS__USGS__Frio_River__4e8764089639.csv",
  "NO3;NOx;PO4", 288L,
  "CAMELS_USGS_N_P.csv", "20221030_masterdata_chem_V2.csv",
  "NOx", 1879L,
  "UMR_si_update_existing_sites.csv", "20221030_masterdata_chem_V2.csv",
  NA_character_, 136L,
  "USGS__USGS__Frio_River__4e8764089639.csv",
  "20221030_masterdata_chem_V2.csv", "DSi", 84L
)
niva_preferred_files <- c(
  "Norway_NIVA_AllSites__NIVA__AAGEVEG__19eac75d0edc.csv",
  "Norway_NIVA_AllSites__NIVA__FINETAN__bcbb2dcfe9fb.csv",
  "Norway_NIVA_AllSites__NIVA__FINEPAS__da624d256e41.csv",
  "Norway_NIVA_AllSites__NIVA__HOREVOS__756e629d446e.csv",
  "Norway_NIVA_AllSites__NIVA__MROEDRI__76754047396e.csv",
  "Norway_NIVA_AllSites__NIVA__OSLEALN__9abcb2378cbb.csv",
  "Norway_NIVA_AllSites__NIVA__ROGEBJE__9eabc44c847b.csv",
  "Norway_NIVA_AllSites__NIVA__ROGEVIK__455000df4ac3.csv",
  "Norway_NIVA_AllSites__NIVA__SFJENAU__44ddce831990.csv",
  "Norway_NIVA_AllSites__NIVA__STRENID__a56da145d7a8.csv"
)
preferred_source_rules <- dplyr::bind_rows(
  preferred_source_rules,
  tibble::tibble(
    preferred_file = niva_preferred_files,
    superseded_file = "20221030_masterdata_chem_V2.csv",
    variables = "DSi",
    expected_overlap = c(61L, 61L, 60L, 60L, 60L, 60L, 59L, 60L, 59L, 59L)
  )
)
expected_superseded_rows <- 93646L

# ---- reviewed unmatched sites ----
reviewed_unmatched_sites <- tibble::tribble(
  ~Raw_Filename, ~Stream_Name, ~reason,
  "CatalinaJemez_chemistry_2009-2019_V2.csv", "MG_S_SEEP", "not_river",
  "WesternAus_AllSites_Filtered_v2.csv", "Collie River - Central Collie", "no_dsi",
  "Finnish_riverine_data_07032025.csv", "Kisko 14 Vanhak va6111", "no_dsi",
  "Finnish_riverine_data_07032025.csv", "Uske 16 Salon yp va6101", "no_dsi",
  "Finnish_riverine_data_07032025.csv", "Pajo 44 Isosilta va6301", "no_dsi",
  "Finnish_riverine_data_07032025.csv", "Aura 54 ohikulku va6401", "no_dsi",
  "Finnish_riverine_data_07032025.csv", "Eura 42 Pori-Rma va6900", "no_dsi",
  "Finnish_riverine_data_07032025.csv", "Kojo 35 Pori-Tre", "no_dsi",
  "Finnish_riverine_data_07032025.csv", "Merikarvianjoki Vaadinni", "no_dsi",
  "UK_nonSi_solutes.csv", "EDEN AT SHEEPMOUNT", "no_dsi",
  "UK_nonSi_solutes.csv", "TWEED AT NORHAM", "no_dsi",
  "UK_nonSi_solutes.csv", "ESK AT SLEIGHTS", "no_dsi",
  "UK_nonSi_solutes.csv", "CALDER AT METHLEY", "no_dsi",
  "Krycklan_Cation_Clean.csv", "Site 3", "no_dsi",
  "Krycklan_Cation_Clean.csv", "Site 8", "no_dsi",
  "Krycklan_Cation_Clean.csv", "Site 11", "no_dsi",
  "Krycklan_Cation_Clean.csv", "Site 12", "no_dsi",
  "Krycklan_Cation_Clean.csv", "Site 15", "no_dsi",
  "Krycklan_Cation_Clean.csv", "Site 17", "no_dsi",
  "Krycklan_Cation_Clean.csv", "Site 18", "no_dsi",
  "Krycklan_Cation_Clean.csv", "Site 19", "no_dsi",
  "Krycklan_Cation_Clean.csv", "Site 20", "no_dsi",
  "Krycklan_Cation_Clean.csv", "Site 21", "no_dsi"
)

# ---- date review ----
# inferred dates keep a flag in the master and an explanation in its metadata
catalina_date_note <- tibble::tibble(
  Dataset = "CZO-Catalina Jemez",
  Raw_Filename = "CatalinaJemez_chemistry_2009-2019_V2.csv",
  LTER = "Catalina Jemez",
  Stream_Name = "MGWEIR",
  date = as.Date("2018-04-19"),
  Date_Flag = "inferred",
  Date_Note = paste0(
    "Source sample AZ_S1793 had no usable provider date (#VALUE!); ",
    "2018-04-19 was inferred from paired sample AZ_S1792 and the ",
    "surrounding weekly sampling sequence."
  )
)
cameroon_date_notes <- tibble::tribble(
  ~Stream_Name, ~workbook_row,
  "Mbalmayo", 514L,
  "Messam", 495L,
  "Nsimi_outlet", 515L,
  "Nsimi_spring", 547L,
  "Olama", 511L,
  "Pont_So'o", 540L
) |>
  dplyr::transmute(
    Dataset = "Cameroon",
    Raw_Filename = "Chem_Cameroon.csv",
    LTER = "Cameroon",
    Stream_Name,
    date = as.Date("2022-06-16"),
    Date_Flag = "inferred",
    Date_Note = paste0(
      "Provider workbook row ", workbook_row,
      " reports 2021-06-16; date set to 2022-06-16 because the row ",
      "falls between 2022-05-31 and 2022-07-06."
    )
  )
date_notes <- dplyr::bind_rows(
  catalina_date_note,
  cameroon_date_notes
)
camrex_manacapuru_dates <- as.Date(c(
  "1982-05-12", "1982-08-26", "1982-12-07", "1983-03-30",
  "1983-07-06", "1983-11-05", "1984-02-25", "1984-07-22",
  "1985-08-11", "1988-12-02", "1990-05-01", "1991-08-30"
))
camrex_date_note <- paste0(
  "Date shifted back one day because the provider defines Julian day 1 as ",
  "January 1, 1982; an earlier conversion treated that date as day 0."
)
camrex_corrected_events <- 182L
camrex_corrected_rows <- 2059L

# ---- flags and daily groups ----
allowed_value_qualifiers <- c("<", ">")
allowed_date_flags <- c("corrected", "inferred", "restored")
allowed_source_lab_flags <- c(
  "above reported limit",
  "above-limit results excluded from daily mean",
  "reported value below historical detection limit",
  "provider < flag; meaning not confirmed",
  "estimated value"
)
allowed_source_lab_flags <- c(allowed_source_lab_flags,
  paste0(setdiff(allowed_source_lab_flags, "estimated value"), "; estimated value"))
valid_lab_flags <- allowed_source_lab_flags
# method flags and notes come from the shared workflow-rules lookup

# ---- exact exclusions ----
source_method_exclusions <- tibble::tibble(
  source_method_flag = c(
    "ECCC SiO2 method 2329, VMV 14108",
    "ECCC SiO2 method 2328, VMV 14109",
    "BNZ Si fraction not documented"
  ),
  allowed_counts = list(0L, 0L, 243L)
)
canada_extractable_filename <- "Canada_WQ_dat.csv"
canada_extractable_label <- "SILICON EXTRACTABLE"
canada_extractable_count <- 1549L
hbr_undocumented_si_count <- 3L
walker_lab_code_columns <- c(
  Al = "al_lab_code",
  Fe = "fe_lab_code",
  K = "k_lab_code",
  Mn = "mn_lab_code",
  Sr = "sr_lab_code",
  Zn = "zn_lab_code"
)
arctic_lake_events <- tibble::tribble(
  ~LTER, ~Stream_Name, ~date,
  "ARC", "I Swamp Outlet", as.Date("2012-08-08"),
  "ARC", "I1 Outlet", as.Date("2012-08-07"),
  "ARC", "I2 Outlet", as.Date("2012-08-07"),
  "ARC", "I3 Outlet", as.Date("2012-08-07"),
  "ARC", "I4 Outlet", as.Date("2012-08-07"),
  "ARC", "I5 Outlet", as.Date("2012-08-07"),
  "ARC", "I6 Outlet", as.Date("2012-08-08"),
  "ARC", "I7 Outlet", as.Date("2012-08-08"),
  "ARC", "I8 Outlet", as.Date("2012-08-08"),
  "ARC", "Toolik Outlet", as.Date("2012-06-04")
)
arctic_lake_measurement_count <- 177L
flowing_types <- c("River", "Creek", "Stream", "Brook")

# ---- source review counts ----
east_river_source_counts <- c(
  ammonia_rows = 1114L,
  nondetect_zeros = 32L,
  missing_codes = 1L
)
east_river_nondetect_value <- "<0.0077"

# ---- expected output ----
# the seine fraction check removes 482 daily si rows and no complete sites
# gems fills reviewed chemistry gaps at existing sites without adding sites
expected_release_counts <- c(
  corrected_date_rows = 2059L,
  inferred_date_rows = 79L,
  restored_date_rows = 0L,
  greater_than_rows = 43L,
  final_rows = 4686420L,
  final_sites = 1170L,
  final_silicon_sites = 1170L,
  final_silicon_rows = 296848L
)
