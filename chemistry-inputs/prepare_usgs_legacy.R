# usgs and older combined-master repairs
# loaded automatically by the other-source preparation script
# uses the same paths and shared functions and keeps the repair order unchanged

# ---- usgs total nitrogen rebuild ----

build_usgs_total_nitrogen_replacement <- function(legacy_data) {
  legacy_filename <- "20221030_masterdata_chem_V2.csv"
  nitrogen_codes <- c("00600", "62855", "71887")
  dissolved_nitrogen_codes <- c("00602", "62854")
  all_codes <- c(nitrogen_codes, dissolved_nitrogen_codes)

  site_reference <- read_source(
    "Site_Reference_Table.csv",
    directory = key_dir
  )
  site_lookup <- data.table::as.data.table(site_reference[
    site_reference$LTER == "USGS",
    c(
      "Stream_Name", "Alt_Stream_Name", "Original_Stream_Name",
      "USGSGageNumber"
    ),
    drop = FALSE
  ])
  site_lookup[, location_id := paste0("USGS-", USGSGageNumber)]

  if (
    any(is_blank(site_lookup$Stream_Name)) ||
      any(is_blank(site_lookup$USGSGageNumber)) ||
      anyDuplicated(site_lookup$Stream_Name) > 0L ||
      anyDuplicated(site_lookup$location_id) > 0L
  ) {
    stop("the reviewed USGS site-id mapping is not one-to-one")
  }

  alternate_names <- strsplit(
    ifelse(is_blank(site_lookup$Alt_Stream_Name), "", site_lookup$Alt_Stream_Name),
    ";",
    fixed = TRUE
  )
  alternate_lookup <- data.table::data.table(
    source_site = trimws(unlist(alternate_names, use.names = FALSE)),
    location_id = rep(site_lookup$location_id, lengths(alternate_names))
  )
  original_lookup <- site_lookup[
    !is_blank(Original_Stream_Name),
    .(source_site = trimws(Original_Stream_Name), location_id)
  ]
  source_site_lookup <- unique(data.table::rbindlist(list(
    site_lookup[, .(
      source_site = Stream_Name,
      location_id
    )],
    alternate_lookup,
    original_lookup
  )))
  source_site_lookup <- source_site_lookup[!is_blank(source_site)]
  alias_conflicts <- source_site_lookup[
    , .(location_ids = data.table::uniqueN(location_id)),
    by = source_site
  ][location_ids > 1L]
  if (nrow(alias_conflicts) > 0L) {
    stop("a USGS site alias maps to more than one station id")
  }

  deduplicated_source <- read_source(
    "Deduplicated_Sites_AllSites_chemistry.csv",
    directory = input_dir
  )
  deduplicated_bad_tn <- find_tds_labeled_tn(deduplicated_source)
  deduplicated_scope <- data.table::as.data.table(deduplicated_source[
    deduplicated_bad_tn,
    c("Stream_Name", "date"),
    drop = FALSE
  ])
  data.table::setnames(deduplicated_scope, "Stream_Name", "source_site")
  deduplicated_scope[, date := data.table::as.IDate(date)]
  deduplicated_scope <- merge(
    deduplicated_scope,
    source_site_lookup,
    by = "source_site",
    all.x = TRUE
  )

  if (anyNA(deduplicated_scope$location_id)) {
    stop("a deduplicated TDS-to-TN row lacks a station id")
  }
  assert_count(nrow(deduplicated_scope), 0L,
               "deduplicated TDS-to-TN provider scope")
  assert_count(data.table::uniqueN(deduplicated_scope$source_site), 0L,
               "deduplicated TDS-to-TN sites")
  assert_count(
    data.table::uniqueN(deduplicated_scope[, .(location_id, date)]),
    0L,
    "deduplicated TDS-to-TN dates"
  )

  current_rows <- which(
    legacy_data$LTER == "USGS" &
      legacy_data$variable == "TN" &
      legacy_data$units == "uM"
  )
  current_scope <- data.table::data.table(
    legacy_row_id = legacy_data[[1L]][current_rows],
    source_site = legacy_data$Site.Stream.Name[current_rows],
    date = data.table::as.IDate(legacy_data$Sampling.Date[current_rows])
  )
  current_scope <- merge(
    current_scope,
    source_site_lookup,
    by = "source_site",
    all.x = TRUE
  )

  if (anyNA(current_scope$location_id)) {
    stop("a residual legacy USGS total-nitrogen row lacks a station id")
  }

  exclusions <- data.table::as.data.table(readRDS(usgs_exclusion_file))
  exclusion_scope <- unique(exclusions[
    source_file == legacy_filename,
    .(source_row, location_id, date = data.table::as.IDate(date))
  ])

  assert_count(nrow(current_scope), 13497L,
               "residual legacy USGS total-nitrogen scope")
  assert_count(data.table::uniqueN(exclusion_scope$source_row), 8625L,
               "excluded legacy USGS total-nitrogen scope")

  current_keys <- unique(current_scope[, .(location_id, date)])
  exclusion_keys <- unique(exclusion_scope[, .(location_id, date)])
  deduplicated_keys <- unique(deduplicated_scope[, .(location_id, date)])
  assert_count(nrow(merge(
    current_keys,
    exclusion_keys,
    by = c("location_id", "date")
  )), 0L, "overlapping legacy USGS total-nitrogen scopes")
  assert_count(nrow(merge(
    data.table::rbindlist(list(current_keys, exclusion_keys)),
    deduplicated_keys,
    by = c("location_id", "date")
  )), 0L, "overlapping deduplicated and legacy total-nitrogen scopes")

  scope_keys <- unique(data.table::rbindlist(list(
    current_keys,
    exclusion_keys,
    deduplicated_keys
  )))
  assert_count(nrow(scope_keys), 22122L,
               "original legacy USGS total-nitrogen dates")
  assert_count(data.table::uniqueN(scope_keys$location_id), 96L,
               "original legacy USGS total-nitrogen sites")

  provider_columns <- c(
    "Location_Identifier",
    "Activity_ActivityIdentifier",
    "Activity_StartDate",
    "Result_ResultDetectionCondition",
    "Result_Characteristic",
    "Result_CharacteristicUserSupplied",
    "Result_MethodSpeciation",
    "Result_SampleFraction",
    "Result_MeasureIdentifier",
    "Result_Measure",
    "Result_MeasureUnit",
    "Result_MeasureQualifierCode",
    "Result_MeasureStatusIdentifier",
    "DetectionLimit_TypeA",
    "DetectionLimit_MeasureA",
    "DetectionLimit_MeasureUnitA",
    "DetectionLimit_TypeB",
    "DetectionLimit_MeasureB",
    "DetectionLimit_MeasureUnitB",
    "USGSpcode"
  )

  read_provider_file <- function(path) {
    provider_rows <- data.table::fread(
      path,
      select = provider_columns,
      colClasses = "character",
      showProgress = FALSE
    )
    provider_rows <- provider_rows[USGSpcode %in% all_codes]
    provider_rows[, provider_file := basename(path)]
    provider_rows
  }

  provider_file_rows <- data.table::rbindlist(
    lapply(c(usgs_provider_files, usgs_clinton_file), read_provider_file),
    use.names = TRUE,
    fill = TRUE
  )
  identity_columns <- setdiff(names(provider_file_rows), "provider_file")
  provider <- provider_file_rows[
    , .(provider_files = paste(sort(unique(provider_file)), collapse = ";")),
    by = identity_columns
  ]
  data.table::setnames(
    provider,
    c(
      "Location_Identifier",
      "Activity_StartDate",
      "Result_Measure",
      "Result_MeasureUnit"
    ),
    c("location_id", "date", "provider_value", "provider_unit")
  )
  provider[, date := data.table::as.IDate(date)]
  provider[, provider_value_text := provider_value]
  provider[, limit_a_text := DetectionLimit_MeasureA]
  provider[, limit_b_text := DetectionLimit_MeasureB]
  provider[, provider_value := suppressWarnings(as.numeric(provider_value))]
  provider[, limit_a := suppressWarnings(as.numeric(DetectionLimit_MeasureA))]
  provider[, limit_b := suppressWarnings(as.numeric(DetectionLimit_MeasureB))]

  code_key <- data.table::data.table(
    USGSpcode = c("00600", "62855", "71887", "00602", "62854"),
    variable = c("TN", "TN", "TN", "TDN", "TDN"),
    basis = c("N", "N", "NO3", "N", "N"),
    code_priority = c(2L, 1L, 3L, 2L, 1L)
  )
  # use official usgs parameter codes to keep tn, tdn, and tds identities separate
  provider <- merge(provider, code_key, by = "USGSpcode")

  nitrogen_molecular_weight <- 14.0067
  nitrate_molecular_weight <- nitrogen_molecular_weight + 15.9994 * 3
  provider[, value_uM := NA_real_]
  provider[basis == "N" & provider_unit == "mg/L", value_uM :=
    provider_value * 1000 / nitrogen_molecular_weight]
  provider[basis == "NO3" & provider_unit == "mg/L", value_uM :=
    provider_value * 1000 / nitrate_molecular_weight]
  provider[, limit_a_uM := NA_real_]
  provider[basis == "N" & DetectionLimit_MeasureUnitA == "mg/L", limit_a_uM :=
    limit_a * 1000 / nitrogen_molecular_weight]
  provider[basis == "NO3" & DetectionLimit_MeasureUnitA == "mg/L", limit_a_uM :=
    limit_a * 1000 / nitrate_molecular_weight]
  provider[, limit_b_uM := NA_real_]
  provider[basis == "N" & DetectionLimit_MeasureUnitB == "mg/L", limit_b_uM :=
    limit_b * 1000 / nitrogen_molecular_weight]
  provider[basis == "NO3" & DetectionLimit_MeasureUnitB == "mg/L", limit_b_uM :=
    limit_b * 1000 / nitrate_molecular_weight]
  provider[, threshold_uM := data.table::fcoalesce(limit_a_uM, limit_b_uM)]
  provider[, detected := !is.na(value_uM)]
  provider[, censored :=
    !detected &
      Result_ResultDetectionCondition == "Not Detected" &
      !is.na(threshold_uM)]
  provider[, above_limit :=
    !detected &
      Result_ResultDetectionCondition ==
        "Present Above Quantification Limit" &
      !is.na(threshold_uM)]
  provider[, unresolved := !detected & !censored & !above_limit]

  scoped <- merge(
    provider,
    scope_keys,
    by = c("location_id", "date")
  )
  expected_scoped_codes <- c(
    "00600" = 15998L,
    "00602" = 13348L,
    "62854" = 9831L,
    "62855" = 1068L,
    "71887" = 1794L
  )
  observed_scoped_codes <- table(factor(
    scoped$USGSpcode,
    levels = names(expected_scoped_codes)
  ))
  if (!identical(as.integer(observed_scoped_codes),
                 unname(expected_scoped_codes))) {
    stop(
      "the scoped USGS total-nitrogen parameter-code counts changed: ",
      paste(names(observed_scoped_codes), observed_scoped_codes, collapse = ", ")
    )
  }
  assert_count(sum(scoped$unresolved), 0L,
               "unresolved provider total-nitrogen representations")
  assert_count(sum(
    scoped$Result_ResultDetectionCondition ==
      "Present Above Quantification Limit"
  ), 2L, "above-quantification provider representations")
  assert_count(sum(scoped$above_limit), 2L,
               "usable above-quantification provider representations")
  assert_count(sum(
    scoped$detected &
      !is_blank(scoped$Result_ResultDetectionCondition)
  ), 0L, "numeric USGS total-nitrogen rows with censor conditions")
  assert_count(sum(
    !is_blank(scoped$Result_MeasureQualifierCode)
  ), 0L, "unexpected USGS total-nitrogen measure qualifiers")
  supported_detection_condition <-
    is_blank(scoped$Result_ResultDetectionCondition) |
    scoped$Result_ResultDetectionCondition %in% c(
      "Not Detected",
      "Present Above Quantification Limit"
    )
  assert_count(sum(!supported_detection_condition), 0L,
               "unsupported USGS total-nitrogen detection conditions")

  if (any(is_blank(scoped$Activity_ActivityIdentifier))) {
    stop("a scoped USGS total-nitrogen row lacks an activity id")
  }
  scoped[, best_priority := min(code_priority), by = .(
    variable,
    location_id,
    date,
    Activity_ActivityIdentifier
  )]
  selected <- scoped[code_priority == best_priority]

  expected_selected_codes <- c(
    "00600" = 15411L,
    "00602" = 4629L,
    "62854" = 9831L,
    "62855" = 1068L,
    "71887" = 0L
  )
  observed_selected_codes <- table(factor(
    selected$USGSpcode,
    levels = names(expected_selected_codes)
  ))
  if (!identical(as.integer(observed_selected_codes),
                 unname(expected_selected_codes))) {
    stop(
      "the preferred USGS total-nitrogen code counts changed: ",
      paste(
        names(observed_selected_codes),
        observed_selected_codes,
        collapse = ", "
      )
    )
  }
  assert_count(sum(selected$unresolved), 0L,
               "unresolved selected USGS total-nitrogen rows")

  selected_above_limit <- selected[above_limit == TRUE]
  assert_count(nrow(selected_above_limit), 1L,
               "selected above-quantification total-nitrogen rows")
  if (
    selected_above_limit$location_id != "USGS-07241550" ||
      selected_above_limit$date != data.table::as.IDate("1978-11-15") ||
      selected_above_limit$USGSpcode != "00600" ||
      selected_above_limit$Activity_ActivityIdentifier !=
        "65602254-5132-49e1-8ea5-5e635eb2c345" ||
      selected_above_limit$Result_MeasureIdentifier !=
        "db18689f-699f-418d-98a7-b3f3e427e224" ||
      selected_above_limit$DetectionLimit_TypeA != "Censoring Level" ||
      selected_above_limit$DetectionLimit_MeasureUnitA != "mg/L"
  ) {
    stop("the reviewed above-quantification total-nitrogen row changed")
  }
  assert_values(
    selected_above_limit$limit_a,
    22,
    "above-quantification total-nitrogen source limit"
  )
  assert_values(
    selected_above_limit$threshold_uM,
    22 * 1000 / nitrogen_molecular_weight,
    "above-quantification total-nitrogen converted limit"
  )

  selected <- selected[unresolved == FALSE]
  assert_count(nrow(selected[variable == "TN"]), 16479L,
               "selected USGS total-nitrogen provider rows")
  assert_count(nrow(selected[variable == "TDN"]), 14460L,
               "selected USGS dissolved-total-nitrogen provider rows")
  assert_count(sum(selected$variable == "TN" & selected$censored), 796L,
               "selected censored USGS total-nitrogen rows")
  assert_count(sum(selected$variable == "TDN" & selected$censored), 518L,
               "selected censored USGS dissolved-total-nitrogen rows")
  assert_count(sum(selected$variable == "TN" & selected$above_limit), 1L,
               "selected above-limit USGS total-nitrogen rows")
  assert_count(
    data.table::uniqueN(selected[variable == "TN"]$location_id),
    85L,
    "selected USGS total-nitrogen sites"
  )
  assert_count(
    data.table::uniqueN(selected[variable == "TDN"]$location_id),
    84L,
    "selected USGS dissolved-total-nitrogen sites"
  )
  assert_count(sum(selected$censored & selected$threshold_uM <= 0), 0L,
               "nonpositive USGS total-nitrogen thresholds")
  assert_count(sum(selected$above_limit & selected$threshold_uM <= 0), 0L,
               "nonpositive USGS total-nitrogen lower limits")

  # detected usgs nitrogen results define the daily mean when the day also has limits
  # all-limit days retain their inequality qualifier
  daily_review <- selected[, {
    if (any(detected)) {
      list(
        value_qualifier = NA_character_,
        detected_rows = sum(detected),
        censored_rows = sum(censored),
        above_limit_rows = sum(above_limit)
      )
    } else if (all(censored)) {
      list(
        value_qualifier = "<",
        detected_rows = 0L,
        censored_rows = sum(censored),
        above_limit_rows = 0L
      )
    } else if (all(above_limit)) {
      list(
        value_qualifier = ">",
        detected_rows = 0L,
        censored_rows = 0L,
        above_limit_rows = sum(above_limit)
      )
    } else {
      stop("mixed USGS total-nitrogen inequality directions remain")
    }
  }, by = .(variable, location_id, date)]
  expected_daily <- data.table::data.table(
    variable = c("TN", "TDN"),
    rows = c(15744L, 14190L),
    detected_days = c(14963L, 13681L),
    censored_days = c(780L, 509L),
    above_limit_days = c(1L, 0L),
    mixed_days = c(9L, 6L)
  )
  observed_daily <- daily_review[, .(
    rows = .N,
    detected_days = sum(is.na(value_qualifier)),
    censored_days = sum(value_qualifier == "<", na.rm = TRUE),
    above_limit_days = sum(value_qualifier == ">", na.rm = TRUE),
    mixed_days = sum(
      detected_rows > 0L & (censored_rows > 0L | above_limit_rows > 0L)
    )
  ), by = variable]
  observed_daily <- observed_daily[match(
    expected_daily$variable,
    observed_daily$variable
  )]
  if (!identical(observed_daily, expected_daily)) {
    stop(
      "the expected USGS total-nitrogen daily results changed: ",
      paste(capture.output(print(observed_daily)), collapse = " ")
    )
  }

  provider_tn_keys <- unique(selected[
    variable == "TN",
    .(location_id, date)
  ])
  matched_legacy_tn <- merge(
    current_scope[, .(legacy_row_id, location_id, date)],
    provider_tn_keys,
    by = c("location_id", "date")
  )
  replaced_legacy_ids <- unique(matched_legacy_tn$legacy_row_id)
  retained_legacy_ids <- setdiff(
    current_scope$legacy_row_id,
    replaced_legacy_ids
  )
  assert_count(length(replaced_legacy_ids), 9124L,
               "legacy total-nitrogen rows with provider replacements")
  assert_count(length(retained_legacy_ids), 4373L,
               "legacy total-nitrogen rows without provider replacements")

  selected <- merge(
    selected,
    site_lookup[, .(location_id, Stream_Name)],
    by = "location_id",
    all.x = TRUE
  )
  if (anyNA(selected$Stream_Name)) {
    stop("a rebuilt USGS total-nitrogen row lacks a reviewed site name")
  }
  data.table::setorder(
    selected,
    location_id,
    date,
    variable,
    USGSpcode,
    Activity_ActivityIdentifier
  )

  selected[, amount_uM := data.table::fifelse(
    detected,
    value_uM,
    threshold_uM
  )]
  value_text <- format(
    selected$amount_uM,
    digits = 15L,
    scientific = FALSE,
    trim = TRUE
  )
  value_text[selected$censored] <- paste0("<", value_text[selected$censored])
  value_text[selected$above_limit] <- paste0(">", value_text[selected$above_limit])

  replacement <- selected[, .(
    V1 = sprintf("USGS_TOTAL_N_%05d", seq_len(.N)),
    variable,
    LTER = "USGS",
    Site.Stream.Name = Stream_Name,
    site = Stream_Name,
    Sampling.Date = as.character(date),
    value = value_text,
    units = "uM",
    USGS.Provider.Parameter.Code = USGSpcode,
    USGS.Provider.Activity.ID = Activity_ActivityIdentifier,
    USGS.Provider.Result.ID = Result_MeasureIdentifier,
    USGS.Provider.Source.Files = provider_files,
    USGS.Provider.Detection.Condition = data.table::fifelse(
      is_blank(Result_ResultDetectionCondition),
      NA_character_,
      Result_ResultDetectionCondition
    ),
    USGS.Provider.Sample.Fraction = Result_SampleFraction,
    USGS.Provider.Measure.Status = Result_MeasureStatusIdentifier,
    USGS.Provider.Characteristic = Result_Characteristic,
    USGS.Provider.Description = Result_CharacteristicUserSupplied,
    USGS.Provider.Method.Speciation = data.table::fifelse(
      is_blank(Result_MethodSpeciation),
      NA_character_,
      Result_MethodSpeciation
    ),
    USGS.Provider.Raw.Value = data.table::fifelse(
      is_blank(provider_value_text),
      NA_character_,
      provider_value_text
    ),
    USGS.Provider.Raw.Unit = data.table::fifelse(
      is_blank(provider_unit),
      NA_character_,
      provider_unit
    ),
    USGS.Provider.Measure.Qualifier = data.table::fifelse(
      is_blank(Result_MeasureQualifierCode),
      NA_character_,
      Result_MeasureQualifierCode
    ),
    USGS.Provider.Detection.Limit.Type.A = data.table::fifelse(
      is_blank(DetectionLimit_TypeA),
      NA_character_,
      DetectionLimit_TypeA
    ),
    USGS.Provider.Detection.Limit.Value.A = data.table::fifelse(
      is_blank(limit_a_text),
      NA_character_,
      limit_a_text
    ),
    USGS.Provider.Detection.Limit.Unit.A = data.table::fifelse(
      is_blank(DetectionLimit_MeasureUnitA),
      NA_character_,
      DetectionLimit_MeasureUnitA
    ),
    USGS.Provider.Detection.Limit.Type.B = data.table::fifelse(
      is_blank(DetectionLimit_TypeB),
      NA_character_,
      DetectionLimit_TypeB
    ),
    USGS.Provider.Detection.Limit.Value.B = data.table::fifelse(
      is_blank(limit_b_text),
      NA_character_,
      limit_b_text
    ),
    USGS.Provider.Detection.Limit.Unit.B = data.table::fifelse(
      is_blank(DetectionLimit_MeasureUnitB),
      NA_character_,
      DetectionLimit_MeasureUnitB
    )
  )]
  data.table::setnames(replacement, "V1", "")

  if (
    nrow(replacement) != 30939L ||
      anyDuplicated(replacement$USGS.Provider.Result.ID) > 0L ||
      sum(startsWith(replacement$value, "<")) != 1314L ||
      sum(startsWith(replacement$value, ">")) != 1L
  ) {
    stop("the rebuilt USGS total-nitrogen provider rows changed")
  }

  replacement <- as.data.frame(replacement, stringsAsFactors = FALSE)
  attr(replacement, "legacy_tn_ids_replaced") <- replaced_legacy_ids
  attr(replacement, "legacy_tn_ids_retained") <- retained_legacy_ids
  replacement
}

# ---- usgs tds backfill ----

build_usgs_tds_backfill <- function() {
  legacy_filename <- "20221030_masterdata_chem_V2.csv"
  tds_codes <- c("00515", "00525", "70300", "70301")

  site_reference <- read_source(
    "Site_Reference_Table.csv",
    directory = key_dir
  )
  site_lookup <- data.table::as.data.table(site_reference[
    site_reference$LTER == "USGS",
    c("Stream_Name", "USGSGageNumber"),
    drop = FALSE
  ])
  site_lookup[, location_id := paste0("USGS-", USGSGageNumber)]

  if (
    any(is_blank(site_lookup$Stream_Name)) ||
      any(is_blank(site_lookup$USGSGageNumber)) ||
      anyDuplicated(site_lookup$Stream_Name) > 0L ||
      anyDuplicated(site_lookup$location_id) > 0L
  ) {
    stop("the reviewed USGS site-id mapping is not one-to-one")
  }

  exclusions <- data.table::as.data.table(readRDS(usgs_exclusion_file))
  required_exclusion_columns <- c(
    "location_id", "date", "provider_result_id", "parameter_code",
    "provider_value", "provider_unit"
  )
  missing_exclusion_columns <- setdiff(
    required_exclusion_columns,
    names(exclusions)
  )
  if (length(missing_exclusion_columns) > 0L) {
    stop(
      "the USGS TDS exclusion review lacks columns: ",
      paste(missing_exclusion_columns, collapse = ", ")
    )
  }

  exact_target <- unique(exclusions[
    !is_blank(provider_result_id),
    .(
      location_id,
      date = data.table::as.IDate(date),
      provider_result_id,
      expected_code = parameter_code,
      expected_value = as.numeric(provider_value),
      expected_unit = provider_unit
    )
  ])

  if (
    nrow(exact_target) != 8835L ||
      data.table::uniqueN(exact_target$provider_result_id) != 8835L ||
      data.table::uniqueN(exact_target, by = c("location_id", "date")) !=
        8621L ||
      data.table::uniqueN(exact_target$location_id) != 26L ||
      anyNA(exact_target$expected_value) ||
      any(exact_target$expected_unit != "mg/L")
  ) {
    stop("the exact-match USGS TDS review scope changed")
  }

  reviewed_susquehanna <- read_source(
    basename(reviewed_susquehanna_file),
    directory = dirname(reviewed_susquehanna_file)
  )
  susquehanna_location_id <- site_lookup[
    Stream_Name == "SUSQUEHANNA RIVER",
    location_id
  ]
  if (length(susquehanna_location_id) != 1L) {
    stop("the reviewed Susquehanna station id changed")
  }
  current_tds_keys <- unique(data.table::data.table(
    location_id = susquehanna_location_id,
    date = data.table::as.IDate(reviewed_susquehanna$date[
      reviewed_susquehanna$LTER == "USGS" &
        reviewed_susquehanna$Stream_Name == "SUSQUEHANNA RIVER" &
        reviewed_susquehanna$solute == "tot dissolved solids" &
        reviewed_susquehanna$solute_units == "mg/L" &
        !is_blank(reviewed_susquehanna$amount)
    ])
  ))
  assert_count(nrow(current_tds_keys), 575L,
               "reviewed Susquehanna TDS dates")

  exact_overlap <- exact_target[
    current_tds_keys,
    on = .(location_id, date),
    nomatch = 0L
  ]
  assert_count(nrow(exact_overlap), 442L,
               "provider-result overlap with reviewed Susquehanna TDS")
  assert_count(
    data.table::uniqueN(exact_overlap, by = c("location_id", "date")),
    420L,
    "date overlap with reviewed Susquehanna TDS"
  )

  exact_target <- exact_target[
    !current_tds_keys,
    on = .(location_id, date)
  ]
  if (
    nrow(exact_target) != 8393L ||
      data.table::uniqueN(exact_target$provider_result_id) != 8393L ||
      data.table::uniqueN(exact_target, by = c("location_id", "date")) !=
        8201L ||
      data.table::uniqueN(exact_target$location_id) != 25L
  ) {
    stop("the missing exact-match USGS TDS scope changed")
  }

  provider_columns <- c(
    "Location_Identifier",
    "Activity_ActivityIdentifier",
    "Activity_StartDate",
    "Result_ResultDetectionCondition",
    "Result_Characteristic",
    "Result_CharacteristicUserSupplied",
    "Result_MethodSpeciation",
    "Result_SampleFraction",
    "Result_MeasureIdentifier",
    "Result_Measure",
    "Result_MeasureUnit",
    "Result_MeasureQualifierCode",
    "Result_MeasureStatusIdentifier",
    "DetectionLimit_TypeA",
    "DetectionLimit_MeasureA",
    "DetectionLimit_MeasureUnitA",
    "DetectionLimit_TypeB",
    "DetectionLimit_MeasureB",
    "DetectionLimit_MeasureUnitB",
    "USGSpcode"
  )

  read_provider_file <- function(path) {
    provider_rows <- data.table::fread(
      path,
      select = provider_columns,
      colClasses = "character",
      showProgress = FALSE
    )
    provider_rows <- provider_rows[USGSpcode %in% tds_codes]
    provider_rows[, provider_file := basename(path)]
    provider_rows
  }

  provider_file_rows <- data.table::rbindlist(
    lapply(c(usgs_provider_files, usgs_clinton_file), read_provider_file),
    use.names = TRUE,
    fill = TRUE
  )
  identity_columns <- setdiff(names(provider_file_rows), "provider_file")
  provider <- provider_file_rows[
    , .(provider_files = paste(sort(unique(provider_file)), collapse = ";")),
    by = identity_columns
  ]
  data.table::setnames(
    provider,
    c(
      "Location_Identifier",
      "Activity_StartDate",
      "Result_Measure",
      "Result_MeasureUnit"
    ),
    c("location_id", "date", "provider_value", "provider_unit")
  )
  provider[, date := data.table::as.IDate(date)]
  provider[, provider_value_text := provider_value]
  provider[, limit_a_text := DetectionLimit_MeasureA]
  provider[, limit_b_text := DetectionLimit_MeasureB]
  provider[, provider_value := suppressWarnings(as.numeric(provider_value))]
  provider[, limit_a := suppressWarnings(as.numeric(DetectionLimit_MeasureA))]
  provider[, limit_b := suppressWarnings(as.numeric(DetectionLimit_MeasureB))]
  provider_output_columns <- names(provider)

  expected_target <- exact_target[, .(
    Result_MeasureIdentifier = provider_result_id,
    expected_location_id = location_id,
    expected_date = date,
    expected_code,
    expected_value,
    expected_unit
  )]
  exact_provider <- merge(
    provider,
    expected_target,
    by = "Result_MeasureIdentifier"
  )

  if (
    nrow(exact_provider) != 8393L ||
      anyDuplicated(exact_provider$Result_MeasureIdentifier) > 0L ||
      length(setdiff(
        expected_target$Result_MeasureIdentifier,
        exact_provider$Result_MeasureIdentifier
      )) > 0L ||
      any(exact_provider$location_id != exact_provider$expected_location_id) ||
      any(exact_provider$date != exact_provider$expected_date) ||
      any(exact_provider$USGSpcode != exact_provider$expected_code) ||
      any(exact_provider$provider_unit != exact_provider$expected_unit) ||
      any(abs(
        exact_provider$provider_value - exact_provider$expected_value
      ) > 1e-10)
  ) {
    stop("the exact provider records for the USGS TDS backfill changed")
  }

  code_priority <- c(
    "70300" = 1L,
    "00515" = 2L,
    "70301" = 3L,
    "00525" = 4L
  )
  exact_provider[, code_priority := unname(code_priority[USGSpcode])]
  if (anyNA(exact_provider$code_priority)) {
    stop("an unsupported USGS TDS parameter code remains")
  }
  exact_provider[, best_priority := min(code_priority), by = .(
    location_id,
    date
  )]
  selected_exact <- exact_provider[code_priority == best_priority]

  expected_exact_codes <- c("70300" = 8206L, "70301" = 2L)
  observed_exact_codes <- table(factor(
    selected_exact$USGSpcode,
    levels = names(expected_exact_codes)
  ))
  exact_daily <- selected_exact[, .(
    rows = .N,
    values = data.table::uniqueN(provider_value),
    methods = data.table::uniqueN(USGSpcode)
  ), by = .(location_id, date)]
  if (
    nrow(selected_exact) != 8208L ||
      !identical(as.integer(observed_exact_codes),
                 unname(expected_exact_codes)) ||
      nrow(exact_daily) != 8201L ||
      sum(exact_daily$rows > 1L) != 7L ||
      max(exact_daily$rows) != 2L ||
      any(exact_daily$values != 1L) ||
      any(exact_daily$methods != 1L)
  ) {
    stop("the preferred-method USGS TDS records changed")
  }
  selected_exact <- selected_exact[, ..provider_output_columns]

  clinton_location_id <- site_lookup[
    Stream_Name == "Mississippi River at Clinton",
    location_id
  ]
  if (
    length(clinton_location_id) != 1L ||
      clinton_location_id != "USGS-05420500"
  ) {
    stop("the reviewed Clinton station id changed")
  }
  clinton_expected <- data.table::data.table(
    date = data.table::as.IDate(c("2005-06-20", "2013-05-13")),
    expected_value = c(258, 186)
  )
  clinton_provider <- provider[
    location_id == clinton_location_id &
      date %in% clinton_expected$date &
      USGSpcode == "70300" &
      Result_Characteristic == "Total dissolved solids" &
      provider_unit == "mg/L" &
      !is.na(provider_value)
  ]
  clinton_check <- merge(
    clinton_provider[, .(date, provider_value)],
    clinton_expected,
    by = "date"
  )
  if (
    nrow(clinton_provider) != 2L ||
      nrow(clinton_check) != 2L ||
      any(abs(
        clinton_check$provider_value - clinton_check$expected_value
      ) > 1e-10)
  ) {
    stop("the reviewed Clinton TDS provider rows changed")
  }

  selected <- data.table::rbindlist(
    list(selected_exact, clinton_provider[, ..provider_output_columns]),
    use.names = TRUE,
    fill = TRUE
  )

  # rebuild all valid tds days for four dsi sites removed by full deduplication
  four_site_ids <- c(
    "USGS-11264500", "USGS-10343500", "USGS-09352900", "USGS-15565447"
  )
  four_site_provider <- provider[
    location_id %in% four_site_ids &
      Result_Characteristic == "Total dissolved solids" &
      provider_unit == "mg/L" &
      is.finite(provider_value) & provider_value > 0 &
      is_blank(Result_ResultDetectionCondition) &
      is_blank(Result_MeasureQualifierCode)
  ]
  four_site_provider[, code_priority := unname(code_priority[USGSpcode])]
  if (anyNA(four_site_provider$code_priority)) {
    stop("an unsupported four-site USGS TDS parameter code remains")
  }
  four_site_provider[, best_priority := min(code_priority), by = .(
    location_id,
    date
  )]
  four_site_selected <- four_site_provider[code_priority == best_priority]

  four_site_scope <- four_site_selected[, .(
    source_rows = .N,
    code_count = data.table::uniqueN(USGSpcode),
    unit_count = data.table::uniqueN(provider_unit),
    characteristic_count = data.table::uniqueN(Result_Characteristic)
  ), by = .(location_id, date)]
  if (
    nrow(four_site_selected) != 2223L ||
      nrow(four_site_scope) != 2163L ||
      sum(four_site_scope$source_rows > 1L) != 59L ||
      max(four_site_scope$source_rows) != 3L ||
      any(four_site_scope$code_count != 1L) ||
      any(four_site_scope$unit_count != 1L) ||
      any(four_site_scope$characteristic_count != 1L)
  ) {
    stop("the four-site provider TDS daily scope changed")
  }

  expected_four_site_days <- c(
    "USGS-09352900" = 696L,
    "USGS-10343500" = 474L,
    "USGS-11264500" = 729L,
    "USGS-15565447" = 264L
  )
  observed_four_site_days <- table(factor(
    four_site_scope$location_id,
    levels = names(expected_four_site_days)
  ))
  if (!identical(
    as.integer(observed_four_site_days),
    unname(expected_four_site_days)
  )) {
    stop("the four-site provider TDS site counts changed")
  }

  expected_four_site_codes <- c(
    "00515" = 13L,
    "70300" = 1021L,
    "70301" = 1129L,
    "00525" = 0L
  )
  observed_four_site_codes <- table(factor(
    four_site_selected[, unique(USGSpcode), by = .(location_id, date)]$V1,
    levels = names(expected_four_site_codes)
  ))
  if (!identical(
    as.integer(observed_four_site_codes),
    unname(expected_four_site_codes)
  )) {
    stop("the four-site provider TDS code counts changed")
  }

  collapse_provider_values <- function(values) {
    values <- values[!is.na(values) & values != ""]
    if (length(values) == 0L) return(NA_character_)
    paste(sort(unique(values)), collapse = ";")
  }
  four_site_daily <- four_site_selected[, {
    daily <- .SD[1L]
    daily$provider_value <- mean(provider_value)
    daily$provider_value_text <- format(
      daily$provider_value,
      digits = 15L,
      scientific = FALSE,
      trim = TRUE
    )
    daily$Activity_ActivityIdentifier <- collapse_provider_values(
      Activity_ActivityIdentifier
    )
    daily$Result_MeasureIdentifier <- collapse_provider_values(
      Result_MeasureIdentifier
    )
    daily$provider_files <- collapse_provider_values(provider_files)
    daily
  }, by = .(location_id, date)]
  four_site_daily <- four_site_daily[, ..provider_output_columns]

  selected <- data.table::rbindlist(
    list(selected, four_site_daily),
    use.names = TRUE,
    fill = TRUE
  )
  selected_daily <- selected[, .(
    rows = .N,
    values = data.table::uniqueN(provider_value),
    methods = data.table::uniqueN(USGSpcode)
  ), by = .(location_id, date)]
  selected_code_counts <- table(factor(
    selected$USGSpcode,
    levels = c("00515", "70300", "70301")
  ))

  if (
    nrow(selected) != 10373L ||
      data.table::uniqueN(selected$Result_MeasureIdentifier) != 10373L ||
      nrow(selected_daily) != 10366L ||
      data.table::uniqueN(selected$location_id) != 29L ||
      !identical(as.integer(selected_code_counts), c(13L, 9229L, 1131L)) ||
      any(selected_daily$values != 1L) ||
      any(selected_daily$methods != 1L) ||
      any(is_blank(selected$Activity_ActivityIdentifier)) ||
      any(is_blank(selected$Result_MeasureIdentifier)) ||
      any(selected$Result_Characteristic != "Total dissolved solids") ||
      any(selected$provider_unit != "mg/L") ||
      any(!is.finite(selected$provider_value)) ||
      any(selected$provider_value <= 0) ||
      any(!is_blank(selected$Result_ResultDetectionCondition)) ||
      any(!is_blank(selected$Result_MeasureQualifierCode))
  ) {
    stop("the final USGS TDS backfill records changed")
  }

  if (nrow(merge(
    unique(selected[, .(location_id, date)]),
    current_tds_keys,
    by = c("location_id", "date")
  )) > 0L) {
    stop("the USGS TDS backfill overlaps the reviewed Susquehanna source")
  }

  unresolved_keys <- data.table::data.table(
    location_id = c("USGS-01578310", "USGS-05420500"),
    date = data.table::as.IDate(c("1996-02-24", "1978-04-22"))
  )
  usable_unresolved <- provider[
    unresolved_keys,
    on = .(location_id, date),
    nomatch = 0L
  ][
    provider_unit == "mg/L" &
      Result_Characteristic == "Total dissolved solids" &
      !is.na(provider_value)
  ]
  assert_count(nrow(usable_unresolved), 0L,
               "usable provider values for the two unresolved TDS dates")
  assert_count(nrow(selected[
    unresolved_keys,
    on = .(location_id, date),
    nomatch = 0L
  ]), 0L, "unresolved TDS dates in the backfill")

  selected <- merge(
    selected,
    site_lookup[, .(location_id, Stream_Name)],
    by = "location_id",
    all.x = TRUE
  )
  if (anyNA(selected$Stream_Name)) {
    stop("a USGS TDS backfill row lacks a reviewed site name")
  }
  data.table::setorder(
    selected,
    location_id,
    date,
    USGSpcode,
    Activity_ActivityIdentifier
  )

  backfill <- selected[, .(
    V1 = sprintf("USGS_TDS_%05d", seq_len(.N)),
    variable = "TDS",
    LTER = "USGS",
    Site.Stream.Name = Stream_Name,
    site = Stream_Name,
    Sampling.Date = as.character(date),
    value = provider_value_text,
    units = "mg/L",
    USGS.Provider.Parameter.Code = USGSpcode,
    USGS.Provider.Activity.ID = Activity_ActivityIdentifier,
    USGS.Provider.Result.ID = Result_MeasureIdentifier,
    USGS.Provider.Source.Files = provider_files,
    USGS.Provider.Detection.Condition = data.table::fifelse(
      is_blank(Result_ResultDetectionCondition),
      NA_character_,
      Result_ResultDetectionCondition
    ),
    USGS.Provider.Sample.Fraction = Result_SampleFraction,
    USGS.Provider.Measure.Status = Result_MeasureStatusIdentifier,
    USGS.Provider.Characteristic = Result_Characteristic,
    USGS.Provider.Description = Result_CharacteristicUserSupplied,
    USGS.Provider.Method.Speciation = data.table::fifelse(
      is_blank(Result_MethodSpeciation),
      NA_character_,
      Result_MethodSpeciation
    ),
    USGS.Provider.Raw.Value = provider_value_text,
    USGS.Provider.Raw.Unit = provider_unit,
    USGS.Provider.Measure.Qualifier = data.table::fifelse(
      is_blank(Result_MeasureQualifierCode),
      NA_character_,
      Result_MeasureQualifierCode
    ),
    USGS.Provider.Detection.Limit.Type.A = data.table::fifelse(
      is_blank(DetectionLimit_TypeA),
      NA_character_,
      DetectionLimit_TypeA
    ),
    USGS.Provider.Detection.Limit.Value.A = data.table::fifelse(
      is_blank(limit_a_text),
      NA_character_,
      limit_a_text
    ),
    USGS.Provider.Detection.Limit.Unit.A = data.table::fifelse(
      is_blank(DetectionLimit_MeasureUnitA),
      NA_character_,
      DetectionLimit_MeasureUnitA
    ),
    USGS.Provider.Detection.Limit.Type.B = data.table::fifelse(
      is_blank(DetectionLimit_TypeB),
      NA_character_,
      DetectionLimit_TypeB
    ),
    USGS.Provider.Detection.Limit.Value.B = data.table::fifelse(
      is_blank(limit_b_text),
      NA_character_,
      limit_b_text
    ),
    USGS.Provider.Detection.Limit.Unit.B = data.table::fifelse(
      is_blank(DetectionLimit_MeasureUnitB),
      NA_character_,
      DetectionLimit_MeasureUnitB
    )
  )]
  data.table::setnames(backfill, "V1", "")

  if (
    nrow(backfill) != 10373L ||
      anyDuplicated(backfill$USGS.Provider.Result.ID) > 0L ||
      any(startsWith(backfill$value, "<")) ||
      any(startsWith(backfill$value, ">"))
  ) {
    stop("the prepared USGS TDS backfill rows changed")
  }

  as.data.frame(backfill, stringsAsFactors = FALSE)
}

# ---- carey prepared input rules ----

build_carey_shift_repair <- function(template_data) {
  if (!requireNamespace("readxl", quietly = TRUE)) {
    stop("the carey repair requires the readxl package")
  }

  source <- readxl::read_excel(
    carey_file,
    sheet = "Raw Stream Data",
    range = "A4:AL200",
    col_names = FALSE,
    .name_repair = "minimal"
  )
  source <- as.data.frame(source, stringsAsFactors = FALSE)
  names(source) <- c(
    "lter", "site", "date", "time", "treatment", "daily_q",
    "instant_q", "stage", "temp", "conductivity",
    "specific_conductance", "turbidity", "tss", "vss", "dsi",
    "tn", "din", "nox", "no3", "no2", "nh4", "tp", "po4",
    "srp", "doc", "toc", "alkalinity", "anc", "ph", "dic",
    "suspended_chl", "benthic_chl", "na", "k", "ca", "mg",
    "so4", "cl"
  )
  source$date <- as.Date(source$date)
  source <- source[
    !is.na(source$date) &
      source$date >= as.Date("2011-06-15") &
      source$date <= as.Date("2011-09-28"),
    ,
    drop = FALSE
  ]

  assert_count(nrow(source), 60L, "carey shifted source rows")
  assert_count(length(unique(source$date)), 15L,
               "carey shifted source dates")
  assert_count(length(unique(source$site)), 4L,
               "carey shifted source sites")

  numeric_columns <- c(
    "temp", "conductivity", "specific_conductance", "turbidity",
    "dsi", "tn", "din", "nox", "no3", "nh4", "po4", "srp"
  )
  for (column_name in numeric_columns) {
    source[[column_name]] <- as_number(source[[column_name]])
  }

  if (
    any(abs(source$dsi - source$tn - source$no3) > 1e-10) ||
      any(abs(source$tn - source$din - source$nox) > 1e-10)
  ) {
    stop("the carey shifted nitrogen identities changed")
  }
  # repeated nitrogen identities confirm one consistent header shift in this window

  site_map <- c(
    "Aberjona" = "Aberjona",
    "Ipswich at Middleton" = "Ipswich at Middleton",
    "Ipswich at Ipswich" = "Ipswich at Ipswich",
    "Parker" = "Parker River at Byfield, MA"
  )
  repaired_site <- unname(site_map[source$site])
  if (any(is.na(repaired_site))) {
    stop("the carey shifted site labels changed")
  }

  conductivity <- ifelse(
    source$date == as.Date("2011-09-28"),
    source$po4,
    source$conductivity
  )
  ph <- ifelse(
    source$date == as.Date("2011-09-28"),
    source$conductivity,
    source$po4
  )
  reviewed_conductivity <- source$site == "Parker" &
    (
      source$date == as.Date("2011-06-24") &
        abs(conductivity - 17.5) < 1e-10 |
        source$date == as.Date("2011-07-02") &
          abs(conductivity - 24.6) < 1e-10
    )
  reviewed_temperature <- source$site == "Parker" &
    source$date == as.Date("2011-07-02") &
    abs(source$temp - 190.5) < 1e-10
  assert_count(sum(reviewed_conductivity), 2L,
               "carey reviewed conductivity rows")
  assert_count(sum(reviewed_temperature), 1L,
               "carey reviewed temperature rows")
  # keep these conductivity and temperature values for later outlier review
  assert_values(
    conductivity[reviewed_conductivity],
    c(17.5, 24.6),
    "prepared Carey reviewed conductivity values"
  )
  assert_values(
    source$temp[reviewed_temperature],
    190.5,
    "prepared Carey reviewed temperature value"
  )

  # rebuild the affected fields from their reviewed workbook positions
  repaired_values <- list(
    "temp" = list(values = source$temp, units = "C"),
    "Conductivity" = list(values = conductivity, units = "uS/cm"),
    "TSS" = list(values = source$specific_conductance, units = "mg/L"),
    "DSi" = list(values = source$turbidity, units = "uM"),
    "DIN" = list(values = source$dsi, units = "uM"),
    "NOx" = list(values = source$tn, units = "uM"),
    "NO3" = list(values = source$din, units = "uM"),
    "NO2" = list(values = source$nox, units = "uM"),
    "NH4" = list(values = source$no3, units = "uM"),
    "PO4" = list(values = source$nh4, units = "uM"),
    "pH" = list(values = ph, units = "SU"),
    "suspended Chl" = list(values = source$srp * 1000, units = "ug/L")
  )

  repaired <- lapply(names(repaired_values), function(variable_name) {
    values <- repaired_values[[variable_name]]$values
    keep <- !is.na(values) & is.finite(values)
    data.frame(
      V1 = NA_character_,
      variable = variable_name,
      LTER = "Ipswitch(Carey)",
      Site.Stream.Name = repaired_site[keep],
      site = repaired_site[keep],
      Sampling.Date = format(source$date[keep], "%Y-%m-%d"),
      value = format(
        values[keep],
        scientific = FALSE,
        trim = TRUE,
        digits = 15
      ),
      units = repaired_values[[variable_name]]$units,
      stringsAsFactors = FALSE,
      check.names = FALSE
    )
  })
  repaired <- do.call(rbind, repaired)
  rownames(repaired) <- NULL

  expected_counts <- c(
    temp = 60L,
    Conductivity = 60L,
    TSS = 55L,
    DSi = 60L,
    DIN = 60L,
    NOx = 60L,
    NO3 = 60L,
    NO2 = 60L,
    NH4 = 60L,
    PO4 = 60L,
    pH = 48L,
    `suspended Chl` = 60L
  )
  observed_counts <- table(factor(
    repaired$variable,
    levels = names(expected_counts)
  ))
  if (!identical(as.integer(observed_counts), unname(expected_counts))) {
    stop("the carey repaired variable counts changed")
  }

  names(repaired)[1L] <- names(template_data)[1L]
  next_id <- max(as_number(template_data[[1L]]), na.rm = TRUE)
  repaired[[1L]] <- as.character(next_id + seq_len(nrow(repaired)))
  extra_columns <- setdiff(names(template_data), names(repaired))
  for (column_name in extra_columns) {
    repaired[[column_name]] <- NA_character_
  }
  column_order <- match(names(template_data), names(repaired))
  if (anyNA(column_order)) {
    stop("the carey repair output columns changed")
  }
  repaired <- repaired[, column_order, drop = FALSE]
  names(repaired) <- names(template_data)
  repaired
}

# ---- prepared arc input rules ----

arc_provider_column_map <- c(
  Temp_C = "Temp.C",
  Cond_uS = "Spec.Cond",
  pH = "pH",
  Alk_ueqL = "alkalinity",
  CO2_uM = "DIC",
  DOC_uM = "DOC",
  NH4_phen_uM = "NH4",
  NH4_OPA_uM = "NH4",
  NH4_uM = "NH4",
  PO4_uM = "PO4",
  NO3_uM = "NOx",
  TDN_uM = "TN",
  TDP_uM = "TP",
  Ca_uM = "Ca",
  Mg_uM = "Mg",
  Na_uM = "Na",
  K_uM = "K",
  Si_uM = "DSi",
  SO4_uM = "SO4",
  Cl_uM = "Cl"
)

read_arc_provider_long <- function(provider_file) {
  source <- read_source(
    basename(provider_file),
    directory = dirname(provider_file)
  )
  required_columns <- c(
    "SortChem", "Site", "Date", "Alk_ueqL", "CO2_uM", "TDN_uM", "TDP_uM"
  )
  missing_columns <- setdiff(required_columns, names(source))
  if (length(missing_columns) > 0L) {
    stop(
      "the ARC provider file is missing: ",
      paste(missing_columns, collapse = ", ")
    )
  }

  sample_rows <- grepl("^[0-9]{4}-[0-9]{4}$", source$SortChem) &
    grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", source$Date)
  expected_samples <- c(
    "1988-2010_Kling_Akchem.csv" = 8187L,
    "legacy_audit_ARC_2002-2024_Kling_AON_Imnavait_Chemistry.csv" = 1022L,
    "legacy_audit_ARC_2012-2020_Kling_Akchem.02.csv" = 1784L
  )
  filename <- basename(provider_file)
  assert_count(
    sum(sample_rows),
    expected_samples[[filename]],
    paste0("ARC provider samples in ", filename)
  )
  source <- source[sample_rows, , drop = FALSE]
  if (!"water_type" %in% names(source)) {
    source$water_type <- NA_character_
  }

  available_columns <- intersect(names(arc_provider_column_map), names(source))
  # turn each measured column into rows, keeping the original column order
  long_parts <- vector("list", length(available_columns))
  for (column_number in seq_along(available_columns)) {
    provider_column <- available_columns[column_number]
    provider_value <- as_number(source[[provider_column]])
    keep <- is.finite(provider_value)
    long_parts[[column_number]] <- data.frame(
      provider_file = filename,
      sample_id = source$SortChem[keep],
      site = source$Site[keep],
      date = source$Date[keep],
      water_type = source$water_type[keep],
      provider_field = provider_column,
      legacy_variable = unname(arc_provider_column_map[[provider_column]]),
      value = provider_value[keep],
      stringsAsFactors = FALSE
    )
  }
  provider_long <- do.call(rbind, long_parts)
  rownames(provider_long) <- NULL

  expected_long_rows <- c(
    "1988-2010_Kling_Akchem.csv" = 96383L,
    "legacy_audit_ARC_2002-2024_Kling_AON_Imnavait_Chemistry.csv" = 12717L,
    "legacy_audit_ARC_2012-2020_Kling_Akchem.02.csv" = 29477L
  )
  assert_count(
    nrow(provider_long),
    expected_long_rows[[filename]],
    paste0("ARC provider measurements in ", filename)
  )
  provider_long
}

find_arc_provider_matches <- function(data, row_numbers, provider, tolerance = 1e-8) {
  provider_key <- paste(
    provider$site,
    provider$date,
    provider$legacy_variable,
    sep = "\r"
  )
  provider_rows_by_key <- split(seq_len(nrow(provider)), provider_key)
  data_key <- paste(
    data$Site.Stream.Name[row_numbers],
    data$Sampling.Date[row_numbers],
    data$variable[row_numbers],
    sep = "\r"
  )
  data_value <- as_number(data$value[row_numbers])

  # a match needs the same site, date, variable, and value within the reviewed tolerance
  matches <- vector("list", length(row_numbers))
  for (position in seq_along(row_numbers)) {
    matches[[position]] <- integer()
    candidates <- provider_rows_by_key[[data_key[[position]]]]
    if (is.null(candidates) || !is.finite(data_value[[position]])) {
      next
    }
    matches[[position]] <- candidates[
      abs(provider$value[candidates] - data_value[[position]]) <= tolerance
    ]
  }
  matches
}

refresh_arc_legacy <- function(data) {
  provider_tables <- vector("list", length(arc_provider_files))
  for (file_number in seq_along(arc_provider_files)) {
    provider_tables[[file_number]] <- read_arc_provider_long(arc_provider_files[file_number])
  }
  provider <- do.call(rbind, provider_tables)
  rownames(provider) <- NULL
  assert_count(nrow(provider), 138577L, "combined ARC provider measurements")

  semantic_variables <- c(
    DIC = "CO2",
    TN = "TDN",
    TP = "TDP",
    alkalinity = "alkalinity"
  )
  expected_semantic_counts <- c(
    DIC = 6651L,
    TN = 6917L,
    TP = 6426L,
    alkalinity = 7519L
  )
  semantic_rows <- which(
    data$LTER == "ARC" & data$variable %in% names(semantic_variables)
  )
  observed_semantic_counts <- table(factor(
    data$variable[semantic_rows],
    levels = names(expected_semantic_counts)
  ))
  if (!identical(
    as.integer(observed_semantic_counts),
    unname(expected_semantic_counts)
  )) {
    stop("the ARC legacy semantic-label counts changed")
  }
  semantic_matches <- find_arc_provider_matches(
    data,
    semantic_rows,
    provider
  )
  if (any(lengths(semantic_matches) == 0L)) {
    stop("an ARC semantic correction lacks an exact provider match")
  }

  mixed_sites <- c(
    "Dennis Lake Outlet",
    "E 01 Inlet",
    "E 01 into Toolik",
    "LTER 347 Outlet",
    "NE 14 Lake Slump Inlet"
  )
  mixed_rows <- which(
    data$LTER == "ARC" & data$Site.Stream.Name %in% mixed_sites
  )
  assert_count(length(mixed_rows), 612L, "mixed-water-type ARC rows")
  mixed_matches <- find_arc_provider_matches(data, mixed_rows, provider)
  if (any(lengths(mixed_matches) == 0L)) {
    stop("a mixed-water-type ARC row lacks an exact provider match")
  }

  matched_sample_counts <- vapply(mixed_matches, function(matches) {
    length(unique(provider$sample_id[matches]))
  }, integer(1))
  matched_water_type <- vapply(mixed_matches, function(matches) {
    water_type <- unique(provider$water_type[matches])
    water_type <- water_type[!is.na(water_type) & trimws(water_type) != ""]
    if (length(water_type) != 1L) {
      return(NA_character_)
    }
    water_type
  }, character(1))
  if (
    any(matched_sample_counts != 1L) ||
      any(is.na(matched_water_type)) ||
      any(!matched_water_type %in% c("ST", "LA"))
  ) {
    stop("the ARC provider sample identity or water type is ambiguous")
  }
  assert_count(sum(matched_water_type == "ST"), 415L,
               "stream-water ARC rows")
  assert_count(sum(matched_water_type == "LA"), 197L,
               "lake-water ARC rows")
  assert_count(
    sum(data$variable[mixed_rows] == "DSi" & matched_water_type == "ST"),
    23L,
    "stream-water ARC Si rows"
  )
  assert_count(
    sum(data$variable[mixed_rows] == "DSi" & matched_water_type == "LA"),
    11L,
    "lake-water ARC Si rows"
  )

  remove_rows <- rep(FALSE, nrow(data))
  remove_rows[mixed_rows[matched_water_type == "LA"]] <- TRUE
  data <- data[!remove_rows, , drop = FALSE]
  rownames(data) <- NULL

  retained_arc <- data$LTER == "ARC"
  data$variable[retained_arc & data$variable == "DIC"] <- "CO2"
  data$variable[retained_arc & data$variable == "TN"] <- "TDN"
  data$variable[retained_arc & data$variable == "TP"] <- "TDP"
  data$units[retained_arc & data$variable == "alkalinity"] <- "ueq/L"

  expected_retained_counts <- c(
    CO2 = 6640L,
    TDN = 6906L,
    TDP = 6418L,
    alkalinity = 7506L
  )
  observed_retained_counts <- c(
    CO2 = sum(retained_arc & data$variable == "CO2"),
    TDN = sum(retained_arc & data$variable == "TDN"),
    TDP = sum(retained_arc & data$variable == "TDP"),
    alkalinity = sum(retained_arc & data$variable == "alkalinity")
  )
  if (!identical(observed_retained_counts, expected_retained_counts)) {
    stop("the prepared ARC semantic-label counts changed")
  }
  assert_count(
    sum(retained_arc & data$variable == "alkalinity" & data$units == "ueq/L"),
    7506L,
    "prepared ARC alkalinity-unit rows"
  )
  assert_count(sum(retained_arc & data$variable %in% c("DIC", "TN", "TP")),
               0L, "prepared ARC old semantic labels")
  data
}

# ---- prepared usgs input rules ----

prepare_legacy_usgs <- function() {
  filename <- "20221030_masterdata_chem_V2.csv"
  before <- read_source(filename)
  after <- before
  value <- as_number(after$value)

  assert_count(nrow(after), 1025369L, "corrected legacy chemistry rows")
  after <- refresh_arc_legacy(after)
  value <- as_number(after$value)
  assert_count(nrow(after), 1025172L,
               "legacy chemistry rows after the ARC provider refresh")

  four_dates <- c("1980-03-24", "1980-04-03", "1996-02-24", "2010-12-07")
  four_tds <- after$LTER == "USGS" &
    after$Site.Stream.Name == "SUSQUEHANNA RIVER" &
    after$Sampling.Date %in% four_dates & after$variable == "TN" &
    after$units == "uM"
  assert_count(sum(four_tds, na.rm = TRUE), 0L,
               "four corrected legacy Susquehanna TDS rows")

  clinton_expected <- c(
    "1978-04-22" = 171,
    "2005-06-20" = 255,
    "2013-05-13" = 196
  )
  clinton_tds <- after$LTER == "USGS" &
    after$Site.Stream.Name == "Misssissippi River at Clinton" &
    after$Sampling.Date %in% names(clinton_expected) &
    after$variable == "TN" & after$units == "uM"
  assert_count(sum(clinton_tds, na.rm = TRUE), 3L,
               "remaining legacy Clinton TDS rows")
  clinton_back_converted <- value[clinton_tds] * 14.0067 / 1000
  clinton_dates <- after$Sampling.Date[clinton_tds]
  assert_values(
    clinton_back_converted[order(clinton_dates)],
    unname(clinton_expected[order(names(clinton_expected))]),
    "remaining legacy Clinton TDS values"
  )

  withdrawn_mackenzie_temp <- after[[1L]] == "871741" &
    after$LTER == "GRO" &
    after$site == "Mackenzie" &
    after$Site.Stream.Name == "Mackenzie" &
    after$Sampling.Date == "2017-02-22" &
    after$variable == "Temp.C" &
    after$units == "uM" &
    value == -2
  withdrawn_mackenzie_temp[is.na(withdrawn_mackenzie_temp)] <- FALSE
  assert_count(sum(withdrawn_mackenzie_temp), 1L,
               "withdrawn Mackenzie temperature row")
  # keep the reported temperature for later outlier review
  assert_values(as_number(after$value[withdrawn_mackenzie_temp]), -2,
                "prepared Mackenzie reviewed temperature value")

  source_name_map <- data.frame(
    LTER = c(
      "USGS", "USGS", "ARC", "ARC", "ARC", "Ipswitch(Carey)",
      "PIE", "PIE", "PIE", "PIE", "PIE"
    ),
    old = c(
      "Misssissippi River at Clinton", "North Sylamore", "I4 into I5",
      "I7 Into I9", "I8 into I9", "Parker", "MB-SAL",
      "YSI-CC", "YSI-IR", "YSI-SB", "10"
    ),
    new = c(
      "Mississippi River at Clinton", "North Slyamore Creek", "I4 Into I5",
      "I7 into I9", "I8 Into I9", "Parker River at Byfield, MA", "MB-62",
      "Cart Creek", "Ipswich at North Reading", "Sawmill Brook", "Parker"
    ),
    expected = c(2751L, 748L, 1279L, 4L, 3411L, 429L, 9L, 35L, 27L, 32L, 44L),
    stringsAsFactors = FALSE
  )

  for (source_row in seq_len(nrow(source_name_map))) {
    target <- after$LTER == source_name_map$LTER[source_row] &
      after$site == source_name_map$old[source_row] &
      after$Site.Stream.Name == source_name_map$old[source_row]
    assert_count(
      sum(target, na.rm = TRUE),
      source_name_map$expected[source_row],
      paste0("legacy source name ", source_name_map$old[source_row])
    )
    after$site[target] <- source_name_map$new[source_row]
    after$Site.Stream.Name[target] <- source_name_map$new[source_row]
  }

  # match the older niva spelling to the site reference name
  niva_osteglo_alias <- after$LTER == "NIVA" &
    after$site == "ASTEGLO" &
    after$Site.Stream.Name == "ØSTEGLO"
  assert_count(sum(niva_osteglo_alias, na.rm = TRUE), 362L,
               "legacy NIVA OSTEGLO alias rows")
  after$site[niva_osteglo_alias] <- "OSTEGLO"
  after$Site.Stream.Name[niva_osteglo_alias] <- "OSTEGLO"

  east_rows <- list(
    doe = after$LTER == "DOE SFA East River" &
      after$site == "Coal Creek11" & after$Site.Stream.Name == "Coal Creek11",
    sfa = after$LTER == "East River SFA" &
      after$site == "Coal Creek" & after$Site.Stream.Name == "Coal Creek"
  )
  assert_count(sum(east_rows$doe), 7515L, "legacy DOE East River rows")
  assert_count(sum(east_rows$sfa), 827L, "legacy SFA East River rows")
  for (target in east_rows) {
    after$LTER[target] <- "EastRiverSFA"
    after$site[target] <- "coal_11"
    after$Site.Stream.Name[target] <- "coal_11"
  }

  east_cations <- after$LTER == "EastRiverSFA" &
    after$site == "coal_11" &
    after$Site.Stream.Name == "coal_11" &
    after$variable %in% c("Ca", "K", "Mg", "Na") &
    after$units == "uM"
  east_cation_counts <- table(after$variable[east_cations])
  expected_east_cation_counts <- c(Ca = 644L, K = 644L, Mg = 644L, Na = 644L)
  assert_count(sum(east_cations), 2576L,
               "legacy East River cation rows")
  if (
    !identical(names(east_cation_counts), names(expected_east_cation_counts)) ||
      !identical(
        as.integer(east_cation_counts),
        unname(expected_east_cation_counts)
      ) ||
      min(after$Sampling.Date[east_cations]) != "2015-12-31" ||
      max(after$Sampling.Date[east_cations]) != "2019-10-18"
  ) {
    stop("the reviewed legacy East River cation scope changed")
  }
  # the original source reports these cations in ppb, equivalent to ug/l
  # the legacy conversion treated them as mg/l and made them 1,000 times high
  after$value[east_cations] <- as.character(value[east_cations] / 1000)

  east_don <- after$LTER == "EastRiverSFA" & after$site == "coal_11" &
    after$variable == "DON" & after$units == "uM"
  krycklan_no3 <- after$site == "Site 7" & after$variable == "NO3" &
    after$units == "uM"
  krycklan_srp <- after$site == "Site 7" & after$variable == "SRP"
  camels_po4 <- after$site %in% c("SOPCHOPPY RIVER", "Wild River") &
    after$variable == "PO4" & after$units == "uM"
  umr_nh4 <- after$LTER == "UMR(Jankowski)" & after$variable == "NH4" &
    after$units == "uM"

  assert_count(sum(east_don), 507L, "legacy East River mislabeled DON rows")
  assert_count(sum(krycklan_no3), 358L, "legacy Krycklan NO3 rows")
  assert_count(sum(krycklan_srp), 375L, "legacy Krycklan SRP rows")
  assert_count(sum(camels_po4), 188L, "legacy CAMELS PO4 rows")
  assert_count(sum(umr_nh4), 5059L, "legacy UMR NH4 rows")
  after$variable[east_don] <- "TDN"
  after$variable[umr_nh4] <- "NHX"

  susquehanna_nhx_dates <- c(
    "1980-06-16", "1981-02-13", "1981-02-14", "1986-09-10",
    "1988-02-24", "1988-07-06", "1996-01-21", "1999-06-09",
    "2004-09-20"
  )
  susquehanna_provider <- read_source(
    basename(reviewed_susquehanna_file),
    directory = dirname(reviewed_susquehanna_file)
  )
  # the restored source uses both nh4 and nhx for reported ammonia
  # use either label only to verify that the legacy date is represented
  susquehanna_provider_ammonia_dates <- unique(susquehanna_provider$date[
    susquehanna_provider$LTER == "USGS" &
      susquehanna_provider$Stream_Name == "SUSQUEHANNA RIVER" &
      susquehanna_provider$solute %in% c("NH4", "NHX") &
      susquehanna_provider$solute_units == "uM"
  ])
  susquehanna_nh4 <- after$LTER == "USGS" &
    after$site == "SUSQUEHANNA RIVER" & after$variable == "NH4" &
    after$units == "uM"
  reviewed_susquehanna_nhx <- susquehanna_nh4 &
    after$Sampling.Date %in% susquehanna_nhx_dates
  remaining_susquehanna_nh4 <- susquehanna_nh4 &
    !reviewed_susquehanna_nhx
  provider_covered_remaining_nh4 <- remaining_susquehanna_nh4 &
    after$Sampling.Date %in% susquehanna_provider_ammonia_dates
  assert_count(sum(susquehanna_nh4), 1087L, "legacy Susquehanna NH4 rows")
  assert_count(sum(reviewed_susquehanna_nhx), 9L,
               "previously reviewed legacy Susquehanna NHX rows")
  assert_count(sum(remaining_susquehanna_nh4), 1078L,
               "remaining legacy Susquehanna NH4 rows")
  assert_count(length(susquehanna_provider_ammonia_dates), 1198L,
               "restored Susquehanna provider ammonia dates")
  assert_count(sum(provider_covered_remaining_nh4), 1078L,
               "provider-covered legacy Susquehanna NH4 rows")
  if (!identical(
    sort(after$Sampling.Date[reviewed_susquehanna_nhx]),
    sort(susquehanna_nhx_dates)
  )) {
    stop("legacy Susquehanna NHX dates changed")
  }
  reviewed_dates_without_restored_match <- setdiff(
    after$Sampling.Date[reviewed_susquehanna_nhx],
    susquehanna_provider_ammonia_dates
  )
  if (!identical(reviewed_dates_without_restored_match, "1980-06-16")) {
    stop("the previously reviewed Susquehanna ammonia exception changed")
  }
  after$variable[susquehanna_nh4] <- "NHX"

  legacy_label_map <- c(
    "Daily.Avg.Q.(Discharge)" = "daily Q",
    "Instantaneous.Q.(Discharge)" = "instant Q",
    "Spec.Cond" = "specific conductivity",
    "Temp.C" = "temp",
    "Suspended.Chl" = "suspended Chl"
  )
  expected_labels <- c(
    "Daily.Avg.Q.(Discharge)" = 3531L,
    "Instantaneous.Q.(Discharge)" = 1033L,
    "Spec.Cond" = 30928L,
    "Temp.C" = 19548L,
    "Suspended.Chl" = 7082L
  )
  observed_labels <- vapply(
    names(legacy_label_map),
    function(label) sum(after$variable == label),
    integer(1)
  )
  if (!identical(observed_labels, expected_labels)) {
    stop("legacy variable-label counts changed")
  }
  invalid_temperature <- after$variable == "Temp.C" & value > 50
  assert_count(sum(invalid_temperature, na.rm = TRUE), 3L,
               "legacy invalid-temperature rows")
  # keep the reported temperatures for later outlier review
  assert_values(
    as_number(after$value[invalid_temperature]),
    c(53.4, 190.5, 64.5),
    "prepared legacy reviewed temperature values"
  )
  after$units[after$variable == "Temp.C"] <- "C"
  for (old_label in names(legacy_label_map)) {
    after$variable[after$variable == old_label] <-
      unname(legacy_label_map[[old_label]])
  }

  carey_shift_window <- after$LTER == "Ipswitch(Carey)" &
    after$Sampling.Date >= "2011-06-15" &
    after$Sampling.Date <= "2011-09-28"
  carey_shifted_labels <- c(
    "Conductivity", "DIN", "DSi", "NH4", "NOx", "PO4",
    "specific conductivity", "temp", "Turbidity"
  )
  carey_shifted <- carey_shift_window &
    after$variable %in% carey_shifted_labels
  carey_unexpected <- carey_shift_window &
    after$variable != "instant Q" &
    !after$variable %in% carey_shifted_labels
  assert_count(sum(carey_shifted), 523L,
               "carey shifted legacy rows replaced")
  assert_count(sum(carey_unexpected), 0L,
               "carey unexpected rows in the shifted date window")
  expected_shifted_counts <- c(
    Conductivity = 60L,
    DIN = 60L,
    DSi = 60L,
    NH4 = 60L,
    NOx = 60L,
    PO4 = 48L,
    `specific conductivity` = 55L,
    temp = 60L,
    Turbidity = 60L
  )
  observed_shifted_counts <- table(factor(
    after$variable[carey_shifted],
    levels = names(expected_shifted_counts)
  ))
  if (!identical(
    as.integer(observed_shifted_counts),
    unname(expected_shifted_counts)
  )) {
    stop("the carey shifted source-variable counts changed")
  }
  carey_repair <- build_carey_shift_repair(after)
  after <- rbind(after[!carey_shifted, , drop = FALSE], carey_repair)
  rownames(after) <- NULL
  value <- as_number(after$value)

  clinton_tds <- after$LTER == "USGS" &
    after$Site.Stream.Name == "Mississippi River at Clinton" &
    after$Sampling.Date %in% names(clinton_expected) &
    after$variable == "TN" &
    after$units == "uM"
  krycklan_no3 <- after$site == "Site 7" & after$variable == "NO3" &
    after$units == "uM"
  krycklan_srp <- after$site == "Site 7" & after$variable == "SRP"
  camels_po4 <- after$site %in% c("SOPCHOPPY RIVER", "Wild River") &
    after$variable == "PO4" & after$units == "uM"

  # standardize the reviewed north slyamore ammonia series
  combined_usgs <- read_source(
    "Deduplicated_Sites_AllSites_chemistry.csv",
    directory = output_dir
  )
  slyamore_provider_dates <- unique(combined_usgs$date[
    combined_usgs$LTER == "USGS" &
      combined_usgs$Stream_Name == "North Slyamore Creek" &
      combined_usgs$solute %in% c("NH4", "NHX") &
      combined_usgs$solute_units == "uM"
  ])
  slyamore_nh4 <- after$LTER == "USGS" &
    after$site == "North Slyamore Creek" &
    after$variable == "NH4" &
    after$units == "uM"
  slyamore_provider_overlap <- slyamore_nh4 &
    after$Sampling.Date %in% slyamore_provider_dates
  slyamore_legacy_only <- slyamore_nh4 & !slyamore_provider_overlap

  assert_count(sum(slyamore_nh4), 150L,
               "legacy North Slyamore NH4 rows")
  assert_count(length(slyamore_provider_dates), 0L,
               "deduplicated North Slyamore ammonia dates")
  assert_count(sum(slyamore_provider_overlap), 0L,
               "overlapping North Slyamore ammonia rows")
  assert_count(sum(slyamore_legacy_only), 150L,
               "retained legacy North Slyamore ammonia rows")
  after$variable[slyamore_nh4] <- "NHX"

  # remove eight exact lower-priority conductivity copies
  conductivity_copy_key <- data.frame(
    site = c(
      "CN00.1M", "I080.2M", "M078.0B", "M764.3A",
      "M786.2C", "MQ02.1M", "SG16.2C", "WP02.6M"
    ),
    date = c(
      "1996-12-31", "1996-12-31", "1996-12-11", "1996-12-30",
      "1996-12-31", "1996-12-31", "1996-12-31", "1996-12-30"
    ),
    value = c(699, 874, 524, 484, 477, 635, 1016, 578),
    stringsAsFactors = FALSE
  )
  conductivity_copy_signature <- paste(
    conductivity_copy_key$site,
    conductivity_copy_key$date,
    conductivity_copy_key$value,
    sep = "\r"
  )
  legacy_conductivity_copy <- after$LTER == "UMR(Jankowski)" &
    after$variable == "specific conductivity" &
    after$units == "uS/cm" &
    paste(after$site, after$Sampling.Date, value, sep = "\r") %in%
      conductivity_copy_signature
  assert_count(sum(legacy_conductivity_copy, na.rm = TRUE), 8L,
               "legacy copied conductivity rows")

  invalid_potomac <- after$site == "POTOMAC RIVER" &
    after$Sampling.Date == "1995-05-15" & after$variable == "DOC" &
    after$units == "uM" & abs(value - 1165627.31564355) < 1e-6
  invalid_cb <- after$site == "CB01.5" &
    after$Sampling.Date == "2016-02-16" &
    after$variable == "specific conductivity" & after$units == "uS/cm" &
    value == 6197
  assert_count(sum(invalid_potomac, na.rm = TRUE), 1L,
               "legacy Potomac invalid DOC row")
  assert_count(sum(invalid_cb, na.rm = TRUE), 1L,
               "legacy CB01.5 invalid conductivity row")
  # keep these reported doc and conductivity values for later outlier review
  assert_values(
    as_number(after$value[invalid_potomac | invalid_cb]),
    c(1165627.31564355, 6197),
    "prepared legacy reviewed source values"
  )

  remove_rows <- clinton_tds | krycklan_no3 | krycklan_srp | camels_po4 |
    slyamore_provider_overlap | legacy_conductivity_copy
  assert_count(sum(remove_rows), 932L, "legacy permanent exclusions")
  after <- after[!remove_rows, , drop = FALSE]
  rownames(after) <- NULL

  assert_count(nrow(after), 1024420L,
               "legacy rows before site-reference exclusions")

  site_reference <- read_source(
    "Site_Reference_Table.csv",
    directory = key_dir
  )
  alternate_names <- unlist(
    strsplit(
      ifelse(is.na(site_reference$Alt_Stream_Name),
             "", site_reference$Alt_Stream_Name),
      ";",
      fixed = TRUE
    ),
    use.names = FALSE
  )
  reference_names <- unique(normalize_site_name(c(
    site_reference$Stream_Name,
    alternate_names,
    site_reference$Original_Stream_Name
  )))
  reference_names <- reference_names[
    !is.na(reference_names) & reference_names != ""
  ]

  finnish_alias_counts <- c(
    "Koskenkyl<e4>njoki 6030" = 3073L,
    "Pyh<e4>joki Hourunk 11400" = 2552L,
    "N<e4>rpi<f6>njoki mts 6761" = 1186L
  )
  for (source_name in names(finnish_alias_counts)) {
    alias_rows <- after$LTER == "Finnish Environmental Institute" &
      after$Site.Stream.Name == source_name
    assert_count(
      sum(alias_rows, na.rm = TRUE),
      finnish_alias_counts[[source_name]],
      paste0("legacy Finnish alias ", source_name)
    )
  }

  unmatched_reference <- !normalize_site_name(after$Site.Stream.Name) %in%
    reference_names
  retained_finnish_alias <-
    after$LTER == "Finnish Environmental Institute" &
    after$Site.Stream.Name %in% names(finnish_alias_counts)
  excluded_site_rows <- unmatched_reference & !retained_finnish_alias

  # reviewed site additions recover 24,996 previously unmatched rows
  assert_count(sum(unmatched_reference), 63355L,
               "legacy rows outside the current site lookup")
  assert_count(sum(retained_finnish_alias), 6811L,
               "legacy Finnish alias rows")
  assert_count(sum(unmatched_reference & retained_finnish_alias), 0L,
               "legacy Finnish aliases outside the current site lookup")
  assert_count(sum(excluded_site_rows), 63355L,
               "legacy accepted site exclusions")
  assert_count(length(unique(after$Site.Stream.Name[excluded_site_rows])),
               615L, "legacy accepted excluded site labels")
  assert_count(sum(
    after$site[excluded_site_rows] !=
      after$Site.Stream.Name[excluded_site_rows],
    na.rm = TRUE
  ), 0L, "legacy excluded rows with differing site fields")

  expected_excluded_rows <- c(
    ARC = 28902L,
    BNZ = 14L,
    BcCZO = 12448L,
    "Finnish Environmental Institute" = 21991L
  )
  expected_excluded_names <- c(
    ARC = 563L,
    BNZ = 1L,
    BcCZO = 44L,
    "Finnish Environmental Institute" = 7L
  )
  observed_excluded_rows <- table(after$LTER[excluded_site_rows])
  observed_excluded_names <- tapply(
    after$Site.Stream.Name[excluded_site_rows],
    after$LTER[excluded_site_rows],
    function(x) length(unique(x))
  )
  if (
    !identical(
      as.integer(observed_excluded_rows[names(expected_excluded_rows)]),
      unname(expected_excluded_rows)
    ) ||
      !identical(
        as.integer(observed_excluded_names[names(expected_excluded_names)]),
        unname(expected_excluded_names)
      ) ||
      length(observed_excluded_rows) != length(expected_excluded_rows)
  ) {
    stop("legacy accepted site-exclusion scope changed")
  }

  exclusion_scope <- aggregate(
    list(rows = after$Site.Stream.Name[excluded_site_rows]),
    list(
      LTER = after$LTER[excluded_site_rows],
      Stream_Name = after$Site.Stream.Name[excluded_site_rows]
    ),
    length
  )
  exclusion_scope <- exclusion_scope[
    order(exclusion_scope$LTER, exclusion_scope$Stream_Name),
    ,
    drop = FALSE
  ]
  signature_path <- tempfile()
  writeLines(
    paste(
      exclusion_scope$LTER,
      exclusion_scope$Stream_Name,
      exclusion_scope$rows,
      sep = "\t"
    ),
    signature_path,
    useBytes = TRUE
  )
  exclusion_signature <- unname(tools::md5sum(signature_path))
  unlink(signature_path)
  if (!identical(exclusion_signature, "b83ba43fa25ae39e6ac867d5afb14be7")) {
    stop(
      "legacy accepted site-exclusion labels changed: ",
      exclusion_signature
    )
  }

  after <- after[!excluded_site_rows, , drop = FALSE]
  rownames(after) <- NULL

  # these totals exclude 197 lake-water rows confirmed in the original source
  assert_count(nrow(after), 961065L, "prepared legacy chemistry rows")
  assert_count(sum(after$LTER == "UMR(Jankowski)" & after$variable == "NH4"),
               0L, "prepared legacy UMR NH4 rows")
  assert_count(sum(after$LTER == "UMR(Jankowski)" & after$variable == "NHX"),
               5059L, "prepared legacy UMR NHX rows")
  assert_count(sum(after$LTER == "USGS" & after$site == "SUSQUEHANNA RIVER" &
    after$variable == "NH4"), 0L, "prepared legacy Susquehanna NH4 rows")
  assert_count(sum(after$LTER == "USGS" & after$site == "SUSQUEHANNA RIVER" &
    after$variable == "NHX"), 1087L, "prepared legacy Susquehanna NHX rows")
  assert_count(sum(after$LTER == "EastRiverSFA" & after$site == "coal_11" &
    after$variable == "DON"), 0L, "prepared legacy East River DON rows")
  assert_count(sum(after$LTER == "EastRiverSFA" & after$site == "coal_11" &
    after$variable == "TDN"), 507L, "prepared legacy East River TDN rows")
  corrected_east_cations <- after$LTER == "EastRiverSFA" &
    after$site == "coal_11" & after$variable %in% c("Ca", "K", "Mg", "Na") &
    after$units == "uM"
  assert_count(sum(corrected_east_cations), 2576L,
               "prepared legacy East River cation rows")
  if (any(as_number(after$value[corrected_east_cations]) >= 500)) {
    stop("the reviewed legacy East River cation correction changed")
  }
  assert_count(sum(after$variable %in% names(legacy_label_map)), 0L,
               "prepared legacy old variable labels")
  assert_count(sum(after$variable == "temp" & after$units != "C"), 0L,
               "prepared legacy temperature units")

  legacy_usgs_tn <- after$LTER == "USGS" &
    after$variable == "TN" &
    after$units == "uM"
  assert_count(sum(legacy_usgs_tn), 13494L,
               "prepared residual legacy USGS total-nitrogen rows")

  usgs_total_nitrogen <- build_usgs_total_nitrogen_replacement(before)
  replaced_legacy_ids <- attr(
    usgs_total_nitrogen,
    "legacy_tn_ids_replaced"
  )
  retained_legacy_ids <- attr(
    usgs_total_nitrogen,
    "legacy_tn_ids_retained"
  )
  replace_legacy_tn <- legacy_usgs_tn &
    after[[1L]] %in% replaced_legacy_ids
  retain_unmatched_tn <- legacy_usgs_tn &
    after[[1L]] %in% retained_legacy_ids
  # two removed clinton tds rows had tn replacements; the third did not
  assert_count(sum(replace_legacy_tn), 9122L,
               "legacy total-nitrogen rows replaced by provider results")
  assert_count(sum(retain_unmatched_tn), 4372L,
               "legacy total-nitrogen rows retained without provider results")
  after <- after[!replace_legacy_tn, , drop = FALSE]
  provider_columns <- setdiff(names(usgs_total_nitrogen), names(after))
  for (column_name in provider_columns) {
    after[[column_name]] <- NA_character_
  }
  missing_replacement_columns <- setdiff(names(after), names(usgs_total_nitrogen))
  if (length(missing_replacement_columns) > 0L) {
    stop(
      "the USGS total-nitrogen replacement lacks source columns: ",
      paste(missing_replacement_columns, collapse = ", ")
    )
  }
  replacement_order <- match(names(after), names(usgs_total_nitrogen))
  usgs_total_nitrogen <- usgs_total_nitrogen[
    , replacement_order,
    drop = FALSE
  ]
  names(usgs_total_nitrogen) <- names(after)
  after <- rbind(after, usgs_total_nitrogen)
  rownames(after) <- NULL

  assert_count(nrow(after), 982882L,
               "prepared chemistry rows after the USGS nitrogen rebuild")
  assert_count(sum(
    after$LTER == "USGS" & after$variable == "TN" & after$units == "uM"
  ), 20851L, "rebuilt USGS total-nitrogen source rows")
  assert_count(sum(
    after$LTER == "USGS" & after$variable == "TDN" & after$units == "uM"
  ), 14460L, "rebuilt USGS dissolved-total-nitrogen source rows")

  usgs_tds_backfill <- build_usgs_tds_backfill()
  missing_tds_columns <- setdiff(names(after), names(usgs_tds_backfill))
  extra_tds_columns <- setdiff(names(usgs_tds_backfill), names(after))
  if (length(missing_tds_columns) > 0L || length(extra_tds_columns) > 0L) {
    stop("the USGS TDS backfill columns do not match the legacy source")
  }
  tds_column_order <- match(names(after), names(usgs_tds_backfill))
  if (anyNA(tds_column_order)) {
    stop("the USGS TDS backfill column order changed")
  }
  usgs_tds_backfill <- usgs_tds_backfill[
    , tds_column_order,
    drop = FALSE
  ]
  names(usgs_tds_backfill) <- names(after)
  after <- rbind(after, usgs_tds_backfill)
  rownames(after) <- NULL

  prepared_tds <- after$LTER == "USGS" &
    after$variable == "TDS" & after$units == "mg/L"
  assert_count(nrow(after), 993255L,
               "prepared chemistry rows after the USGS TDS backfill")
  assert_count(sum(prepared_tds), 10373L,
               "prepared USGS TDS backfill source rows")
  assert_count(length(unique(paste(
    after$Site.Stream.Name[prepared_tds],
    after$Sampling.Date[prepared_tds],
    sep = "\r"
  ))), 10366L, "prepared USGS TDS backfill daily keys")

  after$source_method_flag <- NA_character_
  bnz_date <- as.Date(after$Sampling.Date)
  bnz_filtered_si <- after$LTER == "BNZ" &
    after$variable == "DSi" &
    after$units == "uM" &
    (
      after$site %in% c("C1", "C2", "C4", "P6") |
        (after$site %in% c("C3", "CJ") & bnz_date <= as.Date("2006-12-31"))
    )
  bnz_undocumented_si <- after$LTER == "BNZ" &
    after$variable == "DSi" &
    after$units == "uM" &
    (
      after$site %in% c("CB", "PC", "PJ") |
        (after$site %in% c("C3", "CJ") & bnz_date >= as.Date("2007-01-01"))
    )
  assert_count(sum(bnz_filtered_si), 345L,
               "BNZ filtered-silicon rows")
  assert_count(sum(bnz_undocumented_si), 243L,
               "BNZ undocumented silicon-fraction rows")
  if (!setequal(
    unique(after$site[bnz_filtered_si]),
    c("C1", "C2", "C3", "C4", "CJ", "P6")
  )) {
    stop("the reviewed BNZ filtered-silicon sites changed")
  }
  after$source_method_flag[bnz_filtered_si] <-
    "filtered Si, BNZ stream protocol"
  after$source_method_flag[bnz_undocumented_si] <-
    "BNZ Si fraction not documented"
  retained_unmatched_tn <- after$LTER == "USGS" &
    after$variable == "TN" &
    after$units == "uM" &
    after[[1L]] %in% retained_legacy_ids
  after$source_method_flag[retained_unmatched_tn] <-
    "legacy TN retained without matching USGS result"
  assert_count(sum(retained_unmatched_tn), 4372L,
               "flagged unmatched legacy total-nitrogen rows")

  write_prepared(filename, after)
}

prepare_current_usgs <- function() {
  expected_rows <- c(
    "USGS__USGS__Choptank_River__da9275d27028.csv" = 23088L,
    "USGS__USGS__Frio_River__4e8764089639.csv" = 7192L,
    "USGS__USGS__Mississippi_River_above_Vicksburg__d2360d259841.csv" = 7420L,
    "USGS__USGS__SUSQUEHANNA_RIVER__4db2531edf53.csv" = 31148L
  )

  for (filename in names(expected_rows)) {
    before <- read_source(filename, directory = input_dir)
    after <- before
    assert_count(nrow(before), expected_rows[[filename]],
                 paste0(filename, " rows"))

    if (grepl("Frio_River", filename, fixed = TRUE)) {
      reviewed_do_percent <- after$Stream_Name == "Frio River" &
        after$date == "2016-10-25" & after$solute == "do percent" &
        after$solute_units == "%"
      assert_count(sum(reviewed_do_percent, na.rm = TRUE), 1L,
                   "current Frio reviewed oxygen-percent row")
      reviewed_value <- as_number(after$amount[reviewed_do_percent])
      if (is.finite(reviewed_value) && reviewed_value == 221.5) {
        # keep the reported value for later outlier review
      } else if (is_blank(after$amount[reviewed_do_percent])) {
        raw_source <- read_source(filename, directory = input_dir)
        raw_row <- raw_source$Stream_Name == "Frio River" &
          raw_source$date == "2016-10-25" &
          raw_source$solute == "do percent" &
          raw_source$solute_units == "%"
        assert_count(sum(raw_row, na.rm = TRUE), 1L,
                     "raw Frio reviewed oxygen-percent row")
        assert_values(as_number(raw_source$amount[raw_row]), 221.5,
                      "raw Frio reviewed oxygen-percent value")
        after$amount[reviewed_do_percent] <- "221.5"
      } else {
        stop("the current Frio reviewed oxygen-percent value changed")
      }
    }

    if (grepl("SUSQUEHANNA_RIVER", filename, fixed = TRUE)) {
      exclusions <- readRDS(usgs_exclusion_file)
      exclusion_review <- exclusions[exclusions$source_file == filename, ]
      exclusion_review <- exclusion_review[
        !duplicated(exclusion_review$source_row),
        ,
        drop = FALSE
      ]
      exclusion_review <- exclusion_review[
        order(exclusion_review$source_row),
        ,
        drop = FALSE
      ]
      exclusion_rows <- exclusion_review$source_row
      assert_count(length(exclusion_rows), 424L,
                   "Susquehanna provider-confirmed TDS rows")
      if (any(exclusion_rows < 1L | exclusion_rows > nrow(after))) {
        stop("a Susquehanna exclusion row is outside the source file")
      }
      if (
        any(after$solute[exclusion_rows] != "TN") ||
          any(after$solute_units[exclusion_rows] != "uM") ||
          any(after$date[exclusion_rows] != as.character(exclusion_review$date)) ||
          any(
            abs(
              as_number(after$amount[exclusion_rows]) -
                exclusion_review$source_value
            ) > 1e-8
          )
      ) {
        stop("the Susquehanna TDS-as-TN source rows changed")
      }

      # the saved source retains its reported labels
      # exclude only rows matched to official tds parameter codes here
      after <- after[-exclusion_rows, , drop = FALSE]
      rownames(after) <- NULL
      assert_count(nrow(after), 30724L, "prepared Susquehanna rows")
      assert_count(sum(after$solute == "TN", na.rm = TRUE), 0L,
                   "prepared Susquehanna mislabeled TN rows")
      source_nh4 <- after$solute == "NH4" & after$solute_units == "uM"
      assert_count(sum(source_nh4, na.rm = TRUE), 1078L,
                   "current Susquehanna NH4 rows")
      after$solute[source_nh4] <- "NHX"
    }

    write_prepared(filename, after)
  }
}
