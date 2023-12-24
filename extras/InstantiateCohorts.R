rJava::.jinit(parameters="-Xmx8g", force.init = TRUE)

library(CohortGenerator)
library(CirceR)

cdmDatabaseSchema <- "cdm_optum_ehr_v2247"
serverSuffix <- "optum_ehr"
cohortDatabaseSchema <- "scratch_msuchard"
cohortTable <- "mrna_cohort"

conn <- DatabaseConnector::createConnectionDetails(
    dbms = "redshift",
    server = paste0(keyring::key_get("redshiftServer"), "/", !!serverSuffix),
    port = 5439,
    user = keyring::key_get("redshiftUser"),
    password = keyring::key_get("redshiftPassword"),
    extraSettings = "ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory")

info <- list(
	list(cohortId = 666, cohortName = "Pfizer", fileName = "extras/pfizer.json"),
	list(cohortId = 667, cohortName = "Moderna", fileName = "extras/moderna.json"),
	list(cohortId = 668, cohortName = "Myocarditis/pericarditis", fileName = "extras/MyocarditisPericarditis.json"))

cohortDefinitionSet <- do.call(
    rbind,
    lapply(info, function(cohort) {
        cohortJson <- readChar(cohort$fileName, file.info(cohort$fileName)$size)
        cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
        cohortSql <- CirceR::buildCohortQuery(cohortExpression,
                                              options = CirceR::createGenerateOptions(
                                                  generateStats = FALSE))
        data.frame(
            cohortId = cohort$cohortId,
            cohortName = cohort$cohortName,
            sql = cohortSql,
            json = cohortJson,
            stringsAsFactors = FALSE
        )
    }))


cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)

CohortGenerator::createCohortTables(connectionDetails = conn,
                                    cohortDatabaseSchema = cohortDatabaseSchema,
                                    cohortTableNames = cohortTableNames,
                                    incremental = TRUE)

CohortGenerator::generateCohortSet(connectionDetails = conn,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                   cohortTableNames = cohortTableNames,
                                   cohortDefinitionSet = cohortDefinitionSet,
                                   incremental = TRUE,
                                   incrementalFolder = ".")

CohortGenerator::getCohortCounts(connectionDetails = conn,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable)




# test internal parts

# cdmDatabaseSchema <- "cdm_database_schema"
# cohortDatabaseSchema <- "scratch"
# cohortTable <- "cc_cohorts"
timeAtRisk <- 21
deltaTime <- 22
dbms <- "redshift"
cohortIds <- c(666,667)

sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CohortExtraction.sql",
                                         packageName = "ConcurrentComparator",
                                         dbms = dbms,
                                         cdm_database_schema = cdmDatabaseSchema,
                                         cohort_database_schema = cohortDatabaseSchema,
                                         cohort_table = cohortTable,
                                         time_at_risk = timeAtRisk,
                                         delta_time = deltaTime,
                                         cohort_ids = cohortIds,
                                         warnOnMissingParameters = TRUE)

connection <- DatabaseConnector::connect(conn)

ParallelLogger::logInfo("Creating matched cohorts in database")
DatabaseConnector::executeSql(connection = connection, sql = sql)

# DatabaseConnector::executeSql(connection = connection,
#                               sql = "SELECT * INTO scratch_msuchard.mrna_matched_cohort FROM #matched_cohort")

# DatabaseConnector::querySql(connection = connection,
#                            sql = "SELECT COUNT(subject_id) FROM #matched_cohort")

andromeda <- Andromeda::andromeda()

ParallelLogger::logInfo("Pulling matched cohorts down to local system")
DatabaseConnector::querySqlToAndromeda(connection = connection,
                                       sql = "SELECT * FROM #strata WHERE cohort_definition_id = 667",
                                       andromeda = andromeda,
                                       andromedaTableName = "strata",
                                       snakeCaseToCamelCase = TRUE)

sql <- "
SELECT exposure_id,
       strata_id,
       subject_id,
       DATEDIFF(DAY, cohort_start_date, cohort_end_date) AS time_at_risk
FROM #matched_cohort
WHERE cohort_definition_id = 667
"

DatabaseConnector::querySqlToAndromeda(connection = connection,
                                       sql = sql,
                                       andromeda = andromeda,
                                       andromedaTableName = "matchedCohort",
                                       snakeCaseToCamelCase = TRUE)


sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "GetOutcomes.sql",
                                         packageName = "ConcurrentComparator",
                                         dbms = dbms,
                                         outcome_database_schema = cohortDatabaseSchema,
                                         outcome_table = cohortTable,
                                         outcome_ids = 668,
                                         exposure_ids = 667,
                                         warnOnMissingParameters = TRUE)

DatabaseConnector::querySqlToAndromeda(connection = connection,
                                       sql = sql,
                                       andromeda = andromeda,
                                       andromedaTableName = "outcome",
                                       snakeCaseToCamelCase = TRUE)


fileName <- "t667_matched2.zip"
Andromeda::saveAndromeda(andromeda, fileName = fileName)
ParallelLogger::logInfo("Matched cohorts saved to: ", fileName)

DatabaseConnector::disconnect(connection = connection)

andromeda <- Andromeda::loadAndromeda(fileName = fileName)
names(andromeda)

andromeda$matchedCohort <- andromeda$matchedCohort %>%
    left_join(andromeda$outcome %>%
                  select(subjectId, strataId, daysToEvent, outcomeStartDate),
              by = c("subjectId", "strataId")
    ) %>%
    mutate(daysToEvent = ifelse(is.na(daysToEvent),-1, daysToEvent)) %>%
    mutate(outcome = ifelse(daysToEvent >= 0 & daysToEvent <= timeAtRisk, 1, 0))

sum(andromeda$matchedCohort %>% pull(outcome))

fileName <- "t667_matchedOutcome.zip"
Andromeda::saveAndromeda(andromeda, fileName = fileName)
ParallelLogger::logInfo("Matched cohorts saved to: ", fileName)


andromeda <- Andromeda::loadAndromeda(fileName = fileName)

andromeda$matchedCohort %>% filter(daysToEvent >= 21,
                                   daysToEvent <= 30) %>%
    inner_join(andromeda$strata) %>%
    collect() %>% mutate(osd = restoreDate(outcomeStartDate)) %>%
    select(strataId, subjectId, outcome, daysToEvent, osd, cohortStartDate) %>%
    print(n = 10)

tmp <- andromeda$matchedCohort %>% mutate(dt = format(as.Date(outcomeStartDate)))

noPHI <- andromeda$matchedCohort %>% select(exposureId, strataId, outcome, timeAtRisk)
write.csv(noPHI, file = "no_phi.csv", quote = FALSE)
saveRDS(noPHI %>% collect(), file = "no_phi.Rds")
tmp <- readRDS(file = "no_phi.Rds")

tmp <- noPHI %>% group_by(strataId, exposureId) %>%
    summarize(outcome = sum(outcome),
              timeAtRisk = sum(timeAtRisk)) %>%
    collect()

saveRDS(tmp %>% collect(), file = "no_phi_grouped.Rds")

