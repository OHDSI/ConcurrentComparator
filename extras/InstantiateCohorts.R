rJava::.jinit(parameters="-Xmx8g", force.init = TRUE)

library(CohortGenerator)
library(CirceR)
library(Andromeda)

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


ccData <- getDbConcurrentComparatorData(connectionDetails = conn,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        targetId = 667,
                                        outcomeIds = c(668,0),
                                        studyEndDate = "2021-06-30",
                                        exposureDatabaseSchema = cohortDatabaseSchema,
                                        exposureTable = cohortTable,
                                        outcomeDatabaseSchema = cohortDatabaseSchema,
                                        outcomeTable = cohortTable,
                                        timeAtRiskStart = 1,
                                        timeAtRiskEnd = 21,
                                        washoutTime = 22,
                                        intermediateFileNameStem = NULL)

saveConcurrentComparatorData(ccData, "t667_o668.zip")
ccData <- loadConcurrentComparatorData("t667_o668.zip")

# max times subject in T or C
ccData$matchedCohort %>% group_by(subjectId) %>%
    mutate(count = n()) %>% arrange(-count)

# max times subject in C
ccData$matchedCohort %>% filter(exposureId == 0) %>% group_by(subjectId) %>%
    mutate(count = n()) %>% arrange(-count)

population <- createStudyPopulation(ccData, outcomeId = 668)

fit <- fitOutcomeModel(population = population)

### OLD MATERIAL BELOW


# saveRDS(population %>%
#               select(exposureId, strataId,timeAtRisk,y) %>%
#               collect(),
#           file = "no_phi2.Rds")

# test internal parts

# cdmDatabaseSchema <- "cdm_database_schema"
# cohortDatabaseSchema <- "scratch"
# cohortTable <- "cc_cohorts"
timeAtRiskStart <- 1
timeAtRiskEnd <- 21
deltaTime <- 22
dbms <- "redshift"
cohortIds <- c(666,667)

sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CohortExtraction.sql",
                                         packageName = "ConcurrentComparator",
                                         dbms = dbms,
                                         cdm_database_schema = cdmDatabaseSchema,
                                         cohort_database_schema = cohortDatabaseSchema,
                                         cohort_table = cohortTable,
                                         time_at_risk = timeAtRiskEnd,
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
       cohort_start_date,
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


fileName <- "t667_matched3.zip"
Andromeda::saveAndromeda(andromeda, fileName = fileName)
ParallelLogger::logInfo("Matched cohorts saved to: ", fileName)

DatabaseConnector::disconnect(connection = connection)

andromeda <- Andromeda::loadAndromeda(fileName = fileName)
names(andromeda)

# set end date
endDate <- "2021-06-30"
andromeda$matchedCohort <- andromeda$matchedCohort %>%
    mutate(csd = cohortStartDate) %>%
    collect() %>%
    mutate(truncate = Andromeda::restoreDate(cohortStartDate) > as.Date(endDate))

# check matched cohort

# max times subject in T or C
andromeda$matchedCohort %>% group_by(subjectId) %>%
    mutate(count = n()) %>% arrange(-count)

# max times subject in C
andromeda$matchedCohort %>% filter(exposureId == 0) %>% group_by(subjectId) %>%
    mutate(count = n()) %>% arrange(-count)

# truncate by study end date
andromeda$matchedCohort <- andromeda$matchedCohort %>%
    filter(truncate == 0) %>% select(-truncate) %>%
    collect() %>%
    mutate(csd = as.Date(cohortStartDate))

# max times subject in T or C
andromeda$matchedCohort %>% group_by(subjectId) %>%
    mutate(count = n()) %>% arrange(-count)

# max times subject in C
andromeda$matchedCohort %>% filter(exposureId == 0) %>% group_by(subjectId) %>%
    mutate(count = n()) %>% arrange(-count)

## END DEBUG

andromeda$matchedCohort %>% group_by(exposureId) %>%
    summarise(entries = n(),
              subjects = n_distinct(subjectId))

# exposureId entries subjects
# <dbl>   <int>    <int>
# 1          0  789372   789372
# 2          1 1442798   824328


# Add outcomes

intersection <- andromeda$matchedCohort %>%
    inner_join(andromeda$outcome %>%
                  select(subjectId, strataId, cohortStartDate, daysToEvent, outcomeStartDate),
              by = c("subjectId", "strataId", "cohortStartDate")
    ) %>%
    filter(daysToEvent >= timeAtRiskStart,
           daysToEvent <= timeAtRiskEnd) %>%
    mutate(outcome = 1)


andromeda$matchedCohort <- andromeda$matchedCohort %>%
    left_join(intersection %>%
                  select(exposureId, subjectId, strataId, cohortStartDate, outcome, outcomeStartDate, daysToEvent),
              by = c("exposureId", "subjectId", "strataId", "cohortStartDate")
    ) %>%
    mutate(outcome = ifelse(is.na(outcome), 0, outcome))

# andromeda$matchedCohort %>% filter(outcome == 1) %>% collect() %>%
#     mutate(outcomeStartDate = Andromeda::restoreDate(outcomeStartDate),
#            csd = Andromeda::restoreDate(csd))

# Summary statistics

andromeda$matchedCohort %>% group_by(exposureId) %>%
    summarise(entries = n(),
              subjects = n_distinct(subjectId),
              outcomes = sum(outcome),
              kPtYrs = sum(timeAtRisk / 365.25 / 1000))

# exposureId entries subjects outcomes kPtYrs
# <dbl>   <int>    <int>    <dbl>  <dbl>
# 1          0  789372   789372       22   41.8
# 2          1 1442798   824328       52   82.9

fileName <- "t667_matchedOutcome3.zip"
Andromeda::saveAndromeda(andromeda, fileName = fileName)
ParallelLogger::logInfo("Matched cohorts saved to: ", fileName)


andromeda <- Andromeda::loadAndromeda(fileName = fileName)


andromeda$matchedCohort %>% group_by(subjectId) %>%
    mutate(count = n()) %>% arrange(-count)

length(andromeda$matchedCohort %>% pull(exposureId))

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



###

matchedCohort <- readRDS("extras/no_phi_subjects.Rds") %>%
    filter(timeAtRisk > 0) %>%
    mutate(timeAtRisk = timeAtRisk / 365.25 / 1000.0,
           logTimeAtRisk = log(timeAtRisk))

groupedMatchedCohort <- readRDS("extras/no_phi_grouped.Rds") %>%
    filter(timeAtRisk > 0) %>%
    mutate(timeAtRisk = timeAtRisk / 365.25 / 1000.0,
           logTimeAtRisk = log(timeAtRisk))

matchedCohort %>% group_by(exposureId) %>%
    summarize(t1 = sum(outcome),
              t2 = sum(timeAtRisk),
              rate = t1 / t2)

library(Cyclops)
library(survival)
library(gnm)

cyclopsData <- createCyclopsData(outcome ~ exposureId + strata(strataId) + offset(logTimeAtRisk),
                                 data = matchedCohort,
                                 modelType = "cpr")

fit <- fitCyclopsModel(cyclopsData = cyclopsData)
coef(fit)
confint(fit, parm = "exposureId")

cyclopsData <- createCyclopsData(outcome ~ exposureId + strata(strataId) + offset(logTimeAtRisk),
                                 data = groupedMatchedCohort,
                                 modelType = "cpr")

fit <- fitCyclopsModel(cyclopsData = cyclopsData)
coef(fit)
confint(fit, parm = "exposureId")



# gold <- gnm(outcome ~ exposureId + strata(strataId) + offset(logTimeAtRisk),
#             family = poisson,
#             data = groupedMatchedCohort)
#
# gold <- glm(outcome ~  exposureId,
#
#             # offset = timeAtRisk,
#             data = groupedMatchedCohort, family = poisson())
