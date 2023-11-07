library(CohortGenerator)
library(CirceR)

generateStats <- FALSE

info <- list(
	list(cohortId = 666, cohortName = "Pfizer", fileName = "extras/pfizer.json"),
	list(cohortId = 677, cohortName = "Moderna", fileName = "extras/moderna.json"))

cohortDefinitionSet <- do.call(
    rbind,
    lapply(info, function(cohort) {
        cohortJson <- readChar(cohort$fileName, file.info(cohort$fileName)$size)
        cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
        cohortSql <- CirceR::buildCohortQuery(cohortExpression,
                                              options = CirceR::createGenerateOptions(
                                                  generateStats = generateStats))
        data.frame(
            cohortId = cohort$cohortId,
            cohortName = cohort$cohortName,
            sql = cohortSql,
            json = cohortJson,
            stringsAsFactors = FALSE
        )
    }))


# test internal parts

cdmDatabaseSchema <- "cdm_database_schema"
cohortDatabaseSchema <- "scratch"
cohortTable <- "cc_cohorts"
timeAtRisk <- 21
deltaTime <- 22
dbms <- "sql server"
cohortIds <- c(1,2)

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
