rJava::.jinit(parameters="-Xmx8g", force.init = TRUE)
library(ConcurrentComparator)

# TODO: Turn this document into a package vignette

# These are needed to cohort generation
library(CohortGenerator)
library(CirceR)
library(Andromeda)

# Set up connection details
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

# Generate target and outcome cohorts onto `cohortDatabaseSchema.cohortTable`
info <- list(
	list(cohortId = 666, cohortName = "T666", fileName = "extras/t666.json"),
	list(cohortId = 667, cohortName = "T667", fileName = "extras/t667.json"),
	list(cohortId = 668, cohortName = "T668", fileName = "extras/t668.json"))

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

# Run CC manually
timeAtRiskStart <- 1
timeAtRiskEnd <- 21
deltaTime <- 22
cohortIds <- c(666,667)
