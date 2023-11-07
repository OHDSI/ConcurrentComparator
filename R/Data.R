# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of ConcurrentComparator
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Concurrent Comparator Data
#'
#' @description
#' `ConcurrentComparatorData` is an S4 class that inherits from [Andromeda][Andromeda::Andromeda]. It contains information on the cohorts, their
#' outcomes, and baseline covariates. Information about multiple outcomes can be captured at once for
#' efficiency reasons.
#'
#' A `ConcurrentComparatorData` is typically created using [getDbConcurrentComparatorData()], can only be saved using
#' [saveConcurrentComparatorData()], and loaded using [loadConcurrentComparatorData()].
#'
#' @name ConcurrentComparatorData-class
#' @aliases ConcurrentComparatorData
NULL

#' ConcurrentComparatorData class.
#'
#' @export
#' @import Andromeda
setClass("ConcurrentComparatorData", contains = "Andromeda")

#' @export
getDbConcurrentComparatorData <- function() {
    return(list())
}

#' Get the concurrent comparator from the server
#'
#' @description
#' This function executes a large set of SQL statements against the database in OMOP CDM format to
#' extract the data needed to perform the analysis.
#'
#' @details
#' TODO
#'
#' @param connectionDetails            An R object of type `connectionDetails` created using the
#'                                     [DatabaseConnector::createConnectionDetails()] function.
#' @param cdmDatabaseSchema            The name of the database schema that contains the OMOP CDM
#'                                     instance. Requires read permissions to this database. On SQL
#'                                     Server, this should specify both the database and the schema,
#'                                     so for example 'cdm_instance.dbo'.
#' @param tempEmulationSchema          Some database platforms like Oracle and Impala do not truly support temp tables. To
#'.                                    emulate temp tables, provide a schema with write privileges where temp tables
#'                                    can be created.
#' @param targetId                     A unique identifier to define the target cohort. targetId is
#'                                     used to select the COHORT_DEFINITION_ID in the cohort-like table.
#' @param comparatorId                 A unique identifier to define the comparator cohort. comparatorId
#'                                     is used to select the COHORT_DEFINITION_ID in the cohort-like
#'                                     table. TODO UPDATE
#' @param overwriteComparators         Allow regeneration if comparators if comparatorId already exists
#'                                     TODO what is the table name?
#' @param outcomeIds                   A list of cohort IDs used to define outcomes.
#'
#' @param studyStartDate               A calendar date specifying the minimum date that a cohort index
#'                                     date can appear. Date format is 'yyyymmdd'.
#' @param studyEndDate                 A calendar date specifying the maximum date that a cohort index
#'                                     date can appear. Date format is 'yyyymmdd'. Important: the study
#'                                     end data is also used to truncate risk windows, meaning no
#'                                     outcomes beyond the study end date will be considered.
#' @param exposureDatabaseSchema       The name of the database schema that is the location where the
#'                                     exposure data used to define the exposure cohorts is available.
#' @param exposureTable                The tablename that contains the exposure cohorts has the
#'                                     format of a COHORT table: COHORT_DEFINITION_ID, SUBJECT_ID,
#'                                     COHORT_START_DATE, COHORT_END_DATE.
#' @param outcomeDatabaseSchema        The name of the database schema that is the location where the
#'                                     data used to define the outcome cohorts is available.
#' @param outcomeTable                 The tablename that contains the outcome cohorts has the format of a COHORT table: COHORT_DEFINITION_ID,
#'                                     SUBJECT_ID, COHORT_START_DATE, COHORT_END_DATE.
#' @param timeAtRisk                   The time-at-risk duration in days after subject index date.
#'
#' @return
#' A [ConcurrentComparatorData] object.
#'
#' @export
getDbConcurrentComparatorData <- function(connectionDetails,
                                  cdmDatabaseSchema,
                                  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                  exposureIds,
                                  targetId = 1,
                                  comparatorId = 1,
                                  outcomeIds,
                                  overwriteComparators = FALSE,
                                  studyStartDate = "",
                                  studyEndDate = "",
                                  exposureDatabaseSchema = cdmDatabaseSchema,
                                  exposureTable,
                                  outcomeDatabaseSchema = cdmDatabaseSchema,
                                  outcomeTable,
                                  timeAtRisk) {

    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertClass(connectionDetails, "ConnectionDetails", add = errorMessages)
    checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
    checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
    checkmate::assertIntegerish(exposureIds, add = errorMessages)
    checkmate::assertInt(targetId, add = errorMessages) # TODO Remove
    checkmate::assertInt(comparatorId, add = errorMessages) # TODO Remove
    checkmate::assertIntegerish(outcomeIds, add = errorMessages)
    checkmate::assertCharacter(studyStartDate, len = 1, add = errorMessages)
    checkmate::assertCharacter(studyEndDate, len = 1, add = errorMessages)
    checkmate::assertCharacter(exposureDatabaseSchema, len = 1, add = errorMessages)
    checkmate::assertCharacter(exposureTable, len = 1, add = errorMessages)
    checkmate::assertCharacter(outcomeDatabaseSchema, len = 1, add = errorMessages)
    checkmate::assertCharacter(outcomeTable, len = 1, add = errorMessages)
    checkmate::assertInt(timeAtRisk, lower = 1, add = errorMessages)
    checkmate::assertLogical(overwriteComparators, len = 1, add = errorMessages) # TODO Remove
    checkmate::reportAssertions(collection = errorMessages)


    ccData <- Andromeda::andromeda()

    # DatabaseConnector::querySqlToAndromeda(
    #     connection = conn,
    #     sql = sql,
    #     andromeda = sccsData,
    #     andromedaTableName = "cases",
    #     snakeCaseToCamelCase = TRUE
    # )

    #covariateData$cohorts <- cohorts
    #covariateData$outcomes <- outcomes
    #attr(covariateData, "metaData") <- append(attr(covariateData, "metaData"), metaData)

    class(ccData) <- "ConcurrentComparatorData"
    attr(class(ccData), "package") <- "ConcurrentComparator"
    return(ccData)
}


#' Save the concurrent comparator data to file
#'
#' @description
#' Saves an object of type [ConcurrentComparatorData] to a file.
#'
#' @template ConcurrentComparatorData
#' @param file               The name of the file where the data will be written. If the file already
#'                           exists it will be overwritten.
#'
#' @return
#' Returns no output.
#'
#' @export
saveConcurrentComparatorData <- function(concurrentComparatorData, file) {
    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertClass(concurrentComparatorData, "ConcurrentComparatorData",
                           add = errorMessages)
    checkmate::assertCharacter(file, len = 1, add = errorMessages)
    checkmate::reportAssertions(collection = errorMessages)

    Andromeda::saveAndromeda(concurrentComparatorData, file)
}

#' Load the concurrent comparator data from a file
#'
#' @description
#' Loads an object of type [ConcurrentComparatorData] from a file in the file system.
#'
#' @param file       The name of the file containing the data.
#'
#' @return
#' An object of class [ConcurrentComparatorData].
#'
#' @export
loadConcurrentComparatorData <- function(file) {
    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertCharacter(file, len = 1, add = errorMessages)
    checkmate::reportAssertions(collection = errorMessages)
    if (!file.exists(file)) {
        stop("Cannot find file ", file)
    }
    if (file.info(file)$isdir) {
        stop(file, " is a folder, but should be a file")
    }
    ConcurrentComparatorData <- Andromeda::loadAndromeda(file)
    class(ConcurrentComparatorData) <- "ConcurrentComparatorData"
    attr(class(ConcurrentComparatorData), "package") <- "ConcurrentComparator"
    return(ConcurrentComparatorData)
}
