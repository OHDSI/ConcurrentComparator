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

#' @export
createConcurrentComparatorAnalysis <- function(analysisId = 1,
                                               description = "",
                                               studyStartDate = "",
                                               studyEndDate = "",
                                               timeAtRiskStart = 1,
                                               timeAtRiskEnd = 21,
                                               washoutTime = 22,
                                               stratified = TRUE) {

    analysis <- list()
    for (name in names(formals(createConcurrentComparatorAnalysis))) {
        analysis[[name]] <- get(name)
    }

    class(analysis) <- "ccAnalysis"
    return(analysis)
}
