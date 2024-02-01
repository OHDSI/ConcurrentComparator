/************************************************************************
@file GetOutcomes.sql

Copyright 2023 Observational Health Data Sciences and Informatics

This file is part of ConcurrentComparator

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
************************************************************************/

{@outcome_table == 'condition_era' } ? {

SELECT DISTINCT matched_cohort.subject_id,
    matched_cohort.strata_id,
	ancestor_concept_id AS outcome_id,
	matched_cohort.cohort_start_date AS cohort_start_date,
	DATEDIFF(DAY, matched_cohort.cohort_start_date, condition_era_start_date) AS days_to_event,
	condition_era_start_date AS outcome_start_date
FROM #matched_cohort matched_cohort
INNER JOIN @cdm_database_schema.condition_era
    ON subject_id = person_id
INNER JOIN (
	SELECT descendant_concept_id,
		ancestor_concept_id
	FROM @cdm_database_schema.concept_ancestor
	WHERE ancestor_concept_id IN (@outcome_ids)
	) concept_ancestor
	ON condition_concept_id = descendant_concept_id
WHERE DATEDIFF(DAY, matched_cohort.cohort_start_date, condition_era_start_date) >= @days_from_obs_start
	AND DATEDIFF(DAY, matched_cohort.cohort_start_date, condition_era_start_date) <= @days_to_obs_end

} : {

SELECT DISTINCT matched_cohort.subject_id,
    matched_cohort.strata_id,
	outcome.cohort_definition_id AS outcome_id,
	matched_cohort.cohort_start_date AS cohort_start_date,
	DATEDIFF(DAY, matched_cohort.cohort_start_date, outcome.cohort_start_date) AS days_to_event,
	outcome.cohort_start_date AS outcome_start_date
FROM #matched_cohort matched_cohort
INNER JOIN @outcome_database_schema.@outcome_table outcome
	ON matched_cohort.subject_id = outcome.subject_id
WHERE outcome.cohort_definition_id IN (@outcome_ids)
    AND matched_cohort.cohort_definition_id IN (@exposure_ids)
    AND DATEDIFF(DAY, matched_cohort.cohort_start_date, outcome.cohort_start_date) >= @days_from_obs_start
	AND DATEDIFF(DAY, matched_cohort.cohort_start_date, outcome.cohort_start_date) <= @days_to_obs_end

}
