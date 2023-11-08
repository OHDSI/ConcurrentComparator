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

SELECT matched_cohort.subject_id,
	outcome.cohort_definition_id AS outcome_id,
	DATEDIFF(DAY, matched_cohort.cohort_start_date, outcome.cohort_start_date) AS days_to_event
FROM #matched_cohort matched_cohort
INNER JOIN @outcome_database_schema.@outcome_table outcome
	ON matched_cohort.subject_id = outcome.subject_id
WHERE DATEDIFF(DAY, outcome.cohort_start_date, matched_cohort.cohort_start_date) <= days_from_obs_start
	AND DATEDIFF(DAY, matched_cohort.cohort_start_date, outcome.cohort_start_date) <= days_to_obs_end
	AND outcome.cohort_definition_id IN (@outcome_ids)

