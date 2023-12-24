/************************************************************************
@file CohortExtraction.sql

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

-- {DEFAULT @cdm_database_schema = 'CDM_SIM' }
-- {DEFAULT @cohort_database_schema = 'CDM_SIM' }
-- {DEFAULT @cohort_table = 'cohort' }
-- {DEFAULT @time_at_risk = 21 }
-- {DEFAULT @delta_time = 22 }

--set search_path to results_optum_extended_ses_v2559, cdm_optum_extended_ses_v2559;
--set search_path to results_truven_ccae_v2542, cdm_truven_ccae_v2542;




-- cohort records would come from whereever cohort table is populated, which can be input parameter.  here, im grabbing from where ATLAS writes them, but in package, we'd write cohort to temp table and get from there
-- make cohort end date = start date + fixed interval time-at-risk, right-censored by observation period

drop table if exists #target;

create table #target as
    select c1.cohort_definition_id,
           c1.subject_id,
           c1.cohort_start_date,
           case when dateadd(day, @time_at_risk, c1.cohort_start_date) < op1.observation_period_end_date
            then dateadd(day, @time_at_risk, c1.cohort_start_date)
            else op1.observation_period_end_date
           end as cohort_end_date
    from @cohort_database_schema.@cohort_table c1
    inner join @cdm_database_schema.observation_period op1
    on c1.subject_id = op1.person_id
    and c1.cohort_start_date >= op1.observation_period_start_date
    and c1.cohort_start_date <= op1.observation_period_end_date
    where cohort_definition_id in (@cohort_ids)
;


--comparator,  max(record) in target + time-at-risk + 1, make cohort end date = start date + fixed interval time-at-risk, right-censored by observation period

drop table if exists #comparator;

create table #comparator as
    select c1.cohort_definition_id,
           c1.subject_id,
           c1.cohort_start_date,
           case when dateadd(day, @time_at_risk, c1.cohort_start_date) < op1.observation_period_end_date
            then dateadd(day, @time_at_risk, c1.cohort_start_date)
            else op1.observation_period_end_date
          end as cohort_end_date
    from (
        select cohort_definition_id,
               subject_id,
               dateadd(day, @delta_time, max(cohort_start_date)) as cohort_start_date
        from #target
        group by cohort_definition_id, subject_id
    ) c1
    inner join @cdm_database_schema.observation_period op1
    on c1.subject_id = op1.person_id
    and c1.cohort_start_date >= op1.observation_period_start_date
    and c1.cohort_start_date <= op1.observation_period_end_date
;

--find the distinct strata that each target cohort has

drop table if exists #strata;

create table #strata as
    select cohort_definition_id,
           row_number() over (
            partition by cohort_definition_id
                order by cohort_start_date,
                         age_group,
                         gender_concept_id,
                         race_concept_id,
                         ethnicity_concept_id
           ) as strata_id,
                cohort_start_date,
                age_group,
                gender_concept_id,
                race_concept_id,
                ethnicity_concept_id
    from (
        select distinct t0.cohort_definition_id,
                        t0.cohort_start_date,
                        floor((extract(year from t0.cohort_start_date) - p1.year_of_birth) / 5) as age_group,
                        p1.gender_concept_id,
                        p1.race_concept_id,
                        p1.ethnicity_concept_id
        from #target t0
        inner join @cdm_database_schema.person p1
            on t0.subject_id = p1.person_id
    ) strata
;


--find the strata that each comparator cohort has  (removing strata that are only in target)

drop table if exists #matched_strata;

create table #matched_strata as
    select s1.cohort_definition_id,
           s1.strata_id,
           s1.cohort_start_date,
           s1.age_group,
           s1.gender_concept_id,
           s1.race_concept_id,
           s1.ethnicity_concept_id
    from #strata s1
    inner join (
        select distinct c0.cohort_definition_id,
                        c0.cohort_start_date,
                        floor((extract(year from c0.cohort_start_date) - p1.year_of_birth) / 5) as age_group,
                        p1.gender_concept_id,
                        p1.race_concept_id,
                        p1.ethnicity_concept_id
        from #comparator c0
        inner join @cdm_database_schema.person p1
        on c0.subject_id = p1.person_id
    ) c1
    on s1.cohort_definition_id = c1.cohort_definition_id
    and s1.cohort_start_date = c1.cohort_start_date
    and s1.age_group = c1.age_group
    and s1.gender_concept_id = c1.gender_concept_id
    and s1.race_concept_id = c1.race_concept_id
    and s1.ethnicity_concept_id = c1.ethnicity_concept_id
;


--create person-level dataset that contains only those cohort records that belong to the n:m matched strata, which woudl be used within conditional Possion regression.
-- exposure_id is 1 = target, 0 = comparator
-- carrying along the attributes in addition to strata_id, in case marc wants to do subgroup analysses or multi-variate adjustment

drop table if exists #matched_cohort;

create table #matched_cohort as
    select t1.cohort_definition_id,
           1 as exposure_id  /*target*/,
           s1.strata_id,
           t1.subject_id,
           t1.cohort_start_date,
           t1.cohort_end_date,
           t1.age_group,
           t1.gender_concept_id,
           t1.race_concept_id,
           t1.ethnicity_concept_id
    from (
        select t0.cohort_definition_id,
               t0.subject_id,
               t0.cohort_start_date,
               t0.cohort_end_date,
               floor((extract(year from t0.cohort_start_date) - p1.year_of_birth) / 5) as age_group,
               p1.gender_concept_id,
               p1.race_concept_id,
               p1.ethnicity_concept_id
        from #target t0
        inner join @cdm_database_schema.person p1
        on t0.subject_id = p1.person_id
    ) t1
    inner join #matched_strata s1
    on t1.cohort_definition_id = s1.cohort_definition_id
    and t1.cohort_start_date = s1.cohort_start_date
    and t1.age_group = s1.age_group
    and t1.gender_concept_id = s1.gender_concept_id
    and t1.race_concept_id = s1.race_concept_id
    and t1.ethnicity_concept_id = s1.ethnicity_concept_id

    union all

    select c1.cohort_definition_id,
           0 as exposure_id /*comparator*/,
           s1.strata_id,
           c1.subject_id,
           c1.cohort_start_date,
           c1.cohort_end_date,
           c1.age_group,
           c1.gender_concept_id,
           c1.race_concept_id,
           c1.ethnicity_concept_id
    from (
        select c0.cohort_definition_id,
               c0.subject_id,
               c0.cohort_start_date,
               c0.cohort_end_date,
               floor((extract(year from c0.cohort_start_date) - p1.year_of_birth) / 5) as age_group,
               p1.gender_concept_id,
               p1.race_concept_id,
               p1.ethnicity_concept_id
        from #comparator c0
        inner join @cdm_database_schema.person p1
        on c0.subject_id = p1.person_id
    ) c1
    inner join #matched_strata s1
    on c1.cohort_definition_id = s1.cohort_definition_id
    and c1.cohort_start_date = s1.cohort_start_date
    and c1.age_group = s1.age_group
    and c1.gender_concept_id = s1.gender_concept_id
    and c1.race_concept_id = s1.race_concept_id
    and c1.ethnicity_concept_id = s1.ethnicity_concept_id
;
