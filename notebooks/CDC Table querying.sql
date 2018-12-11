-- Databricks notebook source
-- GROUP BY sex
select count(*), sex from cdc_data
group by sex

-- COMMAND ----------

-- GROUP BY resident_status
select count(*), resident_status from cdc_data
group by resident_status
order by count(*) desc

-- COMMAND ----------

-- GROUP BY age
select count(*), age_recode_12 from cdc_data
group by age_recode_12
order by count(*) desc

-- COMMAND ----------

-- GROUP BY place of injury
select count(*), place_of_injury_for_causes_w00_y34_except_y06_and_y07_ from cdc_data
group by place_of_injury_for_causes_w00_y34_except_y06_and_y07_
order by count(*) desc

-- COMMAND ----------

-- GROUP BY place of death
select count(*), place_of_death_and_decedents_status from cdc_data
group by place_of_death_and_decedents_status
order by count(*) desc

-- COMMAND ----------

-- group by marital status and age
select count(*), marital_status, age_recode_12 from cdc_data
group by marital_status, age_recode_12
order by count(*) desc

-- COMMAND ----------

-- group by marital status age
select count(*), marital_status, sex, age_recode_12, 39_cause_recode, 113_cause_recode from cdc_data
where manner_of_death IN (1,2,3,4,5,6)
group by marital_status, sex, age_recode_12, 39_cause_recode, 113_cause_recode
order by count(*) desc

-- COMMAND ----------

select race, count(*)
from cdc_data
group by race 
order by count(*) desc

-- COMMAND ----------

select distinct manner_of_death, count(*)
from cdc_data
group by manner_of_death
order by manner_of_death desc

-- COMMAND ----------

select count(*), activity_code
from cdc_data
where manner_of_death = 2
group by activity_code
order by count(*) desc

-- COMMAND ----------

select count(*), race,
current_data_year
from cdc_data
where icd_code_10th_revision IN('X72', 'X073', 'X074', 'X094', 'X095', 'Y023', 'Y024')
group by race, current_data_year
order by count(*) desc
limit 5

-- COMMAND ----------

select count(*), method_of_disposition
FROM cdc_data
where education_1989_revision != null
group by method_of_disposition
order by count(*) desc

-- COMMAND ----------

select distinct education_1989_revision from cdc_data

-- COMMAND ----------

select count(*), Hispanic_originrace_recode
FROM cdc_data
where 358_cause_recode BETWEEN 383 AND 399
group by Hispanic_originrace_recode
order by count(*) desc

-- COMMAND ----------

select race, 39_cause_recode, 113_cause_recode, 358_cause_recode, count(*)
from cdc_data
where race != 1
group by race, 39_cause_recode, 113_cause_recode, 358_cause_recode
order by count(*) DESC
