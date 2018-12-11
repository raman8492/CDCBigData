-- Databricks notebook source
select count(*), marital_status, sex, age_recode_12, 39_cause_recode, 113_cause_recode from my_table
where manner_of_death = 7
group by marital_status, sex, age_recode_12, 39_cause_recode, 113_cause_recode
order by count(*) desc


-- COMMAND ----------

--Group by Raceagesex39_cause_recode
select count(*), race, sex,month_of_death,day_of_week_of_death from my_table
--where manner_of_death = 7
group by race, sex,month_of_death,day_of_week_of_death
order by count(*) desc


-- COMMAND ----------

--Group by Raceagesex39_cause_recode
select count(*), manner_of_death from my_table
where race = 7 and sex = 'F' and  month_of_death = 01 and day_of_week_of_death = 7
group by manner_of_death 
order by count(*) desc

-- COMMAND ----------

select count(*), current_data_year, 39_cause_recode,113_cause_recode,race from my_table
--where manner_of_death = 2
group by current_data_year, 39_cause_recode,113_cause_recode,race
order by count(*) desc

-- COMMAND ----------

select count(*) from my_table
where manner_of_death = 6
--group by month_of_death, sex, age_recode_12
order by count(*) desc

-- COMMAND ----------

select count(*),current_data_year,place_of_injury_for_causes_w00_y34_except_y06_and_y07_,place_of_death_and_decedents_status from my_table
where place_of_injury_for_causes_w00_y34_except_y06_and_y07_ is not null
group by current_data_year,place_of_injury_for_causes_w00_y34_except_y06_and_y07_,place_of_death_and_decedents_status
order by count(*) desc

-- COMMAND ----------

select count(*), age_recode_12, method_of_disposition
FROM my_table
where education_2003_revision is not null
group by age_recode_12, method_of_disposition
order by count(*) desc

-- COMMAND ----------

select count(*),  education_2003_revision ,  education_1989_revision
from my_table
where education_2003_revision is NULL or education_1989_revision is null
group by education_2003_revision ,  education_1989_revision

-- COMMAND ----------

select count(*),infant_age_recode_22,130_infant_cause_recode,place_of_death_and_decedents_status 
from my_table
--where education_2003_revision is NULL or education_1989_revision is null
group by infant_age_recode_22,130_infant_cause_recode,place_of_death_and_decedents_status 
order by count(*) DESC

-- COMMAND ----------

select infant_age_recode_22, age_recode_12,39_cause_recode,113_cause_recode, count(*)
from my_table
where infant_age_recode_22 is not null
group by infant_age_recode_22, age_recode_12,39_cause_recode,113_cause_recode
order by count(*) DESC

-- COMMAND ----------

select infant_age_recode_22, age_recode_12,39_cause_recode,113_cause_recode,358_cause_recode, count(*)
from my_table
where infant_age_recode_22 is not null
group by infant_age_recode_22, age_recode_12,39_cause_recode,113_cause_recode,358_cause_recode
order by count(*) DESC

-- COMMAND ----------

select distinct 39_cause_recode,113_cause_recode, 358_cause_recode from my_table
where 39_cause_recode = 33

-- COMMAND ----------

select race,39_cause_recode,113_cause_recode, count(*)
from my_table
--where infant_age_recode_22 is not null
group by race, 39_cause_recode,113_cause_recode
order by count(*) DESC

-- COMMAND ----------

select method_of_disposition ,education_2003_revision, count(*)
from my_table
where method_of_disposition != 'U'
group by method_of_disposition,education_2003_revision
order by count(*) DESC

-- COMMAND ----------

SELECT * FROM my_table LIMIT 10


-- COMMAND ----------


