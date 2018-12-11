# Databricks notebook source
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
import numpy as np
from pandas import *
from ggplot import *

# COMMAND ----------

cdc_data = spark.sql("select * from cdc_data")


# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'resident_status'")
cdc_data=cdc_data.join(df_json_rs, cdc_data['resident_status'] == df_json_rs.Code)
cdc_data=cdc_data.drop('col_name')
cdc_data=cdc_data.drop('code')
cdc_data=cdc_data.withColumnRenamed("comment", "resident_status_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from education_1989_revision_csv where col_name = 'education_1989_revision'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['education_1989_revision'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("description","education_1989_revision_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'education_2003_revision'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['education_2003_revision'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('Code')
cdc_data = cdc_data.withColumnRenamed("comment","education_2003_revision_description")

# COMMAND ----------

cdc_data = cdc_data.join(df_json_rs, cdc_data['education_reporting_flag'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "education_reporting_flag_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'month_of_death'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['month_of_death'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "month_of_death_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'age_recode_12'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['age_recode_12'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "age_recode_12_description")


# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'place_of_death_and_decedents_status'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['place_of_death_and_decedents_status'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "place_of_death_and_decedents_status_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'marital_status'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['marital_status'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "marital_status_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'day_of_week_of_death'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['day_of_week_of_death'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "day_of_week_of_death_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'injury_at_work'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['injury_at_work'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "injury_at_work_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'manner_of_death'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['manner_of_death'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "manner_of_death_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'method_of_disposition'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['method_of_disposition'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "method_of_disposition_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'activity_code'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['activity_code'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "activity_code_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'place_of_injury_for_causes_w00_y34_except_y06_and_y07_'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['place_of_injury_for_causes_w00_y34_except_y06_and_y07_'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "place_of_injury_for_causes_w00_y34_except_y06_and_y07__description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = '130_infant_cause_recode'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['130_infant_cause_recode'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "130_infant_cause_recode_description")


# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = '39_cause_recode'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['39_cause_recode'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "39_cause_recode_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'race'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['race'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "race_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'bridged_race_flag'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['bridged_race_flag'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "bridged_race_flag_description")

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'race_recode_3'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['race_recode_3'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "race_recode_3_description")

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'race_recode_5'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['race_recode_5'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "race_recode_5_description")

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'hispanic_originrace_recode'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['hispanic_originrace_recode'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('code')
cdc_data = cdc_data.withColumnRenamed("comment", "hispanic_originrace_recode_description")




# COMMAND ----------

cdc_data=cdc_data.drop('detail_age_type')
cdc_data=cdc_data.drop('detail_age')
cdc_data=cdc_data.drop('age_substitution_flag')
cdc_data=cdc_data.drop('age_recode_52')
cdc_data=cdc_data.drop('age_recode_27')
cdc_data=cdc_data.drop('autopsy')
cdc_data=cdc_data.drop('358_cause_recode')
cdc_data=cdc_data.drop('113_cause_recode')
cdc_data=cdc_data.drop('race_imputation_flag')
cdc_data=cdc_data.drop('hispanic_origin')
cdc_data=cdc_data.fillna({'place_of_injury_for_causes_w00_y34_except_y06_and_y07_':'10'})
cdc_data=cdc_data.fillna({'activity_code':'10'})
cdc_data=cdc_data.fillna({'bridged_race_flag':'0'})
cdc_data=cdc_data.fillna({'130_infant_cause_recode':'999'})

# COMMAND ----------

from pyspark.sql.functions import when

cdc_data=cdc_data.withColumn("method_of_disposition", \
              when(cdc_data["method_of_disposition"] == "D", "O").otherwise(cdc_data["method_of_disposition"]))
cdc_data=cdc_data.withColumn("method_of_disposition", \
              when(cdc_data["method_of_disposition"] == "E", "O").otherwise(cdc_data["method_of_disposition"]))
cdc_data=cdc_data.withColumn("method_of_disposition", \
              when(cdc_data["method_of_disposition"] == "R", "O").otherwise(cdc_data["method_of_disposition"]))
cdc_data=cdc_data.withColumn("method_of_disposition", \
              when(cdc_data["method_of_disposition"] == "U", "O").otherwise(cdc_data["method_of_disposition"]))

# COMMAND ----------

