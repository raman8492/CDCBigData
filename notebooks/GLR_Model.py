# Databricks notebook source
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
import numpy as np
from pandas import *
from ggplot import *

# COMMAND ----------

# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/cdc_data/"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
     .option("inferSchema", infer_schema) \
     .option("sep", delimiter) \
     .option("header", first_row_is_header) \
     .load(file_location)

display(df)

# COMMAND ----------

## Temp Table Update Code
temp_table_name = "CDCData"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

## Dropping unccessary columns
df=df.drop('detail_age_type')
df=df.drop('detail_age')
df=df.drop('age_substitution_flag')
df=df.drop('age_recode_52')
df=df.drop('age_recode_27')
df=df.drop('autopsy')
df=df.drop('358_cause_recode')
df=df.drop('113_cause_recode')
df=df.drop('race_imputation_flag')
df=df.drop('hispanic_origin')

# COMMAND ----------

df=df.drop('record_condition_1')
df=df.drop('record_condition_2')
df=df.drop('record_condition_3')
df=df.drop('record_condition_4')
df=df.drop('record_condition_5')
df=df.drop('record_condition_6')
df=df.drop('record_condition_7')
df=df.drop('record_condition_8')
df=df.drop('record_condition_9')
df=df.drop('record_condition_10')
df=df.drop('record_condition_11')
df=df.drop('record_condition_12')
df=df.drop('record_condition_13')
df=df.drop('record_condition_14')
df=df.drop('record_condition_15')
df=df.drop('record_condition_16')
df=df.drop('record_condition_17')
df=df.drop('record_condition_18')
df=df.drop('record_condition_19')
df=df.drop('record_condition_20')

# COMMAND ----------

df=df.drop('entity_condition_1')
df=df.drop('entity_condition_2')
df=df.drop('entity_condition_3')
df=df.drop('entity_condition_4')
df=df.drop('entity_condition_5')
df=df.drop('entity_condition_6')
df=df.drop('entity_condition_7')
df=df.drop('entity_condition_8')
df=df.drop('entity_condition_9')
df=df.drop('entity_condition_10')
df=df.drop('entity_condition_11')
df=df.drop('entity_condition_12')
df=df.drop('entity_condition_13')
df=df.drop('entity_condition_14')
df=df.drop('entity_condition_15')
df=df.drop('entity_condition_16')
df=df.drop('entity_condition_17')
df=df.drop('entity_condition_18')
df=df.drop('entity_condition_19')
df=df.drop('entity_condition_20')

# COMMAND ----------

## Filling Nulls
df = df.fillna({'place_of_injury_for_causes_w00_y34_except_y06_and_y07_':'10'})
df = df.fillna({'activity_code':'10'})
df = df.fillna({'bridged_race_flag':'0'})
df = df.fillna({'130_infant_cause_recode':'999'})
df = df.fillna({'manner_of_death':'99'})
df = df.fillna({'infant_age_recode_22':'99'})

# COMMAND ----------

## method_of_disposition --change codes other than B and C to O
from pyspark.sql.functions import when

df = df.withColumn("method_of_disposition", \
              when(df["method_of_disposition"] == "D", "O").otherwise(df["method_of_disposition"]))
df = df.withColumn("method_of_disposition", \
              when(df["method_of_disposition"] == "E", "O").otherwise(df["method_of_disposition"]))
df = df.withColumn("method_of_disposition", \
              when(df["method_of_disposition"] == "R", "O").otherwise(df["method_of_disposition"]))
df = df.withColumn("method_of_disposition", \
              when(df["method_of_disposition"] == "U", "O").otherwise(df["method_of_disposition"]))

# COMMAND ----------

## filter out rows corresponding to O
output = df.where(df.method_of_disposition != "O")

# COMMAND ----------

## Cleaning Education Columns
df = df.fillna({'education_2003_revision':'1989'})
df = df.fillna({'education_1989_revision':'2003'})

# COMMAND ----------

from pyspark.sql.functions import when

df = df.withColumn("education_reporting_flag", \
              when(df["education_1989_revision"] == '00', 2).otherwise(df["education_reporting_flag"]))

# COMMAND ----------

df.filter(df.education_1989_revision == " ").count()

# COMMAND ----------

## dropping education columns
df = df.drop('education_1989_revision')
df = df.drop('education_reporting_flag')

# COMMAND ----------

## Temp Table Backup
temp_table_name = "CDCData"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

df_backup = df

# COMMAND ----------

df.columns

# COMMAND ----------

## Train/Test Split
from pyspark.ml.classification import LogisticRegression
(train, test) = dfnew.randomSplit([0.7, 0.3], seed = 100)
print("Training Dataset Count: " + str(train.count()))
print("Test Dataset Count: " + str(test.count()))

# COMMAND ----------

train.count()

# COMMAND ----------

test.count()

# COMMAND ----------

## Save rows without O in 'output' dataframe
output = df.where(df.method_of_disposition != "O")

# COMMAND ----------

# Output Temp Table Backup
temp_table_name = "CDCData"

output.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

output=output.drop('education_1989_revision')
output=output.drop('education_reporting_flag')

# COMMAND ----------

# Output Temp Table Backup
temp_table_name = "CDCData"

output.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

output.count()