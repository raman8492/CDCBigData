# Databricks notebook source
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
import numpy as np
from pandas import *
from ggplot import *

# COMMAND ----------

cdc_data = spark.sql("select * from cdc_data")
spark.conf.set("spark.sql.crossJoin.enabled", "true")
##df_cdc.columns

##df_cdc = table("cdc_data")


# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'education_2003_revision'")
display(df_json_rs.select("*"))
cdc_data = cdc_data.join(df_json_rs, cdc_data['education_2003_revision'] == df_json_rs.Code)
cdc_data = cdc_data.drop('col_name')
cdc_data = cdc_data.drop('Code')
cdc_data = cdc_data.withColumnRenamed("comment","education_2003_revision_description")

# COMMAND ----------

display(cdc_data)

# COMMAND ----------

df_json_rs = spark.sql("select * from result_new_csv where col_name = 'education_reporting_flag'")
display(df_json_rs.select("*"))

# COMMAND ----------

inner_join = inner_join.join(df_json_rs, inner_join['education_reporting_flag'] == df_json_rs.Code)

# COMMAND ----------

temp_table_name = "CDCData"

inner_join.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

val_df = sqlContext.sql("SELECT * FROM CDCData")

# COMMAND ----------

val_df.write.format("com.databricks.spark.csv").save("/FileStore/tables/newcsv")

# COMMAND ----------

inner_join = inner_join.withColumnRenamed("comment", "education_reporting_flag_description")
##inner_join = inner_join.replace('comment','resident_status_description')

# COMMAND ----------

df= spark.sql("select * from cdc_data")
display(df)

# COMMAND ----------

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
df = df.fillna({'place_of_injury_for_causes_w00_y34_except_y06_and_y07_':'10'})
df = df.fillna({'activity_code':'10'})
df = df.fillna({'bridged_race_flag':'0'})
df = df.fillna({'130_infant_cause_recode':'999'})

# COMMAND ----------

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

output = df.where(df.method_of_disposition != "O")

# COMMAND ----------

import org.apache.spark.sql._

# COMMAND ----------

column_names = df_cdc.columns

# COMMAND ----------

schema = StructType([StructField('Col_Nm', StringType(), True)])

# COMMAND ----------

spark.createDataFrame(column_names, schema).show()

# COMMAND ----------

for var in column_names

# COMMAND ----------

df.coalesce(1).write.format("com.databricks.spark.cvs").save("/FileStore/tables/cdc_data")
dbutils.fs.cp("/FileStore/tables/cdc_data", "/FileStore/tables/jsonmapping")

# COMMAND ----------

df.columns()

# COMMAND ----------

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

dfnew=df
dfnew=dfnew.drop('record_condition_1')
dfnew=dfnew.drop('record_condition_2')
dfnew=dfnew.drop('record_condition_3')
dfnew=dfnew.drop('record_condition_4')
dfnew=dfnew.drop('record_condition_5')
dfnew=dfnew.drop('record_condition_6')
dfnew=dfnew.drop('record_condition_7')
dfnew=dfnew.drop('record_condition_8')
dfnew=dfnew.drop('record_condition_9')
dfnew=dfnew.drop('record_condition_10')
dfnew=dfnew.drop('record_condition_11')
dfnew=dfnew.drop('record_condition_12')
dfnew=dfnew.drop('record_condition_13')
dfnew=dfnew.drop('record_condition_14')
dfnew=dfnew.drop('record_condition_15')
dfnew=dfnew.drop('record_condition_16')
dfnew=dfnew.drop('record_condition_17')
dfnew=dfnew.drop('record_condition_18')
dfnew=dfnew.drop('record_condition_19')
dfnew=dfnew.drop('record_condition_20')

# COMMAND ----------

dfnew=dfnew.drop('entity_condition_1')
dfnew=dfnew.drop('entity_condition_2')
dfnew=dfnew.drop('entity_condition_3')
dfnew=dfnew.drop('entity_condition_4')
dfnew=dfnew.drop('entity_condition_5')
dfnew=dfnew.drop('entity_condition_6')
dfnew=dfnew.drop('entity_condition_7')
dfnew=dfnew.drop('entity_condition_8')
dfnew=dfnew.drop('entity_condition_9')
dfnew=dfnew.drop('entity_condition_10')
dfnew=dfnew.drop('entity_condition_11')
dfnew=dfnew.drop('entity_condition_12')
dfnew=dfnew.drop('entity_condition_13')
dfnew=dfnew.drop('entity_condition_14')
dfnew=dfnew.drop('entity_condition_15')
dfnew=dfnew.drop('entity_condition_16')
dfnew=dfnew.drop('entity_condition_17')
dfnew=dfnew.drop('entity_condition_18')
dfnew=dfnew.drop('entity_condition_19')
dfnew=dfnew.drop('entity_condition_20')

# COMMAND ----------

inner_join.columns

# COMMAND ----------

inner_join=inner_join.drop('detail_age_type')

# COMMAND ----------

inner_join.columns

# COMMAND ----------

inner_join=inner_join.drop('detail_age')
inner_join=inner_join.drop('age_substitution_flag')
inner_join=inner_join.drop('age_recode_52')
inner_join=inner_join.drop('age_recode_27')
inner_join=inner_join.drop('autopsy')
inner_join=inner_join.drop('358_cause_recode')
inner_join=inner_join.drop('113_cause_recode')
inner_join=inner_join.drop('race_imputation_flag')
inner_join=inner_join.drop('hispanic_origin')

# COMMAND ----------

inner_join.columns

# COMMAND ----------

inner_join=inner_join.drop('record_condition_1')

inner_join=inner_join.drop('record_condition_2')

inner_join=inner_join.drop('record_condition_3')

inner_join=inner_join.drop('record_condition_4')

inner_join=inner_join.drop('record_condition_5')

inner_join=inner_join.drop('record_condition_6')

inner_join=inner_join.drop('record_condition_7')

inner_join=inner_join.drop('record_condition_8')

inner_join=inner_join.drop('record_condition_9')

inner_join=inner_join.drop('record_condition_10')

inner_join=inner_join.drop('record_condition_11')

inner_join=inner_join.drop('record_condition_12')

inner_join=inner_join.drop('record_condition_13')

inner_join=inner_join.drop('record_condition_14')

inner_join=inner_join.drop('record_condition_15')

inner_join=inner_join.drop('record_condition_16')

inner_join=inner_join.drop('record_condition_17')

inner_join=inner_join.drop('record_condition_18')

inner_join=inner_join.drop('record_condition_19')

inner_join=inner_join.drop('record_condition_20')

# COMMAND ----------

inner_join.columns

# COMMAND ----------

inner_join=inner_join.drop('entity_condition_1')

inner_join=inner_join.drop('entity_condition_2')

inner_join=inner_join.drop('entity_condition_3')

inner_join=inner_join.drop('entity_condition_4')

inner_join=inner_join.drop('entity_condition_5')

inner_join=inner_join.drop('entity_condition_6')

inner_join=inner_join.drop('entity_condition_7')

inner_join=inner_join.drop('entity_condition_8')

inner_join=inner_join.drop('entity_condition_9')

inner_join=inner_join.drop('entity_condition_10')

inner_join=inner_join.drop('entity_condition_11')

inner_join=inner_join.drop('entity_condition_12')

inner_join=inner_join.drop('entity_condition_13')

inner_join=inner_join.drop('entity_condition_14')

inner_join=inner_join.drop('entity_condition_15')

inner_join=inner_join.drop('entity_condition_16')

inner_join=inner_join.drop('entity_condition_17')

inner_join=inner_join.drop('entity_condition_18')

inner_join=inner_join.drop('entity_condition_19')

inner_join=inner_join.drop('entity_condition_20')

# COMMAND ----------

inner_join.columns

# COMMAND ----------

inner_join = inner_join.fillna({'place_of_injury_for_causes_w00_y34_except_y06_and_y07_':'10'})

# COMMAND ----------

inner_join = inner_join.fillna({'activity_code':'10'})

# COMMAND ----------

inner_join=inner_join.drop('resident_status_description_description')

# COMMAND ----------

inner_join = inner_join.fillna({'bridged_race_flag':'0'})

# COMMAND ----------

inner_join = inner_join.fillna({'130_infant_cause_recode':'999'})

# COMMAND ----------

from pyspark.sql.functions import when


inner_join = inner_join.withColumn("method_of_disposition", \

              when(inner_join["method_of_disposition"] == "D",
"O").otherwise(inner_join["method_of_disposition"]))

inner_join = inner_join.withColumn("method_of_disposition", \

              when(inner_join["method_of_disposition"] == "E", 
"O").otherwise(inner_join["method_of_disposition"]))

inner_join = inner_join.withColumn("method_of_disposition", \

              when(inner_join["method_of_disposition"] == "R",
 "O").otherwise(inner_join["method_of_disposition"]))

inner_join = inner_join.withColumn("method_of_disposition", \

              when(inner_join["method_of_disposition"] == "U",
"O").otherwise(inner_join["method_of_disposition"]))

# COMMAND ----------

inner_join = inner_join.fillna({'manner_of_death':'99'})

# COMMAND ----------

output = inner_join.where(inner_join.method_of_disposition != "O")

# COMMAND ----------

display(inner_join)

# COMMAND ----------

inner_join.show()

# COMMAND ----------



# COMMAND ----------

