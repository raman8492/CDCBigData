# Databricks notebook source
# MAGIC %fs rm -r /FileStore/tables/json

# COMMAND ----------

file_location = "/FileStore/tables/"
file_type = "csv"
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","
df = spark.read.format(file_type) \
     .option("inferSchema", infer_schema) \
     .option("sep", delimiter) \
     .option("header", first_row_is_header) \
     .load(file_location)


display(df)

# COMMAND ----------

from pyspark.mllib.stat import Statistics
import pandas as pd

# COMMAND ----------

features = df.rdd.map(lambda row: row[0:])

# COMMAND ----------

from pyspark.mllib.stat import Statistics
features = df.rdd.map(lambda row: row[0:])
corr_mat=Statistics.corr(features, method="pearson")

# COMMAND ----------

from pyspark.mllib.stat import Statistics
import pandas as pd

from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

datos = sql("""select * from cdcdata""")

var_corr=['39_cause_recode','age_recode_12']

assembler = VectorAssembler(
inputCols=var_corr,
outputCol="features")

datos1=datos.select(var_corr).filter("39_cause_recode is not null")
output = assembler.transform(datos)
r1 = Correlation.corr(output, "features")

# df = sqlCtx.read.format('com.databricks.spark.csv').option('header', 'true').option('inferschema', 'true').load('corr_test.csv')
df = datos
col_names = df.columns
features = df.rdd.map(lambda row: row[0:])
##corr_mat = Statistics.corr(features, method="pearson")
corr_r1 = pd.DataFrame(r1)
corr_r1.index, corr_r1.columns = col_names, col_names

# COMMAND ----------

df = sqlContext.read
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .load("../Downloads/2008.csv")

# COMMAND ----------

df = spark.sql("select * from tables")
display(df.select("*"))

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

cols = df.columns

# COMMAND ----------

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

# COMMAND ----------

schema = StructType([StructField('Col_Nm', StringType(), True)])

# COMMAND ----------

print(cols)

# COMMAND ----------

JSONdata = spark.createDataFrame(cols, StringType()).show()

# COMMAND ----------

JSONdata = JSONdata.withColumnRenamed("value", "column_names")

# COMMAND ----------

for col in col

# COMMAND ----------

