# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/CDCData/"
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

temp_table_name = "CDCData"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

permanent_table_name = "cdc_data2"

df.write.format("csv").saveAsTable(permanent_table_name)

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

df.filter(df.education_1989_revision == " ").count()

# COMMAND ----------

df = df.drop('education_1989_revision')

# COMMAND ----------

df = df.drop('education_reporting_flag')

# COMMAND ----------

df = df.fillna({'education_2003_revision':'1989'})

# COMMAND ----------



# COMMAND ----------

temp_table_name = "CDCData"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

from pyspark.sql.functions import when



# COMMAND ----------

from pyspark.sql.functions import when

df = df.withColumn("education_reporting_flag", \
              when(df["education_1989_revision"] == 99, 0).otherwise(df["education_reporting_flag"]))

# COMMAND ----------

df = df.drop('education_reporting_flag')

# COMMAND ----------

df = df.withColumn("education_reporting_flag", \
              when(df["education_2003_revision"] == 10 , 0).otherwise(df["education_reporting_flag"]))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct education_reporting_flag, education_1989_revision,education_2003_revision
# MAGIC from CDCData
# MAGIC order by education_reporting_flag asc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct education_reporting_flag, education_1989_revision
# MAGIC from CDCData
# MAGIC order by education_1989_revision asc

# COMMAND ----------

df = df.fillna({'infant_age_recode_22':'99'})

# COMMAND ----------

df = df.fillna({'130_infant_cause_recode':'999'})

# COMMAND ----------

df = df.fillna({'bridged_race_flag':'0'})

# COMMAND ----------

temp_table_name = "CDCData"

df2.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

df_backup = df

# COMMAND ----------

df1 = df

# COMMAND ----------

df.columns

# COMMAND ----------

df.filter(df.education_1989_revision == " ").count()

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

temp_table_name = "CDCData"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*), method_of_disposition
# MAGIC from CDCData
# MAGIC group by method_of_disposition
# MAGIC order by count(*) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from CDCData

# COMMAND ----------

df.columns

# COMMAND ----------

dfbackup1 = df

# COMMAND ----------

dfbackuplatest = df

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

df.columns

# COMMAND ----------

dfbackuplatest2 = df


# COMMAND ----------

temp_table_name = "CDCData"

dfbackup2.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct count(icd_code_10th_revision)
# MAGIC from CDCData
# MAGIC where len(icd_code_10th_revision)<3
# MAGIC --group by icd_code_10th_revision
# MAGIC --order by icd_code_10th_revision

# COMMAND ----------

dfnew.count()

# COMMAND ----------

df = df.fillna({'activity_code':'10'})

# COMMAND ----------

df = df.fillna({'place_of_injury_for_causes_w00_y34_except_y06_and_y07_':'10'})


# COMMAND ----------

df = df.fillna({'manner_of_death':'99'})

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
(train, test) = dfnew.randomSplit([0.7, 0.3], seed = 100)
print("Training Dataset Count: " + str(train.count()))
print("Test Dataset Count: " + str(test.count()))

# COMMAND ----------

train.count()

# COMMAND ----------

latestdfbackup = df

# COMMAND ----------

binary = df

# COMMAND ----------

output = df.where(df.method_of_disposition != "O")

# COMMAND ----------

temp_table_name = "CDCData"

output.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from CDCData

# COMMAND ----------

output=output.drop('education_1989_revision')
output=output.drop('education_reporting_flag')

# COMMAND ----------

temp_table_name = "CDCData"

output.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/CleanData/"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
output = spark.read.format(file_type) \
     .option("inferSchema", infer_schema) \
     .option("sep", delimiter) \
     .option("header", first_row_is_header) \
     .load(file_location)

display(output)

# COMMAND ----------

output.count()

# COMMAND ----------

outputbackup = output
Col = output.columns
categorical_data = Col.copy()


# COMMAND ----------

categorical_data.remove('method_of_disposition')

# COMMAND ----------

print(categorical_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct education_1989_revision from CDCData

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
stages = [] # stages in our Pipeline
for categoricalCol in categorical_data:
    # Category Indexing with StringIndexer
    stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
    # Use OneHotEncoder to convert categorical variables into binary SparseVectors
    # encoder = OneHotEncoderEstimator(inputCol=categoricalCol + "Index", outputCol=categoricalCol + "classVec")
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    # Add stages.  These are not run here, but will run all at once later on.
    stages += [stringIndexer, encoder]

# COMMAND ----------

# Convert label into label indices using the StringIndexer
label_stringIdx = StringIndexer(inputCol="method_of_disposition", outputCol="label")
stages += [label_stringIdx]

# COMMAND ----------

# Transform all features into a vector using VectorAssembler
#numericCols = ["age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week"]
assemblerInputs = [c + "classVec" for c in categorical_data]
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
  
partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(output)
preppedDataDF = pipelineModel.transform(output)

# COMMAND ----------

val codeIndexer = new StringIndexer().setInputCol("originalCode").setOutputCol("originalCodeCategory")
codeIndexer.setHandleInvalid("keep")

# COMMAND ----------

lrModel = LogisticRegression().fit(preppedDataDF)

# ROC for training data
display(lrModel, preppedDataDF, "ROC")

# COMMAND ----------

selectedcols = ["label", "features"] + Col
dataset = preppedDataDF.select(selectedcols)
display(dataset)

# COMMAND ----------

(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed=100)
print(trainingData.count())
print(testData.count())

# COMMAND ----------

trainingData.columns

# COMMAND ----------

display(testData)

# COMMAND ----------

#clean_data = output
temp_table_name = "CDCData"
testData.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from CDCData

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from CDCData

# COMMAND ----------

permanent_table_name = "Clean_CDC"

clean_data.write.format("csv").saveAsTable(permanent_table_name)

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

# Create initial LogisticRegression model
lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10, regParam=0.3)

# Train model with Training Data
lrModel = lr.fit(trainingData)

# COMMAND ----------

# Make predictions on test data using the transform() method.
# LogisticRegression.transform() will only use the 'features' column.
predictions = lrModel.transform(testData)

# COMMAND ----------

#print("Weights: " + str(lrModel.Weights))
print("Intercept: " + str(lrModel.intercept))

# COMMAND ----------

trainingSummary = lrModel.summary
trainingSummary.roc.show()

# COMMAND ----------

print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

# COMMAND ----------

#print("T Values: " + str(trainingSummary.tValues))
print("P Values: " + str(trainingSummary.pValues))

# COMMAND ----------

print("Coefficients: \n" + str(lrModel.coefficientMatrix))
##print("Intercept: " + str(lrModel.interceptVector))

# COMMAND ----------

coeff = str(lrModel.coefficientMatrix)

# COMMAND ----------

coeff

# COMMAND ----------

# View model's predictions and probabilities of each prediction class
# You can select any columns in the above schema to view as well. For example's sake we will choose age & occupation

selected = predictions.select("label", "prediction", "probability")
display(selected)

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier

# COMMAND ----------

evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
evaluator.evaluate(predictions)

# COMMAND ----------

tp = selected.where(selected["label"] == 1).where(selected["prediction"] == 1).count()
tn = selected.where(selected["label"] == 0).where(selected["prediction"] == 0).count()
fp = selected.where(selected["label"] == 0).where(selected["prediction"] == 1).count()
fn = selected.where(selected["label"] == 1).where(selected["prediction"] == 0).count()

# COMMAND ----------

print(tp)
print(tn)
print(fp)
print(fn)

# COMMAND ----------

accuracy = (tp + tn) * 100 / (tp + tn + fp + fn)
accuracy

# COMMAND ----------

precision = tp / (tp + fp)
precision

# COMMAND ----------

recall = tp / (tp + fn)
recall

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT manner_of_death 
# MAGIC FROM CDCData
# MAGIC ORDER BY manner_of_death ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT 130_infant_cause_recode 
# MAGIC FROM CDCData
# MAGIC ORDER BY 130_infant_cause_recode ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT infant_age_recode_22 
# MAGIC FROM CDCData
# MAGIC ORDER BY infant_age_recode_22 ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT activity_code 
# MAGIC FROM CDCData
# MAGIC ORDER BY activity_code ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT place_of_injury_for_causes_w00_y34_except_y06_and_y07_ 
# MAGIC FROM CDCData
# MAGIC ORDER BY place_of_injury_for_causes_w00_y34_except_y06_and_y07_ ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT education_1989_revision 
# MAGIC FROM CDCData
# MAGIC ORDER BY education_1989_revision ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT education_2003_revision 
# MAGIC FROM CDCData
# MAGIC ORDER BY education_2003_revision ASC

# COMMAND ----------

output.columns

# COMMAND ----------

df=output

# COMMAND ----------

df_json_rs = spark.sql("select * from mapping_data where column_name = 'resident_status'")

# COMMAND ----------

display(df_json_rs.select("*"))

# COMMAND ----------

inner_join = inner_join.join(df_json_rs, inner_join['resident_status'] == df_json_rs.Code)

# COMMAND ----------

df.registerTempTable("all_data")

# COMMAND ----------


df= spark.sql("select * from all_data")

# COMMAND ----------

df_json_rs = spark.sql("select * from mapping_data where column_name = 'resident_status'")

# COMMAND ----------

df = df.join(df_json_rs, df['resident_status'] == df_json_rs.Code)

# COMMAND ----------

df = df.drop('column_name')
df = df.drop('code')
df = df.withColumnRenamed("Code_Desciption", "resident_status_description")

# COMMAND ----------

display(df)

# COMMAND ----------

df_json_rs = spark.sql("select * from mapping_data where column_name = 'education_1989_revision'")
display(df_json_rs.select("*"))
df = df.join(df_json_rs, df['education_1989_revision'] == df_json_rs.Code)
df = df.drop('column_name')
df = df.drop('code')
df = df.withColumnRenamed("Code_Description","education_1989_revision_description")

# COMMAND ----------

df=df.drop('education_1989_revision_description')

# COMMAND ----------

df_json_rs = spark.sql("select * from mapping_data where column_name = 'education_2003_revision'")
display(df_json_rs.select("*"))
df = df.join(df_json_rs, df['education_2003_revision'] == df_json_rs.Code)
df = df.drop('column_name')
df = df.drop('code')
df = df.withColumnRenamed("Code_Description","education_2003_revision_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from mapping_data where column_name = 'injury_at_work'")
display(df_json_rs.select("*"))
df = df.join(df_json_rs, df['injury_at_work'] == df_json_rs.Code)
df = df.drop('column_name')
df = df.drop('code')
df = df.withColumnRenamed("Code_Description", "injury_at_work_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from mapping_data where column_name = 'method_of_disposition'")
display(df_json_rs.select("*"))
df = df.join(df_json_rs, df['method_of_disposition'] == df_json_rs.Code)
df = df.drop('column_name')
df = df.drop('code')
df = df.withColumnRenamed("Code_Description", "method_of_disposition_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from mapping_data where column_name = 'hispanic_originrace_recode'")
display(df_json_rs.select("*"))
df = df.join(df_json_rs, df['hispanic_originrace_recode'] == df_json_rs.Code)
df = df.drop('column_name')
df = df.drop('code')
df = df.withColumnRenamed("Code_Description", "hispanic_originrace_recode_description")

# COMMAND ----------

df_json_rs = spark.sql("select * from education_1989_revision_csv where col_name = 'education_1989_revision'")
display(df_json_rs.select("*"))
df = df.join(df_json_rs, df['education_1989_revision'] == df_json_rs.Code)
df = df.drop('column_name')
df = df.drop('code')
df = df.withColumnRenamed("description","education_1989_revision_description")

# COMMAND ----------

display(df)

# COMMAND ----------

df_json_rs = spark.sql("select * from education_1989_revision_csv where col_name = 'education_1989_revision'")
display(df_json_rs.select("*"))
df = df.join(df_json_rs, df['education_1989_revision'] == df_json_rs.code)
df = df.drop('col_name')
df = df.drop('code')
df = df.withColumnRenamed("description","education_1989_revision_description")

# COMMAND ----------

df1=df.drop('Code_Description')

# COMMAND ----------

df1.columns

# COMMAND ----------

display(output)

# COMMAND ----------

df_new=output

# COMMAND ----------

df_json_rs = spark.sql("select * from mapping_data where column_name = 'resident_status'")
display(df_json_rs.select("*"))
df = df.join(df_json_rs, df['resident_status'] == df_json_rs.Code)
df = df.drop('column_name')
df = df.drop('code')
df = df.withColumnRenamed("Code_Description", "resident_status_description")

# COMMAND ----------

display(df_new)

# COMMAND ----------

df_new_json_rs = spark.sql("select * from mapping_data where column_name = 'resident_status'")
df_new = df_new.join(df_new_json_rs, df_new['resident_status'] == df_new_json_rs['Code'])
display(df_new)


# COMMAND ----------

display(df_new)

# COMMAND ----------

display(df_n)