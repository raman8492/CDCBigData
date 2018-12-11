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

##df=df.drop('number_of_record_axis_conditions')
##df=df.drop('hispanic_originrace_recode')
##df=df.drop('icd_code_10th_revision')
##df=df.drop('number_of_entity_axis_conditions')
##df=df.drop('race_recode_3')
##df=df.drop('race_recode_5')

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

# COMMAND ----------

display(output)

# COMMAND ----------

Col = output.columns

# COMMAND ----------

## Categorical_data in x values 
categorical_data = output.columns

# COMMAND ----------

categorical_data.remove('method_of_disposition')

# COMMAND ----------

print(categorical_data)

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
assemblerInputs = [c + "classVec" for c in categorical_data]
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
  
partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(output)
preppedDataDF = pipelineModel.transform(output)

# COMMAND ----------

selectedcols = ["label", "features"] + Col
dataset = preppedDataDF.select(selectedcols)
display(dataset)

# COMMAND ----------

## Train/Test Split
from pyspark.ml.classification import LogisticRegression
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed=100)
print("Training Dataset Count: " + str(trainingData.count()))
print("Test Dataset Count: " + str(testData.count()))

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
# MAGIC select distinct count(label),label from CDCData
# MAGIC group by label

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Load and parse the data file, converting it to a DataFrame.
# data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
data = dataset

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(dataset)

# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
   VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)


# COMMAND ----------

## Train/Test Split
from pyspark.ml.classification import LogisticRegression
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed=100)
print("Training Dataset Count: " + str(trainingData.count()))
print("Test Dataset Count: " + str(testData.count()))

# COMMAND ----------

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)

# Convert indexed labels back to original labels.
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)


# COMMAND ----------

# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)

# COMMAND ----------

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

# COMMAND ----------

print(accuracy)

# COMMAND ----------

rfModel = model.stages[2]
print(rfModel)  # summary only

# COMMAND ----------

##end