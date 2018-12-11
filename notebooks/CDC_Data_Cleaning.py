# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/"
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

# COMMAND ----------

## change codes other than B and C to O
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

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM CDCData

# COMMAND ----------

Col = df.columns
categorical_data = Col.copy()

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
#numericCols = ["age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week"]
assemblerInputs = [c + "classVec" for c in categorical_data]
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

# COMMAND ----------

df=df.drop('education_1989_revision')
df=df.drop('education_reporting_flag')

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
  
partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(df)
preppedDataDF = pipelineModel.transform(df)

# COMMAND ----------

val codeIndexer = new StringIndexer().setInputCol("originalCode").setOutputCol("originalCodeCategory")
codeIndexer.setHandleInvalid("keep")

# COMMAND ----------

selectedcols = ["label", "features"] + Col
dataset = preppedDataDF.select(selectedcols)
display(dataset)

# COMMAND ----------

(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed=100)
print(trainingData.count())
print(testData.count())

# COMMAND ----------

from pyspark.ml.regression import GeneralizedLinearRegression

# Load training data
dataset = spark.read.format("libsvm")\
    .load("data/mllib/sample_linear_regression_data.txt")

glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=0.3)

# Fit the model
model = glr.fit(dataset)

# Print the coefficients and intercept for generalized linear regression model
print("Coefficients: " + str(model.coefficients))
print("Intercept: " + str(model.intercept))

# Summarize the model over the training set and print out some metrics
summary = model.summary
print("Coefficient Standard Errors: " + str(summary.coefficientStandardErrors))
print("T Values: " + str(summary.tValues))
print("P Values: " + str(summary.pValues))
print("Dispersion: " + str(summary.dispersion))
print("Null Deviance: " + str(summary.nullDeviance))
print("Residual Degree Of Freedom Null: " + str(summary.residualDegreeOfFreedomNull))
print("Deviance: " + str(summary.deviance))
print("Residual Degree Of Freedom: " + str(summary.residualDegreeOfFreedom))
print("AIC: " + str(summary.aic))
print("Deviance Residuals: ")
summary.residuals().show()