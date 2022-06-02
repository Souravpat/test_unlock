# Databricks notebook source
pip install xlrd==1.2.0

# COMMAND ----------

# Creating run id, an uinque id that define each run
import time
 
run_id = f'{time.time()}'
print(run_id)


# COMMAND ----------

##read and prepare data-- not req
df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/mnt/demo/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/demo/data_test/")

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

import pandas as pd

df = pd.read_excel('/dbfs/mnt/demo/data_test/sales_train_01_01_2013.xlsx', engine='openpyxl')

sparkDf = spark.createDataFrame(df.astype(str))

# COMMAND ----------

# sparkDF = spark.read.format("com.crealytics.spark.excel") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load("dbfs:/mnt/demo/data_test/sales_train_01_01_2013.xlsx")

# COMMAND ----------

# MAGIC %pip install great_expectations

# COMMAND ----------

# creating GE wrapper around spark dataframe
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
gdf = SparkDFDataset(sparkDf)  
gdf.spark_df.show(10)

# COMMAND ----------

# create custom validation suite
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
 
custom_expectation_suite = ExpectationSuite(expectation_suite_name="sales_custom")
 
# now we will add expectations
custom_expectation_suite.add_expectation(ExpectationConfiguration(expectation_type="expect_column_values_to_be_between",
                                                                 kwargs={'column': 'item_price', 'min_value': 1, 'max_value':2000},
                                                                 meta={'reason': 'price should always be in between 1 and 2000'}))

# COMMAND ----------

custom_validation = gdf.validate(custom_expectation_suite,run_id=run_id)

# COMMAND ----------

custom_validation

# COMMAND ----------

# visualizing the validation result page
from great_expectations.render.renderer import ValidationResultsPageRenderer

from great_expectations.render.renderer import ExpectationSuitePageRenderer
# import the view template who will basically convert the document content to HTML
from great_expectations.render.view import DefaultJinjaPageView
 
validation_result_document_content = ValidationResultsPageRenderer().render(custom_validation)
validation_result_HTML = DefaultJinjaPageView().render(validation_result_document_content)


# COMMAND ----------

displayHTML(validation_result_HTML)
