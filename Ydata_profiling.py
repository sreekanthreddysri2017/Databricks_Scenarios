# Databricks notebook source
# MAGIC %pip install ydata-profiling==4.0.0

# COMMAND ----------

from ydata_profiling import ProfileReport
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

schema = StructType([
    StructField("ohrequid", StringType(), True),
    StructField("datapakid", StringType(), True),
    StructField("record", StringType(), True),
    StructField("zeluxbrnd", StringType(), True),
    StructField("zbrnd_grp", StringType(), True),
    StructField("zhierbrn1", StringType(), True),
    StructField("hierbrn_1", StringType(), True),
    StructField("zhierbrn2", StringType(), True),
    StructField("hierbrn_2", StringType(), True),
    StructField("zbflag", StringType(), True),
    StructField("txtsh", StringType(), True)
])

# Create an empty DataFrame with the specified schema
df = spark.createDataFrame([], schema=schema)


# COMMAND ----------

df.columns

# COMMAND ----------

def stack_expr_unpivot(col_lits):

  stack_expr = "stack({}, {})".format(len(col_lits), ', '.join([" '{}', {}".format(col, col) for col in col_lits]))

  return stack_expr
stacked_cols = stack_expr_unpivot(df.columns)

# COMMAND ----------

display(stacked_cols)

# COMMAND ----------

df1=df.selectExpr(stacked_cols,'col0 as null','col1 as total')

# COMMAND ----------

display(df1)

# COMMAND ----------


