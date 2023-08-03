# Databricks notebook source
#Adding columns dynamically

# COMMAND ----------

# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *

data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",1000)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
display(df)

# COMMAND ----------

def addCol(col_name, lit_val):
    df1 = df.withColumn(col_name, lit(lit_val))
    df1.display()
    
addCol('City','Bengaluru')

# COMMAND ----------

def addCol(col_name, lit_val):
    df1 = df.withColumn(col_name, lit(lit_val))
    return df1
    
    
df2=addCol('City','Bengaluru').display()

# COMMAND ----------


