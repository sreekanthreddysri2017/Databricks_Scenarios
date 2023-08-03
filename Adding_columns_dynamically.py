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

#Adding multiple columns

# COMMAND ----------

columns = ['City','State','Country']
lit_values = ['Bengaluru','Karnataka','India']
def add_multiple_cols(col_names, lit_vals):
        df1 = df.withColumn(col_names, lit(lit_vals)) 
        df1.display()
        
add_multiple_cols(columns[1], lit_values[1])
# add_multiple_cols(columns[1], lit_values[1])
# add_multiple_cols(columns[2], lit_values[2])

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

data = [(1, "John", 25), (2, "Alice", 30), (3, "Bob", 35)]
columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)

dynamic_columns = ['City', 'State', 'Country']
lit_values = ['Bengaluru', 'Karnataka', 'India']

for col_name, lit_val in zip(dynamic_columns, lit_values):
    df = df.withColumn(col_name, lit(lit_val))

df.show()


# COMMAND ----------

#

# COMMAND ----------


