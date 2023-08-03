# Databricks notebook source
#Question: - Read the data from csv file and validate if the dataframe schema defined is similar to that present in the source file

# COMMAND ----------

csv_options={'header':True,
             'inferschema':True,
             'delimiter':','}

def read_csv(path,csv_options): 
    return spark.read.options(**csv_options).csv(path)

df=read_csv("dbfs:/FileStore/Book4_1.csv",csv_options)
display(df)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
schema=StructType([StructField('Name',StringType(),True),
                   StructField('Department',StringType(),True)])

# COMMAND ----------

csv_options={'header':True,
             'inferschema':True,
             'delimiter':','}
        
def read_csv(path,csv_options,schema):
    return spark.read.options(**csv_options).schema(schema).csv(path)

df=read_csv("dbfs:/FileStore/Book4_1.csv",csv_options,schema)
display(df)

# COMMAND ----------

# The f before a string, known as an f-string (formatted string literals), is a feature in Python used for string formatting.
#String formatting is a technique in programming used to create strings with dynamic content by inserting variables or expressions
# In the given code, sets are used to efficiently compare the columns of a DataFrame with an expected set of column names.
expected_col=set(["Name","Department","Salary"])
if set(df.columns)==expected_col:
    print('all columns is available')
else:
    missing_columns=expected_col-set(df.columns)
    print(f'columns are missing:{missing_columns}')


# COMMAND ----------


