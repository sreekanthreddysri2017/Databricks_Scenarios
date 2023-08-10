# Databricks notebook source
from pyspark.sql.functions import *

#file location and type
file_location='dbfs:/FileStore/Book_new_null_77_space.csv'
file_type='csv'

#csv_options
infershema=True
delimiter=','
header=True

df=spark.read.format(file_type)\
    .option("inferSchema",infershema)\
    .option('delimiter',delimiter)\
    .option('header',header)\
    .load(file_location)

df.show()

# COMMAND ----------

df1=df.dropna()
df1.show()

# COMMAND ----------

#Adding columns
df2=df1.withColumn('Country',lit('india')).withColumn('New_Salary',col('Salary')*2)
df2.show()

# COMMAND ----------

#Adding Columns Dynamically
dynamic_columns = ['City', 'State', 'New_Country']
lit_values = ['Bengaluru', 'Karnataka', 'India']

for col_name, lit_val in zip(dynamic_columns, lit_values):
    df2=df2.withColumn(col_name, lit(lit_val))
df2.show()

# COMMAND ----------

def add_column(col_name,lit_value):
    df3=df2.withColumn(col_name,lit(lit_value))
    return df3

df4=add_column('New_state','AP')
df4.show()

# COMMAND ----------

#columns = ['City','State','Country']
# lit_values = ['Bengaluru','Karnataka','India']
# def add_multiple_cols(col_names, lit_vals):
#         df1 = df.withColumn(col_names, lit(lit_vals)) 
#         df1.display()
        
# add_multiple_cols(columns[1], lit_values[1])
# add_multiple_cols(columns[1], lit_values[1])
# add_multiple_cols(columns[2], lit_values[2])

# COMMAND ----------

df5=df4.withColumnRenamed('Name','Nick_Name').withColumnRenamed('Age','New_Age')
df5.show()

# COMMAND ----------

df6=df5
df6.show()

# COMMAND ----------

#Renaming columns dynamically

old_columns=['Nick_Name','New_Age']
new_columns=['Name','Age']
Renamed_df=df6
for key,value in zip(old_columns,new_columns):
    Renamed_df=Renamed_df.withColumnRenamed(key,value)
Renamed_df.show()

# COMMAND ----------

new_columns=['State','New_Country','New_state']

# COMMAND ----------

new_df=Renamed_df.drop('State','New_Country','New_state')
new_df.show()

# COMMAND ----------


# # New columns and literal values
# new_columns = ['State', 'New_Country', 'New_state']
# lit_values = ["AP", "USA", "Karnataka"]

# def add_mulcolumns(col_names, lit_vals):
#     # new_df1=new_df
#     for key, value in zip(col_names, lit_vals):
#         new_df2 = new_df.withColumn(key, lit(value))
#     return new_df2

# latest_df = add_mulcolumns(new_columns, lit_values)
# latest_df.show()


# COMMAND ----------

new_columns=['State','New_Country','New_state']
lit_values=["AP","USA","Karnataka"]
def add_mulcolumns(new_df, col_names,lit_vals): ->DataFrame
    for key,value in zip(col_names,lit_vals):
        new_df=new_df.withColumn(key,lit(value))
    return new_df

latest_df=add_mulcolumns(new_df,new_columns,lit_values)
latest_df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Create a Spark session
spark = SparkSession.builder.appName("AddColumns").getOrCreate()

# Sample DataFrame
data = [("John", 25), ("Alice", 30)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Literal values for the columns
lit_values = ['Bengaluru', 'Karnataka', 'India']

def add_multiple_cols(df, col_names, lit_vals):
    for col_name, lit_val in zip(col_names, lit_vals):
        df = df.withColumn(col_name, lit(lit_val))
    return df

# Add multiple columns in a single line
new_columns = ['city', 'state', 'country']
df_with_new_columns = add_multiple_cols(df, new_columns, lit_values)

df_with_new_columns.show()


# COMMAND ----------

#

# COMMAND ----------


