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

#Schema checking

# COMMAND ----------

# Databricks notebook source
#Question: - Read the data from csv file and validate if the dataframe schema defined is similar to that present in the source file

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

data = [("Alice", 100),
        ("Bob", 150),
        ("Charlie", 120),
        ("David", 180),
        ("Eve", 130)]

columns = ["name", "score"]

df = spark.createDataFrame(data, columns)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, avg

window_spec = Window.orderBy(df["score"].desc())

# COMMAND ----------

df_with_row_number = df.withColumn("row_number", row_number().over(window_spec))


# COMMAND ----------

df_with_rank = df.withColumn("rank", rank().over(window_spec))


# COMMAND ----------

df_with_dense_rank = df.withColumn("dense_rank", dense_rank().over(window_spec))
rolling_window_spec = Window.orderBy(df["score"].desc()).rowsBetween(-2, 2)
df_with_rolling_avg = df.withColumn("rolling_avg", avg(df["score"]).over(rolling_window_spec))



# COMMAND ----------

df_with_row_number.show()
df_with_rank.show()
df_with_dense_rank.show()
df_with_rolling_avg.show()

# COMMAND ----------

from datetime import datetime
import pytz

# Get the current time in the UTC timezone using pytz.utc
current_time_utc = datetime.now(pytz.utc)

# Convert the datetime object to a string
timestamp_string = str(current_time_utc)

print("Current UTC time:", timestamp_string)


# COMMAND ----------

from datetime import datetime
import pytz

# Get the current time in the UTC timezone using pytz.utc
current_time_utc = datetime.now(pytz.utc)

# Convert UTC time to Indian Standard Time (IST)
ist_timezone = pytz.timezone("Asia/Kolkata")
current_time_ist = current_time_utc.astimezone(ist_timezone)

# Convert the datetime object to a string
timestamp_string_ist = current_time_ist.strftime("%Y-%m-%d %H:%M:%S %Z%z")

print("Current IST time:", timestamp_string_ist)


# COMMAND ----------



# COMMAND ----------

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

#df1.filter(df1.Age>17).show()

# COMMAND ----------

from pyspark.sql.functions import *
df2 = df1.filter((df1.Age > 17) | (df1.Salary > 30000))
df2.show()

# COMMAND ----------

df2 = df1.filter((df1.Age > 17) & (df1.Salary > 30000))
df2.show()

# COMMAND ----------

df3 = df1.sort((df1.Salary.desc()))
df3.show()

# COMMAND ----------

#sort=orderby
df3 = df1.orderBy(df1.Salary.desc(),df1.Experience.asc())
df3.show()

# COMMAND ----------

df4=df3.union(df2)
df4.show()

# COMMAND ----------

df4.dropDuplicates(["Name","Salary"]).show()

# COMMAND ----------

df4.dropDuplicates(["Name","Salary"]).show()

# COMMAND ----------

df4.dropDuplicates().show()

# COMMAND ----------

df4.distinct().show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("GroupByExample").getOrCreate()

# Sample DataFrame
data = [("Alice", "Sales", 5000),
        ("Bob", "HR", 6000),
        ("Alice", "Sales", 7500),
        ("Carol", "Engineering", 8000),
        ("Bob", "Sales", 4500),
        ("Carol", "Engineering", 9000)]

columns = ["Name", "Department", "Salary"]
df = spark.createDataFrame(data, columns)

# Show the original DataFrame
print("Original DataFrame:")
df.show()

# Group by "Name" and "Department" and calculate the total salary
grouped_df = df.groupBy("Name", "Department") \
               .agg(sum("Salary").alias("TotalSalary"))

# Show the grouped DataFrame
print("Grouped DataFrame:")
grouped_df.show()


# COMMAND ----------

# in order to use multiple aggregate functions use agg(max('salary'),min('salary'))

# COMMAND ----------


