# Databricks notebook source
options={'header':True,
        'delimiter':',',
        'inferschema':True}

def read_file(format,path,options):
    df=spark.read.format(format).options(**options).load(path)
    return df

# COMMAND ----------

df=read_file('csv','dbfs:/FileStore/sreekanth/Book1.csv',options)
df.show()

# COMMAND ----------

#adding columns dynamically
from pyspark.sql.functions import *
from pyspark.sql.types import *

def add_col(col_name,lit_value):
    df1=df.withColumn(col_name,lit(lit_value))
    return df1

# COMMAND ----------

df2=add_col('city','bngl')
df2.show()

# COMMAND ----------

new_col=['state','district']
new_col_value=['Ap','chittoor']

for k,v in zip(new_col,new_col_value):
    df2=df2.withColumn(k,lit(v))
df2.show()



# COMMAND ----------

#changing column names dynamically

def change_col_name(col_name,new_col):
    df3=df2.withColumnRenamed(col_name,new_col)
    return df3

df4=change_col_name('city','CITY')
df4.show()

# COMMAND ----------

#renaming for mul_cols
new_col=['state','district']
new_col_value=['STATE','DISTRICT']

for k,v in zip(new_col,new_col_value):
    df4=df4.withColumnRenamed(k,v)
df4.show()

# COMMAND ----------

df5=df4.filter(df4.name=='sreekanth')
df5.show()

# COMMAND ----------

df5=df4.filter((df4.name=='sreekanth') | (df4.name=='reddy'))
df5.show()

# COMMAND ----------

df5=df4.filter((df4.name=='sreekanth') & (df4.id==1))
df5.show()

# COMMAND ----------

#sort and orderBY

df4.show()

# COMMAND ----------

df4.sort(df4.id.desc(),df4.age).show()

# COMMAND ----------

df4.orderBy(df4.id.desc(),df4.age).show()

# COMMAND ----------

#distinct and dropDuplicate

data=[(1,'sree'),(2,'kanth'),(2,'red')]
schema=['id','name']

df=spark.createDataFrame(data,schema)
df.show()
df.distinct().show()
df.dropDuplicates(['id']).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize a Spark session
spark = SparkSession.builder.appName("DuplicateSalaries").getOrCreate()

# Sample data with duplicate salaries
data = [
    (1, "Alice", 50000),
    (2, "Bob", 60000),
    (3, "Charlie", 60000),  # Duplicate salary
    (4, "David", 70000),
    (5, "Eve", 70000),     # Duplicate salary
]

# Define the DataFrame schema
columns = ["id", "name", "salary"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)
df.show()



# COMMAND ----------

df.groupBy('id','name').agg(max('salary'),min('salary')).show()

# COMMAND ----------

import requests
response=requests.get('https://gist.githubusercontent.com/pradeeppaikateel/a5caf3b8dea7cf215b1e0cf8ebbbba4d/raw/79125d55b44de60f519ad3fe12ce24329492e3e3/nest.json')
db=spark.sparkContext.parallelize([response.text])
df=spark.read.option("multiline",True).json(db)
df.show(truncate=False)


# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *



employees=[StructField('emp_id',IntegerType(),True),
           StructField('emp_name',StringType(),True)]
    
properties=[StructField('name',StringType(),True),
           StructField('store_size',StringType(),True)]

schema=StructType([StructField('employees',ArrayType(StructType(employees)),True),
                  StructField('id',IntegerType(),True),
                StructField('properties',StructType(properties),True)])

# COMMAND ----------

import requests
response=requests.get('https://gist.githubusercontent.com/pradeeppaikateel/a5caf3b8dea7cf215b1e0cf8ebbbba4d/raw/79125d55b44de60f519ad3fe12ce24329492e3e3/nest.json')
db=spark.sparkContext.parallelize([response.text])
df=spark.read.option("multiline",True).schema(schema).json(db)
df.show(truncate=False)

# COMMAND ----------



# COMMAND ----------

df1=df.withColumn('employees',explode(col('employees')))
df1.show()
df2=df1.select('employees.*','id','properties.*')
df2.show()

# COMMAND ----------

df2.write.format('json').mode('overwrite').save('dbfs:/FileStore/sreekanth/output1')

# COMMAND ----------

# path='dbfs:/FileStore/sreekanth/output/part-00003-tid-226459829765363176-b3349ac9-d041-4e45-9333-cbb1260669c7-23-1-c000.json'

# COMMAND ----------

df3=spark.read.format('json').option("multiline",True).json('dbfs:/FileStore/sreekanth/output1/part-00003-tid-1686816690959394574-47acb8db-fb86-4c0d-b4a6-5355fcd3a8da-85-1-c000.json')
df3.show()

# COMMAND ----------

database='sreekanth'
table='output1'
df2.write.format('json').mode('overwrite').saveAsTable(f"{database}.{table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sreekanth.output1

# COMMAND ----------

data=[(1,'maheer',2000,500),(2,'wafa',1000,600),(3,'spark',800,200)]
schema=['id','name','salary','bonus']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
  
def totalpay(s,b):
    return s+b
  
totalpayment=udf(lambda s,b:totalpay(s,b),IntegerType())

# COMMAND ----------

df.withColumn('totalsalary',totalpayment(df.salary,df.bonus)).show()

# COMMAND ----------

def totalpay(s,b):
    return s+b

# COMMAND ----------

df.withColumn('totalsalary_1',totalpay(df.salary,df.bonus)).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.select('*',totalpay(df.salary,df.bonus).alias('tp')).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df1=df.select('id','name',when(df.salary==2000,4000).otherwise(df.salary).alias('salary'))
df1.show()

# COMMAND ----------

df2=df1.sort(df1.id.desc())
df3=df2.withColumn('salary',col('salary').cast('int'))
df2.show()

# COMMAND ----------

df1.show() 

# COMMAND ----------

#date functions in pyspark

# COMMAND ----------

df=spark.range(4)
df.show()

# COMMAND ----------

df1=df.withColumn('current_date',current_date())
df1.show()

# COMMAND ----------

df2=df1.withColumn('current_date',date_format(df1.current_date,'yyyy.MM.dd'))
df2.show()

# COMMAND ----------

df3=df2.withColumn('current_date',to_date(df2.current_date,'yyyy.MM.dd'))
df3.show()

# COMMAND ----------

#timestamp functions

# COMMAND ----------

df=spark.range(2)
df.show()

# COMMAND ----------

df1=df.withColumn('currentTimeStamp',current_timestamp())
df1.show(truncate=False)

# COMMAND ----------

df2=df1.withColumn('stringdateformat',lit('2023-12-15 11-08-34'))
df2.show()

# COMMAND ----------

df3=df2.withColumn('timestamp',to_timestamp(df2.stringdateformat,'yyyy-MM-dd HH-mm-ss'))
df3.show()

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

# In the given code, sets are used to efficiently compare the columns of a DataFrame with an expected set of column names.

expected_col=set(["Name","Department","Salary"])
if set(df.columns)==expected_col:
    print('all columns is available')
else:
    missing_columns=expected_col-set(df.columns)
    print(f'columns are missing:{missing_columns}')


# COMMAND ----------

#camelCase
#snake_case
#PascalCase

# COMMAND ----------

expectedcols=set(['id','name'])
if set(df.columns)==ecpectedcols:
    print('all columns are available')
else:
    missing_columns=expected_cols-set(df.columns)
    print(f"column_,issimf:{missing_columns}")

# COMMAND ----------


# Difference between distinct() vs dropDuplicates()

# Import
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Prepare data
data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4600), \
    ("James", "Sales", 3000)
  ]

columns= ["employee_name", "department", "salary"]

# Create DataFrame
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

# Using distinct()
distinctDF = df.distinct()
distinctDF.show(truncate=False)

# Using dropDuplicates()
dropDisDF = df.dropDuplicates(["department","salary"])
dropDisDF.show(truncate=False)

# Using dropDuplicates() on single column
dropDisDF = df.dropDuplicates(["salary"]).select("salary")
dropDisDF.show(truncate=False)
print(dropDisDF.collect())


# COMMAND ----------

#

# COMMAND ----------

df=read_file('csv','dbfs:/FileStore/sreekanth/Book1.csv',options)

# COMMAND ----------

df.show()

# COMMAND ----------

def rev_str(x):
    new_str=''
    for i in range(len(x)-1,-1,-1):
        new_str=new_str+x[i]
    return new_str

rev_str('sree')


# COMMAND ----------

x='sree'
new_str=''
for i in range(len(x)-1,-1,-1):
    new_str=new_str+x[i]
print(new_str)

# COMMAND ----------


