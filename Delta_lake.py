# Databricks notebook source
#merge conditions
from pyspark.sql.types import *
from pyspark.sql.functions import *
schema=StructType([StructField('id',IntegerType(),True),
                   StructField('name',StringType(),True),
                   StructField('city',StringType(),True),
                   StructField('country',StringType(),True),
                   StructField('contact_no',IntegerType(),True)])
data=[(1000,'michal','columns','USA',123456789)]
df=spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dim_emp(
# MAGIC   id int,
# MAGIC   name string,
# MAGIC   city string,
# MAGIC   country string,
# MAGIC   contact_no int)
# MAGIC using delta
# MAGIC location "dbfs:/dbfs/FileStore/delta_2"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_emp

# COMMAND ----------

#creating tempview
df.createOrReplaceTempView('source_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into dim_emp as target
# MAGIC using source_view as source
# MAGIC on target.id=source.id
# MAGIC when matched
# MAGIC then update set
# MAGIC target.name=source.name,
# MAGIC target.city=source.city,
# MAGIC target.country=source.country,
# MAGIC target.contact_no=source.country
# MAGIC when not matched then
# MAGIC insert(id,name,city,country,contact_no)values(id,name,city,country,contact_no)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_emp

# COMMAND ----------

data=[(1000,'michal','texas','USA',123456789),(2000,'sree','bngl','india',987654321)]
df=spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

#creating tempview
df.createOrReplaceTempView('source_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into dim_emp as target
# MAGIC using source_view as source
# MAGIC on target.id=source.id
# MAGIC when matched
# MAGIC then update set
# MAGIC target.name=source.name,
# MAGIC target.city=source.city,
# MAGIC target.country=source.country,
# MAGIC target.contact_no=source.country
# MAGIC when not matched then
# MAGIC insert(id,name,city,country,contact_no)values(id,name,city,country,contact_no)

# COMMAND ----------

#merge schema:schema evaluation

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table emp_demo(
# MAGIC   id int,
# MAGIC   name string,
# MAGIC   city string,
# MAGIC   country string,
# MAGIC   contact_no int)
# MAGIC using delta
# MAGIC location "dbfs:/dbfs/FileStore/delta_3"

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into emp_demo values(1000,'sreekanth','mpl','india',8765)

# COMMAND ----------

schema=StructType([StructField('id',IntegerType(),True),
                   StructField('name',StringType(),True),
                   StructField('city',StringType(),True),
                   StructField('country',StringType(),True),
                   StructField('contact_no',IntegerType(),True),
                   StructField('Age',IntegerType(),True)])
data=[(1000,'michal','columns','USA',1234,26)]
df=spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df.write.mode('append').option("mergeSchema",True).saveAsTable('emp_demo')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_demo

# COMMAND ----------

#various ways of optimization
#delta table optimization
# optimize command is used to combine smaller files into one larger file


# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table employe(id int,name string,dept string,salary int)
# MAGIC using delta
# MAGIC location "dbfs:/dbfs/FileStore/sreekanth"

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employe values(1,"sreekanth","IT",25000)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employe values(2,"Abhi","IT",40000);
# MAGIC insert into employe values(3,"mani","HR",20000);
# MAGIC insert into employe values(2,"ajay","Finance",35000);

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from employe where name='ajay'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employe;

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/dbfs/FileStore/sreekanth/part-00000-75d54f2d-0a39-4035-aa7c-a88bbc894be3-c000.snappy.parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize employe

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 
# MAGIC dbfs:/dbfs/FileStore/sreekanth

# COMMAND ----------

#	Partitioning
# in pyspark data is partitioned across nodes

# COMMAND ----------

data=[(1,'maheer','male','IT'),(2,'wafa','male','HR'),(3,'asi','female','IT')]
schema=['id','name','gender','dept']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.write.options(header=True,delimiter=',').partitionBy('dept').mode('overwrite').csv('dbfs:/dbfs/FileStore/mani')

# COMMAND ----------

#df = spark.read.format('csv').load('dbfs:/dbfs/FileStore/mani')
df= spark.read.option('header', True).csv('dbfs:/dbfs/FileStore/mani')
df.show()

# COMMAND ----------

#

# COMMAND ----------

#

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls dbfs:/dbfs/FileStore/mani

# COMMAND ----------

df1= spark.read.option('header', True).csv('dbfs:/dbfs/FileStore/mani/dept=IT/')
df1.show()

# COMMAND ----------

#Z-ordering
#it is an extention of optimize
#it will pre order the data which will be helpful in performance improvement

# COMMAND ----------

# %sql
# optimize emp_demo
# zorder by ()


# COMMAND ----------

#Bucketing 
# it is nothing but spilitting large dataset into  multiple buckets based on certain key
# when we perform wide transformation on bucketing data it will improve the performance
