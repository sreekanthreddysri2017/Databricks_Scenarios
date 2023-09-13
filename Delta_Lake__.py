# Databricks notebook source
# DBTITLE 1,Delta table creation
from delta.tables import *

DeltaTable.create(spark)\
    .tableName('employee')\
    .addColumn('id',"int")\
    .addColumn('name',"string")\
    .addColumn('age',"int")\
    .addColumn('salary',"int")\
    .location('dbfs:/FileStore/tables/sreekanth')\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark)\
    .tableName('employee1')\
    .addColumn('id',"int")\
    .addColumn('name',"string")\
    .addColumn('age',"int")\
    .addColumn('salary',"int")\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table emp1(
# MAGIC   id int,
# MAGIC   name string,
# MAGIC   age int
# MAGIC )using delta
# MAGIC location 'dbfs:/FileStore/tables/reddy'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

#delta table instance

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into employee values (1,'sree',25,6000);
# MAGIC insert into employee values (2,'reddy',28,8000);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

deltainstance1=DeltaTable.forPath(spark,'dbfs:/FileStore/tables/sreekanth')

# COMMAND ----------

display(deltainstance1.toDF())

# COMMAND ----------

deltainstance1.delete('id==2')

# COMMAND ----------

display(deltainstance1.toDF())

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls dbfs:/FileStore/tables/sreekanth/_delta_log

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC head dbfs:/FileStore/tables/sreekanth/_delta_log/00000000000000000000.json

# COMMAND ----------

from pyspark.sql.types import *

data=[(2,'samson',32,5000)]
schema=StructType([StructField('id',IntegerType(),True),
                   StructField('name',StringType(),True),
                   StructField('age',IntegerType(),True),
                   StructField('salary',IntegerType(),True)])

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.write.format('delta').mode('append').saveAsTable('employee')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history employee

# COMMAND ----------

display(deltainstance1.history())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from employee where id=2

# COMMAND ----------

display(deltainstance1.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC update employee set salary=10000 where id=1

# COMMAND ----------

display(deltainstance1.toDF())

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC head dbfs:/FileStore/tables/sreekanth/_delta_log/00000000000000000006.json

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/FileStore/tables/sreekanth/part-00007-3e16855b-9ab4-4f6d-8e03-29b08427581b-c000.snappy.parquet

# COMMAND ----------

from pyspark.sql.types import *

data=[(1,'samson',32,5000),(2,'rahul',30,8000)]
schema=StructType([StructField('id',IntegerType(),True),
                   StructField('name',StringType(),True),
                   StructField('age',IntegerType(),True),
                   StructField('salary',IntegerType(),True)])

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.createOrReplaceTempView('source_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into employee as target
# MAGIC using source_view as source
# MAGIC on target.id=source.id
# MAGIC when matched
# MAGIC then update set
# MAGIC target.id=source.id,
# MAGIC target.name=source.name,
# MAGIC target.age=source.age,
# MAGIC target.salary=source.salary
# MAGIC when not matched
# MAGIC then
# MAGIC insert(id,name,age,salary) values (id,name,age,salary)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee

# COMMAND ----------

from pyspark.sql.types import *

data=[(3,'ruthuraj',28,58888,'IT')]
schema=StructType([StructField('id',IntegerType(),True),
                   StructField('name',StringType(),True),
                   StructField('age',IntegerType(),True),
                   StructField('salary',IntegerType(),True),
                   StructField('dept',StringType(),True)])

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.write.option('mergeSchema','true').format('delta').mode('append').saveAsTable('employee')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history employee

# COMMAND ----------

#time travel

# COMMAND ----------

df=spark.read.option("timestamp","2023-09-13T09:46:03.000+0000").table('employee')
display(df)

# COMMAND ----------

display(deltainstance1.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee

# COMMAND ----------

deltainstance1.restoreToVersion(8)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC optimize employee

# COMMAND ----------


