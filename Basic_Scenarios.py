# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
import re

# COMMAND ----------

table_schema = StructType([
                        StructField("Product_Name", StringType(), True),
                        StructField("Issue_Date", StringType(), True),
                        StructField("Price", IntegerType(), True),
                        StructField("Brand", StringType(), True),
                        StructField("Country", StringType(), True),
                        StructField("Product_Number", StringType(), True) ])
table_data = [("Washing Machine", 1648770933000, 20000, "Samsung", "India", "0001"),
              ("Refrigerator", 1648770999000 , 35000, "    LG", None, "0002"),
              ("Air Cooler", 1648770948000, 45000, "     Voltas", None, "0003")]

# COMMAND ----------

#creating the dataframe with table data and schema
table_df = spark.createDataFrame(data=table_data, schema=table_schema)
table_df.display()

# COMMAND ----------

#unix_timestamp is a string which is in seconds format
#to convert the given milli-seconds(Issue_Date column) to unix_timestamp (seconds) we divide it by 1000
#this divided value is passed to fom_unixtime function to convert it into date and time stamp.
df1=table_df.withColumn("issue_date_timestamp", from_unixtime( (col("Issue_Date")/1000) ))
df1.display()

# COMMAND ----------

#converting timestamp into date
df2=df1.withColumn("Date",to_date(col("issue_date_timestamp")))
df2.display()
  

# COMMAND ----------

#creating another table named table 2
table2_schema = StructType([
                        StructField("SourceId", IntegerType(), True),
                        StructField("TransactionNumber", IntegerType(), True),
                        StructField("Language", StringType(), True),
                        StructField("ModelNumber",IntegerType(),True),
                        StructField("StartTime", StringType(), True),
                        StructField("Product_Number", StringType(), True)])
table2_data = [(150711,123456,"EN",456789,"2021-12-27T08:20:29.842+0000","0001"),
                (150439,234567,"UK",345678,"2021-12-27T08:21:14.645+0000","0002"),
                (150647,345678,"ES",234567,"2021-12-27T08:22:42.445+0000","0003")]

# COMMAND ----------

table2_df=spark.createDataFrame(data=table2_data,schema=table2_schema)
table2_df.display()

# COMMAND ----------

from functools import reduce
column_name_list = table2_df.columns
df_column_name = reduce(lambda table2_df, i: table2_df.withColumnRenamed(i, re.sub(r'(?<!^)(?=[A-Z])', '_', i).lower()),column_name_list, table2_df)
df_column_name.display()

# COMMAND ----------

#converting timestamp in table 2 to unix_timestamp

df3=table2_df.withColumn("timestamp",to_timestamp(col("StartTime")))\
    .withColumn("start_time_ms",unix_timestamp(col("timestamp")))          
df3.display()

# COMMAND ----------

df4=table_df.join(table2_df, table_df.Product_Number==table2_df.Product_Number, "inner")
df4.display()

# COMMAND ----------

df5=df4.filter(df4.Language=="EN")
df5.display()

# COMMAND ----------

#working with UDF
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data  =  (["sreekanth",24],["reddy",30])
df = spark.createDataFrame(data, ["name","age"])

# COMMAND ----------

#Define the UDF

def uppercase_string(s):
    if s is not None:
        return s.upper()
    else:
        return None

# COMMAND ----------

#Register the UDF

uppercase_udf = udf(uppercase_string, StringType())
spark.udf.register("uppercase_udf", uppercase_udf)

# COMMAND ----------

df.withColumn ("upper_name", uppercase_udf("name")).show()

# COMMAND ----------

data=[(1,'maheer',2000,500),(2,"wafa",4000,1000)]
schema=['id','name','salary','bonus']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

def totalpay(s,b):
    return s+b
TotalPayment=udf(lambda s,b:totalpay(s,b),IntegerType())

# COMMAND ----------

df.withColumn('Total_Pay',TotalPayment(df.salary,df.bonus)).show()

# COMMAND ----------

@udf(returnType=IntegerType())
def totalpay(s,b):
    return s+b

# COMMAND ----------

df.withColumn('T_S',totalpay(df.salary,df.bonus)).show()

# COMMAND ----------

#Register UDF and use it in sparkSQL
data=[(1,'maheer',2000,500),(2,"wafa",4000,1000)]
schema=['id','name','salary','bonus']
df=spark.createDataFrame(data,schema)
df.createOrReplaceTempView('emps')

def totalpay(s,b):
    return s+b
spark.udf.register(name='TotalPaySQL',f=totalpay,returnType=IntegerType())

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,name,TotalPaySQL(salary,bonus)as totpay from emps

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

data = (["sreekanth",None],["sreekanth","reddy"])
df = spark.createDataFrame(data, ["firstName","LastName"])
@udf(returnType=StringType())
def full_name(col1, col2):
    if col1 is not None and col2 is not None:
        fullName = col1 + "_" + col2
        return fullName
    else:
        return None

# COMMAND ----------

df.withColumn('ful_name',full_name(df.firstName,df.LastName)).show()

# COMMAND ----------


