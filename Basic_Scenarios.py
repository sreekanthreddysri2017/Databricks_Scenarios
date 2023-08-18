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

#how to handle double delimiters/multi delimiters in pyspark

dbutils.fs.put("/schenarios/double_pipe.csv","""id||name||loc
1||ravi||Bangalore
2||Raj||Chennai
3||Mahesh||Hyderabad
4||Prasad||Chennai
5||Sridhar||Pune
""",True)

# COMMAND ----------

df = spark.read.csv("/schenarios/double_pipe.csv",header=True,sep="||")
display(df)

# COMMAND ----------

dbutils.fs.put("/scenarios/multi_sep.csv","""id,name,loc,marks
1,ravi,Bangalore,35|45|55|65
2,Raj,Chennai,35|45|55|65
3,Mahesh,Hyderabad,35|45|55|65
4,Prasad,Chennai,35|45|55|65
5,Sridhar,Pune,35|45|55|65
""",True)

# COMMAND ----------

df_multi = spark.read.csv("/scenarios/multi_sep.csv",header=True)
display(df_multi)

# COMMAND ----------

# MAGIC %sql
# MAGIC select split("1|2|3|4","\\|")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select split("1|2|3|4","[|]")

# COMMAND ----------

from pyspark.sql.functions import split,col
df_multi =df_multi.withColumn("marks_split",split(col("marks"),"[|]"))\
            .withColumn("SUB1",col("marks_split")[0])\
            .withColumn("SUB2",col("marks_split")[1])\
            .withColumn("SUB3",col("marks_split")[2])\
            .withColumn("SUB4",col("marks_split")[3]).drop("marks_split","marks")
display(df_multi)
     

# COMMAND ----------

# how do i select a column name with spaces
dbutils.fs.put("/schenarios/emp_pipe.csv","""id||last name||loc
1||ravi||Bangalore
2||Raj||Chennai
3||Mahesh||Hyderabad
4||Prasad||Chennai
5||Sridhar||Pune
""",True)

# COMMAND ----------

df = spark.read.csv("/schenarios/emp_pipe.csv",header=True,sep="||")
display(df)

# COMMAND ----------

df.createOrReplaceTempView('emp_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select id,`last name`from emp_view;

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

df.withColumn('new_column',col('last name')).show()

# COMMAND ----------

# Removing duplicate rows based on updated date

# COMMAND ----------

dbutils.fs.put("/scenarios/duplicates.csv","""id,name,loc,updated_date
1,ravi,bangalore,2021-01-01
1,ravi,chennai,2022-02-02
1,ravi,Hyderabad,2022-06-10
2,Raj,bangalore,2021-01-01
2,Raj,chennai,2022-02-02
3,Raj,Hyderabad,2022-06-10
4,Prasad,bangalore,2021-01-01
5,Mahesh,chennai,2022-02-02
4,Prasad,Hyderabad,2022-06-10
""")

# COMMAND ----------

df= spark.read.csv("/scenarios/duplicates.csv",header=True,inferSchema=True)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col
display(df.orderBy(col("updated_date").desc()).dropDuplicates(["id"]))

# COMMAND ----------

#Window function with row_number()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
df = df.withColumn("rowid",row_number().over(Window.partitionBy("id").orderBy(col("updated_date").desc())))

# COMMAND ----------

df_uniq = df.filter("rowid=1")

# COMMAND ----------

df_baddata = df.filter("rowid>1")

# COMMAND ----------

display(df_uniq)

# COMMAND ----------


