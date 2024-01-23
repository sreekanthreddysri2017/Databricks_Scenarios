# Databricks notebook source
def mounting_layer_json(source,mountpoint,key,value):
    dbutils.fs.mount(
    source=source,
    mount_point= mountpoint,
    extra_configs={key:value})

# COMMAND ----------

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# COMMAND ----------



# COMMAND ----------

options={'header':True,
         'inferschema':True,
         'delimiter':','}
 
def read_csv(format,options,path):
    return spark.read.format(format).options(**options).load(path)
    
 
df=read_csv('csv',options,'dbfs:/FileStore/emp_tables/pizza_sales.csv')

# COMMAND ----------

#df=spark.read.format('csv').option('header',True).load('dbfs:/FileStore/emp_tables/pizza_sales.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView('pizza_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from pizza_data

# COMMAND ----------

# KPI

# totalpizza sold
# total orers,total revenue,avg pizza perorders
# sales vs order month over month
# daily hours, monthly ,hourly orders trend
# sales by pizza category
# sales by pizza size
# top 5 orders pizza

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select order_id,
# MAGIC quantity,
# MAGIC date_format(order_date,'MMM' ) month_name,
# MAGIC date_format(order_date,'EEEE' ) day_name,
# MAGIC hour(order_time) order_hr_time,
# MAGIC unit_price,
# MAGIC total_price,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from pizza_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(distinct(order_id)) order_id,
# MAGIC sum(quantity) quantity,
# MAGIC date_format(order_date,'MMM' ) month_name,
# MAGIC date_format(order_date,'EEEE' ) day_name,
# MAGIC hour(order_time) order_hr_time,
# MAGIC sum(unit_price) unit_price,
# MAGIC sum(total_price) t0tal_sales,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from pizza_data
# MAGIC
# MAGIC group by month_name,day_name,order_hr_time,pizza_size,pizza_category,pizza_name

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
from pyspark.sql.functions import *

schema=StructType([StructField('product_id',IntegerType(),True),
                   StructField('customer_id',StringType(),True),
                   StructField('order_date',DateType(),True),
                   StructField('location',StringType(),True),
                   StructField('source_order',StringType(),True)])

# COMMAND ----------

options={'header':True,
         'inferschema':True,
         'delimiter':','}
 
def read_csv(format,options,schema,path):
    return spark.read.format(format).schema(schema).options(**options).load(path)
    
 
df=read_csv('csv',options,schema,'dbfs:/FileStore/emp_tables/sales_csv.txt')

# COMMAND ----------

df1=df.withColumn('order_year',year(df.order_date)).withColumn('order_month',month(df.order_date)).withColumn('order_quarter',quarter(df.order_date))

display(df1)

# COMMAND ----------

schema1=StructType([StructField('product_id',IntegerType(),True),
                   StructField('product_name',StringType(),True),
                   StructField('price',StringType(),True),
                  ])

# COMMAND ----------

df2=read_csv('csv',options,schema1,'dbfs:/FileStore/emp_tables/menu_csv.txt')

# COMMAND ----------

display(df2)

# COMMAND ----------

#total amount spent by each user

df3=df1.join(df2,'product_id').groupBy('customer_id').agg(sum('price')).orderBy('customer_id')
display(df3)

# COMMAND ----------

#total amount spent by each food category

df4=df1.join(df2,'product_id').groupBy('product_name').agg(sum('price')).orderBy('product_name')
display(df4)

# COMMAND ----------

#total amount of sales in each month

df5=df1.join(df2,'product_id').groupBy('order_month').agg(sum('price')).orderBy('order_month')
display(df5)

# COMMAND ----------

#How may times each produce purchased

df6=df1.join(df2,'product_id').groupBy('product_id','product_name').agg(count('product_id').alias('product_count')).orderBy('product_count',ascending=(0))
display(df6)

# COMMAND ----------

#Mostly ordered item

df6=df1.join(df2,'product_id').groupBy('product_id','product_name').agg(count('product_id').alias('product_count')).orderBy('product_count',ascending=(0)).limit(1)
display(df6)

# COMMAND ----------

df7=df1.filter(df1.source_order=='Restaurant')
display(df7)

# COMMAND ----------

df1.columns

# COMMAND ----------

columns=['product_id',
 'customer_id',
 'order_date',
 'location',
 'source_order',
 'order_year',
 ]
expected_columns=set(df1.columns)
if expected_columns==set(columns):
     print('all columns available')
else:
    missing_columns=expected_columns-set(columns)
    print(f'missing columns are: {missing_columns}')

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.columns

# COMMAND ----------

    # old_columns=['product_id', 'product_name']
    # new_columns=['product_Id', 'product_Name']
    from pyspark.sql.functions import *

    def change_column_names(old,new):
        df_1=df2.withColumnRenamed(old,new)
        return df_1

    df_2=change_column_names('product_id','product_Id')
    display(df_2)

# COMMAND ----------

def add_column(old,new):
    df_1=df2.withColumn(old,lit(new))
    return df_1

df_3=add_column('total',10000)
display(df_3)

# COMMAND ----------

old_columns=['product_id', 'product_name']
new_columns=['product_Id', 'product_Name']

def change_column_namess(old_c,new_c,df):
    df1=df

    for k,v in zip(old_columns,new_columns):
        df1=df1.withColumnRenamed(k,v)
    return df1
    
df_4=change_column_namess(old_columns,new_columns,df2)
display(df_4)

# COMMAND ----------

import requests

response=requests.get('https://gist.githubusercontent.com/pradeeppaikateel/a5caf3b8dea7cf215b1e0cf8ebbbba4d/raw/79125d55b44de60f519ad3fe12ce24329492e3e3/nest.json%27')
rdd=spark.sparkContext.parallelize([response.text])
data=spark.read.option('multiline',True).json(rdd)
display(data)

# COMMAND ----------

emp=[StructField('emp_id',IntegerType(),True),
     StructField('emp_name',StringType(),True)]
pro=[StructField('name',StringType(),True),
     StructField('store_size',StringType(),True)]
 
 
schema=StructType([StructField('employees',ArrayType(StructType(emp)),True),
                   StructField('id',IntegerType(),True),
                   StructField('properties',StructType(pro),True),
                  ])

# COMMAND ----------

#

# COMMAND ----------

import requests

response=requests.get('https://dummyjson.com/products/category/smartphones')
rdd=spark.sparkContext.parallelize([response.text])
data=spark.read.option('multiline',True).json(rdd)
display(data)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

pro=[ StructField('brand', StringType(), True),
    StructField('category', StringType(), True),
    StructField('description', StringType(), True),
    StructField('discountPercentage', DoubleType(), True),
    StructField('id', IntegerType(), True),
    StructField('images', ArrayType(StringType()), True),
    StructField('price', IntegerType(), True),
    StructField('rating', DoubleType(), True),
    StructField('stock', IntegerType(), True),
    StructField('thumbnail', StringType(), True),
    StructField('title', StringType(), True)]

custom_schema = StructType([
    StructField('limit', IntegerType(), True),
    StructField('products', ArrayType(
        StructType(pro)), True),
    StructField('skip', IntegerType(), True),
    StructField('total', IntegerType(), True)
])

# COMMAND ----------

import requests

response=requests.get('https://dummyjson.com/products/category/smartphones')
rdd=spark.sparkContext.parallelize([response.text])
data=spark.read.option('multiline',True).schema(custom_schema).json(rdd)
display(data)

# COMMAND ----------

df=data.withColumn('products',explode(data.products))

# COMMAND ----------

df1=df.select('limit','products.*','skip','total')
display(df1)

# COMMAND ----------

# It seems like there's an issue with the column types. The error indicates that the "images" column is of type ARRAY<STRING>, and the split function expects a STRING type.

df3=df1.select('id','discountPercentage',explode(df1.images).alias('Images'))
display(df3)

# COMMAND ----------


