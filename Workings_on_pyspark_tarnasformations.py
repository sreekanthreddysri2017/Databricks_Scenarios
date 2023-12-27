# Databricks notebook source
options={'header':True,
        'delimiter':',',
        'inferschema':True}

def read_file(format,path,options):
    df=spark.read.format(format).options(**options).load(path)
    return df

# COMMAND ----------

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# COMMAND ----------

df1=read_file('csv','dbfs:/FileStore/export__1_.csv',options)
df1.display()

# COMMAND ----------

display(df1.distinct())
print("Number of distinct col",str(df1.distinct().count()))

# COMMAND ----------

display(df1.select('Age'))

# COMMAND ----------

display(df1.dropDuplicates(["Education Level","Years of Experience"]))

# COMMAND ----------

df1.filter(df1.Age.isNull()).count()

# COMMAND ----------

#display(df1.filter(df1.Gender.contains("Female")))
display(df1.filter(df1.Gender=='Female'))

# COMMAND ----------

from pyspark.sql.functions import *
df2=df1.withColumn("Age" ,when(df1.Age==25,32).otherwise(df1.Age))
display(df2)

# COMMAND ----------

options={'header':'true',
         'delimiter':',',
         'inferschema':'true'}

def read_csv(format,path,options):
    return spark.read.format(format).options(**options).load(path)

# COMMAND ----------

df3= df1.select([count(when(col(c).isNull(),c)).alias(c) for c in df1.columns])
df3.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1.filter(df1.Age.isNull()).show()
df1.filter(col("Education Level").isNull()).show()

# COMMAND ----------

df1.select([count(when(col(c).isNull(),c).alias(c) for c in df1.columns)])

# COMMAND ----------



# COMMAND ----------

options={'header':True,
        'delimiter':',',
        'inferschema':True}

def read_file(format,path,options):
    df=spark.read.format(format).options(**options).load(path)
    return df

# COMMAND ----------

df1=read_csv('csv','dbfs:/FileStore/Book1.csv',options)
df1.show()

# COMMAND ----------

def add_col(df_name,col_name,lit_value):
    df2=df_name.withColumn(col_name,lit(lit_value))
    return df2

# COMMAND ----------

df2=add_col(df1,'city','bngl')
df2.show()

# COMMAND ----------

def rename_col(df_name,old_col_name,new_col_name):
    return df_name.withColumnRenamed(old_col_name,new_col_name)

# COMMAND ----------

df3=rename_col(df2,'city','CITY')
df3.show()

# COMMAND ----------

def add_columns(df, columns, values):
# Add multiple columns to a DataFrame with specified values.
    if len(columns) != len(values):
        raise ValueError("Number of columns must be equal to the number of values.")

    for col_name, col_value in zip(columns, values):
        df = df.withColumn(col_name, lit(col_value))

    return df

# COMMAND ----------

new_columns = ['state','district']
new_values = ['Ap','chittoor']


df4=add_columns(df3,new_columns,new_values)
df4.show()

# COMMAND ----------

def rename_columns(df, columns, values):
# Add multiple columns to a DataFrame with specified values.
    if len(columns) != len(values):
        raise ValueError("Number of columns must be equal to the number of values.")

    for col_name, col_value in zip(columns, values):
        df = df.withColumnRenamed(col_name, col_value)

    return df

old_columns = ['state','district']
new_columns = ['STATE','DISTRICT']

# COMMAND ----------

df5=rename_columns(df4,old_columns,new_columns)
df5.show()

# COMMAND ----------


