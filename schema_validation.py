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

#writing schema and exploding the nested json file

# COMMAND ----------

df=spark.read.format('json').option("inferSchema",True).option('multiline','true').load('dbfs:/FileStore/usedCars.json')
#df.printSchema()
df.display()

# COMMAND ----------

from pyspark.sql.types import *

usescars=[
        StructField("@type", StringType(), True),
        StructField("adLastUpdated", StringType(), True),
        StructField("bodyType", StringType(), True)]

brand=[
            StructField("@type", StringType(), True),
            StructField("name", StringType(), True)
        ]

extraFeatures=[
            StructField("AdRef#", StringType(), True),
            StructField("Assembly", StringType(), True),
            StructField("AuctionGrade", StringType(), True),
            StructField("BatteryCapacity", StringType(), True),
            StructField("BodyType", StringType(), True),
            StructField("ChassisNo.", StringType(), True),
            StructField("Color", StringType(), True),
            StructField("EngineCapacity", StringType(), True),
            StructField("ImportDate", StringType(), True),
            StructField("LastUpdated", StringType(), True),
            StructField("RegisteredIn", StringType(), True),
            StructField("Warranty", StringType(), True)
        ]


vehicleEngine=[
            StructField("@type", StringType(), True),
            StructField("engineDisplacement", StringType(), True),
            StructField("vehicleTransmission", StringType(), True)
        ]

schema = StructType([
    StructField("usedCars", ArrayType(StructType(usescars)),True),
        StructField("brand", StructType(brand), True),
        StructField("color", StringType(), True),
        StructField("description", StringType(), True),
        StructField("extraFeatures", StructType(extraFeatures), True),
        StructField("features", ArrayType(StringType()), True),
        StructField("fuelType", StringType(), True),
        StructField("image", StringType(), True),
        StructField("itemCondition", StringType(), True),
        StructField("keywords", StringType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("mileageFromOdometer", StringType(), True),
        StructField("model", StringType(), True),
        StructField("modelDate", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("postedFrom", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("priceCurrency", StringType(), True),
        StructField("sellerLocation", StringType(), True),
        StructField("vehicleEngine", StructType(vehicleEngine), True)
    ])


# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.format('json').schema(schema).option('multiline','true').load('dbfs:/FileStore/usedCars.json')
df.display()

# COMMAND ----------

df1= df.withColumn("usedCars",explode_outer("usedCars")).select("*","usedCars.*").drop("usedCars")\
    .select("*","brand.*").drop("brand").select('*','extraFeatures.*').drop("extraFeatures").select('*','vehicleEngine.*')\
    .drop('vehicleEngine')
display(df1)

# COMMAND ----------


