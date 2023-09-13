# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType


# COMMAND ----------

schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False)
])

# COMMAND ----------

# Create an Iceberg table using the Spark Catalog and the defined schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE TABLE demo.sreekanth.taxis
# MAGIC (
# MAGIC   vendor_id int,
# MAGIC   trip_id int,
# MAGIC   trip_distance float,
# MAGIC   fare_amount int,
# MAGIC   store_and_fwd_flag string
# MAGIC )
# MAGIC PARTITIONED BY (vendor_id);

# COMMAND ----------

from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType
schema = StructType([
  StructField("vendor_id", LongType(), True),
  StructField("trip_id", LongType(), True),
  StructField("trip_distance", FloatType(), True),
  StructField("fare_amount", DoubleType(), True),
  StructField("store_and_fwd_flag", StringType(), True)
])
sample_data = [
    {"vendor_id": 1, "trip_id": 101, "trip_distance": 5.2, "fare_amount": 15.0, "store_and_fwd_flag": "N"},
    {"vendor_id": 2, "trip_id": 102, "trip_distance": 3.8, "fare_amount": 12.5, "store_and_fwd_flag": "Y"},
    {"vendor_id": 1, "trip_id": 103, "trip_distance": 2.5, "fare_amount": 10.75, "store_and_fwd_flag": "N"},
    {"vendor_id": 2, "trip_id": 104, "trip_distance": 6.7, "fare_amount": 18.25, "store_and_fwd_flag": "Y"},
    {"vendor_id": 1, "trip_id": 105, "trip_distance": 4.1, "fare_amount": 13.5, "store_and_fwd_flag": "N"}
]
df = spark.createDataFrame(sample_data, schema)
df.writeTo("demo.sreekanth.taxis").create()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE iceberg.db.my_table (name string, age int) USING iceberg;

# COMMAND ----------

# Read data into a DataFrame
df = spark.read.csv("dbfs:/FileStore/Book4_1.csv", header=True, inferSchema=True)

# Specify the Iceberg table path
table_path = "dbfs:/FileStore/iceberg_tables/my_table"

# Write DataFrame to Iceberg table
df.write \
  .format("iceberg")\
  .mode("overwrite")\
  .option("path", table_path)\
  .saveAsTable("my_table")


# COMMAND ----------

schema = spark.table("demo.nyc.taxis").schema
data = [
    (1, 1000371, 1.8, 15.32, "N"),
    (2, 1000372, 2.5, 22.15, "N"),
    (2, 1000373, 0.9, 9.01, "N"),
    (1, 1000374, 8.4, 42.13, "Y")
  ]
df = spark.createDataFrame(data, schema)
df.writeTo("demo.sreekanth.taxis").create()

# COMMAND ----------

spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.3.1

# COMMAND ----------

spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.3.1

# COMMAND ----------

spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.3.1\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse

# COMMAND ----------

spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.3.1 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse


# COMMAND ----------

# MAGIC %sql
# MAGIC --- local is the path-based catalog defined above
# MAGIC CREATE TABLE local.db.table (id bigint, data string) USING iceberg

# COMMAND ----------

from py4j.java_gateway import java_import
java_import(spark._jvm, "org.apache.iceberg.Schema")


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from py4j.java_gateway import java_import

# Create a Spark session
spark = SparkSession.builder \
    .appName("IcebergExample") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .getOrCreate()

# Define your schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True)
])

# Specify the Iceberg table location
table_location = "/path/to/your/table/location"

# Create an Iceberg table
java_import(spark._jvm, "org.apache.iceberg.Schema")
java_import(spark._jvm, "org.apache.iceberg.catalog.Catalog")
java_import(spark._jvm, "org.apache.iceberg.catalog.TableIdentifier")
java_import(spark._jvm, "org.apache.iceberg.spark.SparkCatalog")

schema = spark._jvm.org.apache.iceberg.Schema([
    spark._jvm.org.apache.iceberg.types.Types.NestedField.required(1, "id", spark._jvm.org.apache.iceberg.types.Types.IntegerType.get()),
    spark._jvm.org.apache.iceberg.types.Types.NestedField.optional(2, "name", spark._jvm.org.apache.iceberg.types.Types.StringType.get())
])

# Create an Iceberg catalog
iceberg_catalog = spark._jvm.org.apache.iceberg.spark.SparkCatalog(spark._jsparkSession.conf(), "spark_catalog")

# Create an Iceberg table
table = iceberg_catalog.createTable(spark._jvm.org.apache.iceberg.catalog.TableIdentifier.of("my_database", "my_table"), schema)

# Use the table to write data (you can use other methods to write data)
table.newAppend().appendFile(schema.asStruct(), [1, "Alice"]).commit()

# Don't forget to stop the Spark session when you're done
# spark.stop()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from py4j.java_gateway import java_import

# Create a Spark session
spark = SparkSession.builder \
    .appName("IcebergExample") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .getOrCreate()

# Define your PySpark schema
pyspark_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True)
])

# Specify the Iceberg table location
table_location = "/path/to/your/table/location"

# Import Iceberg Java classes
java_import(spark._jvm, "org.apache.iceberg.Schema")
java_import(spark._jvm, "org.apache.iceberg.catalog.Catalog")
java_import(spark._jvm, "org.apache.iceberg.catalog.TableIdentifier")
java_import(spark._jvm, "org.apache.iceberg.types.Types")

# Create an Iceberg schema
iceberg_schema = spark._jvm.org.apache.iceberg.Schema(
    spark._jvm.org.apache.iceberg.types.Types.NestedField.required(1, "id", spark._jvm.org.apache.iceberg.types.Types.IntegerType.get()),
    spark._jvm.org.apache.iceberg.types.Types.NestedField.optional(2, "name", spark._jvm.org.apache.iceberg.types.Types.StringType.get())
)

# Create an Iceberg catalog
iceberg_catalog = spark._jvm.org.apache.iceberg.spark.SparkCatalog(spark._jsparkSession.conf(), "spark_catalog")

# Create an Iceberg table
table = iceberg_catalog.createTable(spark._jvm.org.apache.iceberg.catalog.TableIdentifier.of("my_database", "my_table"), iceberg_schema)

# Use the table to write data (you can use other methods to write data)
table.newAppend().appendFile(iceberg_schema.asStruct(), [1, "Alice"]).commit()

# Don't forget to stop the Spark session when you're done
# spark.stop()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from py4j.java_gateway import java_import

# Create a Spark session
spark = SparkSession.builder \
    .appName("IcebergExample") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .getOrCreate()

# Define your PySpark schema
pyspark_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True)
])

# Specify the Iceberg table location
table_location = "/path/to/your/table/location"

# Import Iceberg Java classes
java_import(spark._jvm, "org.apache.iceberg.SchemaParser")
java_import(spark._jvm, "org.apache.iceberg.catalog.Catalog")
java_import(spark._jvm, "org.apache.iceberg.catalog.TableIdentifier")

# Parse Iceberg schema from a string
iceberg_schema = spark._jvm.org.apache.iceberg.SchemaParser.fromJson("""
{
  "type": "struct",
  "fields": [
    {"type": "integer", "name": "id", "required": true},
    {"type": "string", "name": "name", "optional": true}
  ]
}
""")

# Create an Iceberg catalog
iceberg_catalog = spark._jvm.org.apache.iceberg.spark.SparkCatalog(spark._jsparkSession.conf(), "spark_catalog")

# Create an Iceberg table
table = iceberg_catalog.createTable(spark._jvm.org.apache.iceberg.catalog.TableIdentifier.of("my_database", "my_table"), iceberg_schema)

# Use the table to write data (you can use other methods to write data)
table.newAppend().appendFile(iceberg_schema.asStruct(), [1, "Alice"]).commit()

# Don't forget to stop the Spark session when you're done
# spark.stop()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE iceberg.sreekanth.my_table (name string, age int) USING iceberg;

# COMMAND ----------

spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
spark.conf.set("spark.sql.catalog.spark_catalog.type", "hive")



from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType
schema = StructType([
  StructField("vendor_id", LongType(), True),
  StructField("trip_id", LongType(), True),
  StructField("trip_distance", FloatType(), True),
  StructField("fare_amount", DoubleType(), True),
  StructField("store_and_fwd_flag", StringType(), True)
])


sample_data = [
    {"vendor_id": 1, "trip_id": 101, "trip_distance": 5.2, "fare_amount": 15.0, "store_and_fwd_flag": "N"},
    {"vendor_id": 2, "trip_id": 102, "trip_distance": 3.8, "fare_amount": 12.5, "store_and_fwd_flag": "Y"},
    {"vendor_id": 1, "trip_id": 103, "trip_distance": 2.5, "fare_amount": 10.75, "store_and_fwd_flag": "N"},
    {"vendor_id": 2, "trip_id": 104, "trip_distance": 6.7, "fare_amount": 18.25, "store_and_fwd_flag": "Y"},
    {"vendor_id": 1, "trip_id": 105,  "trip_distance": 4.1, "fare_amount": 13.5, "store_and_fwd_flag": "N"}
]
df = spark.createDataFrame(sample_data, schema)
df.writeTo("demo.sreekanth.taxis").create()

# COMMAND ----------




# COMMAND ----------


