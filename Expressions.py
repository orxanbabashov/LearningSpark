from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Define custom schema

schema_def = StructType([
   StructField("Id", IntegerType(), False),
   StructField("First", StringType(), False),
   StructField("Last", StringType(), False),
   StructField("Url", StringType(), False),
   StructField("Published", StringType(), False),
   StructField("Hits", IntegerType(), False),
   StructField("Campaigns", ArrayType(StringType()), False)])

# main program
if __name__ == "__main__":
   #create a SparkSession
   spark = (SparkSession
       .builder
       .appName("Example-3_6")
       .getOrCreate())

#Create DataFrame reading the Json File

df_with_schema = spark.read.schema(schema_def) \
                .json("C:/Spark/data/blogs.json")
df_with_schema.show()

df_with_schema.withColumn("NewColumn",concat_ws(",",expr("First"),expr("Last"))).select("NewColumn").show(4)

df_with_schema.withColumn("NewColumn",concat_ws(",",expr("First"),expr("Last"))).show(1)
df_with_schema.sort(expr("Id").desc()).show(truncate=False)