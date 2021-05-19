from pyspark.sql.functions import desc, asc
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (SparkSession
         .builder
         .appName("SparkSqlExample")
         .getOrCreate()
         )


# Create DB in Python
spark.sql("Create database learn_spark_db")
spark.sql("Use learn_spark_db")

# Path to data set
csv_file = "C:/Spark/data/departuredelays.csv"
schema = "data STRING , delay INT, distance INT, origin STRING, destination  STRING"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")



