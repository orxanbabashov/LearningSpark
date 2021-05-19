from pyspark.sql import SparkSession
spark = (SparkSession
         .builder
         .appName("SparkSqlExample")
         .getOrCreate()
         )


# Create DB in Python

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# Create Spark Managed table
spark.sql("""CREATE TABLE managed_us_delay_flights_tbl ( date STRING , delay INT,distance INT, origin STRING,destination  STRING)""")


# Path to data set
csv_file = "C:/Spark/data/departure's.csv"
schema = "date STRING , delay INT, distance INT, origin STRING, destination  STRING"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")
