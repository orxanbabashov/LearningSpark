from pyspark.sql.functions import desc, asc

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (SparkSession
         .builder
         .appName("SparkSqlExample")
         .getOrCreate()
         )
# Path to data set
csv_file = "C:/Spark/data/departuredelays.csv"

# Read and Create a temporary view
df = (spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csv_file))

# Other way around to create data frame with programmatically
# df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# Same result A variant

(df.select("distance", "origin", "destination")
 .where("distance > 1000")
 .orderBy("distance", ascending=False)).show(10)

# Same result B variant

(df.select("distance", "origin", "destination")
 .where(expr("distance") > 1000)
 .orderBy(asc("distance"))).show(10)

df.printSchema()

(df.select("date", "delay", "origin", "destination")
   .filter("delay" > int("120") & "origin" == "SFO" & "destination" == "ORD")
   .orderBy(desc("delay")))
