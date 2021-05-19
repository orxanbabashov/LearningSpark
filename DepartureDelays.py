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
      .option("inferSchema","true")
      .option("header","true")
      .load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")

spark.sql("""SELECT distance, origin,destination from us_delay_flights_tbl  where  distance > 1000
           ORDER BY distance DESC""").show(10)

spark.sql("""Select  * from  us_delay_flights_tbl """).show(5)



