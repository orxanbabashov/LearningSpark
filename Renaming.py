# In python define a schema

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create a SparkSession
# main program
if __name__ == "__main__":
    # create a SparkSession
    spark = (SparkSession
             .builder
             .appName("Example-3_6")
             .getOrCreate())

# Programmatic way to define a schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', IntegerType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])

sf_fire_file = "C:/Spark/data/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedMins")
new_fire_df.show(20)

fire_ts_df = (new_fire_df
              .withColumn("IncidentDate", to_timestamp(expr("CallDate"), "MM/dd/yyyy"))
              .drop("CallDate")
              .withColumn("OnWatch", to_timestamp(expr("WatchDate"), "MM/dd/yyyy"))
              .drop("WatchDate")
              .withColumn("AvailableDtTS", to_timestamp(expr("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
              .drop("AvailableDtTm")
              )
(fire_ts_df
 .select("IncidentDate", "OnWatch", "AvailableDtTS")
 .show(5, truncate=False)
 )
