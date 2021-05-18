from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *


# df_data = Row(1,"Orxan",3,"Hey",["twitter","facebook"])
# print(df_data[-1][0])

spark = (SparkSession.builder.appName("Example").getOrCreate())

rows = [Row(1,"Orxan", "Sumgait"),Row(2,"Ehmed","Novxani")]

schema = StructType([
   StructField("Id",IntegerType(),False),
   StructField("Authors",StringType(), False),
   StructField("State", StringType(), False)])

df_Data = spark.createDataFrame(rows,schema)
df_Data.show()