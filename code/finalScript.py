import pyspark
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
import pyspark.sql.functions as f
spark= SparkSession.builder.appName("nyc").getOrCreate()
df_parquet1 = spark.read.option("header",True).parquet("s3://mytesting12/abc/cab.parquet/")
df_parquet1.na.drop()
df_parquet2 = df_parquet1.filter("fare_amount > '0'")
df_parquet3 = df_parquet2.withColumn("datetype_timestamp",to_timestamp(col("pickup_datetime"),"yyyy-MM-dd HH:mm:ss"))
df_parquet3 = df_parquet3.withColumn('year',year(df_parquet3.datetype_timestamp))
df_parquet3 = df_parquet3.withColumn('month',month(df_parquet3.datetype_timestamp))
df_parquet3 = df_parquet3.withColumn('day',dayofmonth(df_parquet3.datetype_timestamp))
df_parquet3 = df_parquet3.withColumn('weekdays',dayofweek(df_parquet3.datetype_timestamp))
df_parquet3 = df_parquet3.withColumn('hour',hour(df_parquet3.datetype_timestamp))
df_parquet3.createOrReplaceTempView("nyc3")
t2= spark.sql("select year, avg(fare_amount) as avg from nyc3 group by year")
t2.write.saveAsTable('year_fare')
t3= spark.sql("select month , year, avg(fare_amount) as avg from nyc3 group by month,year")
t3.write.saveAsTable('month_fare')
t4= spark.sql("select day,month, year, avg(fare_amount) as avg from nyc3 group by day,month,year")
t4.write.saveAsTable('day_fare')
t5= spark.sql("select weekdays,month, avg(fare_amount) as avg from nyc3 group by weekdays,month")
t5.write.saveAsTable('week_fare')
t6= spark.sql("select hour,year, avg(fare_amount) as avg from nyc3 group by hour,year")
t6.write.saveAsTable('hour_fare')
