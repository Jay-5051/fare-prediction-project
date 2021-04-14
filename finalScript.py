import pyspark
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession

sc = SparkContext()

sqlContext = SQLContext(sc)

spark = SparkSession.builder.master("local").appName("app name").config("spark.some.config.option", 'true').getOrCreate()

df_parquet = spark.read.option("header",True).parquet("s3://cab-project/unzipped/cab.parquet/")

df_parquet1 = df_parquet.dropDuplicates()

df_parquet1.na.drop()

df_parquet2 = df_parquet1.filter("fare_amount > '0'")

df_parquet3 = df_parquet2.withColumn("datetype_timestamp",to_timestamp(col("pickup_datetime"),"yyyy-MM-dd HH:mm:ss"))

#Transformations on pickup datetime column(timestamp)
df_parquet3 = df_parquet3.withColumn('year',year(df_parquet3.datetype_timestamp))
df_parquet3 = df_parquet3.withColumn('month',month(df_parquet3.datetype_timestamp))
df_parquet3 = df_parquet3.withColumn('day',dayofmonth(df_parquet3.datetype_timestamp))
df_parquet3 = df_parquet3.withColumn('weekdays',dayofweek(df_parquet3.datetype_timestamp))
df_parquet3 = df_parquet3.withColumn('hour',hour(df_parquet3.datetype_timestamp))

df_parquet3.createOrReplaceTempView("nyc3")

sqlContext.sql("Create table nyctaxi select * from nyc3")

sqlContext.sql("create table year_fare as select year, avg(fare_amount) from nyc3 group by year")

sqlContext.sql("create table month_fare as select month , year, avg(fare_amount) from nyc3 group by month,year")

sqlContext.sql("create table day_fare as select day,month, year, avg(fare_amount) from nyc3 group by day,month,year")

sqlContext.sql("create table week_fare as select weekdays,month, avg(fare_amount) from nyc3 group by weekdays,month")

sqlContext.sql("create table hour_fare as select hour,year, avg(fare_amount) from nyc3 group by hour,year")

sqlContext.sql("create table year_trip as select year,count(key) from nyc3 group by year")

sqlContext.sql("create table month_trip as select month , year, count(key) from nyc3 group by month,year order by month,year")

sqlContext.sql("create table day_trip as select day,month, year, count(key) from nyc3 group by day,month,year")

sqlContext.sql("create table week_trip as select weekdays,month, count(key) from nyc3 group by weekdays,month")

sqlContext.sql("create table distance as select pickup_longitude , pickup_latitude, dropoff_longitude, dropoff_latitude, year,fare_amount from nyc3 limit 250000")

