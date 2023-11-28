import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import boto3
from pyspark.sql.functions import col, expr, date_format, hour,from_unixtime, unix_timestamp, concat,lit
from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, dayofmonth
from awsglue.job import Job

# ## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# sc = SparkContext()
# glueContext = GlueContext(sc)

# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
# # Initialize Spark session
# spark = glueContext.spark_session
spark = glueContext.spark_session.builder.enableHiveSupport().config("hive.exec.dynamic.partition","true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()
count = 0
 # Define the S3 bucket and paths
bucket = "proj-hbe-poc"
folder_hr="final/joined_result_hr/"
dest_path_hr="final/weather_historical/"


df1 = spark.read.option("header", "true").csv("s3://proj-hbe-poc/final/joined_result_hr/*.csv")



df1 = df1.withColumn("date", date_format(col("date").cast("date"), "yyyy-MM-dd"))
df1 = df1.withColumn("year", col("date").substr(1, 4))
df1 = df1.withColumn("month", col("date").substr(6, 2))
df1 = df1.withColumn("day", col("date").substr(9, 2))

output_path = "f's3://{bucket}/{dest_path_hr}hourly_data/country=us/"
df1.write.partitionBy("year","month","day").option("header", "true").mode("append").parquet(f's3://{bucket}/{dest_path_hr}hourly_data/country=us/')


# The below code I have tried to cretae single parquet file using pandas df 



# # # Collect distinct combinations of year, month, and day
# # partitions = df1.select("year", "month", "day").distinct().collect()

# # # Write each partition separately with a custom file name
# # for partition in partitions:
# #     year = partition.year
# #     month = partition.month
# #     day = partition.day
    
# #     partitioned_df = df1.filter((col("year") == year) & (col("month") == month) & (col("day") == day))
    
# #     # Define the custom file name
# #     file_name = f"hour_{year}_{month}_{day}.parquet"
    
# #     # Write the partition to the specified path with the custom file name
# #     partitioned_df.write.option("header", "true").mode("append").parquet(f"s3://{bucket}/{dest_path_hr}hourly_data/country=us/year={year}/month={month}/day={day}/hourly_{year}_{month}_{day}.parquet", mode="append")
        
# # # df =df1.toPandas()
# # # # df.rename(columns={'timestamp_12hr': 'date'}, inplace=True)
# # # df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
# # # df['year'] = df['date'].dt.year
# # # df['month'] = df['date'].dt.month
# # # df['day'] = df['date'].dt.day
# # # # # Group by hour
# # # # df.rename(columns = {'timestamp_12hr':'datetime'}, inplace = True)
# # # count = count + 1 
# # # print(count)

# # # grouped_time = df.groupby("date")
# # #     # Iterate through each group and perform further processing
# # # for (year, month, day), time_data_df in grouped_time:
    
# # # #     
# # #     file_name=f's3://{bucket}/{dest_path_hr}hourly_data/country=us/year={year}/month={month}/hourly_load_{year}_{month}_{day}.parquet'
# # #     print(file_name)
# # #     time_data_df.to_parquet(file_name, index=False)

# job.commit()