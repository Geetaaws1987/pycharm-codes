from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import to_date, date_format, lead, lit, expr
from pyspark.sql.functions import split
from pyspark.sql.types import DateType
from pyspark.sql.functions import when, col
import datetime as dt
from pyspark.sql.functions import from_unixtime, date_trunc, last_day
spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("First_1")
    .getOrCreate()
)

file = pd.read_excel("D:\\files\\QMaterialCostL1.xlsx")

df = spark.read.csv("D:\\files\QMaterialCost__L1.csv", header =True)


# Extract the date part from the timestamp column and convert it to mm-dd-yyyy
df = df.withColumn("from_date", date_format(to_date("UpdateTimeStamp"), "MM-dd-yyyy"))

split_col = split(df["CostUnitName"], "/")

# Create new columns for the split values
df = df.withColumn("Cost", split_col.getItem(0))
df = df.withColumn("UnitName", split_col.getItem(1))

df1 = df.drop("CostUnitName",'_c0')



df1.createOrReplaceTempView("costtable")


df5= spark.sql("""
with cte as (
  select
    *,
    lead(from_date, 1, '2100-01-01') over (partition by plant, mat_source_code order by from_date desc) as to_date
  from costtable
)
select
  *,
  case
    when from_date = to_date then to_date 
    else '2100-01-01'
  end as end_date
from cte
order by plant, mat_source_code, from_date

""")
df6 = df5.select(F.col("mat_fam_type"), F.col("mat_source_code"),F.col("MaterialDate"),F.col("plant"),F.col("Region"),F.col("UpdateTimeStamp"),F.col("Sum(Cost)"),F.col("Cost"),F.col("UnitName"),F.col("from_date"),F.col("to_date"),to_date(F.col("from_date"),"MM-dd-yyyy"),to_date(F.col("to_date"),"MM-dd-yyyy"))
# df6.show()
#df6.printSchema()
df7 = df5.select(F.col("to_date"),to_date(F.col("to_date"),"MM-dd-yyyy").alias("date_t"))

df8 = df6.withColumn('to_date(to_date, MM-dd-yyyy)', when(F.col('to_date(to_date, MM-dd-yyyy)').isNull(), dt.date(2100, 1, 1)).otherwise(F.col('to_date(to_date, MM-dd-yyyy)')))
df8.show()

d6 = df8.drop(df8.from_date)
d7 =d6.drop(d6.to_date)
d7.show()

d7 = d7.withColumnRenamed("to_date(from_date, MM-dd-yyyy)", "from_date")

d7 = d7.withColumnRenamed("to_date(to_date, MM-dd-yyyy)", "to_date")




d7= d7.withColumn("from_date", date_trunc("month", F.col("from_date")))
d7 = d7.withColumn("to_date", date_trunc("month", F.col("to_date")))


d8 = d7.select(F.col("mat_fam_type"), F.col("mat_source_code"),F.col("MaterialDate"),F.col("plant"),F.col("Region"),F.col("UpdateTimeStamp"),F.col("Sum(Cost)"),F.col("Cost"),F.col("UnitName"),F.col("from_date"),F.col("to_date"),to_date(F.col("from_date"),"MM-dd-yyyy"),to_date(F.col("to_date"),"MM-dd-yyyy"))
d9 = d8.drop(d8.from_date)
d10 =d9.drop(d9.to_date)
d10 = d10.withColumnRenamed("to_date(from_date, MM-dd-yyyy)", "from_date")

d10 = d10.withColumnRenamed("to_date(to_date, MM-dd-yyyy)", "to_date")
d10 = d10.withColumnRenamed("Cost", "Cost_unit")
d10 = d10.withColumnRenamed("Sum(Cost)", "Sum_Cost")

d10.coalesce(1).write.csv("D:\output\revised_QMaterialCost_L1.csv", header=True, sep=',', mode="overwrite")