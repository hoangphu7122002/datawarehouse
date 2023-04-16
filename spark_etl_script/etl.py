import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,sum,udf,when
from pyspark.sql.types import StringType,IntegerType

def convert_date(x):
    if '/' in x:
        ele = x.split('/')
    else:
        ele = x.split('-')
    if len(ele[0]) == 4:
        return ele[0] + '-' + ele[1] + '-' + ele[2]
    return ele[2] + '-' + ele[1] + '-' + ele[0]

def convert_vnd_to_dollar(x):
    return int(x) * 40


parser = argparse.ArgumentParser()
parser.add_argument("--exe_date")
args = parser.parse_args()

exe_date = ""
if args.exe_date:
	exe_date = args.exe_date

runTime = exe_date.split("-")
year = runTime[0]
month = runTime[1]
day = runTime[2]

spark = SparkSession.builder \
        .master('local[*]') \
        .appName("Daily Report2") \
        .config('hive.metastore.urls','thrift://localhost:9083') \
        .config('hive.exec.dynamic.partition','true') \
        .config('hive.exec.dynamic.partition.mode',"nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

ordersDf = spark.read.parquet("hdfs://master:9000/datalake/orders").drop("year","month","day")
orderDetailDf = spark.read.parquet("hdfs://master:9000/datalake/order_detail").drop("year","month","day")
productsDf = spark.read.parquet("hdfs://master:9000/datalake/product").drop("year","month","day","created_at")
inventoryDf = spark.read.parquet("hdfs://master:9000/datalake/inventory").drop("year","month","day")

#get null df
null_df_orders = ordersDf.filter(col("quantity").isNull() | col("created_at").isNull())
#null_df_ordersDetail = orderDetailDf.filter(col("user_id").isNull() | col("total").isNull())
df_null = null_df_orders.join(orderDetailDf, null_df_orders["product_id"] == orderDetailDf["order_id"],'left_outer')

#remove null and convert date
dateUdf = udf(convert_date,StringType())
ordersDf = ordersDf.na.drop()
ordersDf.filter(col("quantity").isNull() | col("created_at").isNull()).show()
ordersDf = ordersDf.withColumn("created_at",dateUdf(col("created_at")))

#remove null and convert dollar
dollarUdf = udf(convert_vnd_to_dollar,IntegerType())
#orderDetailDf = orderDetailDf.na.drop()
orderDetailDf = orderDetailDf.withColumn("total",when(col("unit") == "VND",dollarUdf(col("total"))).otherwise(col("total")))


preDF = ordersDf.filter(ordersDf["created_at"] == exe_date) \
        .join(orderDetailDf, ordersDf["product_id"] == orderDetailDf["order_id"],"inner") \
        .join(productsDf,ordersDf["product_id"] == productsDf["id"],"inner") \
        .join(inventoryDf.select(col("quantity"). \
        alias("inv_quantity"),col("id")),productsDf["inventory_id"] \
        == inventoryDf["id"],"inner")


mapDf = preDF.groupBy("Make","Model","Category","product_id","inv_quantity") \
                .agg(sum("quantity").alias("Sales"),sum("total").alias("Revenue"))

resultDf = mapDf.withColumn("LeftOver",col("inv_quantity") - col("Sales")) \
                .withColumn("year",lit(year)) \
                .withColumn("month",lit(month)) \
                .withColumn("day",lit(day)) \
                .select("Make","Model","Category","Sales","Revenue","LeftOver","year","month","day").limit(20);

spark.sql("CREATE DATABASE IF NOT EXISTS reports")

resultDf.write \
        .format("hive") \
        .partitionBy("year","month","day") \
        .mode("append") \
        .saveAsTable("reports.daily_gross_revenue")

print("----------------------------DONE!!-----------------------------------")
tblLocation = "hdfs://master:9000/errors/order_error"

df_null.drop("id").write. \
	mode("append") \
	.parquet(tblLocation)


#resultDf.write \
#				.format("hive") \
#				.partitionBy("year","month","day") \
#				.mode("append") \
#				.parquet(tblLocation)
