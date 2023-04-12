mport argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,sum

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

#tblLocation = "hdfs://master:9000/user/hive/warehouse/reports/daily_gross_revenue"

#resultDf.write \
#				.format("hive") \
#				.partitionBy("year","month","day") \
#				.mode("append") \
#				.parquet(tblLocation)