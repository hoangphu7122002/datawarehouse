import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
#from pyspark import SparkContext

parser = argparse.ArgumentParser()
parser.add_argument("--table_name")
parser.add_argument("--exe_date")
args = parser.parse_args()

#get parameter
table_name = ""
exe_date = ""
if args.table_name:
	table_name = args.table_name
if args.exe_date:
	exe_date = args.exe_date

runTime = exe_date.split("-")
year = runTime[0]
month = runTime[1]
day = runTime[2]

#create spark session
spark = SparkSession.builder \
		.master('local[*]') \
		.appName("ETL from MYSQL to HIVE") \
		.getOrCreate()

#get the lastest record_id in datalake
sc = spark.sparkContext
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
exists = fs.exists(sc._jvm.org.apache.hadoop.fs.Path(f"/datalake/{table_name}"))
tblLocation = f"hdfs://master:9000/datalake/{table_name}"
tblQuery = ""
if exists:
	df = spark.read.parquet(tblLocation)
	record_id = df.agg(max("id")).head().getLong(0)
	tblQuery = f"(SELECT * FROM {table_name} WHERE id > {record_id}) AS tmp"
else:
	tblQuery = f"(SELECT * FROM {table_name}) AS tmp"

print("!!!!!!!!!!!!!!!!")
print(tblQuery)
print("!!!!!!!!!!!!!!!!")
#get latest records from mysql
jdbcDF = spark.read.format("jdbc") \
	 .option("url","jdbc:mysql://localhost:3306/test_demo") \
	 .option("user","root") \
	 .option("password","071202") \
				 .option("dbtable",tblQuery) \
				 .option("driver",'com.mysql.jdbc.Driver') \
				 .load()

#save to data lake
outputDF = jdbcDF.withColumn("year",lit(year)) \
				 .withColumn("month",lit(month)) \
				 .withColumn("day",lit(day))
outputDF. \
		write. \
		partitionBy("year","month","day"). \
		mode("append") \
		.parquet(tblLocation)