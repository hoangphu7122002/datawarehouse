import pyspark
from pyspark.sql import SparkSession

#create spark session
spark = SparkSession.builder \
										.master('local[*]') \
										.appName("read parquet from HDFS") \
										.getOrCreate()

def print_table(table,link):
		tblLocation = f"hdfs://master:9000/datalake/{table}/year=2023/month=04/day=06/{link}"
		df = spark.read.parquet(tblLocation)

		print("!!!!!!!!!!SCHEMAE!!!!!!!!!!")
		df.printSchema()
		print("!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		df.show(5)

print_table('order_detail','part-00000-84284ba7-0325-446c-987f-1f0da2160d65.c000.snappy.parquet')
print_table('orders','part-00000-6cbb8b62-970d-4f7e-b3f1-e855dd46ac68.c000.snappy.parquet')
print_table('inventory','part-00000-e3182c8d-5637-4775-bf1b-ec315c8d91ea.c000.snappy.parquet')
print_table('product','part-00000-f03971e5-3618-4d7e-927a-a8689b5e4c1e.c000.snappy.parquet')