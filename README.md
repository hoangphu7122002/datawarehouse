### instruction
#### spark-submit ingest
```shell
spark-submit --packages mysql:mysql-connector-java:8.0.32 project_warehouse/ingest.py --table_name orders --exe_date 2023-04-10
```

#### spark-sumbit etl
```shell
spark-submit project_warehouse/etl.py --exe_date 2023-04-10
```

#### start hiveserver and beeline
```shell
- create hive in hadoop:
hdfs dfs -mkdir /tmp
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/hive
hdfs dfs -mkdir /user/hive/warehouse
hadoop fs -chmod g+w /tmp
hadoop fs -chmod /user/hive/warehouse
- start server
$HIVE_HOME/bin/hiveserver2

- init derby
$HIVE_HOME/bin/schematool -dbType derby -initSchema

- start metastore server
$HIVE_HOME/bin/hive --service metastore

- start beeline
$HIVE_HOME/bin/beeline -n hadoop_user -u jdbc:hive2://master:10000
```

#### start apache superset
```shell
export FLASK_APP=superset
export SUPERSET_CONFIG_PATH=/home/hadoop_user/superset_config.py
superset run -p 8089 --with-threads --reload --debugger

#sqlachemy
hive://hive@master:10000/reports
```
