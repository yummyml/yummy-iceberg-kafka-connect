from pyspark.sql import SparkSession
from pyspark import SparkConf

spark = SparkSession.builder.config(conf=SparkConf().setAll(
    [
        ("spark.master", "local[*]"),
        ("spark.ui.enabled", "false"),
        ("spark.eventLog.enabled", "false"),
        ("spark.sql.session.timeZone", "UTC"),
        ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        #("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog"),
        #("spark.sql.catalog.spark_catalog.type","hive"),
        ("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.catalog.local.type","hadoop"),
        ("spark.sql.catalog.local.warehouse","s3a://mybucket"),
        ("spark.hadoop.fs.s3a.endpoint","http://minio:9000"),
        ("spark.hadoop.fs.s3a.access.key","minioadmin"),
        ("spark.hadoop.fs.s3a.secret.key","minioadmin"),
        ("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
        ("spark.hadoop.fs.s3a.path.style.access","true"),
        ("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"),
    ]
)).getOrCreate()

#dd = spark.sql("select * from local.mytable_dbz.debeziumcdc_postgres_public_dbz_test").collect()
dd1 = spark.read.format('iceberg').load("local.mytable_dbz.debeziumcdc_postgres_public_mystats_fv1").toPandas()
dd2 = spark.read.format('iceberg').load("local.mytable_dbz.debeziumcdc_postgres_public_mystats_fv2").toPandas()

print(dd1)
print(dd2)
print(dd1.dtypes)


dd = spark.sql("describe table local.mytable_dbz.debeziumcdc_postgres_public_mystats_fv1").collect()
print(dd)