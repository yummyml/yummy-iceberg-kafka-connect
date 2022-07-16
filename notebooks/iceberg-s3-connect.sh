#!/bin/bash

PYSPARK_PYTHON=/opt/conda/bin/python3 PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python3 FEAST_USAGE=False IS_TEST=True spark-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2,io.delta:delta-core_2.12:1.1.0,org.apache.hadoop:hadoop-aws:3.3.1,software.amazon.awssdk:s3:2.17.131 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.driver.memory=5g" \
    --conf "spark.executor.memory=5g" \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog" \
    --conf "spark.sql.catalog.spark_catalog.type=hive" \
    --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.local.type=hadoop" \
    --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
    --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
    --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
    ./iceberg-s3-connect.py

