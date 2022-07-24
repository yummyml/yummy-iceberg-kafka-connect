#!/bin/bash

export FEAST_USAGE=False
export PYSPARK_PYTHON=/opt/conda/bin/python3 
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --notebook-dir=/home/jovyan --ip='0.0.0.0' --port=8888 --no-browser --allow-root --NotebookApp.password='' --NotebookApp.token=''"

#pip3 install rise

pyspark \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2,org.apache.hadoop:hadoop-aws:3.3.1,software.amazon.awssdk:s3:2.17.131 \
    --conf "spark.driver.memory=5g" \
    --conf "spark.executor.memory=5g" \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.local.type=hadoop" \
    --conf "spark.sql.catalog.local.warehouse=s3a://mybucket" \
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
    --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
    --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false"

