import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession
from pyspark import SparkConf
from google.protobuf.duration_pb2 import Duration
from pathlib import Path
from feast import FeatureStore
from feast.repo_config import load_repo_config, RepoConfig
from feast import Entity, Feature, FeatureView, ValueType
from yummy import ParquetDataSource, CsvDataSource, DeltaDataSource, IcebergDataSource


entity = Entity(name="entity_id", value_type=ValueType.INT64, description="entity id",)

features1 = [
    Feature(name="f0", dtype=ValueType.FLOAT),
    Feature(name="f1", dtype=ValueType.FLOAT),
    Feature(name="f2", dtype=ValueType.FLOAT),
    Feature(name="f3", dtype=ValueType.FLOAT),
    Feature(name="f4", dtype=ValueType.FLOAT),
    #Feature(name="y", dtype=ValueType.FLOAT),
]

features2 = [
    Feature(name="f5", dtype=ValueType.FLOAT),
    Feature(name="f6", dtype=ValueType.FLOAT),
    Feature(name="f7", dtype=ValueType.FLOAT),
    Feature(name="f8", dtype=ValueType.FLOAT),
    Feature(name="f9", dtype=ValueType.FLOAT),
    Feature(name="y", dtype=ValueType.FLOAT),
]

fv1 = FeatureView(
    name="debeziumcdc_postgres_public_mystats_fv1",
    entities=["entity_id"],
    ttl=Duration(seconds=3600*24*20),
    features=features1,
    online=True,
    input=IcebergDataSource(
            path="local.mytable_dbz.debeziumcdc_postgres_public_mystats_fv1",
            event_timestamp_column="__source_ts",
    ),
    tags={},
)

fv2 = FeatureView(
    name="debeziumcdc_postgres_public_mystats_fv2",
    entities=["entity_id"],
    ttl=Duration(seconds=3600*24*20),
    features=features2,
    online=True,
    input=IcebergDataSource(
            path="local.mytable_dbz.debeziumcdc_postgres_public_mystats_fv2",
            event_timestamp_column="__source_ts",
    ),
    tags={},
)

example_repo_path = "."
tmp_dir = "./tmp"
repo_config = load_repo_config(repo_path=Path(example_repo_path))
repo_config.registry = str(Path(tmp_dir) / "registry.db")
repo_config.online_store.path = str(Path(tmp_dir) / "online_store.db")

feature_store = FeatureStore(config=repo_config)
feature_store.repo_path = str(example_repo_path)

feature_store.config.offline_store.config["spark.master"]="local[*]"
feature_store.config.offline_store.config["spark.ui.enabled"]="false"
feature_store.config.offline_store.config["spark.eventLog.enabled"]="false"
feature_store.config.offline_store.config["spark.sql.session.timeZone"]="UTC"
feature_store.config.offline_store.config["spark.sql.extensions"]="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
feature_store.config.offline_store.config["spark.sql.catalog.local"]="org.apache.iceberg.spark.SparkCatalog"
feature_store.config.offline_store.config["spark.sql.catalog.local.type"]="hadoop"
feature_store.config.offline_store.config["spark.sql.catalog.local.warehouse"]="s3a://mybucket"
feature_store.config.offline_store.config["spark.hadoop.fs.s3a.endpoint"]="http://minio:9000"
feature_store.config.offline_store.config["spark.hadoop.fs.s3a.access.key"]="minioadmin"
feature_store.config.offline_store.config["spark.hadoop.fs.s3a.secret.key"]="minioadmin"
feature_store.config.offline_store.config["spark.hadoop.fs.s3a.impl"]="org.apache.hadoop.fs.s3a.S3AFileSystem"
feature_store.config.offline_store.config["spark.hadoop.fs.s3a.path.style.access"]="true"
feature_store.config.offline_store.config["spark.hadoop.fs.s3a.connection.ssl.enabled"]="false"

feature_store.apply([entity, fv1, fv2])

def generate_entities(size: int):
    return np.random.choice(size, size=size, replace=False)


def entity_df(size:int = 10):
    entities=generate_entities(size)
    entity_df = pd.DataFrame(data=entities, columns=['entity_id'])
    entity_df["event_timestamp"]=datetime(2022, 7, 24, 23, 59, 42, tzinfo=timezone.utc)
    return entity_df


entity_df = entity_df()

feature_vector = feature_store.get_historical_features(
    features=[
        "debeziumcdc_postgres_public_mystats_fv1:f0",
        "debeziumcdc_postgres_public_mystats_fv1:f1",
        "debeziumcdc_postgres_public_mystats_fv1:f2",
        "debeziumcdc_postgres_public_mystats_fv1:f3",
        "debeziumcdc_postgres_public_mystats_fv1:f4",
        "debeziumcdc_postgres_public_mystats_fv2:f5",
        "debeziumcdc_postgres_public_mystats_fv2:f6",
        "debeziumcdc_postgres_public_mystats_fv2:f7",
        "debeziumcdc_postgres_public_mystats_fv2:f8",
        "debeziumcdc_postgres_public_mystats_fv2:f9",
        "debeziumcdc_postgres_public_mystats_fv2:y",
    ], entity_df=entity_df, full_feature_names=True
).to_df()


print(feature_vector)
#spark = SparkSession.builder.config(conf=SparkConf().setAll(
#    [
#        ("spark.master", "local[*]"),
#        ("spark.ui.enabled", "false"),
#        ("spark.eventLog.enabled", "false"),
#        ("spark.sql.session.timeZone", "UTC"),
#        ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
#        #("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog"),
#        #("spark.sql.catalog.spark_catalog.type","hive"),
#        ("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog"),
#        ("spark.sql.catalog.local.type","hadoop"),
#        ("spark.sql.catalog.local.warehouse","s3a://mybucket"),
#        ("spark.hadoop.fs.s3a.endpoint","http://minio:9000"),
#        ("spark.hadoop.fs.s3a.access.key","minioadmin"),
#        ("spark.hadoop.fs.s3a.secret.key","minioadmin"),
#        ("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
#        ("spark.hadoop.fs.s3a.path.style.access","true"),
#        ("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"),
#    ]
#)).getOrCreate()
#
##dd = spark.sql("select * from local.mytable_dbz.debeziumcdc_postgres_public_dbz_test").collect()
#dd1 = spark.read.format('iceberg').load("local.mytable_dbz.debeziumcdc_postgres_public_mystats_fv1").toPandas()
#dd2 = spark.read.format('iceberg').load("local.mytable_dbz.debeziumcdc_postgres_public_mystats_fv2").toPandas()
#
#print(dd1)
#print(dd2)
