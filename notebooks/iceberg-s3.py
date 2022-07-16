from typing import Callable, List, Optional, Tuple, Union, Dict, Any
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from sklearn.datasets import make_hastie_10_2
from enum import Enum
from google.protobuf.duration_pb2 import Duration
from feast import Entity, Feature, FeatureView, ValueType
from yummy import ParquetDataSource, CsvDataSource, DeltaDataSource


class DataType(str, Enum):
    csv = "csv"
    parquet = "parquet"
    delta = "delta"

class Generator(ABC):

    @staticmethod
    def entity() -> Entity:
        return Entity(name="entity_id", value_type=ValueType.INT64, description="entity id",)

    @staticmethod
    def generate_entities(size: int):
        return np.random.choice(size, size=size, replace=False)

    @staticmethod
    def entity_df(size:int = 10):
        entities=Generator.generate_entities(size)
        entity_df = pd.DataFrame(data=entities, columns=['entity_id'])
        entity_df["event_timestamp"]=datetime(2021, 10, 1, 23, 59, 42, tzinfo=timezone.utc)
        return entity_df

    @staticmethod
    def generate_data(entities, year=2021, month=10, day=1) -> pd.DataFrame:
        n_samples=len(entities)
        X, y = make_hastie_10_2(n_samples=n_samples, random_state=0)
        df = pd.DataFrame(X, columns=["f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9"])
        df["y"]=y
        df['entity_id'] = entities
        df['datetime'] = pd.to_datetime(
                np.random.randint(
                    datetime(year, month, day, 0,tzinfo=timezone.utc).timestamp(),
                    datetime(year, month, day, 22,tzinfo=timezone.utc).timestamp(),
                    size=n_samples),
            unit="s", #utc=True
        )
        df['created'] = pd.to_datetime(
                datetime.now(), #utc=True
                )
        df['month_year'] = pd.to_datetime(datetime(year, month, day, 0, tzinfo=timezone.utc), utc=True)
        return df

    @property
    def data_type(self) -> DataType:
        raise NotImplementedError("Data type not defined")

    @property
    def features(self):
        return [
            Feature(name="f0", dtype=ValueType.FLOAT),
            Feature(name="f1", dtype=ValueType.FLOAT),
            Feature(name="f2", dtype=ValueType.FLOAT),
            Feature(name="f3", dtype=ValueType.FLOAT),
            Feature(name="f4", dtype=ValueType.FLOAT),
            Feature(name="f5", dtype=ValueType.FLOAT),
            Feature(name="f6", dtype=ValueType.FLOAT),
            Feature(name="f7", dtype=ValueType.FLOAT),
            Feature(name="f8", dtype=ValueType.FLOAT),
            Feature(name="f9", dtype=ValueType.FLOAT),
            Feature(name="y", dtype=ValueType.FLOAT),
        ]

    def generate(self, path: str, size: int = 10, year: int = 2021, month: int = 10, day: int = 1) -> Tuple[FeatureView, str]:
        entities = Generator.generate_entities(size)
        df = Generator.generate_data(entities, year, month, day)
        self.write_data(df, path)
        return self.prepare_features(path)

    @abstractmethod
    def write_data(self, df: pd.DataFrame, path: str):
        ...

    @abstractmethod
    def prepare_source(self, path: str):
        ...

    def prepare_features(self, path: str) -> Tuple[FeatureView, str]:
        source = self.prepare_source(path)
        name = f"fv_{self.data_type}"
        return FeatureView(
            name=name,
            entities=["entity_id"],
            ttl=Duration(seconds=3600*24*20),
            features=self.features,
            online=True,
            input=source,
            tags={},), name


class CsvGenerator(Generator):

    @property
    def data_type(self) -> DataType:
        return DataType.csv

    def write_data(self, df: pd.DataFrame, path: str):
        df.to_csv(path)

    def prepare_source(self, path: str):
        return CsvDataSource(
            path=path,
            event_timestamp_column="datetime",
        )

class ParquetGenerator(Generator):

    @property
    def data_type(self) -> DataType:
        return DataType.parquet

    def write_data(self, df: pd.DataFrame, path: str):
        df.to_parquet(path)

    def prepare_source(self, path: str):
        return ParquetDataSource(
            path=path,
            event_timestamp_column="datetime",
        )

class DeltaGenerator(Generator):

    @property
    def data_type(self) -> DataType:
        return DataType.delta

    def write_data(self, df: pd.DataFrame, path: str):
        from pyspark.sql import SparkSession
        from pyspark import SparkConf

        spark = SparkSession.builder.config(conf=SparkConf().setAll(
            [
                ("spark.master", "local[*]"),
                ("spark.ui.enabled", "false"),
                ("spark.eventLog.enabled", "false"),
                ("spark.sql.session.timeZone", "UTC"),
                ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
                ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            ]
        )).getOrCreate()

        spark.createDataFrame(df).write.format("delta").mode("append").save(path)

    def prepare_source(self, path: str):
        return DeltaDataSource(
            path=path,
            event_timestamp_column="datetime",
        )



class IcebergGenerator(Generator):

    @property
    def data_type(self) -> DataType:
        return DataType.delta

    def write_data(self, df: pd.DataFrame, path: str):
        import os
        from pyspark.sql import SparkSession
        from pyspark import SparkConf

        dir_name=os.path.dirname(path)
        db_name=os.path.basename(path)

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
                #("spark.sql.catalog.local.warehouse",dir_name),
                #("spark.sql.catalog.local.catalog-impl","org.apache.iceberg.hadoop.HadoopCatalog"),
                #("spark.sql.catalog.local.io-impl","org.apache.iceberg.aws.s3.S3FileIO"),
                ("spark.sql.catalog.local.warehouse","s3a://mybucket3"),
                ("spark.hadoop.fs.s3a.endpoint","http://minio:9000"),
                ("spark.hadoop.fs.s3a.access.key","minioadmin"),
                ("spark.hadoop.fs.s3a.secret.key","minioadmin"),
                ("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
                ("spark.hadoop.fs.s3a.path.style.access","true"),
                #("fs.s3a.path.style.access","true"),
                #("fs.s3a.aws.credentials.provider",'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'),
                ("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"),
            ]
        )).getOrCreate()

        spark.sql(f"CREATE TABLE local.db.table (f0 float, f1 float, f2 float, f3 float, f4 float, f5 float, f6 float, f7 float, f8 float, f9 float, y float, entity_id int, datetime date, created date, month_year date) USING iceberg")
        spark.createDataFrame(df).write.format("iceberg").mode("append").save(db_name)

    def prepare_source(self, path: str):
        return DeltaDataSource(
            path=path,
            event_timestamp_column="datetime",
        )


generator = IcebergGenerator()

generator.generate("/home/jovyan/notebooks/iceberg/warehouse/local.db.table")



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
        ("spark.sql.catalog.local.warehouse","s3a://mybucket2"),
        ("spark.hadoop.fs.s3a.endpoint","http://minio:9000"),
        ("spark.hadoop.fs.s3a.access.key","minioadmin"),
        ("spark.hadoop.fs.s3a.secret.key","minioadmin"),
        ("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
        ("spark.hadoop.fs.s3a.path.style.access","true"),
        ("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"),
    ]
)).getOrCreate()

#spark.sql("select * from local.db.table").collect()
dd = spark.read.format('iceberg').load("local.db.table").collect()
print(dd)
