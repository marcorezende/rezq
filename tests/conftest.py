import shutil

import pytest
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[2]")
            .appName("local-pytest")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.fs.s3a.fast.upload", "true")
            .config("spark.fs.s3a.fast.upload.buffer", "bytebuffer")
            .config("spark.databricks.delta.enableFastS3AListFrom", "true")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
            .enableHiveSupport()
            .getOrCreate())


@pytest.fixture(scope="session")
def mock_delta_table(spark):
    location = "./tmp/delta_mock"

    df = spark.createDataFrame(data=[("Marco", 23), ("Maiara", 29), ("Mario", 24), ("Vinicius", 25), ("Pedro", 21)],
                               schema="name STRING, age INT")
    df.write.format("delta").mode('overwrite').save(location)
    delta_table = DeltaTable.forPath(spark, location)
    yield delta_table
    shutil.rmtree(location)


@pytest.fixture(scope="session")
def mock_delta_table_with_name_constraint(spark):
    location = f"./tmp/delta_mock_with_name_constraint"

    df = spark.createDataFrame(data=[("Marco", 23), ("Maiara", 29), ("Mario", 24), ("Vinicius", 25), ("Pedro", 21)],
                               schema="name STRING, age INT")
    df.write.format("delta").mode('overwrite').save(location)
    delta_table = DeltaTable.forPath(spark, location)
    query = f"""
        ALTER TABLE delta.`{delta_table.detail().select("location").take(1)[0][0]}`
        ADD CONSTRAINT name_is_not_null CHECK (name is not null)
    """
    spark.sql(query)
    yield delta_table
    shutil.rmtree(location)


@pytest.fixture(scope="session")
def mock_delta_table_with_age_constraint(spark):
    location = f"./tmp/delta_mock_with_age_constraint"

    df = spark.createDataFrame(data=[("Marco", 23), ("Maiara", 29), ("Mario", 24), ("Vinicius", 25), ("Pedro", 21)],
                               schema="name STRING, age INT")
    df.write.format("delta").mode('overwrite').save(location)
    delta_table = DeltaTable.forPath(spark, location)
    query = f"""
        ALTER TABLE delta.`{delta_table.detail().select("location").take(1)[0][0]}`
        ADD CONSTRAINT age_higher_than_20 CHECK (age > 20)
    """
    spark.sql(query)
    yield delta_table
    shutil.rmtree(location)


@pytest.fixture(scope="session")
def mock_delta_table_with_name_and_age_constraint(spark):
    location = "./tmp/delta_mock_with_name_and_age_constraint"

    df = spark.createDataFrame(data=[("Marco", 23), ("Maiara", 29), ("Mario", 24), ("Vinicius", 25), ("Pedro", 21)],
                               schema="name STRING, age INT")
    df.write.format("delta").mode('overwrite').save(location)
    delta_table = DeltaTable.forPath(spark, location)
    query = f"""
        ALTER TABLE delta.`{delta_table.detail().select("location").take(1)[0][0]}`
        ADD CONSTRAINT name_is_not_null CHECK (name is not null)
    """
    spark.sql(query)
    query = f"""
        ALTER TABLE delta.`{delta_table.detail().select("location").take(1)[0][0]}`
        ADD CONSTRAINT age_higher_than_20 CHECK (age > 20)
    """
    spark.sql(query)
    yield delta_table
    shutil.rmtree(location)


@pytest.fixture(scope="session")
def df_with_problems(spark):
    return spark.createDataFrame(
        data=[('Monica', 30), ('Jonatas', 24), ('Franksuel', 15)],
        schema='name STRING, age INT').withColumn('name',
                                                  when(col('name') == 'Jonatas', lit(None)).otherwise(col('name')))
