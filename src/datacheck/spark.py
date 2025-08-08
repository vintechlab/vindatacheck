from pyspark.sql import SparkSession


def get_spark_session(app_name: str):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .config("spark.sql.sources.bucketing.enabled", "true")
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.sql.parquet.writeLegacyFormat", "true")
        .getOrCreate()
    )
