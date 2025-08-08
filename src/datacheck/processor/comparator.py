from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when

from datacheck.base.checksum import BaseChecksum
from datacheck.base.metadata import BaseTableMetadata
from datacheck.engine import get_loader, get_metadata, get_checksum

from .typecaster.factory import TypeCasterFactory


def load_metadata(spark: SparkSession, config: any) -> BaseTableMetadata:
    loader = get_loader(config.type, spark, config)
    return get_metadata(config.type, loader, config)


def load_checksum(
    spark: SparkSession,
    config: any,
    metadata: BaseTableMetadata,
    cast_column_hook: Callable[[str, str], str],
) -> BaseChecksum:
    loader = get_loader(config.type, spark, config)
    checksum = get_checksum(config.type, loader, config, metadata, cast_column_hook)
    return checksum.load()


def check_mismatches(df_source: DataFrame, df_target: DataFrame, primary_key: str) -> DataFrame:
    mismatches = (
        df_source.join(df_target, df_source[primary_key] == df_target[primary_key], "outer")
        .withColumn(
            "checksum_match",
            when(df_source["checksum"] == df_target["checksum"], "MATCH")
            .when(df_source["checksum"].isNull() | df_target["checksum"].isNull(), "MISSING_DATA")
            .otherwise("MISMATCH"),
        )
        .filter((col("checksum_match") != "MATCH"))
    )
    return mismatches


def compare(spark: SparkSession, source_config: any, target_config: any) -> None:
    metadata = load_metadata(spark, source_config)
    string_caster = TypeCasterFactory.get_string_caster(source_config.type, target_config.type)

    source_checksum_data = load_checksum(spark, source_config, metadata, string_caster.cast_source)
    target_checksum_data = load_checksum(spark, target_config, metadata, string_caster.cast_target)

    mismatches = check_mismatches(source_checksum_data, target_checksum_data, metadata.primary_key)
    mismatches.show(truncate=False)

    return mismatches
