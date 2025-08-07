from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Callable
from pyspark.sql import DataFrame
from datacheck.config import DataSourceConfig

from .loader import BaseLoader
from .metadata import BaseTableMetadata


@dataclass
class BaseChecksum(ABC):
    loader: BaseLoader
    config: DataSourceConfig
    metadata: BaseTableMetadata
    cast_column: Callable[[str, str], str]

    @abstractmethod
    def build_checksum_expression(self, columns):
        pass

    @property
    def columns(self) -> list[tuple[str, str]]:
        return self.metadata.metadata.select("COLUMN_NAME", "DATA_TYPE").collect()

    @property
    def filtered_columns(self) -> list[tuple[str, str]]:
        ignore_columns = self.config.ignore_columns.split(",")
        ignore_data_types = self.config.ignore_data_types.split(",")

        return [
            column
            for column in self.columns
            if column["COLUMN_NAME"] not in ignore_columns and column["DATA_TYPE"].lower() not in ignore_data_types
        ]

    @property
    def extra_columns(self) -> list[str]:
        extra_columns = self.config.extra_columns.split(",")
        extra_columns = [
            column_name
            for column_name in extra_columns
            if column_name in [column.COLUMN_NAME for column in self.columns]
        ]

        return extra_columns

    def build_checksum_query(self):
        table_schema = self.metadata.table_schema
        table_name = self.metadata.table_name
        filter_data = self.config.filter_data

        cast_columns = [self.cast_column(column_name, data_type) for column_name, data_type in self.filtered_columns]
        checksum_expression = self.build_checksum_expression(cast_columns)

        return f"""
            SELECT
                {self.metadata.primary_key},
                {", ".join(self.extra_columns) + "," if self.extra_columns else ""}
                {checksum_expression} AS checksum
            FROM {table_schema}.{table_name}
            {f"WHERE {filter_data}" if filter_data else ""}
        """

    def load(self) -> DataFrame:
        return self.loader.load(self.build_checksum_query())
