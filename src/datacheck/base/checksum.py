from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Callable
from pyspark.sql import DataFrame
from datacheck.config import DataSourceConfig

from .loader import BaseLoader
from .metadata import BaseTableMetadata


CHECKSUM_QUERY_TEMPLATE = """
    SELECT
        {primary_key},
        {created_at_column}
        {updated_at_column}
        {checksum_expression} AS checksum
    FROM {table_schema}.{table_name}
    {where_clause}
"""


@dataclass
class BaseChecksum(ABC):
    loader: BaseLoader
    config: DataSourceConfig
    metadata: BaseTableMetadata
    cast_column_hook: Callable[[str, str], str]

    @abstractmethod
    def build_checksum_expression(self, cast_columns: list[str]) -> str:
        pass

    @abstractmethod
    def load(self) -> DataFrame:
        pass

    @property
    def columns(self) -> list[tuple[str, str]]:
        ignore_columns = [col.strip() for col in self.config.ignore_columns.split(",") if col.strip()]
        ignore_data_types = [dt.strip() for dt in self.config.ignore_data_types.split(",") if dt.strip()]

        df = self.metadata.metadata.select("COLUMN_NAME", "DATA_TYPE")
        if ignore_columns:
            df = df.filter(~df["COLUMN_NAME"].isin(ignore_columns))
        if ignore_data_types:
            df = df.filter(~df["DATA_TYPE"].isin(ignore_data_types))
        rows = df.collect()
        return [(row.COLUMN_NAME, row.DATA_TYPE) for row in rows]

    def build_checksum_query(self):
        cast_columns = [self.cast_column_hook(column_name, data_type) for column_name, data_type in self.columns]
        checksum_expression = self.build_checksum_expression(cast_columns)

        created_at_column = f"{self.metadata.created_at_column},\n" if self.metadata.created_at_column else ""
        updated_at_column = f"{self.metadata.updated_at_column},\n" if self.metadata.updated_at_column else ""
        where_clause = f"WHERE {self.config.filter_data}" if self.config.filter_data else ""

        return CHECKSUM_QUERY_TEMPLATE.format(
            primary_key=self.metadata.primary_key,
            created_at_column=created_at_column,
            updated_at_column=updated_at_column,
            checksum_expression=checksum_expression,
            table_schema=self.metadata.table_schema,
            table_name=self.metadata.table_name,
            where_clause=where_clause,
        )
