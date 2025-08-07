from dataclasses import dataclass
from pyspark.sql import DataFrame
from datacheck.core.metadata import BaseTableMetadata
from ..registry import register_metadata


METADATA_QUERY_TEMPLATE = """
    SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY = 'PRI' AS IS_PRIMARY_KEY
    FROM information_schema.COLUMNS
    WHERE table_schema = '{table_schema}'
        AND table_name = '{table_name}'
    ORDER BY COLUMN_NAME
"""


@register_metadata("mysql")
@dataclass
class TableMetadata(BaseTableMetadata):
    def update_properties(self) -> None:
        self.data_source = self.config.type
        self.table_schema = self.config.database
        self.table_name = self.config.table_name

    def update_metadata(self) -> DataFrame:
        query = METADATA_QUERY_TEMPLATE.format(
            table_schema=self.table_schema,
            table_name=self.table_name,
        )
        self.metadata = self.loader.load(query)
