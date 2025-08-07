from dataclasses import dataclass
from pyspark.sql import DataFrame
from datacheck.core.metadata import BaseTableMetadata
from ..registry import register_metadata


METADATA_QUERY_TEMPLATE = """
    SELECT t1.COLUMN_NAME, t1.DATA_TYPE, t2.COLUMN_NAME IS NOT NULL AS IS_PRIMARY_KEY
    FROM `{dataset}.INFORMATION_SCHEMA.COLUMNS` t1
    LEFT JOIN `{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` t2
        USING(TABLE_NAME, COLUMN_NAME)
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY t1.COLUMN_NAME
"""


@register_metadata("bigquery")
@dataclass
class TableMetadata(BaseTableMetadata):
    def update_properties(self) -> None:
        self.data_source = self.config.type
        self.table_schema = self.config.database
        self.table_name = self.config.table_name

    def update_metadata(self) -> DataFrame:
        query = METADATA_QUERY_TEMPLATE.format(
            dataset=self.dataset,
            table_name=self.table_name,
        )
        self.metadata = self.loader.load(query)
