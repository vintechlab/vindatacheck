from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from datacheck.config import DataSourceConfig

from .loader import BaseLoader


@dataclass
class BaseTableMetadata(ABC):
    loader: BaseLoader
    config: DataSourceConfig

    data_source: str = field(init=False)
    metadata: DataFrame = field(init=False)
    table_schema: str = field(init=False)
    table_name: str = field(init=False)

    def __post_init__(self):
        self.update_properties()
        self.update_metadata()
        if self.metadata.filter("IS_PRIMARY_KEY = 1").count() == 0:
            raise ValueError(f"No primary key found for table {self.table_name}")

    @abstractmethod
    def update_properties(self) -> None:
        pass

    @abstractmethod
    def update_metadata(self) -> DataFrame:
        pass

    @property
    def primary_key(self) -> str:
        return self.metadata.filter("IS_PRIMARY_KEY = 1").collect()[0]["COLUMN_NAME"]

    @property
    def primary_key_data_type(self) -> str:
        return self.metadata.filter("IS_PRIMARY_KEY = 1").collect()[0]["DATA_TYPE"]

    @property
    def column_names(self) -> list[str]:
        return [
            row["COLUMN_NAME"] for row in self.metadata.select("COLUMN_NAME").filter("IS_PRIMARY_KEY = 0").collect()
        ]

    @property
    def created_at_column(self) -> str | None:
        if self.config.created_at_column in self.column_names:
            return self.config.created_at_column

    @property
    def updated_at_column(self) -> str | None:
        if self.config.updated_at_column in self.column_names:
            return self.config.updated_at_column
