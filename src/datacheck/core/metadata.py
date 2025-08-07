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
    primary_key: str = field(init=False)
    table_schema: str = field(init=False)
    table_name: str = field(init=False)

    def __post_init__(self):
        self.update_properties()
        self.update_metadata()
        self.update_primary_key()

    @abstractmethod
    def update_properties(self) -> None:
        pass

    @abstractmethod
    def update_metadata(self) -> DataFrame:
        pass

    def update_primary_key(self) -> None:
        primary_key_row = self.metadata.filter("IS_PRIMARY_KEY = 1").collect()
        if len(primary_key_row) == 0:
            raise ValueError(f"No primary key found for table {self.table_name}")
        self.primary_key = primary_key_row[0]["COLUMN_NAME"]
