from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame

from datacheck.config import DataSourceConfig


@dataclass
class BaseLoader(ABC):
    spark: SparkSession
    config: DataSourceConfig

    name: str = field(init=False)

    def __post_init__(self):
        for key, value in self.spark_confs.items():
            self.spark.conf.set(key, value)

    @abstractmethod
    def load(self, query: str) -> DataFrame:
        pass

    @property
    @abstractmethod
    def spark_confs(self) -> dict:
        pass

    @property
    @abstractmethod
    def spark_options(self) -> dict:
        pass
