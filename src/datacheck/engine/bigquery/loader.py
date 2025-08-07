from dataclasses import dataclass
from pyspark.sql import DataFrame
from datacheck.core.loader import BaseLoader
from ..registry import register_loader


@register_loader("bigquery")
@dataclass
class Loader(BaseLoader):
    name = "bigquery"

    @property
    def spark_confs(self):
        return dict(
            viewsEnabled="true",
            project=self.config.project,
            materializationProject=self.config.materializationProject,
            materializationDataset=self.config.materializationDataset,
        )

    @property
    def spark_options(self):
        return dict(
            parentProject=self.config.project,
        )

    def load(self, query: str) -> DataFrame:
        return self.spark.read.format("bigquery").options(**self.spark_options).option("query", query).load()
