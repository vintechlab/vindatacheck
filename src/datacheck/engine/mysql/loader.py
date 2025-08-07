import os
from dataclasses import dataclass
from pyspark.sql import DataFrame

from datacheck.core.loader import BaseLoader
from ..registry import register_loader


@register_loader("mysql")
@dataclass
class Loader(BaseLoader):
    name = "mysql"

    @property
    def spark_confs(self):
        return {}

    @property
    def spark_options(self):
        return dict(
            driver="com.mysql.cj.jdbc.Driver",
            url=self.config.url,
            user=self.config.mysql_user,
            password=self.config.mysql_password,
        )

    def load(self, query: str) -> DataFrame:
        return self.spark.read.format("jdbc").options(**self.spark_options).option("query", query).load()
