from dataclasses import dataclass
from pyspark.sql import DataFrame

from datacheck.core.loader import BaseLoader

# from datacheck.utils.common import gen_prefixes
from ..registry import register_loader


NUM_PARTITIONS = 1
FETCH_SIZE = 10000


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

    def load_with_predicates(
        self,
        query: str,
        predicates: list[str],
        num_partitions: int = NUM_PARTITIONS,
        fetch_size: int = FETCH_SIZE,
    ) -> DataFrame:
        return (
            self.spark.read.option("numPartitions", num_partitions)
            .option("fetchSize", fetch_size)
            .jdbc(
                url=self.config.url,
                table=f"({query}) as tmp",
                properties=self.spark_options,
                predicates=predicates,
            )
        )

    def load_with_min_max_values(
        self,
        query: str,
        partition_column: str,
        max_value: str,
        min_value: str,
        num_partitions: int = NUM_PARTITIONS,
        fetch_size: int = FETCH_SIZE,
    ) -> DataFrame:
        return (
            self.spark.read.format("jdbc")
            .options(
                **self.spark_options,
                dbtable=f"({query}) as tmp",
                partitionColumn=partition_column,
                numPartitions=num_partitions,
                fetchSize=fetch_size,
                lowerBound=min_value,
                upperBound=max_value,
            )
            .load()
        )

    def load(self, query: str) -> DataFrame:
        return (
            self.spark.read.format("jdbc")
            .options(
                **self.spark_options,
                query=query,
            )
            .load()
        )
