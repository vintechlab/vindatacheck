from dataclasses import dataclass
from pyspark.sql import DataFrame
from datacheck.base.checksum import BaseChecksum
from datacheck.utils.common import gen_prefixes


MIN_MAX_VALUES_QUERY_TEMPLATE = """
    SELECT
        MIN({primary_key}) AS min_value,
        MAX({primary_key}) AS max_value
    FROM {table_schema}.{table_name}
"""


@dataclass
class MySQLChecksum(BaseChecksum):
    def get_min_max_values(self) -> tuple[str, str]:
        query = MIN_MAX_VALUES_QUERY_TEMPLATE.format(
            primary_key=self.metadata.primary_key,
            table_schema=self.metadata.table_schema,
            table_name=self.metadata.table_name,
        )
        rows = self.loader.load(query).collect()
        return rows[0].min_value, rows[0].max_value

    def build_checksum_expression(self, casted_columns: list[str]) -> str:
        return f"MD5(CONCAT_WS('|', {', '.join(casted_columns)}))"

    def load(self) -> DataFrame:
        match self.metadata.primary_key_data_type.lower():
            case "char" | "varchar":
                prefixes = gen_prefixes(length=3, use_uppercase=False)
                predicates = [f"{self.metadata.primary_key} LIKE '{prefix}%'" for prefix in prefixes]
                return self.loader.load_with_predicates(
                    query=self.build_checksum_query(),
                    predicates=predicates,
                    num_partitions=self.config.num_partitions,
                    fetch_size=self.config.fetch_size,
                )
            case (
                "tinyint"
                | "smallint"
                | "mediumint"
                | "int"
                | "bigint"
                | "float"
                | "double"
                | "decimal"
                | "numeric"
                | "date"
                | "datetime"
                | "timestamp"
            ):
                min_val, max_val = self.get_min_max_values()
                return self.loader.load_with_min_max_values(
                    query=self.build_checksum_query(),
                    partition_column=self.metadata.primary_key,
                    min_value=min_val,
                    max_value=max_val,
                    num_partitions=self.config.num_partitions,
                    fetch_size=self.config.fetch_size,
                )
            case _:
                raise ValueError(f"Unsupported data type: {self.metadata.primary_key_data_type}")
