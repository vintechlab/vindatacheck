from dataclasses import dataclass
from pyspark.sql import DataFrame
from datacheck.base.checksum import BaseChecksum


@dataclass
class BigQueryChecksum(BaseChecksum):
    def build_checksum_expression(self, casted_columns: list[str]) -> str:
        return f"TO_HEX(MD5(ARRAY_TO_STRING([{', '.join(casted_columns)}], '|')))"

    def load(self) -> DataFrame:
        return self.loader.load(self.build_checksum_query())
