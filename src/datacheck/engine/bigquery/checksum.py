from dataclasses import dataclass
from pyspark.sql import DataFrame
from datacheck.core.checksum import BaseChecksum

from ..registry import register_checksum


@register_checksum("bigquery")
@dataclass
class Checksum(BaseChecksum):
    def build_checksum_expression(self, casted_columns: list[str]) -> str:
        return f"TO_HEX(MD5(ARRAY_TO_STRING([{', '.join(casted_columns)}], '|')))"

    def load(self) -> DataFrame:
        return self.loader.load(self.build_checksum_query())
