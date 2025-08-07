from dataclasses import dataclass
from datacheck.core.checksum import BaseChecksum

from ..registry import register_checksum


@register_checksum("bigquery")
@dataclass
class Checksum(BaseChecksum):
    def build_checksum_expression(self, columns: list[str]) -> str:
        return f"TO_HEX(MD5(ARRAY_TO_STRING([{', '.join(columns)}], '|')))"
