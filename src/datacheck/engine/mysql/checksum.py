from dataclasses import dataclass
from datacheck.core.checksum import BaseChecksum

from ..registry import register_checksum


@register_checksum("mysql")
@dataclass
class Checksum(BaseChecksum):
    def build_checksum_expression(self, columns: list[str]) -> str:
        return f"MD5(CONCAT_WS('|', {', '.join(columns)}))"
