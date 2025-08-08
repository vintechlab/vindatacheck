from .stringcaster import MySQLToBigQueryStringCast
from .base import BaseStringCast


class TypeCasterFactory:
    @staticmethod
    def get_string_caster(source_type: str, target_type: str) -> BaseStringCast:
        match source_type.lower(), target_type.lower():
            case "mysql", "bigquery":
                return MySQLToBigQueryStringCast()
            case _:
                raise ValueError(f"No string caster found for {source_type} to {target_type}")
