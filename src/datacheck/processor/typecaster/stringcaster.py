from .base import BaseStringCast


class MySQLToBigQueryStringCast(BaseStringCast):
    def cast_source(self, column_name: str, data_type: str) -> str:
        match data_type.lower():
            case "datetime" | "timestamp":
                return f"DATE_FORMAT({column_name}, '%Y-%m-%d %H:%i:%s.%f')"
            case "date":
                return f"DATE_FORMAT({column_name}, '%Y-%m-%d')"
            case "time":
                return f"DATE_FORMAT({column_name}, '%H:%i:%s.%f')"
            case "decimal" | "numeric" | "float" | "double" | "real":
                return f"ROUND({column_name}, 3)"
            case "char" | "varchar":
                return column_name
            case _:
                return f"CAST({column_name} AS CHAR)"

    def cast_target(self, column_name: str, data_type: str) -> str:
        match data_type.lower():
            case "datetime" | "timestamp":
                return f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', {column_name})"
            case "date":
                return f"FORMAT_DATE('%Y-%m-%d', {column_name})"
            case "time":
                return f"FORMAT_TIME('%H:%M:%E6S', {column_name})"
            case "decimal" | "numeric" | "float" | "double" | "real":
                return f"FORMAT('%.3f', {column_name})"
            case "string":
                return column_name
            case _:
                return f"CAST({column_name} AS STRING)"
