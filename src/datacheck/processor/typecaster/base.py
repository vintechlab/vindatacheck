from abc import ABC, abstractmethod


class BaseStringCast(ABC):
    @abstractmethod
    def cast_source(self, column_name: str, data_type: str) -> str:
        pass

    @abstractmethod
    def cast_target(self, column_name: str, data_type: str) -> str:
        pass
