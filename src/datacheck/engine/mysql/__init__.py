from .loader import MySQLLoader
from .metadata import MySQLTableMetadata
from .checksum import MySQLChecksum

from ..registry import engine_registry

engine_registry.register(
    "mysql",
    loader=MySQLLoader,
    checksum=MySQLChecksum,
    metadata=MySQLTableMetadata,
)
