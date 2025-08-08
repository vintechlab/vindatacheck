from .loader import MySQLLoader
from .metadata import MySQLTableMetadata
from .checksum import MySQLChecksum

from ..registry import register_engine

register_engine(
    "mysql",
    loader=MySQLLoader,
    checksum=MySQLChecksum,
    metadata=MySQLTableMetadata,
)
