from .loader import BigQueryLoader
from .metadata import BigQueryTableMetadata
from .checksum import BigQueryChecksum

from ..registry import register_engine

register_engine(
    "bigquery",
    loader=BigQueryLoader,
    checksum=BigQueryChecksum,
    metadata=BigQueryTableMetadata,
)
