from .loader import BigQueryLoader
from .metadata import BigQueryTableMetadata
from .checksum import BigQueryChecksum

from ..registry import engine_registry

engine_registry.register(
    "bigquery",
    loader=BigQueryLoader,
    checksum=BigQueryChecksum,
    metadata=BigQueryTableMetadata,
)
