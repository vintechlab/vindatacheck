from typing import TYPE_CHECKING, Callable
from datacheck.utils.common import register_import_modules
from .registry import engine_registry

if TYPE_CHECKING:
    from datacheck.base.loader import BaseLoader
    from datacheck.base.checksum import BaseChecksum
    from datacheck.base.metadata import BaseTableMetadata

register_import_modules(__name__, __path__)

get_loader: Callable[..., "BaseLoader"] = engine_registry.create_getter("loader")
get_checksum: Callable[..., "BaseChecksum"] = engine_registry.create_getter("checksum")
get_metadata: Callable[..., "BaseTableMetadata"] = engine_registry.create_getter("metadata")

__all__ = ["get_loader", "get_checksum", "get_metadata"]
