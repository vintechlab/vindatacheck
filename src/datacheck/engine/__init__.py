from datacheck.utils.common import register_import_modules
from .registry import create_getter, get_engine_components


register_import_modules(__name__, __path__)

_getters = {f"get_{component}": create_getter(component) for component in get_engine_components()}
globals().update(_getters)

__all__ = list(_getters.keys())
