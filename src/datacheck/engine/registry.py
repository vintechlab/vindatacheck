from typing import Callable, Dict

REGISTRIES: Dict[str, Dict[str, Callable]] = {
    "loader": {},
    "metadata": {},
    "checksum": {},
}


def create_register_function(registry_name: str):
    def decorator(name: str):
        def wrapper(cls: Callable):
            REGISTRIES[registry_name][name] = cls
            return cls

        return wrapper

    return decorator


def create_get_function(registry_name: str):
    def getter(name: str, *args, **kwargs):
        return REGISTRIES[registry_name][name](*args, **kwargs)

    return getter


register_loader = create_register_function("loader")
register_metadata = create_register_function("metadata")
register_checksum = create_register_function("checksum")

get_loader = create_get_function("loader")
get_metadata = create_get_function("metadata")
get_checksum = create_get_function("checksum")
