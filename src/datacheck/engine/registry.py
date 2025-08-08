from typing import Type, Callable, Dict, Any, Set
from inspect import signature
from typing import TypedDict


class EngineConfig(TypedDict):
    loader: Type[Callable]
    checksum: Type[Callable]
    metadata: Type[Callable]


_ENGINE_REGISTRY: Dict[str, EngineConfig] = {}


def register_engine(
    name: str,
    loader: Type[Callable] = None,
    checksum: Type[Callable] = None,
    metadata: Type[Callable] = None,
    **kwargs: Any,
) -> None:
    if not name:
        raise ValueError("Engine name cannot be empty.")
    if name in _ENGINE_REGISTRY:
        raise ValueError(f"Engine '{name}' is already registered.")

    # Validate components are callable (classes or functions)
    for component_name, component in [("loader", loader), ("checksum", checksum), ("metadata", metadata)]:
        if component is not None and not (callable(component) or isinstance(component, type)):
            raise TypeError(f"Component '{component_name}' for engine '{name}' must be a callable or class.")

    _ENGINE_REGISTRY[name] = dict(
        loader=loader,
        checksum=checksum,
        metadata=metadata,
        **kwargs,
    )


def create_getter(component: str) -> Callable:
    def getter(engine_name: str, *args, **kwargs) -> Any:
        if engine_name not in _ENGINE_REGISTRY:
            raise KeyError(f"Engine '{engine_name}' not found in registry.")
        if component not in _ENGINE_REGISTRY[engine_name]:
            raise KeyError(f"Component '{component}' not found for engine '{engine_name}'.")
        component_func = _ENGINE_REGISTRY[engine_name][component]
        if component_func is None:
            raise ValueError(f"Component '{component}' is not defined for engine '{engine_name}'.")
        return component_func(*args, **kwargs)

    getter.__name__ = f"get_{component}"
    getter.__doc__ = f"Retrieve the '{component}' component for the specified engine."
    return getter


def get_engine_components() -> Set[str]:
    return set(signature(register_engine).parameters.keys()) - {"name", "kwargs"}
