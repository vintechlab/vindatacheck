from dataclasses import dataclass, field
from typing import Callable, Dict, Any


@dataclass
class EngineRegistry:
    _registry: Dict[str, Dict[str, Callable[..., Any]]] = field(default_factory=dict)

    def register(
        self,
        name: str,
        loader: Callable[..., Any] = None,
        checksum: Callable[..., Any] = None,
        metadata: Callable[..., Any] = None,
        **kwargs: Any,
    ) -> None:
        if not name:
            raise ValueError("Engine name cannot be empty.")
        if name in self._registry:
            raise ValueError(f"Engine '{name}' is already registered.")

        self._registry[name] = dict(
            loader=loader,
            checksum=checksum,
            metadata=metadata,
            **kwargs,
        )

    def create_getter(self, component: str) -> Callable[..., Any]:
        def getter(engine_name: str, *args, **kwargs) -> Any:
            component_func = self._registry[engine_name][component]
            return component_func(*args, **kwargs)

        getter.__name__ = f"get_{component}"
        getter.__doc__ = f"Retrieve the '{component}' component for the specified engine."
        return getter


engine_registry = EngineRegistry()
