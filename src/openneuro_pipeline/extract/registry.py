"""Registry of available extractors and their run order."""

from __future__ import annotations

from collections.abc import Iterable

from ..errors import ConfigError
from .base import Extractor

_REGISTRY: dict[str, type[Extractor]] = {}


def register(cls: type[Extractor]) -> type[Extractor]:
    """Class decorator that makes an extractor available by its `name`."""
    if cls.name in _REGISTRY:
        raise ConfigError(f"Extractor '{cls.name}' is registered twice")
    _REGISTRY[cls.name] = cls
    return cls


def get_extractor(name: str) -> type[Extractor]:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise ConfigError(f"Unknown extractor '{name}'. Available: {available()}") from None


def available() -> list[str]:
    return sorted(_REGISTRY)


def execution_order(names: Iterable[str] | None = None) -> list[str]:
    """Order extractors so each runs after the ones it depends on.

    A depth-first topological sort; dependencies are pulled in automatically,
    so asking for 'participants' also schedules 'datasets'.
    """
    ordered: list[str] = []
    visiting: set[str] = set()

    def visit(name: str) -> None:
        if name in ordered:
            return
        if name in visiting:
            raise ConfigError(f"Circular extractor dependency involving '{name}'")
        visiting.add(name)
        for dependency in get_extractor(name).depends_on:
            visit(dependency)
        visiting.discard(name)
        ordered.append(name)

    for name in names if names is not None else available():
        visit(name)
    return ordered
