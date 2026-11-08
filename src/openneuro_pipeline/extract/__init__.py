"""Extractors: move one entity each from a source into the raw layer.

Importing this package registers every built-in extractor.
"""

from . import datasets, participants  # noqa: F401  (imported for registration)
from .base import Extractor, ExtractParams, ExtractResult, RunContext
from .registry import available, execution_order, get_extractor

__all__ = [
    "ExtractParams",
    "ExtractResult",
    "Extractor",
    "RunContext",
    "available",
    "execution_order",
    "get_extractor",
]
