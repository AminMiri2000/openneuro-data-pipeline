"""Dagster entry point, loaded by `openneuro-pipeline dagster`.

Kept to one line so that importing the builder (e.g. in tests) does not load
definitions from the environment as a side effect.
"""

from .builder import build_definitions

defs = build_definitions()
