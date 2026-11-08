"""Exception hierarchy.

A single base class lets callers (the CLI, later the orchestrator) catch every
expected pipeline failure in one place, while unexpected bugs still surface as
ordinary Python exceptions with full tracebacks.
"""


class PipelineError(Exception):
    """Base class for all expected pipeline failures."""


class ConfigError(PipelineError):
    """Invalid configuration value."""


class SourceError(PipelineError):
    """The upstream source returned an unusable response."""


class StorageError(PipelineError):
    """A raw-layer object is missing or unreadable."""
