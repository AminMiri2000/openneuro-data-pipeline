"""OpenNeuro metadata pipeline.

Package layout (each layer only depends on the ones listed before it):

    config, errors      settings and exception types
    sources/            clients for upstream systems (OpenNeuro)
    storage/            raw-layer object store, path layout, state, manifests
    extract/            extractors: source -> raw layer, one class per entity
    load/               raw layer -> DuckDB warehouse
    pipeline            wires the layers together; the only thing entry points call
    cli                 command-line entry point
"""

__version__ = "0.2.0"
