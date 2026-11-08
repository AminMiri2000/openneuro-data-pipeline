"""Static dashboard built from the dbt marts (see build.py)."""

from .build import DashboardBuild, build

__all__ = ["DashboardBuild", "build"]
