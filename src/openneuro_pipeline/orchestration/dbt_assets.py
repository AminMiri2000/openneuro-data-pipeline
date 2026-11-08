"""dbt models as Dagster assets, one asset per model and seed.

dagster-dbt reads the dbt manifest to create the assets and turns every dbt
data test into a Dagster asset check, so model-level lineage and test results
appear in the UI. At run time it calls the `dbt` CLI in a subprocess.
"""

import os
import shutil
import sys
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)

from ..config import Settings
from ..transform import DbtProject

# Files whose changes require re-parsing the dbt project.
_PROJECT_FILE_PATTERNS = ("*.sql", "*.yml", "*.yaml", "*.csv")


def _manifest_is_stale(project_dir: Path, manifest: Path) -> bool:
    """True when any project file changed after the manifest was written."""
    if not manifest.is_file():
        return True
    built_at = manifest.stat().st_mtime
    return any(
        path.stat().st_mtime > built_at
        for pattern in _PROJECT_FILE_PATTERNS
        for path in project_dir.rglob(pattern)
        if "target" not in path.parts and "logs" not in path.parts
    )


def prepare_manifest(settings: Settings) -> Path:
    """Return an up-to-date manifest.json, running `dbt parse` only when needed.

    Dagster imports this module in several processes (UI, daemon, each run),
    so skipping the parse when nothing changed keeps start-up fast.
    """
    project = DbtProject(settings)
    manifest = project.target_dir / "manifest.json"
    if _manifest_is_stale(project.project_dir, manifest):
        project.invoke(["parse"])
    return manifest


def dbt_executable() -> str:
    """Path of the `dbt` command installed next to this Python interpreter.

    The virtualenv's script folder is not on PATH unless it is activated, so
    resolving it here makes Dagster work the same either way, on every OS.
    """
    name = "dbt.exe" if sys.platform == "win32" else "dbt"
    beside_python = Path(sys.executable).parent / name
    if beside_python.is_file():
        return str(beside_python)
    return shutil.which("dbt") or "dbt"


class LayeredTranslator(DagsterDbtTranslator):
    """Key and group dbt assets by warehouse schema.

    Models and seeds become e.g. `marts/dim_dataset` in group `marts`, which
    mirrors the warehouse and keeps names unique across layers. Sources keep
    the default `openneuro/<table>` keys, which the load assets produce.
    """

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if dbt_resource_props["resource_type"] in {"model", "seed", "snapshot"}:
            return AssetKey([dbt_resource_props["schema"], dbt_resource_props["name"]])
        key: AssetKey = super().get_asset_key(dbt_resource_props)
        return key

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        if dbt_resource_props["resource_type"] in {"model", "seed", "snapshot"}:
            return str(dbt_resource_props["schema"])
        return None


def build_dbt_assets(settings: Settings) -> Any:
    """Create the dbt assets definition and the resource that runs dbt."""
    # profiles.yml reads the warehouse path from this variable; the dbt
    # subprocess inherits it.
    os.environ["PIPELINE_WAREHOUSE_PATH"] = str(settings.warehouse_path.resolve())

    translator = LayeredTranslator(settings=DagsterDbtTranslatorSettings(enable_asset_checks=True))

    @dbt_assets(
        manifest=prepare_manifest(settings),
        dagster_dbt_translator=translator,
        name="dbt_models",
    )
    def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
        # `build` runs seeds, models and tests in dependency order; Dagster
        # passes the selected subset, so materialising one model works too.
        yield from dbt.cli(["build"], context=context).stream()

    project_dir = str(settings.dbt_project_dir.resolve())
    resource = DbtCliResource(
        project_dir=project_dir,
        profiles_dir=project_dir,
        dbt_executable=dbt_executable(),
    )
    return dbt_models, resource
