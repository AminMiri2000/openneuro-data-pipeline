"""scripts/dev.py: the Makefile's cross-platform helper."""

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / "scripts" / "dev.py"
spec = importlib.util.spec_from_file_location("dev_script", SCRIPT)
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


def _make_venv(root: Path, version: str) -> None:
    (root / ".venv").mkdir()
    (root / ".venv" / "pyvenv.cfg").write_text(f"home = /x\nversion = {version}\n")


def test_check_python_accepts_supported_setup(tmp_path):
    _make_venv(tmp_path, "3.13.1")
    assert dev.check_python(tmp_path) == 0


def test_check_python_rejects_unsupported_venv(tmp_path, capsys):
    _make_venv(tmp_path, "3.15.0")
    assert dev.check_python(tmp_path) == 1
    assert "make clean-venv" in capsys.readouterr().err


def test_check_python_rejects_unsupported_interpreter(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sys, "version_info", (3, 15, 0))
    assert dev.check_python(tmp_path) == 1
    assert 'SYSTEM_PYTHON="py -3.13"' in capsys.readouterr().err


@pytest.mark.parametrize("target", ["clean", "clean-venv"])
def test_clean_targets(tmp_path, monkeypatch, target):
    (tmp_path / "data" / "raw").mkdir(parents=True)
    (tmp_path / "data" / "dbt").mkdir()
    (tmp_path / ".venv").mkdir()
    monkeypatch.chdir(tmp_path)

    assert dev.main([target]) == 0

    if target == "clean":
        assert not (tmp_path / "data" / "raw").exists() and (tmp_path / ".venv").exists()
    else:
        assert not (tmp_path / ".venv").exists() and (tmp_path / "data" / "raw").exists()


def test_clean_venv_refuses_to_delete_the_running_venv(tmp_path, monkeypatch, capsys):
    (tmp_path / ".venv").mkdir()
    monkeypatch.setattr(sys, "prefix", str(tmp_path / ".venv"))
    assert dev.clean_venv(tmp_path) == 1
    assert (tmp_path / ".venv").exists()
    assert "deactivate" in capsys.readouterr().err


def test_clean_venv_explains_locked_files(tmp_path, monkeypatch, capsys):
    (tmp_path / ".venv").mkdir()

    def locked(_path):
        raise PermissionError("[WinError 5] Access is denied")

    monkeypatch.setattr(dev.shutil, "rmtree", locked)
    assert dev.clean_venv(tmp_path) == 1
    assert "Another program is using it" in capsys.readouterr().err
