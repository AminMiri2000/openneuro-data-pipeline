import pytest

from openneuro_pipeline.errors import ConfigError
from openneuro_pipeline.extract import available, execution_order, get_extractor


def test_builtin_extractors_registered():
    assert available() == ["datasets", "participants"]


def test_dependencies_are_scheduled_first():
    assert execution_order(["participants"]) == ["datasets", "participants"]
    assert execution_order() == ["datasets", "participants"]


def test_unknown_extractor():
    with pytest.raises(ConfigError, match="Unknown extractor"):
        get_extractor("nope")
