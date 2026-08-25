"""Unit tests for pipeline transformations."""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


def test_env_config():
    """Environment config returns expected catalog and schema."""
    from configs.env import get_catalog, get_schema
    assert get_catalog() == "workspace"
    assert get_schema("gold") == "mention_dw"
    assert get_schema("silver") == "mention_dw"


def test_env_default():
    """Default environment is prod."""
    from configs.env import get_environment
    env = get_environment()
    assert env == "prod"
