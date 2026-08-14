"""Smoke test: the rearchitected module skeleton imports cleanly.

Guards against broken package layout while modules are migrated one at a time.
Each new module gets its own behavioural tests as logic is moved into it.
"""
import importlib

import pytest

NEW_MODULES = [
    "network_idx.sources",
    "network_idx.sources.data_download",
    "network_idx.features",
    "network_idx.features.telecom.transform",
    "network_idx.features.telecom.engineered",
    "network_idx.features.demographic.transform",
    "network_idx.features.demographic.engineered",
    "network_idx.features.location.transform",
    "network_idx.features.location.engineered",
    "network_idx.features.rextag.transform",
    "network_idx.features.rextag.engineered",
    "network_idx.grain_transfer",
    "network_idx.grain_transfer.adapters",
    "network_idx.modeling",
    "network_idx.monitoring",
    "network_idx.validation",
    "network_idx.validation.internal",
    "network_idx.validation.external",
    "network_idx.validation.temporal",
    "network_idx.validation.expert",
]


@pytest.mark.parametrize("module_name", NEW_MODULES)
def test_module_imports(module_name):
    module = importlib.import_module(module_name)
    assert module.__doc__, f"{module_name} is missing a purpose docstring"
