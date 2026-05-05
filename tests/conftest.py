from __future__ import annotations

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    del config
    for item in items:
        if item.path.name.endswith("_live.py"):
            item.add_marker(pytest.mark.live)
