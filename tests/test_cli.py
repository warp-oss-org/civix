from __future__ import annotations

import subprocess
import sys


def test_module_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "civix", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Civix tooling scaffold." in result.stdout
