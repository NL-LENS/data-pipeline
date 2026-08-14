import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch
import pytest
import data_pipeline.cli


@pytest.mark.parametrize(
    "lens_command",
    [["lens", "--help"], ["lens", "init", "--help"], ["lens", "build", "bronze", "--help"]],
    ids=["base", "init", "build"],
)
def test_cli_can_be_called(lens_command: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that --help works for the main and for subcommands."""
    workflow_rev = os.environ.get("GITHUB_WORKFLOW_REF", "")
    if workflow_rev and "build_cbs.yml" in workflow_rev:
        # Add current venv to path -- the cli executable is installed there
        venv_path = Path(sys.executable).parent
        monkeypatch.setenv("PATH", str(venv_path), prepend=os.pathsep)

    result = subprocess.run(lens_command, check=True)  # noqa: S603
    assert result.returncode == 0


def test_cli_init(monkeypatch: pytest.MonkeyPatch):
    """Test that init calls run_init."""
    monkeypatch.setattr(sys, "argv", ["lens", "init", "--root", "/path/to/data/root/", "--db_file", "/path/to/db.db"])
    with patch("data_pipeline.cli.run_init") as init_patch:
        data_pipeline.cli.cli_main()
        init_patch.assert_called_once_with(Path("/path/to/data/root"), Path("/path/to/db.db"))


def test_cli_build(monkeypatch: pytest.MonkeyPatch):
    """Test that init calls run_init."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "lens",
            "build",
            "bronze",
            "--db_file",
            "/path/to/db.db",
            "--ref_period",
            "2020",
            "2021",
            "--source_regex",
            "SPOLIS",
            "--dest_dir",
            "path/to/processed/data",
        ],
    )
    with patch("data_pipeline.cli.build_bronze") as build_bronze_patch:
        data_pipeline.cli.cli_main()
        build_bronze_patch.assert_called_once_with(
            Path("/path/to/db.db"), "SPOLIS", 2020, 2021, Path("path/to/processed/data")
        )
        build_bronze_patch.assert_called_once()
