"""Pytest configuration for nt-data-pipelines."""

import sys
from pathlib import Path

# Add pipelines directory to path so imports work
pipelines_dir = Path(__file__).parent / "pipelines"
sys.path.insert(0, str(pipelines_dir))
