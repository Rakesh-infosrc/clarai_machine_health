"""Unit tests for pipeline utilities and helper functions."""

import sys
import pathlib

# Add src/ to path for importing local modules
src_path = str(pathlib.Path(__file__).parent.parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)


def test_scope_machine_type():
    """Verify that scope helper generates proper SQL WHERE clause."""
    from common_utils import scope
    assert scope("Fan") == "machine_type = 'Fan'"
    assert scope("Motor") == "machine_type = 'Motor'"
    assert scope("Compressor") == "machine_type = 'Compressor'"


def test_audit_columns_present():
    """Verify that COLS contains all required ClairAI audit fields."""
    from common_utils import COLS
    required_fields = [
        "workspace_id", "job_id", "job_run_id", "task_key",
        "machine_type", "status", "start_time", "end_time"
    ]
    for field in required_fields:
        assert field in COLS
