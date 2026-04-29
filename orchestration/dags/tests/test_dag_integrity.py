"""Unit tests for Airflow DAG integrity (no cycles, correct imports, TaskGroups)."""

from __future__ import annotations
import pytest


def test_dag_loads_without_errors():
    """DAG module must import without raising any exception."""
    from orchestration.dags import fintech_master_dag  # noqa: F401


def test_dag_id_is_correct():
    """DAG ID must match expected value."""
    from orchestration.dags.fintech_master_dag import dag
    assert dag.dag_id == "fintech_master_pipeline"


def test_dag_schedule_is_hourly():
    """DAG schedule must be @hourly."""
    from orchestration.dags.fintech_master_dag import dag
    assert dag.schedule_interval == "@hourly"


def test_dag_has_no_cycles():
    """DAG must be a valid DAG (no cycles)."""
    from orchestration.dags.fintech_master_dag import dag
    dag.test_cycle()  # Raises if cycle detected


def test_dag_has_five_task_groups():
    """DAG must have exactly 5 TaskGroups."""
    from orchestration.dags.fintech_master_dag import dag
    task_group_ids = {tg.group_id for tg in dag.task_group.children.values()
                      if hasattr(tg, 'group_id')}
    expected_groups = {
        "data_generation", "ingestion", "transformation",
        "quality_and_modeling", "anomaly_detection"
    }
    assert expected_groups.issubset(task_group_ids), (
        f"Missing task groups. Found: {task_group_ids}"
    )


def test_dag_max_active_runs_is_one():
    """DAG must have max_active_runs=1 to prevent concurrent hourly runs."""
    from orchestration.dags.fintech_master_dag import dag
    assert dag.max_active_runs == 1


def test_dag_catchup_is_false():
    """DAG catchup must be disabled."""
    from orchestration.dags.fintech_master_dag import dag
    assert dag.catchup is False
