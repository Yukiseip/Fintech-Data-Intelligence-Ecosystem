"""Structured JSON audit logger for pipeline jobs."""

from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from typing import Any


class AuditLogger:
    """Writes structured JSON audit entries for pipeline runs.

    Each log entry includes: job_name, run_id, input_rows,
    output_rows, duration_seconds, and timestamp.

    Example:
        audit = AuditLogger("bronze_ingest")
        audit.log(run_id="abc-123", input_rows=1000, output_rows=998, duration_seconds=12.3)
    """

    def __init__(self, job_name: str) -> None:
        """Initialize AuditLogger.

        Args:
            job_name: Name of the pipeline job (e.g., 'bronze_ingest').
        """
        self.job_name = job_name
        self._logger = logging.getLogger(f"audit.{job_name}")

    def log(self, run_id: str, **kwargs: Any) -> None:
        """Write a structured audit log entry.

        Args:
            run_id: Unique identifier for this pipeline run.
            **kwargs: Additional key-value metrics to include
                      (e.g., input_rows, output_rows, duration_seconds,
                      rejected_rows).
        """
        entry = {
            "job_name": self.job_name,
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }
        self._logger.info(json.dumps(entry))
