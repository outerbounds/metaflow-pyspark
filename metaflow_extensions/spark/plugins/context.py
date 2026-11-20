"""Backend-neutral types shared by every backend.

Nothing in here may import a cloud SDK or pyspark at module level: these types are
constructed in the Metaflow task process, which is not guaranteed to have any of the
backend dependencies installed.
"""

import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


class JobState:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    TERMINAL = frozenset({SUCCESS, FAILED, CANCELLED})

    @classmethod
    def is_terminal(cls, state):
        return state in cls.TERMINAL


@dataclass
class StageProgress:
    """Coarse progress for one stage or task of a remote job, for log lines."""

    stage_id: Any
    name: Optional[str] = None
    num_tasks: Optional[int] = None
    num_completed: Optional[int] = None
    status: Optional[str] = None


@dataclass
class JobStatus:
    state: str
    message: Optional[str] = None
    error_class: Optional[str] = None
    ui_url: Optional[str] = None
    spark_ui_url: Optional[str] = None
    stages: List[StageProgress] = field(default_factory=list)

    @property
    def terminal(self):
        return JobState.is_terminal(self.state)

    @property
    def ok(self):
        return self.state == JobState.SUCCESS


@dataclass
class JobHandle:
    """A picklable reference to a submitted statement or job run.

    Stored as task metadata for jobs, so a run can be inspected or cancelled after the
    fact from the client API.
    """

    backend: str
    job_id: str
    ui_url: Optional[str] = None
    spark_ui_url: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self):
        return {
            "backend": self.backend,
            "job_id": self.job_id,
            "ui_url": self.ui_url,
            "spark_ui_url": self.spark_ui_url,
            "extra": self.extra,
        }


@dataclass
class TaskContext:
    """The Metaflow task remote work runs for, used for tags, parameters, and logging."""

    step_name: str
    pathspec: str
    flow_name: str
    run_id: str
    task_id: str
    attempt: int
    user: Optional[str]
    tags: Dict[str, str]
    inputs: Dict[str, Any] = field(default_factory=dict)
    timeout_minutes: Optional[int] = None
    logger: Optional[Callable] = None

    def log(self, msg, job_id=None, stream="stdout"):
        if self.logger is not None:
            self.logger(msg, job_id=job_id, stream=stream)
        else:
            log(msg, job_id=job_id, stream=stream)


def log(msg, job_id=None, stream="stdout", prefix="@spark"):
    if job_id:
        prefix += "[%s]" % job_id
    print("%s: %s" % (prefix, msg), file=getattr(sys, stream), flush=True)
