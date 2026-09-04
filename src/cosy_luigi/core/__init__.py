"""This module contains all core functionality."""

from importlib.util import find_spec

from cosy_luigi.core import synthesis
from cosy_luigi.core.combinatorics import Task, TaskParameter
from cosy_luigi.core.console import LiveLineConsole
from cosy_luigi.core.execution import Log, run_pipelines
from cosy_luigi.core.repository import Repository
from cosy_luigi.core.targets import LocalTarget, MemoryTarget, Target

# Installing the cosy extra makes CoSy the default
if find_spec("cosy"):
    from cosy_luigi.core.cosy_adapter import CoSyMaestro

    Maestro: type[synthesis.Maestro | CoSyMaestro] = CoSyMaestro
else:
    Maestro = synthesis.Maestro

__all__ = [
    "LiveLineConsole",
    "LocalTarget",
    "Log",
    "Maestro",
    "MemoryTarget",
    "Repository",
    "Target",
    "Task",
    "TaskParameter",
    "run_pipelines",
]
