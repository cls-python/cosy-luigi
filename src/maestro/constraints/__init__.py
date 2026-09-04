"""This module contains common constraints that may be needed when modeling pipelines."""

from maestro.constraints.unique import is_unique_in_prior_tasks

__all__ = ["is_unique_in_prior_tasks"]
