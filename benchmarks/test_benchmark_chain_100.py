"""Benchmark synthesis performance for deep chains."""

from abc import ABC

import pytest

from cosy_luigi import Maestro, Repository, Task, TaskParameter


class ChainLink(Task, ABC):
    """An abstract class representing a chain link in an infinite chain."""

    chain_link: TaskParameter | None


class StartingLink(ChainLink):
    """A class that terminates the chain by needing no further chain links."""

    chain_link = None


class RepeatingLink(ChainLink):
    """A class that recurses the chain by requiring a further chain link."""

    chain_link = TaskParameter(ChainLink)


@pytest.fixture
def repo():
    """Creates a Repository that contains the StartingLink and the RepeatingLink.

    Returns:
        Repository: The created Repository.
    """
    return Repository(ChainLink)


def create_infinite_chain(repo):
    """Synthesizes all pipelines up to those that carry out the same step 100 times.

    Args:
        repo (Repository): The repository to use for synthesis.
    """
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    list(maestro.query(RepeatingLink.target(), max_count=100))


def test_benchmark_chain_creation(repo, benchmark):
    """Benchmarks how long synthesizing and enumerating the pipelines takes.

    Args:
        repo (Repository): The repository to use for synthesis.
        benchmark (BenchmarkFixture): The benchmark fixture.
    """
    benchmark(create_infinite_chain, repo)


if __name__ == "__main__":
    create_infinite_chain(Repository(ChainLink))
