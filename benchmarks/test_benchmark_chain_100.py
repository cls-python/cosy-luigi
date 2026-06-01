"""_summary_."""

import itertools
from abc import ABC

import pytest
from cosy.maestro import Maestro
from luigi.mock import MockTarget

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter

counter = itertools.count()


class ChainLink(CoSyLuigiTask, ABC):
    """An abstract class representing a chain link in an infinite chain."""

    chain_link: CoSyLuigiTaskParameter | None


class StartingLink(ChainLink):
    """A class that terminates the chain by needing no further chain links."""

    chain_link = None


class RepeatingLink(ChainLink):
    """A class that recurses the chain by requiring a further chain link."""

    chain_link = CoSyLuigiTaskParameter(ChainLink)

    def output(self):
        """Assign each chain link a unique identifier. This is required because CoSy-Luigi considers tasks with
        identical names, identical requirements, and identical outputs to be Singletons. This differs from Luigi,
        which considers tasks with identical names and identical requirements Singletons.

        Returns:
            Mapping[str, MockTarget]_: The named unique target for each chain link.
        """
        return {"counter": MockTarget(str(next(counter)))}


@pytest.fixture
def repo():
    """Creates a CoSyLuigiRepo that contains the StartingLink and the RepeatingLink.

    Returns:
        CoSyLuigiRepo: The created CoSyLuigiRepo.
    """
    return CoSyLuigiRepo(ChainLink)


def create_infinite_chain(repo):
    """Synthesizes all pipelines up to those that carry out the same step 100 times.

    Args:
        repo (CoSyLuigiRepo): The repository to use for synthesis.
    """
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    list(maestro.query(RepeatingLink.target(), max_count=100))


def test_benchmark_chain_creation(repo, benchmark):
    """Benchmarks how long synthesizing and enumerating the pipelines takes.

    Args:
        repo (CoSyLuigiRepo): The repository to use for synthesis.
        benchmark (BenchmarkFixture): The benchmark fixture.
    """
    benchmark(create_infinite_chain, repo)


if __name__ == "__main__":
    create_infinite_chain(repo)
