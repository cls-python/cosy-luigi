"""_summary_."""

import itertools
from abc import ABC

import pytest
from cosy.maestro import Maestro
from luigi.mock import MockTarget

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter

counter = itertools.count()


class ChainLink(CoSyLuigiTask, ABC):
    """_summary_."""

    chain_link: CoSyLuigiTaskParameter | None


class StartingLink(ChainLink):
    """_summary_."""

    chain_link = None


class RepeatingLink(ChainLink):
    """_summary_."""

    chain_link = CoSyLuigiTaskParameter(ChainLink)

    def output(self):
        """_summary_.

        Returns:
            _type_: _description_
        """
        return {"counter": MockTarget(str(next(counter)))}


@pytest.fixture
def repo():
    """_summary_.

    Returns:
        _type_: _description_
    """
    return CoSyLuigiRepo(ChainLink)


def create_infinite_chain(repo):
    """_summary_.

    Args:
        repo (_type_): _description_
    """
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    list(maestro.query(RepeatingLink.target(), max_count=100))


def test_benchmark_chain_creation(repo, benchmark):
    """_summary_.

    Args:
        repo (_type_): _description_
        benchmark (_type_): _description_
    """
    benchmark(create_infinite_chain, repo)


if __name__ == "__main__":
    create_infinite_chain(repo)
