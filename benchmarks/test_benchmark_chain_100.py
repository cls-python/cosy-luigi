import itertools
from abc import ABC

import pytest
from cosy.maestro import Maestro
from luigi.mock import MockTarget

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter

counter = itertools.count()


class ChainLink(CoSyLuigiTask, ABC):
    chain_link: CoSyLuigiTaskParameter | None


class StartingLink(ChainLink):
    chain_link = None


class RepeatingLink(ChainLink):
    chain_link = CoSyLuigiTaskParameter(ChainLink)

    def output(self):
        return {"counter": MockTarget(str(next(counter)))}


@pytest.fixture
def repo():
    return CoSyLuigiRepo(ChainLink)


def create_infinite_chain(repo):
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    list(maestro.query(RepeatingLink.target(), max_count=100))


def test_benchmark_chain_creation(repo, benchmark):
    benchmark(create_infinite_chain, repo)


if __name__ == "__main__":
    create_infinite_chain(repo)
