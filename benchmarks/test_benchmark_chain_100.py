from abc import ABC

import pytest
from cosy.maestro import Maestro
from luigi.mock import MockTarget

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter


class ChainLink(CoSyLuigiTask, ABC):
    chain_link: CoSyLuigiTaskParameter | None

    def output(self):
        return MockTarget("ChainLink")

    def run(self):
        self.output().open("w").write("Ok.")


class StartingLink(ChainLink):
    chain_link = None


class RepeatingLink(ChainLink):
    chain_link = CoSyLuigiTaskParameter(ChainLink)


class FinalLink(CoSyLuigiTask):
    chain_link = CoSyLuigiTaskParameter(ChainLink)

    def output(self):
        return MockTarget("FinalLink")

    def run(self):
        self.output().open("w").write("Ok.")


@pytest.fixture
def repo():
    return CoSyLuigiRepo(ChainLink, FinalLink)


def create_infinite_chain(repo):
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    list(maestro.query(FinalLink.target(), max_count=100))


def test_benchmark_chain_creation(repo, benchmark):
    benchmark(create_infinite_chain, repo)
