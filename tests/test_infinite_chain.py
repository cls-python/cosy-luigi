import pytest
from cosy.maestro import Maestro
from luigi.mock import MockTarget

from cosy_luigi.combinatorics import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter


class ChainLink(CoSyLuigiTask):
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
    return CoSyLuigiRepo(StartingLink, RepeatingLink, FinalLink)


def test_infinite_chain(repo):
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    results = list(maestro.query(FinalLink.target(), max_count=10))

    # Check for shapes of the pipelines
    for i, result in enumerate(results):
        current_link = result
        assert isinstance(current_link, FinalLink)
        current_link = current_link.chain_link
        for _ in range(i):
            assert isinstance(current_link, RepeatingLink)
            current_link = current_link.chain_link
        assert isinstance(current_link, StartingLink)
