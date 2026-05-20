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


def test_infinite_chain(repo):
    """_summary_.

    Args:
        repo (_type_): _description_
    """
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    results = list(maestro.query(RepeatingLink.target(), max_count=10))

    # Check for shapes of the pipelines
    for i, result in enumerate(results):
        current_link = result
        for _ in range(i):
            assert isinstance(current_link, RepeatingLink)
            current_link = current_link.chain_link
        current_link = current_link.chain_link
        assert isinstance(current_link, StartingLink)
