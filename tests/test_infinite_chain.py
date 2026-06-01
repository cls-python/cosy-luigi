"""Test if chaining the same task or sequence of tasks infinitely to each other is possible, i.e. ensure that the
caching done by Luigi is not restricting the set of results."""

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


def test_infinite_chain(repo):
    """Tests if the results of pipeline synthesis are the pipelines that incrementally contain one more repeating
    link. If CoSy-Luigi does not correctly override the task_id generation from Luigi, or if alternatively the
    instance cache of the global Luigi Registry is not disabled, this test will fail.

    Args:
        repo (CoSyLuigiRepo): The repository to use for synthesis.
    """
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    results = list(maestro.query(RepeatingLink.target(), max_count=10))

    # Check shapes of the pipelines
    for i, result in enumerate(results):
        current_link = result
        for _ in range(i):
            assert isinstance(current_link, RepeatingLink)
            current_link = current_link.chain_link
        current_link = current_link.chain_link
        assert isinstance(current_link, StartingLink)
