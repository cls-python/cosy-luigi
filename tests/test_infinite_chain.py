"""Test if chaining the same task or sequence of tasks infinitely to each other is possible, i.e. ensure that the
caching done by the engine is not restricting the set of results."""

from abc import ABC

import pytest

from maestro import Maestro, Repository, Task, TaskParameter


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


def test_infinite_chain(repo):
    """Tests if the results of pipeline synthesis are the pipelines that incrementally contain one more repeating link.
    Nothing sets the links apart by hand: the Maestro identifies a task by the whole walk beneath it, so links at
    different depths are already different tasks. Were identity any shallower, they would collapse into one and this
    test would fail.

    Args:
        repo (Repository): The repository to use for synthesis.
    """
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    results = list(maestro.query(RepeatingLink.target(), max_size=11))

    # Check shapes of the pipelines
    for i, result in enumerate(results):
        current_link = result
        for _ in range(i):
            assert isinstance(current_link, RepeatingLink)
            current_link = current_link.chain_link
        current_link = current_link.chain_link
        assert isinstance(current_link, StartingLink)
