"""Fixtures for all tests."""

import pytest

from cosy_luigi.core.cosy_adapter import CoSyMaestro
from cosy_luigi.core.synthesis import Maestro


@pytest.fixture(params=[Maestro, CoSyMaestro], ids=["tree-grammar", "cosy"])
def engine(request: pytest.FixtureRequest) -> type[Maestro | CoSyMaestro]:
    """Tests run once on each engine."""
    return request.param
