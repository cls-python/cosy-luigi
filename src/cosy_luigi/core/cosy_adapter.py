"""The adapter to CoSy."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cosy.core import Constructor, SpecificationBuilder
from cosy.maestro import Maestro

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from cosy.core.synthesizer import Specification

    from cosy_luigi.core import synthesis


def _replay(specification: synthesis.Specification) -> Specification:
    """Replays Specification one to one into CoSy SpecBuilder."""
    builder = SpecificationBuilder()
    for name, argument in specification.arguments:
        builder = builder.argument(name, Constructor(argument.name))
    for predicate in specification.predicates:
        builder = builder.constraint(predicate)
    return builder.suffix(Constructor(specification.target.name))


def _cosy_quirks(interpretation: Callable[..., Any], specification: synthesis.Specification) -> Callable[..., Any]:
    """CoSy requires this check, if there are no arguments the class must be called explicitly."""
    return interpretation if specification.arguments else lambda: interpretation()


class CoSyMaestro:
    """Thin wrapper that assigns the Maestro from CoSy a different name."""

    def __init__(
        self,
        components: Sequence[tuple[str, Callable[..., Any], synthesis.Specification]],
        taxonomy: Mapping[str, set[str]] | None = None,
    ) -> None:
        """Hands things over to CoSy's Maestro.

        Args:
            components (Sequence[tuple[str, Callable[..., Any], synthesis.Specification]]): The components.
            taxonomy (Mapping[str, set[str]] | None): The taxonomy.
        """
        self._cosy = Maestro(
            [(name, _cosy_quirks(interpretation, spec), _replay(spec)) for name, interpretation, spec in components],
            taxonomy,
        )

    def query(self, target: synthesis.Constructor, max_count: int | None = None) -> Iterable[Any]:
        """Query the Maestro for results.

        Args:
            target (synthesis.Constructor): The type to inhabit.
            max_count (int | None): Maximum number of terms. Unbounded so that switching engine doesn't truncate results.

        Returns:
            Iterable[Any]: Stream of terms.
        """
        return self._cosy.query(Constructor(target.name), max_count)
