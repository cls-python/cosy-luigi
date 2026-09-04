"""Contains the synthesis algorithm: Builds a tree grammar and enumerates it by term size."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from itertools import product
from math import inf
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

Predicate = Callable[[Mapping[str, Any]], bool]


@dataclass(frozen=True)
class Constructor:
    """A Constructor"""

    name: str


@dataclass(frozen=True)
class Specification:
    """Specification of a component, holds arguments by name and predicates.

    Raises:
        ValueError: Arguments may not have duplicate name if we want Predicates.
    """

    arguments: tuple[tuple[str, Constructor], ...]
    predicates: tuple[Predicate, ...]
    target: Constructor

    def __post_init__(self) -> None:
        names = [name for name, _ in self.arguments]
        if len(set(names)) < len(names):
            msg = f"Duplicate argument names {names} in the specification of {self.target.name}."
            raise ValueError(msg)


class SpecificationBuilder:
    """Builder for Specifications."""

    def __init__(self) -> None:
        self._arguments: list[tuple[str, Constructor]] = []
        self._predicates: list[Predicate] = []

    def argument(self, name: str, specification: Constructor) -> SpecificationBuilder:
        """Adds an argument to be inhabited."""
        self._arguments.append((name, specification))
        return self

    def constraint(self, predicate: Predicate) -> SpecificationBuilder:
        """Adds a predicate."""
        self._predicates.append(predicate)
        return self

    def suffix(self, target: Constructor) -> Specification:
        """Adds return type."""
        return Specification(tuple(self._arguments), tuple(self._predicates), target)


class Solutions:
    """Lazy results of query."""

    def __init__(self, produce: Callable[[], Iterator[Any]]) -> None:
        self._produce = produce

    def __iter__(self) -> Iterator[Any]:
        return self._produce()


class Maestro:
    """Synthesizes pipelines by enumerating a regular tree grammar.

    Every task is a MultiArrow type, with the non-rightmost types being children and the rightmost being the node.

    Names are a bit confusing, a component is both a rule and a non-terminal.
    """

    def __init__(
        self,
        components: Sequence[tuple[str, Callable[..., Any], Specification]],
        taxonomy: Mapping[str, Iterable[str]] | None = None,
    ) -> None:
        """Build the grammar and its size bounds.

        Args:
            components (Sequence[tuple[str, Callable[..., Any], Specification]]): name, interpretation and type of each concrete task.
            taxonomy (Mapping[str, Iterable[str]] | None): Taxonomy to describe subtyping.
        """
        # Read-only views: You need one Maestro per Repo
        self._interpretations = MappingProxyType({name: interpretation for name, interpretation, _ in components})
        self._specification = MappingProxyType({name: specification for name, _, specification in components})
        self._children = MappingProxyType(
            {name: tuple(child.name for _, child in spec.arguments) for name, spec in self._specification.items()}
        )
        rules_per_nonterminal: dict[str, list[str]] = {}
        for component in self._specification:
            # Every component can make itself or make any of its supertypes
            for nonterminal in {component, *(taxonomy or {}).get(component, ())}:
                rules_per_nonterminal.setdefault(nonterminal, []).append(component)

        # A smallest term can't have cycles or repetitions by definition.
        # The largest smallest term would be each rule applied exactly once
        # So that is the bound for the loop
        self._minimum_term_size_for_nonterminal: Mapping[str, int] = {}
        for _ in range(len(rules_per_nonterminal)):
            # Every round prices one more layer, the first can only reach leaves, the next what becomes productive by those leaves being available and so on
            self._minimum_term_size_for_nonterminal = {
                nonterminal: min(sizes)
                for nonterminal, names in rules_per_nonterminal.items()
                if (
                    sizes := [
                        1 + sum(self._minimum_term_size_for_nonterminal[child] for child in self._children[rule])
                        for rule in names
                        if self._productive(rule)
                    ]
                )
            }
        self._minimum_term_size_for_nonterminal = MappingProxyType(self._minimum_term_size_for_nonterminal)
        # Rules that are still unproductive can't ever become productive, so dropping them is free
        self._rules_per_nonterminal: Mapping[str, tuple[str, ...]] = MappingProxyType(
            {
                nonterminal: buildable
                for nonterminal, names in rules_per_nonterminal.items()
                if (buildable := tuple(rule for rule in names if self._productive(rule)))
            }
        )
        # Only the two caches stay writable, both are derived from the frozen grammar and fill in on demand
        # Largest observed pipeline per type
        self._maximum_term_size_for_nonterminal: dict[str, float] = {}
        # Cache per component and size
        self._cache: dict[tuple[str, int], list[Any]] = {}

    def _productive(self, component: str) -> bool:
        """If all the component's children have a size, they can be built, and if all the children can be built, the
        component can be built as well. That's what it means to be productive.
        """
        return all(child in self._minimum_term_size_for_nonterminal for child in self._children[component])

    def _compute_maximum_term_size(self, nonterminal: str, path: frozenset[str] = frozenset()) -> float:
        """Largest term a non-terminal can derive."""
        # Seeing a non-terminal again while its on the path means it can chain or loop, so size is infinite
        if nonterminal in path:
            return inf
        if nonterminal not in self._maximum_term_size_for_nonterminal:
            # Maximum size of term a rule can produce. Equals min unless variants can differ in size.
            sizes = [
                1 + sum(self._compute_maximum_term_size(child, path | {nonterminal}) for child in self._children[rule])
                for rule in self._rules_per_nonterminal[nonterminal]
            ]
            self._maximum_term_size_for_nonterminal[nonterminal] = max(sizes)
        return self._maximum_term_size_for_nonterminal[nonterminal]

    def _compute_allocations_of_size_to_children(self, children: Sequence[str], size: int) -> Iterator[tuple[int, ...]]:
        """Streams every valid way of spreading the size bound across children."""
        if not children:
            # The total requested size must be exactly allocated
            if size == 0:
                yield ()
            return
        head, rest = children[0], children[1:]
        # Either its own minimum or the excess the other children can not "eat", whichever is larger
        minimum_head_must_take = max(
            self._minimum_term_size_for_nonterminal[head],
            size - sum(self._compute_maximum_term_size(child) for child in rest),
        )
        # Either its own maximum or no more than what would cause the other children to "starve", whichever is smaller
        maximum_head_can_take = min(
            self._compute_maximum_term_size(head),
            size - sum(self._minimum_term_size_for_nonterminal[child] for child in rest),
        )
        for share in range(int(minimum_head_must_take), int(maximum_head_can_take) + 1):
            # Lazy recursion, only recurses as much as needed for what the caller wants to process next
            yield from ((share, *tail) for tail in self._compute_allocations_of_size_to_children(rest, size - share))

    def _build_terms_for_nonterminal(self, nonterminal: str, size: int) -> list[Any]:
        """All terms of the given size that inhabit a non-terminal."""
        return [
            term
            for component in self._rules_per_nonterminal[nonterminal]
            for term in self._build_terms_for_single_rule(component, size)
        ]

    def _build_terms_for_single_rule(self, component: str, size: int) -> list[Any]:
        """All terms of the given size that inhabit a rule."""
        if (component, size) not in self._cache:
            spec, children = self._specification[component], self._children[component]
            argument_names = [name for name, _ in spec.arguments]
            self._cache[component, size] = [
                # Build Tasks
                self._interpretations[component](*args)
                # From a stream of possible splits
                for sizes in self._compute_allocations_of_size_to_children(children, size - 1)
                # That is used to make all possible terms that could fill the arguments
                for args in product(
                    *(self._build_terms_for_nonterminal(child, s) for child, s in zip(children, sizes, strict=True))
                )
                # But only if all predicates hold
                if all(predicate(dict(zip(argument_names, args, strict=True))) for predicate in spec.predicates)
            ]
        return self._cache[component, size]

    def _enumerate(self, target: str, max_size: float) -> Iterator[Any]:
        """Stream all results for a target, up to the size the caller asked for."""
        if target in self._rules_per_nonterminal:
            # Lazy isn't optional, max size can be inf
            size, largest = (
                self._minimum_term_size_for_nonterminal[target],
                min(self._compute_maximum_term_size(target), max_size),
            )
            while size <= largest:
                yield from self._build_terms_for_nonterminal(target, size)
                size += 1

    def query(self, target: Constructor, max_size: float = inf) -> Solutions:
        """Synthesizes terms for target type, smallest to largest.

        Args:
            target (Constructor): The type to inhabit.
            max_size (float): Maximum size terms may have.

        Returns:
            Solutions: Stream of terms.

        Raises:
            ValueError: The target is recursive and there is no size bound, so the enumeration would never end.
        """
        if (
            max_size == inf
            and target.name in self._rules_per_nonterminal
            and self._compute_maximum_term_size(target.name) == inf
        ):
            msg = f"{target.name} creates infinite pipelines; pass a max_size to enumerate every pipeline up to it."
            raise ValueError(msg)
        return Solutions(lambda: self._enumerate(target.name, max_size))
