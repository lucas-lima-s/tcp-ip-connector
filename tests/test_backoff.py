from __future__ import annotations

import itertools
import random
from typing import Any

import pytest

from tcpconn.backoff import ExponentialBackoff


def _expected_capped(backoff: ExponentialBackoff, attempts: int) -> list[float]:
    capped = []
    delay = backoff.initial
    for _ in range(attempts):
        capped.append(min(delay, backoff.maximum))
        delay = min(delay * backoff.multiplier, backoff.maximum)
    return capped


def test_delays_are_reproducible_with_a_seeded_rng() -> None:
    backoff = ExponentialBackoff(
        initial=0.1, multiplier=2.0, maximum=5.0, jitter=0.3, max_attempts=6
    )
    first = list(backoff.delays(random.Random(0)))
    second = list(backoff.delays(random.Random(0)))
    assert first == second


def test_delays_respect_max_attempts() -> None:
    backoff = ExponentialBackoff(initial=0.1, maximum=1.0, max_attempts=4)
    assert len(list(backoff.delays(random.Random(1)))) == 4


def test_delays_are_clamped_and_monotonically_non_decreasing_before_the_cap() -> None:
    backoff = ExponentialBackoff(
        initial=0.1, multiplier=3.0, maximum=1.0, jitter=0.25, max_attempts=8
    )
    capped = _expected_capped(backoff, 8)
    values = list(backoff.delays(random.Random(42)))
    for value, cap in zip(values, capped, strict=True):
        assert value <= backoff.maximum
        assert cap * (1 - backoff.jitter) - 1e-9 <= value <= cap + 1e-9
    for earlier, later in itertools.pairwise(capped):
        assert earlier <= later


def test_delays_every_value_within_jitter_band() -> None:
    backoff = ExponentialBackoff(
        initial=1.0, multiplier=2.0, maximum=8.0, jitter=0.5, max_attempts=5
    )
    capped = _expected_capped(backoff, 5)
    values = list(backoff.delays(random.Random(7)))
    for value, cap in zip(values, capped, strict=True):
        assert cap * 0.5 - 1e-9 <= value <= cap + 1e-9


@pytest.mark.parametrize(
    "kwargs",
    [
        {"initial": 0.0},
        {"initial": -1.0},
        {"multiplier": 0.5},
        {"maximum": 0.1, "initial": 1.0},
        {"jitter": -0.1},
        {"jitter": 1.5},
    ],
)
def test_post_init_validation_raises_value_error(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match=r".+"):
        ExponentialBackoff(**kwargs)
