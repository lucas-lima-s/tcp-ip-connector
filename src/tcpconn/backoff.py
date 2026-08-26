from __future__ import annotations

import random
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ExponentialBackoff:
    initial: float = 0.5
    multiplier: float = 2.0
    maximum: float = 30.0
    jitter: float = 0.2
    max_attempts: int | None = None

    def __post_init__(self) -> None:
        if self.initial <= 0:
            raise ValueError(f"initial must be > 0, got {self.initial}")
        if self.multiplier < 1:
            raise ValueError(f"multiplier must be >= 1, got {self.multiplier}")
        if self.maximum < self.initial:
            raise ValueError(f"maximum must be >= initial, got {self.maximum} < {self.initial}")
        if not 0 <= self.jitter <= 1:
            raise ValueError(f"jitter must be within [0, 1], got {self.jitter}")

    def delays(self, rng: random.Random | None = None) -> Iterator[float]:
        """Yield jittered delays; each value lies in [capped * (1 - jitter), capped]."""
        rng = rng if rng is not None else random.Random()
        delay = self.initial
        attempt = 0
        while self.max_attempts is None or attempt < self.max_attempts:
            capped = min(delay, self.maximum)
            floor = capped * (1 - self.jitter)
            yield floor + rng.random() * (capped - floor)
            delay = min(delay * self.multiplier, self.maximum)
            attempt += 1


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    backoff: ExponentialBackoff = field(default_factory=ExponentialBackoff)
    on_reconnect: Callable[[int], None] | None = None
