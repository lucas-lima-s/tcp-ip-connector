from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HeartbeatConfig:
    interval: float = 15.0
    timeout: float = 45.0

    def __post_init__(self) -> None:
        if self.timeout <= self.interval:
            raise ValueError(
                f"timeout must be greater than interval, got timeout={self.timeout} "
                f"interval={self.interval}"
            )
