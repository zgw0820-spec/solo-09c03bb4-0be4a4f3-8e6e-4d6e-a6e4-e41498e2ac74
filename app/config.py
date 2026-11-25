from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")
    return value


def _env_clock_mode(name: str, default: str = "realtime") -> str:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    mode = raw.strip().lower()
    # "fastforward" is an intuitive alias for the virtual clock.
    aliases = {"fast_forward": "virtual", "fastforward": "virtual", "fast-forward": "virtual"}
    mode = aliases.get(mode, mode)
    if mode not in ("realtime", "virtual"):
        raise ValueError(
            f"{name} must be one of realtime|virtual (or fastforward), got {raw!r}"
        )
    return mode


@dataclass(frozen=True)
class LinkConfig:
    link_id: str
    delivery_delay_ms: int
    ack_delay_ms: int


@dataclass(frozen=True)
class EngineConfig:
    """Initial (and reset) parameters for the two simulated member links."""

    link_a: LinkConfig = LinkConfig("A", 300, 200)
    link_b: LinkConfig = LinkConfig("B", 150, 80)
    # realtime: delays elapse on the wall clock (default).
    # virtual: the clock only moves via POST /api/clock/advance (fast-forward).
    clock_mode: str = "realtime"

    def links(self) -> dict[str, LinkConfig]:
        return {self.link_a.link_id: self.link_a, self.link_b.link_id: self.link_b}


def config_from_env() -> EngineConfig:
    return EngineConfig(
        link_a=LinkConfig(
            "A",
            _env_int("DRILL_A_DELIVERY_MS", 300),
            _env_int("DRILL_A_ACK_MS", 200),
        ),
        link_b=LinkConfig(
            "B",
            _env_int("DRILL_B_DELIVERY_MS", 150),
            _env_int("DRILL_B_ACK_MS", 80),
        ),
        clock_mode=_env_clock_mode("DRILL_CLOCK_MODE", "realtime"),
    )
