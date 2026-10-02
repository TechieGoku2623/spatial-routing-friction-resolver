"""Deterministic checks and a 5000-iteration latency benchmark."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import sys
import time
import tracemalloc

from .engine import (
    EARTH_RADIUS_M,
    TURN_PENALTY_M,
    SpatialRoutingFrictionResolver,
    haversine_m,
)
from .exceptions import EngineKernelException
from .wire import pack_waypoint, unpack_waypoint

ITERATIONS: int = 5_000
SEED: int = 28_000


def _percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    index = math.ceil(fraction * len(ordered)) - 1
    return ordered[max(0, min(index, len(ordered) - 1))]


def _point(ident: int, lat: float, lon: float, congestion: float) -> dict[str, object]:
    return {"id": ident, "lat": lat, "lon": lon, "congestion": congestion}


async def _checks(failures: list[str]) -> None:
    payload = pack_waypoint(7, 40.5, -74.25, 0.25)
    frame = unpack_waypoint(payload)
    if (
        frame["id"] != 7
        or frame["lat"] != 40.5
        or frame["lon"] != -74.25
        or frame["congestion"] != 0.25
    ):
        failures.append("waypoint fields did not round-trip")
    if payload != pack_waypoint(7, 40.5, -74.25, 0.25):
        failures.append("waypoint pack was not stable")
    try:
        unpack_waypoint(b"\x00\x01")
        failures.append("short waypoint did not raise")
    except EngineKernelException as exc:
        if not str(exc):
            failures.append("short waypoint exception had no message")

    arc = haversine_m(0.0, 0.0, 1.0, 0.0)
    expected_arc = math.pi / 180.0 * EARTH_RADIUS_M
    if abs(arc - expected_arc) > 1.0:
        failures.append("haversine missed the one-degree latitude arc")

    direct = await SpatialRoutingFrictionResolver().run(
        [
            _point(1, 0.0, 0.0, 0.0),
            _point(2, 0.0, 5.0, 0.0),
            _point(3, 0.0, 0.1, 0.0),
        ]
    )
    if direct["path"] != [1, 3]:
        failures.append(f"detour was not skipped: {direct['path']}")
    if len(direct["edges"]) != 1 or float(direct["total_friction_m"]) <= 10_000.0:
        failures.append("direct edge friction was not reported")

    low = await SpatialRoutingFrictionResolver().run(
        [_point(1, 0.0, 0.0, 0.0), _point(2, 0.0, 1.0, 0.0)]
    )
    high = await SpatialRoutingFrictionResolver().run(
        [_point(1, 0.0, 0.0, 1.0), _point(2, 0.0, 1.0, 1.0)]
    )
    ratio = float(high["total_friction_m"]) / float(low["total_friction_m"])
    if not math.isclose(ratio, 2.0, rel_tol=1e-6, abs_tol=1e-6):
        failures.append(f"congestion ratio was {ratio}")

    duplicated = await SpatialRoutingFrictionResolver().run(
        [
            _point(1, 40.0, -74.0, 0.0),
            _point(9, 40.0, -74.0, 0.2),
            _point(2, 41.0, -73.0, 0.1),
        ]
    )
    if duplicated["path"] != [1, 2] or duplicated["duplicate_dropped"] != 1:
        failures.append(f"consecutive duplicate survived: {duplicated['path']}")
    if 9 in duplicated["path"]:
        failures.append("dropped waypoint id remained on the path")
    if any(float(edge["distance_m"]) <= 0.0 for edge in duplicated["edges"]):
        failures.append("duplicate removal left a zero-length edge")

    folded = await SpatialRoutingFrictionResolver().run(
        [
            _point(1, 0.0, 0.0, 0.0),
            _point(2, 0.0, 1.0, 0.0),
            _point(3, 0.0, 0.0, 0.0),
        ]
    )
    if folded["path"] != [1, 2, 3]:
        failures.append(f"zero-length pair was not dropped: {folded['path']}")
    if int(folded["zero_length_dropped"]) < 1:
        failures.append("zero-length counter did not move")
    for edge in folded["edges"]:
        if float(edge["distance_m"]) <= 0.0 or not math.isfinite(
            float(edge["friction_m"])
        ):
            failures.append("zero-length edge remained in the path")
    turn = float(folded["edges"][1]["turn_penalty_m"])
    if not math.isclose(turn, TURN_PENALTY_M, rel_tol=1e-6, abs_tol=1e-4):
        failures.append(f"u-turn penalty was {turn}")
    distance = haversine_m(0.0, 0.0, 0.0, 1.0)
    expected = 2.0 * distance + TURN_PENALTY_M
    if not math.isclose(
        float(folded["total_friction_m"]), expected, rel_tol=1e-6, abs_tol=1e-3
    ):
        failures.append(f"folded friction was {folded['total_friction_m']}")

    try:
        await SpatialRoutingFrictionResolver().run(
            [_point(1, 0.0, 0.0, 0.0), _point(2, 0.0, 0.0, 0.0)]
        )
        failures.append("pure zero-length pair did not raise")
    except ZeroDivisionError:
        failures.append("pure zero-length pair divided by zero")
    except EngineKernelException as exc:
        if not str(exc):
            failures.append("zero-length exception had no message")

    try:
        await SpatialRoutingFrictionResolver().run(
            [_point(1, 0.0, 0.0, 0.0), _point(2, 91.0, 0.0, 0.0)]
        )
        failures.append("latitude 91 did not raise")
    except EngineKernelException as exc:
        if not str(exc):
            failures.append("latitude exception had no message")
    try:
        await SpatialRoutingFrictionResolver().run(
            [_point(1, 0.0, 0.0, 0.0), _point(2, 0.0, float("nan"), 0.0)]
        )
        failures.append("NaN longitude did not raise")
    except EngineKernelException as exc:
        if not str(exc):
            failures.append("NaN longitude exception had no message")
    try:
        await SpatialRoutingFrictionResolver().run(
            [_point(1, 0.0, 0.0, 0.0), _point(2, 0.0, 1.0, 1.2)]
        )
        failures.append("congestion above 1 did not raise")
    except EngineKernelException as exc:
        if not str(exc):
            failures.append("congestion exception had no message")

    cached = SpatialRoutingFrictionResolver()
    matrix = [
        _point(1, 40.0, -74.0, 0.2),
        _point(2, 40.5, -73.5, 0.4),
        _point(3, 41.0, -73.0, 0.1),
    ]
    first = await cached.run(matrix)
    second = await cached.run(matrix)
    if int(second["cache_hits"]) <= int(first["cache_hits"]):
        failures.append("warm edge cache did not increase hits")


def _matrix(rng: random.Random) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(6):
        rows.append(
            _point(
                index + 1,
                35.0 + index * 0.3 + rng.random() * 0.01,
                -100.0 + index * 0.3,
                rng.random(),
            )
        )
    return rows


async def _benchmark(matrix: list[dict[str, object]]) -> list[float]:
    resolver = SpatialRoutingFrictionResolver()
    samples: list[float] = []
    for _ in range(ITERATIONS):
        started = time.perf_counter_ns()
        await resolver.run(matrix)
        samples.append((time.perf_counter_ns() - started) / 1_000.0)
    return samples


def main() -> int:
    """Print one status dict and exit 0 only when every check passes."""
    logging.getLogger("spatial_routing_friction_resolver").setLevel(logging.ERROR)
    failures: list[str] = []
    asyncio.run(_checks(failures))
    probe = SpatialRoutingFrictionResolver()
    started = time.perf_counter_ns()
    asyncio.run(
        probe.run(
            [
                _point(1, 40.0, -74.0, 0.2),
                _point(2, 40.4, -73.8, 0.3),
            ]
        )
    )
    latency_us = (time.perf_counter_ns() - started) / 1_000.0
    matrix = _matrix(random.Random(SEED))
    tracemalloc.start()
    samples = asyncio.run(_benchmark(matrix))
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    status = {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "latency_us": round(latency_us, 3),
        "memory_peak_bytes": peak,
        "benchmark_iterations": len(samples),
        "benchmark_avg_us": round(sum(samples) / len(samples), 3),
        "benchmark_p99_us": round(_percentile(samples, 0.99), 3),
    }
    sys.stdout.write(json.dumps(status) + "\n")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
