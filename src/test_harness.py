"""Deterministic checks and a short latency benchmark for the friction router."""

from __future__ import annotations

import asyncio
import math
import random
import sys
import time
import tracemalloc
from pathlib import Path


def _load():
    root = Path(__file__).resolve().parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    import main

    return main


def _percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    index = math.ceil(fraction * len(ordered)) - 1
    index = max(0, min(index, len(ordered) - 1))
    return ordered[index]


async def _checks(mod, failures: list[str]) -> None:
    rng = random.Random(28000)
    engine = mod.SpatialRoutingFrictionResolver(pool_size=2)
    distance = mod.haversine_m(0.0, 0.0, 1.0, 0.0)
    expected = math.pi / 180.0 * mod.EARTH_RADIUS_M
    if abs(distance - expected) > 1.0:
        failures.append("haversine missed the one-degree latitude arc")
    if mod.turn_delta_deg(0.0, 180.0) != 180.0:
        failures.append("turn delta missed a reversal")
    if mod.turn_delta_deg(10.0, 350.0) != 20.0:
        failures.append("turn delta missed the short wrap")

    low = engine.plan(
        [
            engine.pack_waypoint(0.0, 0.0, 0.0, 0),
            engine.pack_waypoint(0.0, 1.0, 0.0, 1),
        ]
    )
    high = engine.plan(
        [
            engine.pack_waypoint(0.0, 0.0, 0.0, 0),
            engine.pack_waypoint(0.0, 1.0, 100.0, 1),
        ]
    )
    if high["total_friction"] <= low["total_friction"]:
        failures.append("congestion did not increase friction")

    hairpin = engine.plan(
        [
            engine.pack_waypoint(0.0, 0.0, 5.0, 0),
            engine.pack_waypoint(0.0, 0.1, 5.0, 1),
            engine.pack_waypoint(0.0, -1.0, 5.0, 2),
        ]
    )
    turns = [float(edge["turn_deg"]) for edge in hairpin["edges"]]
    if max(turns) < 150.0:
        failures.append("hairpin turn was not scored")

    jagged = engine.plan(
        [
            engine.pack_waypoint(40.0, -74.0, 10.0, 0),
            engine.pack_waypoint(40.0, -74.0, 11.0, 1),
            engine.pack_waypoint(40.000001, -74.0, 9.0, 2),
            engine.pack_waypoint(41.0, -73.0, 20.0, 3),
        ]
    )
    if jagged["duplicate_clamps"] != 1:
        failures.append("consecutive duplicate was not clamped")
    if jagged["zero_length_segments"] < 1:
        failures.append("sub-meter segment was not marked zero-length")
    if len(jagged["route"]) != 3:
        failures.append("duplicate removal did not shorten the route")
    epochs = [int(point["seq"]) for point in jagged["route"]]
    if epochs[0] != 0:
        failures.append("route did not keep the depot as the start")

    try:
        engine.plan(
            [
                engine.pack_waypoint(0.0, 0.0, 1.0, 0),
                engine.pack_waypoint(91.0, 0.0, 1.0, 1),
            ]
        )
        failures.append("latitude 91 did not raise")
    except mod.EngineKernelException as exc:
        if not str(exc):
            failures.append("kernel exception had no message")
    try:
        engine.plan(
            [
                engine.pack_waypoint(0.0, 0.0, 1.0, 0),
                engine.pack_waypoint(0.0, float("nan"), 1.0, 1),
            ]
        )
        failures.append("NaN longitude did not raise")
    except mod.EngineKernelException as exc:
        if not str(exc):
            failures.append("kernel exception had no message")
    try:
        engine.plan(
            [
                engine.pack_waypoint(1.0, 2.0, 3.0, 0),
                engine.pack_waypoint(1.0, 2.0, 3.0, 1),
            ]
        )
        failures.append("collapsed route did not raise")
    except mod.EngineKernelException as exc:
        if not str(exc):
            failures.append("kernel exception had no message")
    try:
        engine.plan([engine.pack_waypoint(1.0, 2.0, 3.0, 0)])
        failures.append("single waypoint did not raise")
    except mod.EngineKernelException as exc:
        if not str(exc):
            failures.append("kernel exception had no message")

    cached = mod.SpatialRoutingFrictionResolver()
    matrix = [
        cached.pack_waypoint(40.0, -74.0, 10.0, 0),
        cached.pack_waypoint(40.5, -73.5, 15.0, 1),
        cached.pack_waypoint(41.0, -73.0, 12.0, 2),
    ]
    first = await cached.resolve(matrix)
    second = await cached.resolve(matrix)
    if first["cache_hits"] != 0:
        failures.append("cold cache reported hits")
    if second["cache_hits"] != second["edge_count"]:
        failures.append("warm cache missed a repeated edge")
    if first["topic"] != "routing.edges":
        failures.append("edge topic mismatch")

    pool = mod.SpatialRoutingFrictionResolver(pool_size=2)
    matrices = []
    for job in range(3):
        lat = 10.0 + job
        lon = -20.0 + rng.random()
        matrices.append(
            [
                pool.pack_waypoint(lat, lon, 8.0, 0),
                pool.pack_waypoint(lat + 0.4, lon + 0.2, 9.0, 1),
                pool.pack_waypoint(lat + 0.2, lon + 0.6, 7.0, 2),
            ]
        )
    bad = [
        pool.pack_waypoint(0.0, 0.0, 1.0, 0),
        pool.pack_waypoint(0.0, 181.0, 1.0, 1),
    ]
    pooled = await pool.run_worker(matrices + [bad])
    if pooled["job_count"] != 4:
        failures.append("worker pool dropped a job")
    if any(route.get("rejected") for route in pooled["routes"][:3]):
        failures.append("valid matrix was rejected by the pool")
    if not pooled["routes"][3].get("rejected"):
        failures.append("corrupt matrix was not rejected by the pool")
    if pooled["routes"][0]["route"][0]["lat"] != 10.0:
        failures.append("worker pool reordered jobs")

    empty = mod.SpatialRoutingFrictionResolver()
    try:
        await empty.run_worker([])
        failures.append("empty job set did not raise")
    except mod.EngineKernelException as exc:
        if not str(exc):
            failures.append("kernel exception had no message")


def _benchmark(mod) -> list[float]:
    rng = random.Random(28000)
    engine = mod.SpatialRoutingFrictionResolver()
    matrix = []
    for index in range(6):
        matrix.append(
            engine.pack_waypoint(
                35.0 + rng.random(),
                -100.0 + rng.random() * 20.0,
                10.0 + rng.random() * 40.0,
                index,
            )
        )
    samples: list[float] = []
    for _ in range(8_000):
        started = time.perf_counter_ns()
        engine.plan(matrix)
        samples.append((time.perf_counter_ns() - started) / 1_000.0)
    return samples


def main() -> int:
    mod = _load()
    failures: list[str] = []
    asyncio.run(_checks(mod, failures))
    engine = mod.SpatialRoutingFrictionResolver()
    probe = [
        engine.pack_waypoint(40.0, -74.0, 10.0, 0),
        engine.pack_waypoint(40.4, -73.8, 12.0, 1),
    ]
    started = time.perf_counter_ns()
    engine.plan(probe)
    latency_us = (time.perf_counter_ns() - started) / 1_000.0
    tracemalloc.start()
    samples = _benchmark(mod)
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
    sys.stdout.write(repr(status) + "\n")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
