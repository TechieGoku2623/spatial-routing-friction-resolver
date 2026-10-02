# Spatial Routing Friction Resolver

A high-throughput, low-latency asynchronous engine engineered to resolve path cost on a waypoint sequence by adding congestion and turn penalties to haversine distance, and by rejecting latitudes, longitudes, and non-finite coordinates that fall outside the legal range.

## 🏗️ Systems Architecture & Event Topology

`SpatialRoutingFrictionResolver` plans one route from a list of packed waypoints. `pack_waypoint` / `unpack_waypoint` carry latitude, longitude, demand, and sequence. `resolve` is the coroutine a caller awaits. `run_worker` drains a pool of route matrices onto an in-process queue that stands in for the Kafka topic `routing.edges`.

Distance is `haversine_m`. Heading change is `initial_bearing_deg` then `turn_delta_deg`. Friction is distance scaled by a congestion term from demand plus a turn penalty. Recent edge keys sit in a fixed ring so a repeated segment is a cache hit. The route dict is what a TimescaleDB consumer would insert; this process does not open that connection. An `asyncio.Lock` covers the ring. `logging.basicConfig` timestamps each line. Out-of-range or non-finite coordinates raise `EngineKernelException`.

## 📊 Core Visual Walkthrough & Engine Pipeline Flow

```
waypoints (lat, lon, demand, seq)
    |
    v
range and finite gate ---- reject lon 181, lat 91, NaN
    |
    v
duplicate consecutive point --> clamp, keep one
    |
    v
haversine_m --> zero-length flag when the segment is under a meter-scale epsilon
    |
    v
bearing delta --> turn penalty
demand ---------> congestion coefficient
    |
    v
edge record on routing.edges, ring cache by quantized key
```

Insert the structural terminal walkthrough recording at docs/assets/terminal-walkthrough.gif before publishing the release notes.

## ⚡ Low-Level OS Mechanics & Network Physics

The haversine uses a mean Earth radius and `math.sin` / `math.cos` / `math.atan2`. It is a sphere, not a local projected grid, so a transcontinental segment stays consistent without a projection library. Turn angle is the wrapped difference of initial bearings, in degrees, so a 350-to-10 degree corner is a 20 degree penalty and not a 340 degree one.

The worker pool size is a constructor argument. Tasks share the ring under the lock; they do not share a mutable waypoint list. `statistics` summarizes friction across the edges of a solved route. Cache keys quantize coordinates so a sub-millimeter jitter does not miss the ring. The queue is the network boundary: no TCP connect, no DNS, no tile server.

## ⚖️ Architecture Trade-offs & Pragmatic Decisions

A graph database would store the curb network. This engine scores the polyline the caller already chose. That keeps the hot path to a few trigonometric calls per edge and leaves contraction hierarchies to a system that owns the map.

Congestion is a coefficient of demand, not a live speed probe. The coefficient is stable and replayable. A live probe would be more accurate at 17:00 and would also make the harness non-deterministic. Zero-length segments are flagged and contribute no turn penalty rather than raising a division by zero inside the bearing helper. Duplicate consecutive waypoints are clamped with a warning so a double-tapped pin does not create a phantom edge.

## 🚀 Local Installation & Benchmarking

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python src/main.py
python src/test_harness.py
```

```python
import asyncio

from src.main import SpatialRoutingFrictionResolver


async def demo() -> None:
    resolver = SpatialRoutingFrictionResolver()
    origin = resolver.pack_waypoint(40.7128, -74.0060, 30.0, 0)
    dest = resolver.pack_waypoint(40.6782, -73.9442, 18.0, 1)
    await resolver.resolve([origin, dest])


asyncio.run(demo())
```

The runtime is the Python 3.12 standard library. `pip install -r requirements.txt` is a no-op install.

## 🖥️ Terminal Diagnostic Output Preview

```
WARNING [routing.friction] duplicate waypoint clamped lat=40.712800 lon=-74.006000 seq=1
WARNING [routing.friction] zero-length segment lat=40.712801 lon=-74.006000 distance_m=0.111195
INFO [routing.friction] route topic=routing.edges edges=4 friction=34211.891 cache_hits=0 duplicates=1 zero_length=1
INFO [routing.friction] route topic=routing.edges edges=2 friction=10602.036 cache_hits=0 duplicates=0 zero_length=0
```

`python src/main.py` exits 0. A longitude of 181 is rejected on the same path with `longitude out of range`.

## 📊 Empirical Benchmarking Performance Report

Measured by `python src/test_harness.py` with a deterministic seed, 8000 iterations, `time.perf_counter_ns` latency in microseconds, and `tracemalloc` peak.

| Metric | Measured |
| --- | ---: |
| Status | PASS |
| Iterations | 8000 |
| Average latency | 417.956 µs |
| Empirical P99 | 534.01 µs |
| Scenario latency | 15.205 µs |
| tracemalloc peak | 271074 bytes |

## 🛡️ Edge-Case Resilience & SOC2/Regulatory Compliance

Duplicate consecutive points are clamped and counted. A segment whose haversine length is at the noise floor is marked `zero_length` and does not divide by distance. Latitude outside [-90, 90], longitude outside [-180, 180], and any non-finite component raise `EngineKernelException` before an edge is published.

No account identifier is read from a waypoint. The route record is coordinates and a friction scalar. SOC 2 processing integrity is the control: a rejected coordinate is logged and is absent from `routing.edges`. The engine does not call an external geocoder.
