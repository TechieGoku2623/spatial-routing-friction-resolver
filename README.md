# Spatial Routing Friction Resolver

> Prices waypoint edges with haversine distance, congestion, and turn penalty, then returns the lowest-friction path from Dijkstra.

<p>
  <a href="https://github.com/TechieGoku2623/spatial-routing-friction-resolver/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/TechieGoku2623/spatial-routing-friction-resolver/actions/workflows/ci.yml/badge.svg"></a>
  <img alt="Python 3.12" src="https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white">
  <img alt="MIT license" src="https://img.shields.io/badge/license-MIT-2ea043">
</p>

| | |
| --- | --- |
| **Website** | https://github.com/TechieGoku2623/spatial-routing-friction-resolver |
| **Topics** | `python` `asyncio` `logistics` `routing` `geospatial` `dijkstra` |

## Walkthrough

Three recordings from this repository. Each one is the command in the frame, not a drawing.

### Engine

`python3 -m spatial_routing_friction_resolver`

![Engine run](docs/assets/terminal-walkthrough.gif)

Out-of-range coordinates raise. Duplicate points and zero-length segments are dropped instead of dividing by zero.

### Benchmark

`python3 -m spatial_routing_friction_resolver.harness`

![Benchmark harness](docs/assets/benchmark-walkthrough.gif)

5000 iterations of a six-waypoint matrix. `random.Random(28000)`. The frame ends on the status line and `echo $?`.

### Tests

`python3 -m unittest discover -s tests -v`

![Unit tests](docs/assets/tests-walkthrough.gif)

Wire round-trip, the happy path, and both edge cases below.

## Pipeline

```
waypoints
  |
  v
validate lat/lon
  |
  v
haversine * (1 + congestion) + turn penalty
  |
  v
Dijkstra
  |
  v
{path, total_friction_m, edges}
```

## Quick start

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
python -m spatial_routing_friction_resolver
python -m spatial_routing_friction_resolver.harness
python -m unittest discover -s tests -v
```

Python 3.12. The runtime is the standard library. `black` and `flake8` are the `dev` extra.

## Use it

```python
import asyncio

from spatial_routing_friction_resolver import SpatialRoutingFrictionResolver


async def demo() -> None:
    resolver = SpatialRoutingFrictionResolver()
    report = await resolver.run(
        [
            {"id": 1, "lat": 40.7128, "lon": -74.0060, "congestion": 0.2},
            {"id": 2, "lat": 40.6782, "lon": -73.9442, "congestion": 0.15},
        ]
    )
    path = report["path"]
    friction = float(report["total_friction_m"])
    print(path, friction)


asyncio.run(demo())
```

## Bounds

| | |
| --- | ---: |
| Iterations | 5000 |
| Average | 965.438 µs |
| P99 | 1171.283 µs |
| tracemalloc peak | 200508 bytes |

Figures are from the harness on the machine that published them. A later host moves the microseconds. The pass/fail result does not.

## What it refuses

- Latitude outside [-90, 90], longitude outside [-180, 180], or a non-finite coordinate raises `EngineKernelException`.
- A duplicate consecutive point and a zero-length segment are omitted. The solver never divides by a zero distance.

Process-local edge cache. A Redis ring would be the distributed cache. It is not used on this path.

## Tree

```
src/spatial_routing_friction_resolver/
  engine.py       kernel
  wire.py         struct frames
  harness.py      benchmark
  __main__.py     demo entry
tests/test_engine.py
Dockerfile        non-root, uid 10001
```
