"""Waypoint friction router.

Distance is haversine meters. Friction adds a congestion coefficient derived
from demand and a turn penalty derived from the change in initial bearing.
The worker pool publishes edge records on an in-process queue that stands in
for the Kafka topic ``routing.edges``. Recent edge costs sit in a fixed ring.
Redis would be that cache once the process is no longer alone; the ring skips
the serialization hop. TimescaleDB is the intended store for the route dict
this process returns and does not open a connection itself.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import statistics
import struct
import sys
from collections import deque
from typing import Final

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("routing.friction")

TOPIC: Final[str] = "routing.edges"
WAYPOINT: Final[struct.Struct] = struct.Struct("<dddI")
EARTH_RADIUS_M: Final[float] = 6_371_000.0
TURN_WEIGHT_M: Final[float] = 250.0
ZERO_LENGTH_M: Final[float] = 1.0
DEMAND_REF: Final[float] = 100.0
RING_CAPACITY: Final[int] = 1024
POOL_SIZE: Final[int] = 4


class EngineKernelException(Exception):
    """Raised when a waypoint matrix cannot be routed."""


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters."""
    lat1_r = math.radians(lat1)
    lat2_r = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    hav = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2.0) ** 2
    )
    hav = min(1.0, max(0.0, hav))
    return 2.0 * EARTH_RADIUS_M * math.atan2(math.sqrt(hav), math.sqrt(1.0 - hav))


def initial_bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing in degrees, normalized to [0, 360)."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    y_comp = math.sin(dlon) * math.cos(phi2)
    x_comp = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(
        phi2
    ) * math.cos(dlon)
    degrees = math.degrees(math.atan2(y_comp, x_comp))
    return math.fmod(degrees + 360.0, 360.0)


def turn_delta_deg(inbound: float, outbound: float) -> float:
    """Smaller angle between two bearings, in degrees."""
    diff = abs(outbound - inbound) % 360.0
    if diff > 180.0:
        return 360.0 - diff
    return diff


def _mean_stdev(samples: list[float]) -> tuple[float, float]:
    if not samples:
        return 0.0, 0.0
    center = statistics.fmean(samples)
    if len(samples) == 1:
        return center, 0.0
    return center, statistics.pstdev(samples)


class SpatialRoutingFrictionResolver:
    """Order waypoints by friction and cache recent edge costs in a ring."""

    def __init__(
        self,
        pool_size: int = POOL_SIZE,
        ring_capacity: int = RING_CAPACITY,
    ) -> None:
        if pool_size < 1:
            raise EngineKernelException("pool size must be positive")
        if ring_capacity < 1:
            raise EngineKernelException("ring capacity must be positive")
        self._pool_size = pool_size
        self._lock = asyncio.Lock()
        self._edges: asyncio.Queue[bytes] = asyncio.Queue(maxsize=256)
        self._cache: deque[tuple[tuple[int, int, int, int], float]] = deque(
            maxlen=ring_capacity
        )
        self._cache_index: dict[tuple[int, int, int, int], float] = {}
        self._published = 0

    def pack_waypoint(self, lat: float, lon: float, demand: float, seq: int) -> bytes:
        """Pack one waypoint. Layout: lat, lon, demand, sequence."""
        if seq < 0:
            raise EngineKernelException(f"negative waypoint sequence {seq}")
        try:
            return WAYPOINT.pack(float(lat), float(lon), float(demand), seq)
        except (struct.error, OverflowError) as exc:
            raise EngineKernelException("waypoint pack failed") from exc

    def unpack_waypoint(self, payload: bytes) -> dict[str, float]:
        """Unpack one waypoint."""
        if len(payload) != WAYPOINT.size:
            raise EngineKernelException(
                f"waypoint length {len(payload)} != {WAYPOINT.size}"
            )
        try:
            lat, lon, demand, seq = WAYPOINT.unpack(payload)
        except struct.error as exc:
            raise EngineKernelException("waypoint unpack failed") from exc
        return {
            "lat": float(lat),
            "lon": float(lon),
            "demand": float(demand),
            "seq": float(seq),
        }

    def _validate(self, point: dict[str, float]) -> None:
        lat = point["lat"]
        lon = point["lon"]
        demand = point["demand"]
        if (
            not math.isfinite(lat)
            or not math.isfinite(lon)
            or not math.isfinite(demand)
        ):
            raise EngineKernelException(
                f"non-finite waypoint lat={lat} lon={lon} demand={demand}"
            )
        if lat < -90.0 or lat > 90.0:
            raise EngineKernelException(f"latitude out of range {lat}")
        if lon < -180.0 or lon > 180.0:
            raise EngineKernelException(f"longitude out of range {lon}")
        if demand < 0.0:
            raise EngineKernelException(f"negative demand {demand}")

    def _dedupe(
        self, points: list[dict[str, float]]
    ) -> tuple[list[dict[str, float]], int]:
        kept = [points[0]]
        clamps = 0
        for point in points[1:]:
            prior = kept[-1]
            if point["lat"] == prior["lat"] and point["lon"] == prior["lon"]:
                clamps += 1
                LOGGER.warning(
                    "duplicate waypoint clamped lat=%.6f lon=%.6f seq=%d",
                    point["lat"],
                    point["lon"],
                    int(point["seq"]),
                )
                continue
            kept.append(point)
        return kept, clamps

    def _edge_key(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
    ) -> tuple[int, int, int, int]:
        scale = 100_000.0
        return (
            int(round(lat1 * scale)),
            int(round(lon1 * scale)),
            int(round(lat2 * scale)),
            int(round(lon2 * scale)),
        )

    def _score_edge(
        self,
        previous: dict[str, float] | None,
        origin: dict[str, float],
        dest: dict[str, float],
    ) -> dict[str, object]:
        distance = haversine_m(origin["lat"], origin["lon"], dest["lat"], dest["lon"])
        zero_length = distance < ZERO_LENGTH_M
        if zero_length:
            LOGGER.warning(
                "zero-length segment lat=%.6f lon=%.6f distance_m=%.6f",
                dest["lat"],
                dest["lon"],
                distance,
            )
            distance_term = 0.0
            turn = 0.0
        else:
            distance_term = distance
            if previous is None:
                turn = 0.0
            else:
                inbound = initial_bearing_deg(
                    previous["lat"],
                    previous["lon"],
                    origin["lat"],
                    origin["lon"],
                )
                outbound = initial_bearing_deg(
                    origin["lat"],
                    origin["lon"],
                    dest["lat"],
                    dest["lon"],
                )
                turn = turn_delta_deg(inbound, outbound)
        congestion = min(1.5, max(0.0, dest["demand"] / DEMAND_REF))
        friction = distance_term * (1.0 + congestion) + (turn / 180.0) * TURN_WEIGHT_M
        return {
            "from_lat": origin["lat"],
            "from_lon": origin["lon"],
            "to_lat": dest["lat"],
            "to_lon": dest["lon"],
            "distance_m": distance,
            "turn_deg": turn,
            "congestion": congestion,
            "friction": friction,
            "zero_length": zero_length,
            "key": self._edge_key(
                origin["lat"], origin["lon"], dest["lat"], dest["lon"]
            ),
        }

    def _order(
        self, points: list[dict[str, float]]
    ) -> tuple[list[dict[str, float]], list[dict[str, object]]]:
        remaining = list(points[1:])
        route = [points[0]]
        edges: list[dict[str, object]] = []
        while remaining:
            origin = route[-1]
            previous = route[-2] if len(route) > 1 else None
            best_index = 0
            best_edge: dict[str, object] | None = None
            best_cost = math.inf
            for index, candidate in enumerate(remaining):
                edge = self._score_edge(previous, origin, candidate)
                cost = float(edge["friction"])
                if cost < best_cost:
                    best_cost = cost
                    best_index = index
                    best_edge = edge
            if best_edge is None:
                raise EngineKernelException("route search failed")
            route.append(remaining.pop(best_index))
            edges.append(best_edge)
        return route, edges

    def plan(self, payloads: list[bytes]) -> dict[str, object]:
        """Validate and order one matrix. Does not touch the shared cache."""
        if len(payloads) < 2:
            raise EngineKernelException("waypoint matrix needs at least two points")
        points = [self.unpack_waypoint(payload) for payload in payloads]
        for point in points:
            self._validate(point)
        deduped, duplicate_clamps = self._dedupe(points)
        if len(deduped) < 2:
            raise EngineKernelException("route collapsed below two waypoints")
        ordered, edges = self._order(deduped)
        frictions = [float(edge["friction"]) for edge in edges]
        mean_friction, stdev_friction = _mean_stdev(frictions)
        total = math.fsum(frictions)
        public_edges: list[dict[str, object]] = []
        for edge in edges:
            raw_key = edge["key"]
            if not isinstance(raw_key, tuple):
                raise EngineKernelException("edge key missing")
            public_edges.append(
                {
                    "from_lat": edge["from_lat"],
                    "from_lon": edge["from_lon"],
                    "to_lat": edge["to_lat"],
                    "to_lon": edge["to_lon"],
                    "distance_m": edge["distance_m"],
                    "turn_deg": edge["turn_deg"],
                    "congestion": edge["congestion"],
                    "friction": edge["friction"],
                    "zero_length": edge["zero_length"],
                    "key": [int(part) for part in raw_key],
                }
            )
        route_view = [
            {
                "lat": point["lat"],
                "lon": point["lon"],
                "demand": point["demand"],
                "seq": int(point["seq"]),
            }
            for point in ordered
        ]
        return {
            "topic": TOPIC,
            "route": route_view,
            "edges": public_edges,
            "total_friction": total,
            "mean_edge_friction": mean_friction,
            "stdev_edge_friction": stdev_friction,
            "duplicate_clamps": duplicate_clamps,
            "zero_length_segments": sum(
                1 for edge in edges if bool(edge["zero_length"])
            ),
            "edge_count": len(public_edges),
        }

    def _remember_locked(self, key: tuple[int, int, int, int], cost: float) -> None:
        if key in self._cache_index:
            return
        if self._cache.maxlen is not None and len(self._cache) >= self._cache.maxlen:
            old_key, _old_cost = self._cache.popleft()
            self._cache_index.pop(old_key, None)
        self._cache.append((key, cost))
        self._cache_index[key] = cost

    async def _publish(self, payload: bytes) -> None:
        if self._edges.full():
            try:
                self._edges.get_nowait()
            except asyncio.QueueEmpty:
                return
        await self._edges.put(payload)
        self._published += 1

    async def resolve(self, payloads: list[bytes]) -> dict[str, object]:
        """Plan one matrix, update the ring, and publish edges."""
        planned = self.plan(payloads)
        raw_edges = planned["edges"]
        if not isinstance(raw_edges, list):
            raise EngineKernelException("route edges missing")
        edges: list[dict[str, object]] = raw_edges
        hits = 0
        async with self._lock:
            for edge in edges:
                raw_key = edge["key"]
                if not isinstance(raw_key, list) or len(raw_key) != 4:
                    raise EngineKernelException("edge key missing")
                key = (
                    int(raw_key[0]),
                    int(raw_key[1]),
                    int(raw_key[2]),
                    int(raw_key[3]),
                )
                cached = self._cache_index.get(key)
                if cached is not None:
                    hits += 1
                    edge["friction"] = cached
                    edge["cache_hit"] = True
                else:
                    self._remember_locked(key, float(edge["friction"]))
                    edge["cache_hit"] = False
            cache_depth = len(self._cache)
        for edge in edges:
            raw_key = edge["key"]
            if not isinstance(raw_key, list):
                raise EngineKernelException("edge key missing")
            blob = WAYPOINT.pack(
                float(edge["from_lat"]),
                float(edge["from_lon"]),
                float(edge["friction"]),
                int(raw_key[0]) & 0xFFFFFFFF,
            )
            await self._publish(blob)
        frictions = [float(edge["friction"]) for edge in edges]
        mean_friction, stdev_friction = _mean_stdev(frictions)
        planned["cache_hits"] = hits
        planned["cache_depth"] = cache_depth
        planned["mean_edge_friction"] = mean_friction
        planned["stdev_edge_friction"] = stdev_friction
        planned["total_friction"] = math.fsum(frictions)
        planned["published_total"] = self._published
        decoded: dict[str, object] = json.loads(json.dumps(planned))
        LOGGER.info(
            "route topic=%s edges=%d friction=%.3f cache_hits=%d duplicates=%d "
            "zero_length=%d",
            TOPIC,
            decoded["edge_count"],
            decoded["total_friction"],
            decoded["cache_hits"],
            decoded["duplicate_clamps"],
            decoded["zero_length_segments"],
        )
        return decoded

    async def run_worker(self, matrices: list[list[bytes]]) -> dict[str, object]:
        """Solve matrices on a small asyncio worker pool."""
        if not matrices:
            raise EngineKernelException("empty job set")
        jobs: asyncio.Queue[tuple[int, list[bytes]] | None] = asyncio.Queue()
        for index, matrix in enumerate(matrices):
            await jobs.put((index, matrix))
        workers = min(self._pool_size, len(matrices))
        results: list[dict[str, object] | None] = [None] * len(matrices)

        async def _consume() -> None:
            while True:
                item = await jobs.get()
                try:
                    if item is None:
                        return
                    index, matrix = item
                    try:
                        results[index] = await self.resolve(matrix)
                    except EngineKernelException as exc:
                        LOGGER.warning("route rejected error=%s", exc)
                        results[index] = {
                            "topic": TOPIC,
                            "rejected": True,
                            "error": str(exc),
                            "route": [],
                            "edge_count": 0,
                            "total_friction": 0.0,
                        }
                finally:
                    jobs.task_done()

        tasks = [asyncio.create_task(_consume()) for _ in range(workers)]
        await jobs.join()
        for _ in range(workers):
            await jobs.put(None)
        await asyncio.gather(*tasks)
        routes = [row for row in results if row is not None]
        friction_values = [
            float(row["total_friction"]) for row in routes if not row.get("rejected")
        ]
        mean_friction, stdev_friction = _mean_stdev(friction_values)
        report = {
            "topic": TOPIC,
            "routes": routes,
            "job_count": len(matrices),
            "mean_total_friction": mean_friction,
            "stdev_total_friction": stdev_friction,
            "queue_depth": self._edges.qsize(),
            "published_total": self._published,
        }
        return json.loads(json.dumps(report))


async def _scenario() -> None:
    engine = SpatialRoutingFrictionResolver(pool_size=2)
    downtown = [
        engine.pack_waypoint(40.7128, -74.0060, 30.0, 0),
        engine.pack_waypoint(40.7128, -74.0060, 30.0, 1),
        engine.pack_waypoint(40.712801, -74.0060, 12.0, 2),
        engine.pack_waypoint(40.7306, -73.9352, 55.0, 3),
        engine.pack_waypoint(40.6782, -73.9442, 18.0, 4),
        engine.pack_waypoint(40.8448, -73.8648, 22.0, 5),
    ]
    harbor = [
        engine.pack_waypoint(40.6892, -74.0445, 10.0, 0),
        engine.pack_waypoint(40.7484, -73.9857, 16.0, 1),
        engine.pack_waypoint(40.7061, -73.9969, 12.0, 2),
    ]
    report = await engine.run_worker([downtown, harbor])
    sys.stdout.write(json.dumps(report) + "\n")


if __name__ == "__main__":
    asyncio.run(_scenario())
