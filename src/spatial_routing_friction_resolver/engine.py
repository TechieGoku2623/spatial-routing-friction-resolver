"""Haversine friction and a turn-aware Dijkstra search."""

from __future__ import annotations

import asyncio
import heapq
import json
import logging
import math
import statistics
import struct
from collections import deque
from typing import Mapping, Sequence

from .exceptions import EngineKernelException
from .wire import FORMAT, pack_waypoint, unpack_waypoint

LOGGER = logging.getLogger(__name__)

TOPIC: str = "routing.edges"
EARTH_RADIUS_M: float = 6_371_000.0
TURN_PENALTY_M: float = 25.0
ZERO_LENGTH_M: float = 1e-9
CACHE_CAPACITY: int = 256
_SENTINEL: int = -1


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters on a sphere of radius 6371000."""
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


def bearing_rad(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing in radians. Callers must not pass a zero-length pair."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    y_comp = math.sin(dlon) * math.cos(phi2)
    x_comp = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(
        phi2
    ) * math.cos(dlon)
    return math.atan2(y_comp, x_comp)


def _heading_change(inbound: float, outbound: float) -> float:
    delta = outbound - inbound
    return abs(math.atan2(math.sin(delta), math.cos(delta)))


class _Node:
    """One waypoint after the wire decode."""

    __slots__ = ("ident", "lat", "lon", "congestion")

    def __init__(self, ident: int, lat: float, lon: float, congestion: float) -> None:
        self.ident = ident
        self.lat = lat
        self.lon = lon
        self.congestion = congestion


def _as_int(record: Mapping[str, object], key: str) -> int:
    raw = record[key]
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise EngineKernelException(f"{key} must be an integer")
    return raw


def _as_float(record: Mapping[str, object], key: str) -> float:
    raw = record[key]
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise EngineKernelException(f"{key} must be numeric")
    return float(raw)


def _check_size(payload: bytes) -> None:
    expected = struct.calcsize(FORMAT)
    if len(payload) != expected:
        raise EngineKernelException(f"waypoint length {len(payload)} != {expected}")


def _json_object(report: dict[str, object]) -> dict[str, object]:
    decoded = json.loads(json.dumps(report, allow_nan=False))
    if not isinstance(decoded, dict):
        raise EngineKernelException("report was not a JSON object")
    return decoded


class SpatialRoutingFrictionResolver:
    """Lowest-friction path on the complete waypoint graph."""

    def __init__(self, cache_capacity: int = CACHE_CAPACITY) -> None:
        if cache_capacity < 1:
            raise EngineKernelException("cache capacity must be positive")
        self._cache_capacity = cache_capacity
        self._lock = asyncio.Lock()
        self._inbound: asyncio.Queue[bytes] = asyncio.Queue()
        self._outbound: asyncio.Queue[bytes] = asyncio.Queue(maxsize=cache_capacity)
        self._cache: dict[tuple[int, int], float] = {}
        self._order: deque[tuple[int, int]] = deque()
        self._cache_hits = 0
        self._zero_skips = 0
        self._frame_size = struct.calcsize(FORMAT)

    async def run(self, records: Sequence[Mapping[str, object]]) -> dict[str, object]:
        """Solve one waypoint set and return a JSON-serializable route."""
        batch = list(records)
        if len(batch) < 2:
            raise EngineKernelException("waypoint matrix needs at least two points")
        async with self._lock:
            nodes: list[_Node] = []
            for record in batch:
                payload = pack_waypoint(
                    _as_int(record, "id"),
                    _as_float(record, "lat"),
                    _as_float(record, "lon"),
                    _as_float(record, "congestion"),
                )
                _check_size(payload)
                if len(payload) != self._frame_size:
                    raise EngineKernelException("frame size mismatch")
                await self._inbound.put(payload)
                frame = unpack_waypoint(await self._inbound.get())
                nodes.append(self.ingest(frame))
            report = self._solve(nodes)
            for edge in report["edges"]:
                if not isinstance(edge, dict):
                    raise EngineKernelException("edge record missing")
                blob = struct.pack(
                    "<IId",
                    int(edge["from_id"]),
                    int(edge["to_id"]),
                    float(edge["friction_m"]),
                )
                await self._publish(blob)
        LOGGER.info(
            "topic=%s path=%s total_friction_m=%.3f edges=%d duplicates=%d "
            "zero_length_dropped=%d cache_hits=%d",
            TOPIC,
            report["path"],
            report["total_friction_m"],
            len(report["edges"]),
            report["duplicate_dropped"],
            report["zero_length_dropped"],
            report["cache_hits"],
        )
        return _json_object(report)

    def ingest(self, frame: Mapping[str, object]) -> _Node:
        """Validate one unpacked waypoint. ``run`` holds the lock."""
        ident = int(frame["id"])
        lat = float(frame["lat"])
        lon = float(frame["lon"])
        congestion = float(frame["congestion"])
        if math.isnan(lat) or math.isnan(lon) or math.isnan(congestion):
            raise EngineKernelException(
                f"NaN waypoint id={ident} lat={lat} lon={lon} congestion={congestion}"
            )
        if (
            not math.isfinite(lat)
            or not math.isfinite(lon)
            or not math.isfinite(congestion)
        ):
            raise EngineKernelException(
                f"non-finite waypoint id={ident} lat={lat} lon={lon}"
            )
        if lat < -90.0 or lat > 90.0:
            raise EngineKernelException(f"latitude out of range {lat}")
        if lon < -180.0 or lon > 180.0:
            raise EngineKernelException(f"longitude out of range {lon}")
        if congestion < 0.0 or congestion > 1.0:
            raise EngineKernelException(f"congestion out of range {congestion}")
        return _Node(ident, lat, lon, congestion)

    def _remember(self, key: tuple[int, int], cost: float) -> None:
        if key in self._cache:
            self._cache[key] = cost
            return
        if len(self._order) >= self._cache_capacity:
            old = self._order.popleft()
            self._cache.pop(old, None)
        self._order.append(key)
        self._cache[key] = cost

    def _base_cost(self, origin: _Node, dest: _Node, distance: float) -> float:
        congestion_avg = 0.5 * (origin.congestion + dest.congestion)
        base = distance * (1.0 + congestion_avg)
        key = (origin.ident, dest.ident)
        cached = self._cache.get(key)
        if cached is None:
            self._remember(key, base)
            return base
        self._cache_hits += 1
        return cached

    def _friction(
        self,
        prev: _Node | None,
        origin: _Node,
        dest: _Node,
        distance: float,
    ) -> tuple[float, float, float]:
        base = self._base_cost(origin, dest, distance)
        turn = 0.0
        if prev is not None:
            inbound = bearing_rad(prev.lat, prev.lon, origin.lat, origin.lon)
            outbound = bearing_rad(origin.lat, origin.lon, dest.lat, dest.lon)
            turn = (_heading_change(inbound, outbound) / math.pi) * TURN_PENALTY_M
        return base + turn, base, turn

    def _dedupe(self, nodes: Sequence[_Node]) -> tuple[list[_Node], int]:
        kept: list[_Node] = []
        dropped = 0
        for node in nodes:
            if kept:
                prev = kept[-1]
                same_point = node.lat == prev.lat and node.lon == prev.lon
                if same_point or node.ident == prev.ident:
                    dropped += 1
                    LOGGER.warning(
                        "duplicate waypoint dropped id=%d lat=%.8f lon=%.8f",
                        node.ident,
                        node.lat,
                        node.lon,
                    )
                    continue
            kept.append(node)
        return kept, dropped

    def _solve(self, nodes: Sequence[_Node]) -> dict[str, object]:
        self._cache_hits = 0
        self._zero_skips = 0
        kept, dropped = self._dedupe(nodes)
        if len(kept) < 2:
            raise EngineKernelException("route collapsed below two waypoints")
        seen: set[int] = set()
        for node in kept:
            if node.ident in seen:
                raise EngineKernelException(f"duplicate waypoint id {node.ident}")
            seen.add(node.ident)
        path, edges = self._dijkstra(kept)
        frictions = [float(edge["friction_m"]) for edge in edges]
        mean_edge = statistics.fmean(frictions)
        if len(frictions) > 1:
            spread = statistics.pstdev(frictions)
        else:
            spread = 0.0
        return {
            "topic": TOPIC,
            "path": path,
            "total_friction_m": math.fsum(frictions),
            "edges": edges,
            "duplicate_dropped": dropped,
            "zero_length_dropped": self._zero_skips,
            "cache_hits": self._cache_hits,
            "cache_depth": len(self._cache),
            "mean_edge_friction_m": mean_edge,
            "stdev_edge_friction_m": spread,
        }

    def _consider(
        self,
        prev: _Node | None,
        origin: _Node,
        dest: _Node,
    ) -> tuple[float, float, float, float] | None:
        distance = haversine_m(origin.lat, origin.lon, dest.lat, dest.lon)
        if distance <= ZERO_LENGTH_M:
            self._zero_skips += 1
            if self._zero_skips == 1:
                LOGGER.warning(
                    "zero-length segment dropped from_id=%d to_id=%d",
                    origin.ident,
                    dest.ident,
                )
            return None
        friction, _base, turn = self._friction(prev, origin, dest, distance)
        congestion_avg = 0.5 * (origin.congestion + dest.congestion)
        return friction, distance, turn, congestion_avg

    def _dijkstra(
        self, nodes: Sequence[_Node]
    ) -> tuple[list[int], list[dict[str, object]]]:
        start = nodes[0]
        goal = nodes[-1]
        index = {node.ident: node for node in nodes}
        best: dict[tuple[int, int], float] = {(start.ident, _SENTINEL): 0.0}
        parent: dict[tuple[int, int], tuple[int, int]] = {}
        heap: list[tuple[float, int, int, int]] = [(0.0, 0, start.ident, _SENTINEL)]
        counter = 0
        winning: tuple[int, int] | None = None
        while heap:
            cost, _order, node_id, prev_id = heapq.heappop(heap)
            state = (node_id, prev_id)
            if cost > best.get(state, math.inf):
                continue
            if node_id == goal.ident and prev_id != _SENTINEL:
                winning = state
                break
            origin = index[node_id]
            prev = None if prev_id == _SENTINEL else index[prev_id]
            for dest in nodes:
                if dest.ident == node_id:
                    continue
                scored = self._consider(prev, origin, dest)
                if scored is None:
                    continue
                friction, _distance, _turn, _congestion = scored
                nxt_cost = cost + friction
                nxt_state = (dest.ident, node_id)
                if nxt_cost < best.get(nxt_state, math.inf):
                    best[nxt_state] = nxt_cost
                    parent[nxt_state] = state
                    counter += 1
                    heapq.heappush(heap, (nxt_cost, counter, dest.ident, node_id))
        if winning is None:
            raise EngineKernelException("no finite path between endpoints")
        chain: list[int] = []
        cursor: tuple[int, int] | None = winning
        guard = 0
        limit = len(nodes) * len(nodes) + 2
        while cursor is not None:
            chain.append(cursor[0])
            if cursor[1] == _SENTINEL:
                break
            cursor = parent.get(cursor)
            guard += 1
            if guard > limit:
                raise EngineKernelException("path reconstruction failed")
        chain.reverse()
        edges: list[dict[str, object]] = []
        for step in range(1, len(chain)):
            origin = index[chain[step - 1]]
            dest = index[chain[step]]
            prev = index[chain[step - 2]] if step >= 2 else None
            scored = self._consider(prev, origin, dest)
            if scored is None:
                raise EngineKernelException("reconstructed edge has zero length")
            friction, distance, turn, congestion_avg = scored
            edges.append(
                {
                    "from_id": origin.ident,
                    "to_id": dest.ident,
                    "distance_m": distance,
                    "congestion_avg": congestion_avg,
                    "turn_penalty_m": turn,
                    "friction_m": friction,
                }
            )
        return chain, edges

    async def _publish(self, blob: bytes) -> None:
        if self._outbound.full():
            self._outbound.get_nowait()
        await self._outbound.put(blob)
